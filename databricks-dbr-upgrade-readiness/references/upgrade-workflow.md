# Upgrade Sequencing, Testing & Rollback

How to actually move the workload once the readiness report (Steps 1–3) is done.

## Stepping-stone sequencing

A DBR upgrade bundles many behavior changes. The more Spark minors the jump crosses, the more changes land at once, and the harder a failed run is to diagnose. Stage the jump so each landing is small enough to reason about.

**Rule of thumb:**

- Jump stays within **one Spark minor** (e.g. 14.3 → 15.4, both Spark 3.5): go direct.
- Jump crosses **2+ Spark minors** (e.g. 10.4/Spark 3.2 → 15.4/Spark 3.5) **or 4+ DBR majors**: land on an intermediate LTS first.

**Example staged path** for DBR 10.4 LTS → 16.4 LTS:

```
10.4 (Spark 3.2)  →  12.2 LTS (Spark 3.3)  →  14.3 LTS (Spark 3.5)  →  16.4 LTS (Spark 3.5, Py 3.12)
```

Each arrow is its own readiness pass, A/B test, and cutover. Most of the SQL-semantics changes (ANSI, datetime, decimal) surface at the 3.2→3.3 and 3.3→3.4 steps; the Python 3.12 stdlib removals surface only at the final step.

You can collapse stages once you have proven a clean parity run — but start staged.

## Pin libraries first

Before changing the runtime, **pin every library** (`%pip install pkg==ver`, cluster libraries, `requirements.txt`) to the versions currently in production. This makes the runtime the *only* variable that changes. Then, after the runtime upgrade lands clean:

1. Unpin one library at a time (or move to the target DBR's preinstalled version).
2. Re-run the parity test after each unpin.

This separates "the runtime broke something" from "a library upgrade broke something" — two failures that otherwise look identical in a stack trace.

## Bridge flags: land first, fix later

For Category-1 findings, set the legacy flag so the upgrade can land before all code is fixed:

```python
spark.conf.set("spark.sql.ansi.enabled", "false")                 # ANSI bridge
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")     # datetime bridge
spark.conf.set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
```

Treat each as **temporary debt**. Track them, land the upgrade, prove parity, then remove them one at a time in follow-up changes (each removal is a small, reviewable diff with its own parity check). A bridge flag left in place forever means the workload never actually adopted the new semantics — it just runs the old behavior on a new runtime.

## A/B parity test

The core safety check. Run the workload on both runtimes against the **same sampled input**, compare outputs.

1. Create a test catalog with sampled copies of the upstream tables (keep schema names identical to production, change only the catalog):

```sql
CREATE TABLE IF NOT EXISTS test_catalog.sales.orders
AS SELECT * FROM prod_catalog.sales.orders LIMIT 1000;
```

2. Parameterize the catalog so one notebook runs against either:

```python
dbutils.widgets.text("catalog", "prod_catalog")
catalog = dbutils.widgets.get("catalog")
df = spark.table(f"{catalog}.sales.orders")
```

3. Run the job twice — once on a cluster at the **source** DBR, once at the **target** DBR — writing to distinct output tables, then compare:

```python
old_df = spark.read.table("test_catalog.out.results_source")
new_df = spark.read.table("test_catalog.out.results_target")

assert old_df.count() == new_df.count(), "Row count mismatch"
assert old_df.schema == new_df.schema, "Schema mismatch"
diff = old_df.exceptAll(new_df).unionByName(new_df.exceptAll(old_df))
assert diff.count() == 0, f"{diff.count()} differing rows"
```

A diff is usually one of the catalogued behavior changes (ANSI null→error, datetime parse shift, decimal rounding). Trace it to a finding, fix or bridge it, re-run.

## Delta protocol coordination (do not skip)

Writing to an existing Delta table from a newer DBR can **upgrade the table's protocol** (raise `minReaderVersion` / `minWriterVersion`) when a new table feature is enabled — e.g. deletion vectors or column mapping. After that, **readers still on the old DBR can no longer read the table.**

Before the cutover:

- List the tables the workload **writes**, and identify every other job/consumer that **reads** them.
- If any reader is still on the old DBR, either:
  - upgrade the readers first, or
  - explicitly disable the new table feature on the write (e.g. `delta.enableDeletionVectors=false`) until all readers are upgraded.
- Run the A/B test against **test** tables, never the shared production tables, so a parity run cannot protocol-upgrade a table other jobs depend on.

## Rollback

Classic DBR upgrades are reversible — the runtime is a cluster setting, not a data migration. To roll back:

1. Revert `spark_version` on the job/cluster to the source DBR.
2. Re-pin any libraries you unpinned.
3. **The exception is Delta protocol:** a protocol upgrade on a production table is *not* trivially reversible. This is why protocol coordination above is a hard pre-cutover gate, and why A/B runs target test tables.

## Deliverables checklist

A complete readiness engagement produces:

- [ ] **Readiness report** per workload (verdict, findings by category, effort estimate) — see `assets/sample_readiness_report.md`.
- [ ] **Upgrade path** (direct or staged, with intermediate LTS stops).
- [ ] **Bridge-flag list** (Category-1 flags to set, marked temporary, with removal follow-ups).
- [ ] **A/B parity result** (row count, schema, row-diff) for each stage.
- [ ] **Delta protocol impact** (tables written, readers identified, coordination plan).
- [ ] **Cutover + rollback steps** for the production job.

If any deliverable is missing, the assessment is incomplete.
