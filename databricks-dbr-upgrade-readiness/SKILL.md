---
name: databricks-dbr-upgrade-readiness
description: Assess migration readiness for upgrading a Databricks workload from an older Databricks Runtime (DBR) to a newer one (e.g. 9.1/10.4/11.3/12.2 LTS → 14.3/15.4/16.4 LTS). Statically scans notebooks, jobs, and cluster configs for breaking behavior changes across the Spark/Python/Scala/library versions the upgrade crosses, classifies each finding (auto-handled / code-change / hard-blocker), and produces a per-workload readiness report plus a stepping-stone upgrade path and A/B test plan. Use for "DBR upgrade", "runtime upgrade readiness", "is my job ready for DBR 15.4", "upgrade from DBR 10.4 to 14.3", "Spark 3.1 to 3.5 migration", "LTS migration", or assessing breakage before bumping spark_version. NOT for classic→serverless migration (use databricks-serverless-migration) — this is classic DBR version-to-version.
compatibility: Static analysis runs offline. Live test runs require the databricks CLI and a target workspace.
metadata:
  version: "0.1.0"
---

# Databricks Runtime (DBR) Upgrade Readiness

Assess whether a workload that runs on an older Databricks Runtime is ready to move to a newer one, and produce the plan to get it there safely. The risk in a DBR upgrade is not the version number — it is the **stack of behavior changes** the upgrade crosses: a new Spark version, a new Python interpreter, bumped preinstalled libraries, and Databricks platform changes (ANSI default, init-script rules, Delta protocol). This skill enumerates those changes for the *specific* source→target jump, scans the code for the patterns that hit them, and sequences a safe upgrade.

This skill does **static** readiness analysis and planning. It does not rewrite code by itself for every finding — for the two highest-frequency rewrite surfaces it hands off to dedicated sibling skills (see [Sibling skills](#sibling-skills-when-to-hand-off)).

The workflow follows a 5-step lifecycle: **Inventory** the workload and pin the exact jump → **Assess** with a static scan → **Categorize** each finding → **Plan** the stepping-stone upgrade path → **Test** with an A/B parity run, then validate and cut over.

## When to use this skill

- Upgrading a notebook, job, or pipeline from one classic DBR version to a newer classic DBR version (including LTS-to-LTS).
- Checking what will break before bumping `spark_version` on a cluster or job.
- A workload is on an end-of-support DBR (e.g. 9.1 / 10.4 / 11.3 LTS) and needs to move to a supported LTS.
- Estimating the effort/risk of a fleet-wide runtime upgrade.
- Sequencing a large jump (e.g. Spark 3.1 → 3.5) so it lands in reviewable stages.

## When NOT to use this skill

- **Classic → serverless migration** → use `databricks-serverless-migration`. That crosses the Spark Connect architecture boundary (RDDs, DBFS, init scripts, Environments), which is a different surface. DBR upgrades keep you on classic compute.
- **Moving jobs off all-purpose/interactive compute to job clusters** → use `databricks-cluster-migration` (cost/engine, not version).
- **Pure Hive-Metastore → Unity-Catalog table reference rewrites** → use `databricks-metastore-remap`.

These are orthogonal; a real engagement often does a DBR upgrade *and* one of the above. Do the readiness assessment here first, then hand off the rewrite-heavy parts.

## Understanding upgrade blockers

Every finding falls into one of three categories. Spend your effort on Category 2 — that is where this skill adds the most.

| Category | Meaning | Action |
|----------|---------|--------|
| **1. Bridgeable by config** | The new DBR changed a default, but a legacy flag restores old behavior (e.g. `spark.sql.legacy.timeParserPolicy=LEGACY`, `spark.sql.ansi.enabled=false`). | Set the flag to land the upgrade fast, then schedule a follow-up to remove the flag and fix the code properly. The flag is a bridge, not a destination. |
| **2. Code/config change needed** | Code uses a pattern whose behavior or availability changed and no clean legacy flag exists (removed API, pandas 2.x break, Python stdlib removal, init-script relocation). | **This skill helps here** — it detects the pattern and gives the fix or routes to the sibling skill that owns it. |
| **3. Hard blocker** | The new DBR removed something the workload depends on, or a pinned third-party library has no version compatible with the new Spark/Python. | Surface to the user with options (stay on current LTS until EOS, replace the dependency, re-architect). Do not invent a workaround. |

## Inputs you need before starting

1. **Source DBR** — the version the workload runs on today (read it from the cluster/job spec `spark_version`, e.g. `10.4.x-scala2.12`).
2. **Target DBR** — where it is going. Default to the **latest LTS** unless the user names one. Prefer LTS targets; avoid landing a production workload on a non-LTS runtime.
3. **The workload source** — notebook files, job JSON, cluster config, init scripts, `requirements.txt` / `%pip` lines, cluster libraries (Maven/JAR/PyPI).
4. **Workload shape** — batch job, streaming, interactive, or pipeline; Python / SQL / Scala / R mix; whether it writes Delta tables that other readers consume.

If the source or target version is unknown, ask. Everything downstream (which behavior changes apply) depends on the exact jump.

## Workflow

### Step 1 — Inventory and pin the exact jump

1. Read the cluster/job spec and record the **source** `spark_version` and the **target** the user wants (or default to latest LTS).
2. Resolve both endpoints in the version matrix — see [references/version-matrix.md](references/version-matrix.md) — to learn what *actually* changes: Spark version, Python version, Scala version, and the headline preinstalled-library jumps (pandas, numpy, pyarrow). The DBR number is a label; the matrix is the real diff.
3. Enumerate every source artifact: user notebooks, `_resources`/setup notebooks, job JSON, init scripts, library specs.
4. **Multi-notebook workloads (≥ 3 user notebooks, or any notebook > ~5 KB):** process them **one at a time**. Enumerate paths first (do not read all bodies into context at once), then for each notebook read → scan → record a compact structured finding summary → drop the source from working memory. Synthesize the unified report from the summaries. This avoids context thrashing on large jobs. For 1–2 small notebooks, a single pass is fine.

Record per notebook: `notebook_path`, `detected_findings[]` (pattern IDs), `categories[]`, `blockers[]` (Category 3), `recommended_fixes[]`.

### Step 2 — Assess: static scan for breaking changes

**Read the code before running it.** A static scan finds most upgrade breakage faster than iterating on failed runs, and many DBR-upgrade failures (removed APIs, date-parser changes, pandas 2.x) are obvious statically but expensive to debug from a stack trace.

Run the scanner over the source tree:

```bash
python scripts/scan_dbr_readiness.py --source-dbr 10.4 --target-dbr 15.4 <path-to-notebooks-or-repo>
```

It emits a findings JSON (pattern ID, severity, file, line, category, fix hint) you fold into the report. The scanner is a first pass — confirm each hit by reading the surrounding code, because some patterns (e.g. a `cast` that is already safe, a date format that is ISO-8601) are benign.

The full detection catalog with detect/fix for every pattern is in [references/behavior-changes.md](references/behavior-changes.md). The high-frequency surfaces, by jump:

| Surface | Where it bites | Detail |
|---------|----------------|--------|
| **ANSI SQL default** | Invalid casts that used to return `NULL` now raise `CAST_INVALID_INPUT`; `x/0` raises `DIVIDE_BY_ZERO`; out-of-range inserts raise. Crossing into a DBR where ANSI is on by default. | Route the rewrite to **`databricks-ansi-remediation`** (try_cast vs session-flag). Bridge: `spark.sql.ansi.enabled=false`. |
| **Datetime parsing** | Spark 3.0+ Java-8 time API. Legacy patterns (`yyyy`, week-year `YYYY`, `u`/`e` day fields) parse differently or throw. | Fix the patterns, or bridge with `spark.sql.legacy.timeParserPolicy=LEGACY`. See behavior-changes catalog. |
| **pandas 2.x** | Preinstalled pandas jumps 1.x → 2.x across DBR generations. Removed `DataFrame.append`, `.iteritems()`, changed `dtype` inference, stricter `inplace`. | Code change (Category 2). Pin or rewrite. |
| **Python interpreter** | Interpreter jumps (e.g. 3.8 → 3.10/3.11/3.12). `distutils` removed in 3.12; `imp`, `asyncio.coroutine`, `collections` ABC aliases gone. | Code change. Check `import distutils`, `imp`, deprecated stdlib. |
| **Init scripts** | Legacy global / DBFS-stored init scripts are disabled on newer DBR; must move to workspace files, UC volumes, or cloud storage. | Config change (Category 2). Relocate + re-reference. |
| **Delta protocol / table features** | Writing from a newer DBR can auto-upgrade a table's reader/writer protocol (deletion vectors, column mapping), breaking older readers still on the old DBR. | Plan/coordination item — call out explicitly. See workflow doc. |
| **Removed/deprecated Spark APIs** | APIs removed across Spark 3.1→3.5 (some `SQLContext`, deprecated `functions`, removed configs). | Code change. |
| **HMS → Unity Catalog** | Often bundled with the upgrade but orthogonal. | Route to **`databricks-metastore-remap`**. |
| **Scala JARs** | Classic DBR has stayed Scala 2.12, so a classic→classic upgrade usually needs **no** JAR recompile — unlike serverless (2.13). Still re-test against the new Spark minor. | Re-test; recompile only if you also change Scala. |

For each finding report: **Pattern** (what + where), **Category** (1/2/3), **Severity** (Blocker / Warning / Info), **Fix** (concrete remediation or sibling-skill handoff), and **Applies-to-this-jump** (only flag changes that actually fall between source and target).

### Step 3 — Categorize and produce the readiness report

Roll the findings up into a single readiness report per workload. Use the template in [assets/sample_readiness_report.md](assets/sample_readiness_report.md). It must contain:

- The exact jump (source DBR → target DBR, with the Spark/Python/Scala/library deltas from the matrix).
- A **readiness verdict**: Ready / Ready-with-changes / Blocked.
- The findings table, grouped by category, each with severity and fix.
- The bridge configs that would let the upgrade land immediately (Category 1), flagged as temporary.
- The Category 3 blockers (if any) with the decision the user must make.
- An effort estimate (count of code-change sites).

### Step 4 — Plan the upgrade path

Do not jump many versions blindly. Sequence the upgrade so each landing is reviewable and reversible. See [references/upgrade-workflow.md](references/upgrade-workflow.md) for the full sequencing, A/B test, and rollback playbook. Key rules:

- **Prefer LTS stepping stones for large jumps.** Crossing several Spark minors (e.g. 3.1 → 3.5) in one shot bundles too many behavior changes to debug together. Land on an intermediate LTS first if the jump spans 2+ Spark minor versions or 4+ DBR major versions.
- **Pin libraries before upgrading**, so the only variable that changes is the runtime. Then unpin deliberately.
- **Use bridge flags to decouple "land the upgrade" from "fix the code."** Set the legacy flags, land on the new DBR, prove parity, then remove flags one at a time in follow-up changes.

### Step 5 — Test (A/B parity), validate, cut over

1. Stand up a **test copy** of the job on the **target DBR**, pointed at a sampled/test catalog (do not point a test job at production data or production Delta tables you might protocol-upgrade).
2. Run both the current (source DBR) and the new (target DBR) job and compare outputs:

```python
old_df = spark.read.table("test_catalog.out.results_dbr_10_4")
new_df = spark.read.table("test_catalog.out.results_dbr_15_4")

assert old_df.count() == new_df.count(), "Row count mismatch"
assert old_df.schema == new_df.schema, "Schema mismatch"
diff = old_df.exceptAll(new_df)
assert diff.count() == 0, f"{diff.count()} differing rows"
```

3. Resolve diffs (often an ANSI/datetime/decimal behavior change is the culprit — that is signal, not noise).
4. Once parity holds, bump `spark_version` on the production job, monitor the first scheduled runs, then remove temporary bridge flags one at a time.

### Stopping conditions

Surface to the user and stop — do not improvise — when you hit:
- A Category 3 hard blocker (removed feature the workload depends on; pinned library with no compatible version).
- A pinned third-party dependency that has no release for the target Python/Spark.
- Permission failures on source tables, the test catalog, or the workspace.
- Repeated test failures (~5+) with no new information in the trace.

## Sibling skills (when to hand off)

This skill assesses and plans. For the rewrite-heavy surfaces, hand off:

| Surface | Skill | Why |
|---------|-------|-----|
| ANSI cast/divide failures after upgrade | `databricks-ansi-remediation` | Owns the try_cast-vs-session-flag rewrite with per-cell diffs. |
| `hive_metastore.*` → `catalog.schema.*` rewrites | `databricks-metastore-remap` | Owns the safe three-level-namespace rewrite. |
| Jobs on all-purpose compute → job clusters | `databricks-cluster-migration` | Engine/cost migration, often sequenced with the DBR floor. |
| Going to serverless instead of a newer classic DBR | `databricks-serverless-migration` | Different architecture (Spark Connect), different blocker surface. |

When you finish a readiness assessment and the user is also cost-optimizing, mention `fe-cost-optimization-report` can turn the plan into an exec-facing brief.

## Reference guides

- [references/version-matrix.md](references/version-matrix.md) — DBR ↔ Spark ↔ Python ↔ Scala ↔ headline-library mapping, LTS cadence, and how to confirm exact versions against release notes.
- [references/behavior-changes.md](references/behavior-changes.md) — the full detection catalog: every breaking change with detect/fix/bridge, indexed by the Spark/Python/DBR version that introduced it.
- [references/upgrade-workflow.md](references/upgrade-workflow.md) — stepping-stone sequencing, library pinning, A/B parity testing, Delta protocol coordination, rollback, and deliverables checklist.

## Documentation

Always confirm version-specific facts against the official release notes for the exact source and target DBR — preinstalled library versions and behavior-change defaults change between maintenance releases.

- DBR release notes (per-version behavior changes): https://docs.databricks.com/aws/en/release-notes/runtime/
- DBR maintenance & support lifecycle: https://docs.databricks.com/aws/en/release-notes/runtime/supported
- Apache Spark migration guides (per-version behavior changes): https://spark.apache.org/docs/latest/sql-migration-guide.html
- ANSI compliance: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-ansi-compliance
- Init scripts (location requirements): https://docs.databricks.com/aws/en/init-scripts/
- Delta Lake table protocol / features: https://docs.databricks.com/aws/en/delta/feature-compatibility
