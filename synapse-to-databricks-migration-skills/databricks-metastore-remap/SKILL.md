---
name: databricks-metastore-remap
description: Rewrite hive_metastore table references to their Unity Catalog equivalents when migrating Synapse Spark notebooks to Databricks. The UC external tables are registered against the same ADLS Gen2 Delta paths, so the edit is a pure rename — hive_metastore.<db>.<table> (and quoted/SQL-position/unqualified refs) become <catalog>.<schema>.<table>. This is the single highest-frequency edit in the migration. The skill builds/loads a mapping, rewrites every safe reference form, leaves unmapped tables and shadowed names (CTEs, temp views, Python identifiers) untouched, and emits a reviewable per-cell diff the engineer approves. Use for Synapse-to-Databricks notebook migration, "remap hive_metastore", "hive to Unity Catalog references", "three-level namespace migration", or replacing legacy metastore table names in notebooks.
---

# Databricks Metastore Reference Remapping (hive_metastore → Unity Catalog)

Synapse-origin Spark notebooks reference tables as `hive_metastore.<db>.<table>`.
After dual-registration the same data is exposed as UC external tables on the
**same** ADLS Gen2 Delta paths, so converting a notebook is a pure rename to
`<catalog>.<schema>.<table>`. It is the single highest-frequency edit in the
migration — and the easiest to get subtly wrong with naive find-and-replace.

This skill does it safely: it builds/loads a mapping, rewrites every reference
form that is genuinely a table reference, refuses to touch anything ambiguous
(unmapped tables, CTE names, temp views, Python identifiers, data literals), and
emits a **per-cell diff with rationale** for the engineer to approve. It never
writes without `--write`.

Like its sibling `databricks-ansi-remediation`, this is a deliberately small,
one-task skill meant to double as a **teaching reference** a data team can
copy when authoring their own skills during an innovation week.

## When to use

- Migrating Synapse Spark notebooks to DBR and replacing `hive_metastore` table
  references with their UC equivalents.
- Someone asks to "remap hive_metastore", "point notebooks at Unity Catalog", or
  "fix three-level namespace references".
- You want a repeatable, reviewable rename across many notebooks rather than
  hand-editing each one.

Pair with: `databricks-ansi-remediation` (the other flagship — run both on each
notebook), `databricks-resource-deployment` (deploy the fixed notebook), and the
team-built post-conversion validation skill (confirm the rewritten notebook reads
the same rows from UC as it did from hive).

## Prerequisites

1. **The notebook(s)** in Databricks `.py` source format (preferred), `.sql`, or
   `.ipynb`.
2. **A verified mapping** of `hive_metastore.<db>.<table>` → `<catalog>.<schema>.
   <table>`, where each UC target is the table on the **same storage path** as
   its hive source (confirm by path, not by name — see Phase 1).
3. **Python 3.9+** (standard library; PyYAML used if present, otherwise a built-in
   minimal YAML reader handles the documented mapping shape).

## Workflow

### Phase 0 — Scope
Confirm the notebook(s), the target catalog/schema convention, and whether a
single `default_database` applies (needed to remap unqualified bare names).

### Phase 1 — Build / verify the mapping
Scaffold a mapping from the references actually used, then verify each target:

```bash
cd ~/.claude/skills/databricks-metastore-remap/scripts
python build_mapping.py <notebook.py> --catalog <target_catalog> > mapping.yaml
```

The `uc:` values are a **naming-convention draft** — confirm every one points to
the UC table on the same ADLS Gen2 Delta path as its hive source:

```sql
SELECT table_catalog, table_schema, table_name, storage_path
FROM system.information_schema.tables WHERE storage_path IS NOT NULL;
```

Match each hive table's `DESCRIBE EXTENDED <table>` → `Location` to a row above,
and edit `mapping.yaml`. Set `default_database:` if you want bare-name remap.

### Phase 2 — Remap and review the diff
By default the remapper writes nothing — it prints a per-cell diff and an UNMAPPED
report:

```bash
python remap_refs.py <notebook.py> --mapping mapping.yaml
```

Review the diff. Confirm: every rewrite is a real table reference; the UNMAPPED
list is expected (add any genuine tables to the mapping and re-run); shadowed
names (CTEs/temp views) are listed as excluded.

### Phase 3 — Apply
```bash
python remap_refs.py <notebook.py> --mapping mapping.yaml --write    # keeps .bak
# or to a new file:
python remap_refs.py <notebook.py> --mapping mapping.yaml --out <notebook>.uc.py
```

### Phase 4 — Validate
Run the rewritten notebook against UC and confirm it reads the same rows it read
from hive (row counts / schema / a sample diff). Hand to the post-conversion
validation skill if it exists.

### Phase 5 — Output
Deliver: the verified `mapping.yaml`, the approved per-cell diff, the rewritten
notebook (`.bak` retained), and the UNMAPPED list so nothing is silently missed.

## Worked example — the bundled demo

`assets/sample_synapse_notebook.py` is a before fixture with every reference form
and three traps: a Python variable named `routes` (must not be rewritten), a CTE
`daily_summary` (must not be rewritten), and `hive_metastore.transit.raw_taps_2019_archive`
(unmapped — must be reported, not guessed). `assets/sample_mapping.yaml` maps two
tables and declares a `default_database`.

Expected result: 6 references rewritten across 4 cells (fully-qualified in PySpark
strings and SQL, plus one bare `FROM routes` via the default database); the
variable `routes` and the CTE `daily_summary` untouched; the archive table listed
as UNMAPPED. `assets/demo_before_after.py` is the runnable workshop walkthrough.

## Notes for the assistant

- **Fully-qualified `hive_metastore.db.table` is the safe, dominant case** —
  rewrite it anywhere. Two-part and bare names are only tables in SQL position or
  (for dotted names) inside quotes; never rewrite a bare Python identifier.
- **Never guess an unmapped table.** Report it and ask the engineer to confirm the
  UC target by storage path. A wrong path is a silent data-correctness bug.
- **Respect shadowing.** CTEs, temp views, and aliases locally redefine names —
  the script excludes them; don't bypass that with a blanket regex.
- The mapping is verified **by storage path, not by name**. Same-name ≠ same data.
- Magic `%sql` cells round-trip through `nb_io.py` (the `# MAGIC` prefix is
  stripped for matching and re-added for output) — don't bypass it.

## References
- Reference-form catalog + traps + limitations: `references/reference-patterns.md`
- Demo walkthrough: `assets/demo_before_after.py`
- UC three-level namespace: https://docs.databricks.com/en/catalogs/index.html
- Migrate to Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html
