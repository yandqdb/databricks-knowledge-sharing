# Table reference patterns — what gets remapped, and the traps

Reference for the `databricks-metastore-remap` skill. The UC external tables are
registered against the **same** ADLS Gen2 Delta paths as the hive_metastore
tables, so remapping is a pure rename. The risk is not the rename — it is
rewriting something that only *looks* like a table reference.

## The three-level namespace

`<catalog>.<schema>.<table>`. Synapse-origin code uses the legacy two-level
`hive_metastore.<db>.<table>` (where `hive_metastore` is the catalog and `<db>`
the schema). Unqualified names resolve against the current `USE CATALOG` /
`USE SCHEMA` (the "default database").

## Reference forms and how the skill treats each

| Form | Example | Rewrite rule |
|---|---|---|
| Fully qualified | `hive_metastore.transit.routes` | **always** — unambiguous token, safe anywhere (PySpark string or SQL) |
| Two-part, quoted | `spark.table("transit.routes")` | rewrite — a dotted name in quotes is a table ref |
| Two-part, SQL position | `FROM transit.routes` | rewrite — after a SQL keyword |
| Bare name, SQL position | `FROM routes` | rewrite **only** if a `default_database` is declared, matches, and the name is not shadowed |
| Bare name, anywhere else | `routes = spark.table(...)` | **never** — a bare identifier is not a table ref |

"SQL position" = immediately after `FROM`, `JOIN`, `INTO`, `UPDATE`, or `TABLE`.

## The traps (why naive find-and-replace breaks)

1. **Python identifiers.** `routes = spark.table("...")` — the variable `routes`
   is not a table. A bare-name rule must require SQL keyword context, never touch
   a bare identifier. (This skill does.)
2. **CTEs.** `WITH daily_summary AS (...)` defines a local name. Even if a hive
   table `daily_summary` exists, the CTE reference must not be remapped. The
   skill collects CTE names and excludes them from bare-name remap.
3. **Temp views.** `createOrReplaceTempView("routes")` / `CREATE TEMP VIEW routes`
   shadow the catalog name for the rest of the session. Same exclusion.
4. **Aliases.** `FROM hive_metastore.transit.routes r` — `r` is an alias; only the
   qualified name is rewritten, the alias is left alone (it is not a keyword
   target).
5. **String literals that aren't tables.** `WHERE city = 'routes'` — a quoted bare
   word is not treated as a table (only quoted *dotted* names and SQL-position
   names are). This avoids rewriting data values.
6. **Unmapped tables.** Any `hive_metastore.*` reference with no mapping entry is
   **reported and left untouched** — never guessed. Add it to the mapping and
   re-run.

## Known limitations (call these out in review)

- Comma-separated FROM lists (`FROM a, b`) only rewrite the first item reliably;
  the bare `b` is not keyword-preceded. Prefer explicit `JOIN`, or qualify.
- `USE SCHEMA` statements that change the default mid-notebook are not tracked;
  set `default_database` for the dominant schema and review bare-name diffs.
- Dynamic SQL built from f-strings/variables can't be statically rewritten; the
  unmapped report won't see names that aren't literal. Grep separately.

## Building the mapping

The mapping is the input of record. Each `uc:` value MUST be the UC table on the
**same storage path** as the hive source — confirm by path, not by name:

```sql
-- UC tables and their storage paths
SELECT table_catalog, table_schema, table_name, storage_path
FROM system.information_schema.tables
WHERE storage_path IS NOT NULL;
```

Match each hive table's `DESCRIBE EXTENDED <hive_table>` → `Location` to a row
above. `build_mapping.py --catalog X` emits a naming-convention *draft* to edit;
it is never authoritative until the path match is confirmed.

## Sources
- UC three-level namespace: https://docs.databricks.com/en/catalogs/index.html
- Upgrade tables to UC: https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html
- `information_schema.tables`: https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html
