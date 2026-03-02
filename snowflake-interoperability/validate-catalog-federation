# Validation Procedure: Snowflake Catalog Federation Setup

## Overview

This procedure verifies whether Databricks is using **catalog federation** (direct Iceberg reads, Databricks compute) versus **query federation** (JDBC fallback, Snowflake compute) for a given Snowflake table.

---

## Step 1: Run the Diagnostic Query

In Databricks SQL, run:

```sql
DESCRIBE EXTENDED <catalog>.<schema>.<table>;
```

**Interpret the result:**

| `provider` / `data source` value | Mode in use |
|---|---|
| `iceberg` | Catalog federation (Databricks compute) |
| `snowflake` | Query federation / JDBC fallback (Snowflake compute) |

- If result is `iceberg` → setup is correct. **Stop here.**
- If result is `snowflake` → continue to Step 2.

You can also check in **Catalog Explorer**: a table showing **source format = Iceberg** confirms catalog federation is active.

---

## Step 2: Validate the Snowflake Table

On the Snowflake side, verify the target table:

- [ ] **Table format is Iceberg** (not native Snowflake format).
- [ ] **Storage scheme is supported**: `s3`, `s3a`, `s3n`, `abfs`, `abfss`, `gs`, `r2`, `wasb`, or `wasbs`.
- [ ] **Metadata location is inside the table location** (not outside).
- [ ] **URI has no incompatible characters** in the path.
- [ ] *(Azure only)* Storage endpoint uses `dfs.core.windows.net`, **not** `blob.core.windows.net`.

> If the table fails any of these checks, it can **only** be served via query federation. The fix requires changing how the table is stored/managed in Snowflake.

---

## Step 3: Validate the Unity Catalog Foreign Catalog Configuration

In Unity Catalog, open the foreign catalog for Snowflake and verify:

- [ ] A **Snowflake connection** exists in UC (required for Horizon).
- [ ] An **external location + storage credential** is configured pointing to the Iceberg table paths in object storage.
- [ ] The foreign catalog has an **external storage root location** set (not the default storage root).
- [ ] **Authorized paths** in the foreign catalog cover the base path of the Snowflake Iceberg table(s).
- [ ] The **external location(s)** match the paths where Snowflake writes Iceberg data.

> If the Iceberg table's storage path is not under any authorized path, Databricks will always fall back to JDBC.

---

## Step 4: Validate Permissions

Confirm the querying user has all required privileges:

- [ ] `USE CATALOG` on the foreign catalog.
- [ ] `USE SCHEMA` on the target schema.
- [ ] `SELECT` on the target table.
- [ ] `MODIFY` on the UC federated table *(required for catalog federation Iceberg reads)*.

---

## Step 5: Re-run the Diagnostic

After completing Steps 2–4, re-run:

```sql
DESCRIBE EXTENDED <catalog>.<schema>.<table>;
```

**Expected outcome:**

- `provider = iceberg` → catalog federation is active. **Setup is correct.**
- `provider = snowflake` → fallback is still occurring. Review Steps 2–4 again, or check if the table has an unsupported Iceberg pattern that cannot be resolved without changes on the Snowflake side.

---

## Reference: Fallback Trigger Conditions

Databricks automatically falls back to query federation (JDBC) if **any** of the following are true:

1. The table is not Iceberg in Snowflake.
2. The URI has incompatible characters.
3. The metadata location is outside the table location.
4. The storage scheme is not in the supported set.
5. The table's storage path is not under any authorized path in the UC foreign catalog.
6. *(Azure)* Metadata path uses wrong endpoint (`blob` instead of `dfs`).
