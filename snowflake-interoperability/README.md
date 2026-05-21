# Snowflake ↔ Databricks Catalog Federation Validators

Validators and procedures for verifying a **Snowflake foreign catalog** in Databricks Unity Catalog — confirming that reads use **catalog federation** (direct Iceberg reads on Databricks compute) rather than falling back to **query federation** (JDBC via Snowflake compute).

## Contents

| File | Side | Purpose |
|---|---|---|
| `snowflake_validator.sql` | Snowflake | Validates the Snowflake-side setup: catalog integration enabled, type is `ICEBERG_REST`, sample table accessible. Run as `ACCOUNTADMIN`. |
| `validate_snowflake_federation.ipynb` | Databricks | Validates the Databricks-side foreign catalog: confirms `FOREIGN_CATALOG` type, runs `DESCRIBE EXTENDED`, and includes the **BCR-1935 diagnostic** for Snowflake's `blob.core.windows.net` path issue on Azure. |
| `validate-catalog-federation.md` | Procedure | Step-by-step manual validation walkthrough — interpret `DESCRIBE EXTENDED` output (`iceberg` = catalog federation, `snowflake` = JDBC fallback) and check storage scheme/endpoint requirements. |

## What it shows

- How to confirm whether a federated Snowflake catalog is doing direct Iceberg reads
- The supported storage schemes and endpoint requirements (Azure must use `dfs.core.windows.net`, not `blob.core.windows.net`)
- A reproducible diagnostic for BCR-1935 (Snowflake metadata path regression)
- Both sides of the setup (Snowflake `SHOW CATALOG INTEGRATIONS` + Databricks `DESCRIBE CATALOG EXTENDED`)

## Running the notebook

In Databricks:

1. Open `validate_snowflake_federation.ipynb` in a workspace
2. Set the `catalog_name` widget to your foreign catalog name
3. Run all cells — failures print specific remediation steps
