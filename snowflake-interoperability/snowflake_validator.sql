-- =============================================================================
-- SET YOUR VARIABLES HERE
-- =============================================================================
SET CATALOG_INTEGRATION_NAME = 'DATABRICKS_UC_ICEBERG';
SET ICEBERG_DB_NAME = 'ICEBERG_DB';
SET SAMPLE_TABLE_FQDN = 'ICEBERG_DB.ICEBERG_SCHEMA.SAMPLE_ICEBERG_TABLE';
USE ROLE ACCOUNTADMIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1: List and Validate Catalog Integrations
-- ─────────────────────────────────────────────────────────────────────────────
SHOW CATALOG INTEGRATIONS;

SELECT 
    "name",
    "type",
    "enabled",
    CASE 
        WHEN "enabled" = 'false' THEN 'FAIL: Integration is disabled'
        WHEN "type" != 'ICEBERG_REST' THEN 'WARN: Not an Iceberg REST integration'
        ELSE 'PASS'
    END AS status
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = $CATALOG_INTEGRATION_NAME;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 2: Deep Dive into REST Config
-- ─────────────────────────────────────────────────────────────────────────────
DESCRIBE CATALOG INTEGRATION IDENTIFIER($CATALOG_INTEGRATION_NAME);

SELECT 
    "property",
    "property_value",
    CASE
        WHEN "property" = 'ENABLED' AND "property_value" = 'false' 
            THEN 'FAIL: Integration disabled'
        WHEN "property" = 'CATALOG_SOURCE' AND "property_value" != 'ICEBERG_REST' 
            THEN 'FAIL: Must be ICEBERG_REST for Databricks federation'
        WHEN "property" = 'REST_AUTHENTICATION' AND "property_value" LIKE '%BEARER%' 
            THEN 'WARN: Bearer token used. Consider switching to OAuth (M2M) to avoid expiration.'
        WHEN "property" = 'REST_CONFIG' AND "property_value" NOT LIKE '%unity-catalog/iceberg-rest%' 
            THEN 'FAIL: CATALOG_URI missing /api/2.1/unity-catalog/iceberg-rest'
        ELSE 'PASS'
    END AS validation_check
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 3: External Volume Health
-- ─────────────────────────────────────────────────────────────────────────────
SHOW EXTERNAL VOLUMES;

SELECT 
    "name",
    "allow_writes",
    CASE 
        WHEN "allow_writes" = 'false' THEN 'WARN: Volume is read-only. Managed Iceberg tables require write access.'
        ELSE 'PASS'
    END AS status
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 4: Iceberg Table Sync Status
-- ─────────────────────────────────────────────────────────────────────────────
-- This check identifies if Databricks will see "null" metadata
SHOW ICEBERG TABLES IN DATABASE IDENTIFIER($ICEBERG_DB_NAME);

SELECT 
    "name",
    "catalog_sync_name",
    "auto_refresh_status",
    CASE 
        WHEN "catalog_sync_name" IS NULL OR "catalog_sync_name" = '' 
            THEN 'FAIL: No CATALOG_SYNC. Databricks will see null metadata.'
        WHEN "auto_refresh_status" LIKE '%invalid%' OR "auto_refresh_status" LIKE '%error%'
            THEN 'FAIL: Sync error detected. Check auto_refresh_status column.'
        ELSE 'PASS'
    END AS sync_health
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "schema_name" != 'INFORMATION_SCHEMA';

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 5: Metadata Path Resolution
-- ─────────────────────────────────────────────────────────────────────────────
-- Verifies Snowflake can actually "see" the Iceberg manifest files
SELECT 'ICEBERG_DB.ICEBERG_SCHEMA.SAMPLE_ICEBERG_TABLE' AS table_name,
       SYSTEM$GET_ICEBERG_TABLE_INFORMATION(
           'ICEBERG_DB.ICEBERG_SCHEMA.SAMPLE_ICEBERG_TABLE'
       ) AS metadata_info,
       CASE
           WHEN metadata_info LIKE '%"status":"success"%'
               THEN 'PASS: Metadata location is resolvable'
           ELSE 'FAIL: Cannot resolve metadata — check external volume and storage permissions'
       END AS status;
