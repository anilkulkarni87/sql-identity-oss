# IDR End-to-End Testing Guide

This guide walks you through testing the Identity Resolution (IDR) system from data generation through validation across all supported platforms.

---

## Prerequisites

```bash
cd /path/to/sql-identity-resolution/tools/scale_test
pip install numpy pyarrow faker
```

---

## Step 1: Generate Test Data

```bash
# Generate 100K scale dataset
python generate_global_retail_idr.py --config config_100k.json
```

**Output:** Creates `idr_parquet_100k/` with 10 tables including `truth_links` for validation.

---

## Platform-Specific Setup

### DuckDB

```bash
# 1. Initialize database schema
duckdb idr.duckdb < ../../sql/ddl/duckdb.sql

# 2. Load data
python duckdb_load.py ./idr_parquet_100k --db idr.duckdb --schema idr_input

# 3. Load metadata configuration
duckdb idr.duckdb < setup_retail_metadata.sql

# 4. Run IDR (FULL mode)
# 4. Run IDR (FULL mode)
idr run --platform duckdb --db idr.duckdb --mode FULL

# 5. Run validation
python run_validation.py --platform duckdb --db idr.duckdb
```

---

### BigQuery

```bash
# 1. Upload data to GCS
./upload_to_gcs.sh ./idr_parquet_100k gs://your-bucket/idr_test

# 2. Create external tables
python bq_create_external_tables.py \
  --project your-project \
  --dataset idr_input \
  --bucket your-bucket \
  --prefix idr_test

# 3. Initialize schema (run in BQ console or bq command)
bq query --use_legacy_sql=false < ../../sql/ddl/bigquery.sql

# 4. Load metadata (adapt setup_retail_metadata.sql for BQ syntax)
# Use BigQuery console to run the INSERT statements

# 5. Run IDR
# 5. Run IDR
idr run --platform bigquery --project your-project --mode FULL


# 6. Run validation
python run_validation.py --platform bigquery --project your-project
```

---

### Snowflake

#### Prerequisites
- Snowflake account with ACCOUNTADMIN or SECURITYADMIN role (for storage integration)
- Access to GCS bucket `gs://sqlidr/idr_parquet_1M/`

#### Step 1: Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-user
export SNOWFLAKE_PASSWORD=your-password
export SNOWFLAKE_WAREHOUSE=IDR_WH
export SNOWFLAKE_DATABASE=IDR_TEST
```

#### Step 2: Create Database and Schemas

```sql
-- Run in Snowflake worksheet
CREATE DATABASE IF NOT EXISTS IDR_TEST;
USE DATABASE IDR_TEST;

CREATE SCHEMA IF NOT EXISTS IDR_INPUT;
CREATE SCHEMA IF NOT EXISTS IDR_META;
CREATE SCHEMA IF NOT EXISTS IDR_WORK;
CREATE SCHEMA IF NOT EXISTS IDR_OUT;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS IDR_WH WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

USE WAREHOUSE IDR_WH;
```

#### Step 3: Create GCS Storage Integration

```sql
-- Requires ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create storage integration for GCS
CREATE OR REPLACE STORAGE INTEGRATION gcs_idr_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://sqlidr/');

-- Get the service account to grant GCS permissions
DESC STORAGE INTEGRATION gcs_idr_integration;
-- Copy the STORAGE_GCP_SERVICE_ACCOUNT value
-- Grant this service account "Storage Object Viewer" role on gs://sqlidr bucket in GCP Console

-- Grant usage to your role
GRANT USAGE ON INTEGRATION gcs_idr_integration TO ROLE SYSADMIN;
```

#### Step 4: Create Stage and Load Tables

```sql
USE ROLE SYSADMIN;
USE DATABASE IDR_TEST;
USE SCHEMA IDR_INPUT;

-- Create file format
CREATE OR REPLACE FILE FORMAT ff_parquet TYPE = PARQUET;

-- Create stage pointing to GCS
CREATE OR REPLACE STAGE st_idr_gcs
  URL = 'gcs://sqlidr/idr_parquet_1M/'
  STORAGE_INTEGRATION = gcs_idr_integration
  FILE_FORMAT = ff_parquet;

-- Verify stage access
LIST @st_idr_gcs;

-- Load DIGITAL_CUSTOMER_ACCOUNT (1M rows)
CREATE OR REPLACE TABLE DIGITAL_CUSTOMER_ACCOUNT USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/digital_customer_account/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO DIGITAL_CUSTOMER_ACCOUNT FROM @st_idr_gcs/digital_customer_account/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load POS_CUSTOMER (1M rows)
CREATE OR REPLACE TABLE POS_CUSTOMER USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/pos_customer/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO POS_CUSTOMER FROM @st_idr_gcs/pos_customer/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load ECOM_ORDER (1M rows)
CREATE OR REPLACE TABLE ECOM_ORDER USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/ecom_order/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO ECOM_ORDER FROM @st_idr_gcs/ecom_order/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load STORE_ORDER (1M rows)
CREATE OR REPLACE TABLE STORE_ORDER USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/store_order/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO STORE_ORDER FROM @st_idr_gcs/store_order/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load SDK_EVENT (1M rows)
CREATE OR REPLACE TABLE SDK_EVENT USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/sdk_event/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO SDK_EVENT FROM @st_idr_gcs/sdk_event/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load BAZAARVOICE_SURVEY (1M rows)
CREATE OR REPLACE TABLE BAZAARVOICE_SURVEY USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/bazaarvoice_survey/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO BAZAARVOICE_SURVEY FROM @st_idr_gcs/bazaarvoice_survey/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load PRODUCT_REVIEW (1M rows)
CREATE OR REPLACE TABLE PRODUCT_REVIEW USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/product_review/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO PRODUCT_REVIEW FROM @st_idr_gcs/product_review/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load TRUTH_LINKS (for validation)
CREATE OR REPLACE TABLE TRUTH_LINKS USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@st_idr_gcs/truth_links/', FILE_FORMAT => 'ff_parquet'))
);
COPY INTO TRUTH_LINKS FROM @st_idr_gcs/truth_links/
FILE_FORMAT = (TYPE=PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Verify row counts
SELECT 'DIGITAL_CUSTOMER_ACCOUNT' AS tbl, COUNT(*) AS cnt FROM DIGITAL_CUSTOMER_ACCOUNT
UNION ALL SELECT 'POS_CUSTOMER', COUNT(*) FROM POS_CUSTOMER
UNION ALL SELECT 'ECOM_ORDER', COUNT(*) FROM ECOM_ORDER
UNION ALL SELECT 'STORE_ORDER', COUNT(*) FROM STORE_ORDER
UNION ALL SELECT 'SDK_EVENT', COUNT(*) FROM SDK_EVENT
UNION ALL SELECT 'BAZAARVOICE_SURVEY', COUNT(*) FROM BAZAARVOICE_SURVEY
UNION ALL SELECT 'PRODUCT_REVIEW', COUNT(*) FROM PRODUCT_REVIEW
UNION ALL SELECT 'TRUTH_LINKS', COUNT(*) FROM TRUTH_LINKS;
```

#### Step 5: Initialize IDR Schema

```sql
-- Initialize via CLI is preferred:
-- idr init --platform snowflake ...

-- Or manually run the DDL script: sql/ddl/snowflake.sql
-- This creates idr_meta, idr_work, and idr_out tables
```

#### Step 6: Load Metadata Configuration

```sql
-- Run: tools/scale_test/setup_retail_metadata.sql
-- This configures source tables, identifier mappings, and rules
```

#### Step 7: Create and Run IDR Stored Procedure

```sql
-- Run IDR using the CLI:
-- idr run --platform snowflake --mode FULL
```

#### Step 8: Validate Results

```bash
python run_validation.py \
  --platform snowflake \
  --account $SNOWFLAKE_ACCOUNT \
  --user $SNOWFLAKE_USER \
  --password $SNOWFLAKE_PASSWORD \
  --warehouse $SNOWFLAKE_WAREHOUSE \
  --database $SNOWFLAKE_DATABASE
```

#### Step 9: Explore Results

```bash
python run_results_exploration.py \
  --platform snowflake \
  --account $SNOWFLAKE_ACCOUNT \
  --user $SNOWFLAKE_USER \
  --password $SNOWFLAKE_PASSWORD \
  --warehouse $SNOWFLAKE_WAREHOUSE \
  --database $SNOWFLAKE_DATABASE
```

#### Expected Output (7M Dataset)

| Metric | Expected Value |
|--------|----------------|
| Total Entities | 7,000,000 |
| Total Clusters | ~2,593,385 |
| Total Edges | ~6,551,391 |
| LP Iterations | ~9 |
| Duration | 3-5 minutes |

---

### Databricks

```python
# In Databricks notebook:

# 1. Load data from GCS/S3/ADLS
# %run ./databricks_load.py

# 2. Initialize schema
# %run ../../sql/ddl/databricks.sql

# 3. Configure metadata (adapt setup_retail_metadata.sql)

# 4. Run IDR
# 4. Run IDR
# Use idr_core library in the notebook
```

---

## Verification Queries

These queries work across all platforms (adjust syntax slightly for each).

### 1. Check Cluster Statistics

```sql
-- Cluster size distribution
SELECT cluster_size, COUNT(*) AS cluster_count
FROM idr_out.identity_clusters_current
GROUP BY cluster_size
ORDER BY cluster_size;
```

### 2. Sample Golden Profiles

```sql
-- View sample golden profiles
SELECT resolved_id, email_primary, phone_primary, first_name, last_name
FROM idr_out.golden_profile_current
WHERE email_primary IS NOT NULL AND first_name IS NOT NULL
LIMIT 10;
```

### 3. Cross-Source Matching

```sql
-- Find identities spanning multiple sources
SELECT m.resolved_id,
       COUNT(DISTINCT m.source_id) AS source_count,
       -- For BigQuery use STRING_AGG, for others use LISTAGG or equivalent
       LISTAGG(DISTINCT m.source_id, ', ') AS sources
FROM idr_out.identity_resolved_membership_current m
GROUP BY m.resolved_id
HAVING COUNT(DISTINCT m.source_id) >= 3
LIMIT 20;
```

### 4. Precision/Recall vs Truth

```sql
-- Resolution accuracy against ground truth
WITH source_map AS (
  SELECT 'bazaarvoice_survey' AS truth_table, 'SURVEY' AS idr_source UNION ALL
  SELECT 'digital_customer_account', 'DIGITAL' UNION ALL
  SELECT 'pos_customer', 'POS' UNION ALL
  SELECT 'ecom_order', 'ECOM' UNION ALL
  SELECT 'store_order', 'STORE' UNION ALL
  SELECT 'sdk_event', 'SDK' UNION ALL
  SELECT 'product_review', 'REVIEW'
),
mapped AS (
  SELECT t.person_id, m.resolved_id
  FROM idr_input.truth_links t
  JOIN source_map sm ON sm.truth_table = t.source_table
  LEFT JOIN idr_out.identity_resolved_membership_current m
    ON m.source_id = sm.idr_source AND m.source_key = t.source_record_id
)
SELECT
  ROUND(100.0 * SUM(CASE WHEN cluster_cnt = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_perfect_resolution,
  ROUND(AVG(cluster_cnt), 2) AS avg_clusters_per_person
FROM (
  SELECT person_id, COUNT(DISTINCT resolved_id) AS cluster_cnt
  FROM mapped GROUP BY person_id
) x;
```

### 5. Fragmented Identities

```sql
-- Find truth persons split across multiple clusters
WITH source_map AS (
  SELECT 'bazaarvoice_survey' AS truth_table, 'SURVEY' AS idr_source UNION ALL
  SELECT 'digital_customer_account', 'DIGITAL' UNION ALL
  SELECT 'pos_customer', 'POS' UNION ALL
  SELECT 'ecom_order', 'ECOM' UNION ALL
  SELECT 'store_order', 'STORE' UNION ALL
  SELECT 'sdk_event', 'SDK' UNION ALL
  SELECT 'product_review', 'REVIEW'
)
SELECT
  t.person_id,
  COUNT(*) AS records,
  COUNT(DISTINCT m.resolved_id) AS idr_clusters
FROM idr_input.truth_links t
JOIN source_map sm ON sm.truth_table = t.source_table
LEFT JOIN idr_out.identity_resolved_membership_current m
  ON m.source_id = sm.idr_source AND m.source_key = t.source_record_id
GROUP BY t.person_id
HAVING COUNT(DISTINCT m.resolved_id) > 1
LIMIT 20;
```

### 6. Orphaned Records Check

```sql
-- Check identifier coverage in product_review
SELECT
  COUNT(*) AS total_reviews,
  COUNT(email_norm) AS with_email,
  COUNT(digital_customer_id) AS with_customer_id,
  COUNT(CASE WHEN email_norm IS NULL AND digital_customer_id IS NULL THEN 1 END) AS orphaned
FROM idr_input.product_review;
```

### 7. Run History

```sql
-- View IDR run history
SELECT run_id, run_mode, started_at, ended_at, status, entities_processed, edges_created
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 10;
```

### 8. Survivorship Verification

```sql
-- Compare golden profile with source values for a cluster
WITH sample_cluster AS (
  SELECT resolved_id FROM idr_out.identity_clusters_current WHERE cluster_size = 3 LIMIT 1
)
SELECT
  m.resolved_id,
  m.source_id,
  ea.email,
  ea.first_name,
  ea.record_updated_at,
  g.email_primary AS golden_email,
  g.first_name AS golden_first_name
FROM idr_out.identity_resolved_membership_current m
JOIN sample_cluster s ON m.resolved_id = s.resolved_id
LEFT JOIN idr_work.entities_all ea ON ea.entity_key = m.entity_key
LEFT JOIN idr_out.golden_profile_current g ON g.resolved_id = m.resolved_id
ORDER BY m.resolved_id, ea.record_updated_at DESC;
```

---

## Expected Results

| Metric | Expected |
|--------|----------|
| Resolution Ratio | 0.4 - 0.6 |
| Avg Cluster Size | 2 - 5 |
| Max Cluster Size | < 50 |
| Perfect Resolution | > 50% (with ensure_one_identifier) |
| Orphaned Records | 0% (with ensure_one_identifier) |

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| 0% resolution | Join mismatch between truth and IDR | Check source_table vs source_id naming |
| High fragmentation | Missing identifiers | Enable `ensure_one_identifier` in config |
| Large clusters | Over-matching | Tune `max_group_size` in rules |
