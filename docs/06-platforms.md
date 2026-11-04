# Platforms & Connectivity

IDR provides **first-class, native SQL engine parity** across 4 major data warehouse platforms. All core logic (extraction, edge building, label propagation, fuzzy matching, and survivorship) runs entirely inside the data warehouse.

---

## 🦆 DuckDB (Local & In-Memory)

Ideal for local testing, rapid CI/CD runs, and embedded edge workloads (< 10M rows).

### Connection Options
- **File Database**: Local file path (e.g. `production.duckdb`).
- **In-Memory**: `:memory:` for ephemeral test suites.

### Installation
```bash
pip install "sql-identity-resolution[duckdb]"
```

### CLI Execution
```bash
# Initialize metadata & output schemas
idr init --platform duckdb --db production.duckdb

# Apply configuration
idr config apply --platform duckdb --db production.duckdb --file config.yaml

# Run full pipeline
idr run --platform duckdb --db production.duckdb --mode FULL
```

### Best Practices & Sizing
- Set memory limits for massive local datasets:
  ```sql
  SET memory_limit = '16GB';
  SET threads = 8;
  ```

- Use DuckDB `:memory:` mode in automated unit/integration tests to eliminate disk I/O and avoid file lock contention.

---

## ❄️ Snowflake

Enterprise cloud data warehouse with auto-scaling compute.

### Installation
```bash
pip install "sql-identity-resolution[snowflake]"
```

### Authentication & Environment Variables
IDR supports both Snowpark Python sessions and standard DB-API connections. Set the following environment variables or use `~/.snowflake/connections.toml`:

```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="idr_service_user"
export SNOWFLAKE_PASSWORD="YourPasswordHere"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="CUSTOMER_360"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"   # optional
```

### CLI Execution
```bash
# Initialize schemas (idr_meta, idr_work, idr_out)
idr init --platform snowflake

# Apply configuration
idr config apply --platform snowflake --file config.yaml

# Execute resolution
idr run --platform snowflake --mode FULL
```

### Warehouse Optimization
- **Cluster Keys**: For tables with >10M rows, cluster identity tables for fast lookups:
  ```sql
  ALTER TABLE idr_out.identity_edges_current CLUSTER BY (identifier_type);
  ALTER TABLE idr_out.identity_identifiers_current CLUSTER BY (identifier_type);
  ```

- **Warehouse Sizing**: Use a Medium or Large warehouse for initial full loads (>10M rows); an X-Small or Small warehouse is sufficient for daily incremental (`INCR`) runs.

---

## ☁️ Google Cloud BigQuery

Serverless, highly scalable SQL engine.

### Installation
```bash
pip install "sql-identity-resolution[bigquery]"
```

### Authentication & IAM Permissions
Authenticate via Google Application Default Credentials (ADC) or service account key:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa-key.json"
export IDR_PROJECT="my-gcp-project-id"
```

Required IAM Roles for the service account:

- `roles/bigquery.dataEditor` (or `roles/bigquery.dataOwner` if resetting datasets)
- `roles/bigquery.jobUser`

### CLI Execution
```bash
# Initialize datasets (idr_meta, idr_work, idr_out)
idr init --platform bigquery --project $IDR_PROJECT

# Apply configuration
idr config apply --platform bigquery --project $IDR_PROJECT --file config.yaml

# Execute resolution
idr run --platform bigquery --project $IDR_PROJECT --mode FULL
```

### BigQuery-Specific Considerations
- **Column Types**: BigQuery uses `STRING`, `INT64`, `FLOAT64`, `TIMESTAMP`, and `BOOL`.
- **Primary Keys**: BigQuery primary keys are defined as `NOT ENFORCED`.
- **String Hashing**: Uses `TO_HEX(SHA256(CAST(expr AS STRING)))`.
- **Dataset Location**: Datasets default to `US` unless specified via `--location`.

---

## 🧱 Databricks (Delta Lake / Unity Catalog)

Distributed data lakehouse powered by Apache Spark and Delta Lake.

### Installation
```bash
pip install "sql-identity-resolution[databricks]"
```

### Authentication & Environment Variables
IDR connects via the official `databricks-sql-connector` to SQL Warehouses (Serverless or Classic) or All-Purpose Clusters:

```bash
export DATABRICKS_HOST="dbc-abcdef-1234.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abcdef1234567890"
export DATABRICKS_CATALOG="main"   # Unity Catalog name (optional, defaults to default catalog)
```

### CLI Execution
```bash
# Initialize schemas
idr init --platform databricks

# Apply configuration
idr config apply --platform databricks --file config.yaml

# Execute resolution
idr run --platform databricks --mode FULL
```

### Delta Lake & Unity Catalog Optimizations
- **Column Defaults Compatibility**: IDR DDL automatically omits `DEFAULT` constraints on Databricks Delta tables. This avoids the `[WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED]` error on Delta tables without requiring custom `TBLPROPERTIES`.
- **Table Swapping**: Internal stage table swaps use transactional `CREATE OR REPLACE TABLE ... AS SELECT * FROM ...` + `DROP TABLE`, avoiding cross-schema rename issues in Unity Catalog.
- **Table Maintenance**: Periodically optimize and vacuum identity tables:
  ```sql
  OPTIMIZE idr_out.identity_resolved_membership_current ZORDER BY (resolved_id);
  OPTIMIZE idr_out.identity_edges_current ZORDER BY (identifier_type);
  ```

---

## 🧪 Testing Cloud Dialects

To run the automated smoke test suite against your live cloud instances, set the credentials above and run:

```bash
pytest tests/test_cloud_dialects_live.py -v
```

This test dynamically verifies schema initialization, data ingestion, `FULL` resolution, `DRY_RUN` validation, and Golden Profile survivorship against your live warehouse.
