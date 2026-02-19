# Platforms and Connectivity

Detailed deployment guides for supported platforms.

## DuckDB (Local / Single Node)

Ideal for local development, testing, and small-scale production (< 10M rows).

### Setup
1.  **Install**: `pip install duckdb`
2.  **Initialize**:
    ```bash
    idr config apply --platform duckdb --db production.duckdb --file config.yaml
    ```
3.  **Run**:
    ```bash
    idr run --platform duckdb --db production.duckdb --mode FULL
    ```

### Production Advice
*   Use a persistent volume for the `.duckdb` file.
*   Allocate sufficient RAM (2x dataset size recommended).

---

## Snowflake

Scalable cloud data warehouse.

### Prerequisites
*   Account with `CREATE DATABASE` and `CREATE SCHEMA` privileges.
*   Python environment with `snowflake-connector-python`.

### Setup
1.  **Environment Variables**:
    ```bash
    export SNOWFLAKE_ACCOUNT=...
    export SNOWFLAKE_USER=...
    export SNOWFLAKE_PASSWORD=...
    export SNOWFLAKE_WAREHOUSE=...
    export SNOWFLAKE_DATABASE=...
    ```
2.  **Initialize**:
    ```bash
    idr config apply --platform snowflake --file config.yaml
    ```
3.  **Run**:
    ```bash
    idr run --platform snowflake --mode FULL
    ```

### Production Advice
*   **Clustering**: `ALTER TABLE idr_out.identity_edges_current CLUSTER BY (identifier_type)`.
*   **Performance**: Use larger warehouses (L/XL) for initial >100M loads.

---

## BigQuery

Serverless, auto-scaling.

### Prerequisites
*   GCP Project with BigQuery API enabled.
*   Service Account with `BigQuery Job User` and `Data Editor`.

### Setup
1.  **Auth**: `export GOOGLE_APPLICATION_CREDENTIALS=key.json`
2.  **Initialize**:
    ```bash
    idr config apply --platform bigquery --project my-project --file config.yaml
    ```
3.  **Run**:
    ```bash
    idr run --platform bigquery --project my-project --mode FULL
    ```

### Production Advice
*   **Fuzzy Matching**: Uses native SQL functions (Levenshtein) optimized for BigQuery.
*   **Costs**: Use slot reservations for predictable pricing on massive workloads.

---

## Databricks

Spark-based distributed processing.

### Prerequisites
*   Databricks Workspace (AWS/Azure/GCP).
*   SQL Warehouse or All-Purpose Cluster.
*   Unity Catalog (Recommended).

### Setup
1.  **Env Vars**:
    ```bash
    export DATABRICKS_HOST=...
    export DATABRICKS_TOKEN=...
    export DATABRICKS_HTTP_PATH=...
    ```
2.  **Initialize**:
    ```bash
    export DATABRICKS_CATALOG=hive_metastore  # optional
    idr config apply --platform databricks --file config.yaml
    ```
3.  **Run**:
    ```bash
    idr run --platform databricks --mode FULL
    ```

### Production Advice
*   **Photon**: Enable Photon engine for 2-3x faster localized processing.
*   **Optimization**: `OPTIMIZE` tables regularly to compact delta files.
