---
title: Getting Started & Installation
---

# Getting Started & Installation

`sql-identity-resolution` runs entirely inside your existing SQL data warehouse. Python (`idr_core`) compiles your declarative rules into dialect-native SQL statements executed directly by **DuckDB**, **Snowflake**, **BigQuery**, or **Databricks**.

---

## 1. Installation

Install the package with the extras matching your warehouse and desired interfaces:

=== "All Extras (Recommended)"

    ```bash
    pip install "sql-identity-resolution[all]"
    ```

=== "DuckDB + Web UI + MCP"

    ```bash
    pip install "sql-identity-resolution[duckdb,ui,mcp]"
    ```

=== "Snowflake"

    ```bash
    pip install "sql-identity-resolution[snowflake]"
    ```

=== "BigQuery"

    ```bash
    pip install "sql-identity-resolution[bigquery]"
    ```

=== "Databricks"

    ```bash
    pip install "sql-identity-resolution[databricks]"
    ```

=== "From Source (Editable)"

    ```bash
    git clone https://github.com/anilkulkarni87/sql-identity-oss.git
    cd sql-identity-oss
    pip install -e ".[all,dev]"
    ```

### Available Extras Summary

| Extra | Installed Packages | Purpose |
| :--- | :--- | :--- |
| `duckdb` | `duckdb` | Local embedded analytics and zero-setup testing |
| `snowflake` | `snowflake-connector-python` | Snowflake cloud data warehouse adapter |
| `bigquery` | `google-cloud-bigquery` | Google Cloud BigQuery adapter |
| `databricks` | `databricks-sql-connector` | Databricks SQL Warehouse / Unity Catalog adapter |
| `ui` / `api` | `fastapi`, `uvicorn`, `pydantic`, `pyjwt` | FastAPI backend and React Web UI server (`idr ui`) |
| `mcp` | `fastmcp` | Model Context Protocol server (`idr mcp`) for AI assistants |
| `enterprise` | `redis` | Distributed Redis session lease store for multi-replica API deployments |

---

## 2. Five-Minute End-to-End Walkthrough

### Step 1: Run the Synthetic Demo (or Initialize Schemas)

To generate 10,000 realistic retail records across CRM, Orders, and POS tables and resolve them immediately in DuckDB:

```bash
idr quickstart --rows 10000 --output demo.duckdb
```

Or, if connecting to an existing database with your own source tables, initialize the `idr_meta`, `idr_work`, and `idr_out` schemas first:

```bash
idr init --platform duckdb --db demo.duckdb
```

### Step 2: Apply a Declarative Configuration YAML

Create a `config.yaml` file (or use `examples/config_retail.yaml`) and load it into `idr_meta.*`:

```bash
idr apply-config --platform duckdb --db demo.duckdb --config examples/config_retail.yaml
```

### Step 3: Execute the Resolution Pipeline

Run a full graph build (`--mode FULL`) or an incremental delta run (`--mode INCR`):

```bash
# First run: full graph resolution
idr run --platform duckdb --db demo.duckdb --mode FULL

# Subsequent runs: incremental watermark & state-diff resolution
idr run --platform duckdb --db demo.duckdb --mode INCR
```

!!! tip "Dry Run Preview"
    Add `--dry-run` to preview cluster merges, new entities, and largest cluster changes in `idr_out.dry_run_results` and `idr_out.dry_run_summary` without mutating production tables.

### Step 4: Launch the Interactive Web UI

Start the integrated FastAPI + React Web UI server:

```bash
idr ui --db demo.duckdb
```

Open **[http://127.0.0.1:8000](http://127.0.0.1:8000)** in your browser to:

- Configure sources, canonicalizers, exclusions, and survivorship visually in the **6-Step Setup Wizard**.
- Trigger runs with a **Live 8-Stage Execution Progress** tracker.
- Search entities, inspect confidence scores, create **Manual Overrides** (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`), or execute **GDPR Erasure** in the **Cluster Explorer**.

---

## 3. Zero-Config Credential Discovery

Both the CLI (`idr`) and the Web UI (`idr ui`) automatically discover local cloud credentials via `idr_core/credential_discovery.py` so you rarely need to type connection strings manually:

| Warehouse | Auto-Discovered Local Configs | Environment Variables |
| :--- | :--- | :--- |
| **DuckDB** | Local `*.duckdb` files in the current workspace | `IDR_DATABASE` (or `--db`) |
| **BigQuery** | `~/.config/gcloud/application_default_credentials.json` & active `gcloud` project | `IDR_PROJECT` / `GOOGLE_CLOUD_PROJECT`, `IDR_LOCATION` |
| **Snowflake** | `~/.snowflake/connections.toml` and `~/.snowflake/config` | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_ROLE` |
| **Databricks** | `~/.databrickscfg` profiles (`HOST`, `token`, `http_path`) | `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG` |

!!! note "Automatic `--platform` Detection"
    If you pass `--db my_file.duckdb`, `--project my-gcp-project`, or set `IDR_PLATFORM`, the `idr` CLI automatically resolves `--platform` for you.

---

## 4. Downstream Consumption & Reverse-ETL SQL Cookbook

Once `idr run` finishes, all resolved outputs live in `idr_out.*` inside your warehouse—ready for BI, dbt models, or Reverse ETL tools (Hightouch, Census, RudderStack).

### Recipe 1: Join `resolved_id` Back to Fact & Event Tables

Every source row maps to its cluster via `entity_key = CONCAT('<source_id>:', CAST(<entity_key_col> AS VARCHAR))` (or `STRING` on BigQuery/Databricks):

```sql
-- Enrich online orders with the unified resolved_id and golden email
SELECT
    o.order_id,
    o.order_total,
    o.order_ts,
    m.resolved_id,
    gp.email AS golden_email
FROM raw.orders_online o
LEFT JOIN idr_out.identity_resolved_membership_current m
    ON m.entity_key = CONCAT('orders_online:', CAST(o.order_id AS VARCHAR))
LEFT JOIN idr_out.golden_profiles gp
    ON gp.resolved_id = m.resolved_id;
```

### Recipe 2: Incremental Reverse-ETL Export (Only Changed Profiles)

Avoid re-syncing millions of unchanged profiles to your CRM or ad platforms by filtering `idr_out.golden_profiles` to the latest run (or `updated_at`) and gating on cluster confidence:

```sql
-- Export only golden profiles updated in the latest successful run
WITH latest_run AS (
    SELECT run_id
    FROM idr_out.run_history
    WHERE status = 'SUCCESS'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT
    gp.*,
    c.cluster_size,
    c.confidence_score
FROM idr_out.golden_profiles gp
JOIN latest_run lr
    ON gp.run_id = lr.run_id
JOIN idr_out.identity_clusters_current c
    ON gp.resolved_id = c.resolved_id
WHERE c.confidence_score >= 0.50;
```

### Recipe 3: Handle Merged / Retired `resolved_id`s Downstream

When two clusters merge on a subsequent incremental run, `idr_out.resolved_id_crosswalk` records which `previous_resolved_id` folded into `current_resolved_id` so downstream systems can merge or retire stale IDs:

```sql
SELECT
    previous_resolved_id,
    current_resolved_id,
    change_type,
    entities_moved,
    run_id,
    updated_at
FROM idr_out.resolved_id_crosswalk
WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
ORDER BY updated_at DESC;
```

### Recipe 4: Customer 360 Summary Mart

Roll up multi-channel transactions to the unified `resolved_id` grain:

```sql
SELECT
    gp.resolved_id,
    gp.email,
    gp.first_name,
    gp.last_name,
    c.cluster_size,
    c.confidence_score,
    COUNT(DISTINCT o.order_id) AS lifetime_orders,
    COALESCE(SUM(o.order_total), 0) AS lifetime_revenue
FROM idr_out.golden_profiles gp
JOIN idr_out.identity_clusters_current c
    ON gp.resolved_id = c.resolved_id
LEFT JOIN idr_out.identity_resolved_membership_current m
    ON m.resolved_id = gp.resolved_id
   AND m.entity_key LIKE 'orders_online:%'
LEFT JOIN raw.orders_online o
    ON m.entity_key = CONCAT('orders_online:', CAST(o.order_id AS VARCHAR))
GROUP BY 1, 2, 3, 4, 5, 6;
```

