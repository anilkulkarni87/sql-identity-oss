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

