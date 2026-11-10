# CLI Reference

The `idr` command-line interface is the primary operational tool for configuring, provisioning, and executing SQL Identity Resolution pipelines across DuckDB, Snowflake, BigQuery, and Databricks.

---

## 🛠️ Global Usage

```bash
idr [--help] <command> [options]
```

### Commands Summary

| Command | Description |
| :--- | :--- |
| `idr ui` | Launch the interactive IDR Studio UI & API server in one command (opens browser automatically) |
| `idr run` | Execute full or incremental identity resolution pipelines (`--platform` auto-detected if omitted) |
| `idr init` | Provision or reset system schemas (`idr_meta`, `idr_work`, `idr_out`) |
| `idr config` | Validate, generate SQL DDL, apply YAML configurations, or export to `dbt_idr` seeds |
| `idr dbt export` | Export YAML or live warehouse metadata directly into the 6 standard `dbt_idr` seed CSVs |
| `idr quickstart` | Generate synthetic multi-channel retail data and run an automated local demo |
| `idr mcp` / `idr-mcp` | Start the Model Context Protocol (MCP) server for AI assistants (19 tools) |
| `idr serve` | Launch the FastAPI REST service, bundled UI, and Prometheus metrics endpoint |
| `idr version` | Print current package and engine version |

---

## 🚀 Command Details

### 1. `idr run`

Executes the 6-stage identity resolution pipeline against your target warehouse.

```bash
idr run --platform=<platform> [connection-flags] [options]
```

#### Options

| Option | Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `--platform` | `-p` | String | Auto-detected | Target warehouse: `duckdb`, `snowflake`, `bigquery`, or `databricks` (inferred from `--db`, `--project`, `IDR_PLATFORM`, or local credentials if omitted) |
| `--mode` | `-m` | String | `FULL` | Pipeline execution mode: `FULL` (complete rebuild) or `INCR` (delta watermarks) |
| `--config` | | Path | None | Path to declarative YAML configuration to apply prior to run |
| `--max-iters` | | Integer | `30` | Maximum iterations for distributed label propagation |
| `--dry-run` | | Flag | `False` | Computes cluster impact and diffs without mutating production tables |
| `--strict` | | Flag | `False` | Deterministic mode: strictly disable fuzzy similarity matching |

#### Warehouse Connection Flags

* **DuckDB**:
  * `--db PATH`: Path to local DuckDB database file (e.g. `--db=analytics.duckdb`).
* **BigQuery**:
  * `--project ID`: Google Cloud project ID.
  * `--location`: BigQuery dataset region (default: `US`).
  * `--dataset`: Output dataset name (default: `idr_out`).
  * `--meta-dataset`: Metadata dataset name (default: `idr_meta`).
  * `--work-dataset`: Working dataset name (default: `idr_work`).
* **Snowflake**:
  * Configured via environment variables (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`) or standard Snowflake connection profile (`~/.snowflake/connections.toml`).
* **Databricks**:
  * Configured via environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`).

#### Examples

```bash
# Run full resolution on local DuckDB
idr run --platform=duckdb --db=demo.duckdb --mode=FULL

# Incremental run on Snowflake with strict deterministic matching
idr run --platform=snowflake --mode=INCR --strict

# Dry-run analysis on BigQuery with custom YAML configuration
idr run --platform=bigquery --project=my-gcp-project --config=idr_config.yaml --dry-run
```

---

### 2. `idr init`

Initializes metadata tables and serving schema DDL in the target warehouse.

```bash
idr init --platform=<platform> [connection-flags] [--reset]
```

#### Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--platform` | String | **Required** | Target warehouse (`duckdb`, `snowflake`, `bigquery`, `databricks`) |
| `--reset` | Flag | `False` | **Destructive**: Drops and recreates existing metadata tables |

#### Example

```bash
# Initialize schema on Databricks
idr init --platform=databricks

# Reset local DuckDB schemas
idr init --platform=duckdb --db=test.duckdb --reset
```

---

### 3. `idr config`

Manage, validate, and generate SQL from declarative YAML configurations.

#### Subcommands

* **`idr config validate --file=<path>`**:
  Validates syntax, schema definitions, and survivorship rules against the IDR configuration specification.

  ```bash
  idr config validate --file=config/retail_rules.yaml
  ```

* **`idr config generate --file=<path> [--dialect=<dialect>]`**:
  Translates declarative YAML configuration into dialect-specific SQL DDL/DML statements (`duckdb`, `snowflake`, `bigquery`, `databricks`).

  ```bash
  idr config generate --file=rules.yaml --dialect=snowflake > deploy_rules.sql
  ```

* **`idr config apply --file=<path> [--platform=<platform>] [connection-flags]`**:
  Executes generated SQL statements directly against your target warehouse to update metadata tables.

  ```bash
  idr config apply --file=rules.yaml --db=demo.duckdb
  ```

* **`idr dbt export` (or `idr config export-dbt`)**:
  Compiles a YAML configuration file (`--file`) or live warehouse `idr_meta` tables (`--db` / `--platform`) into the 6 standard `dbt_idr` seed CSV files (`idr_sources.csv`, `idr_identifier_mappings.csv`, `idr_attribute_mappings.csv`, `idr_rules.csv`, `idr_exclusions.csv`, `idr_survivorship_rules.csv`).

  ```bash
  # Export from YAML manifest to your dbt project's seeds directory
  idr dbt export --file=idr_config.yaml --output-dir=dbt_idr/seeds

  # Export from live DuckDB metadata tables configured via the Web UI
  idr dbt export --db=demo.duckdb --output-dir=dbt_idr/seeds
  ```

---

### 4. `idr quickstart`

Generates synthetic multi-channel customer records across Web, Store, Mobile, and Call Center, creates a local DuckDB instance, runs full identity resolution, and displays a summary table.

```bash
idr quickstart [--rows=10000] [--output=quickstart_demo.duckdb] [--seed=42]
```

#### Options

| Option | Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `--rows` | | Integer | `10000` | Number of synthetic customer records to generate |
| `--output` | `-o` | Path | `quickstart_demo.duckdb` | Output DuckDB database file path |
| `--seed` | | Integer | `42` | Random seed for deterministic reproducibility |

---

### 5. `idr mcp`

Launches the Model Context Protocol (MCP) server, exposing 17 operational tools to AI assistants (Claude Desktop, Cursor, Antigravity).

```bash
idr mcp [--transport=stdio|sse]
```

#### Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--transport` | String | `stdio` | Transport communication channel: `stdio` (default) or `sse` |

---

### 6. `idr ui` & `idr serve`

Starts the FastAPI REST server, serves the embedded React Studio SPA, and exposes Swagger UI documentation (`/docs`) and Prometheus metrics (`/metrics`).

```bash
# Launch UI + API and automatically open http://localhost:8000 in your browser:
idr ui [--host=127.0.0.1] [--port=8000] [--no-browser]

# Zero-install execution via uvx:
uvx --from "sql-identity-resolution[all]" idr ui

# Headless API / UI server mode:
idr serve [--host=0.0.0.0] [--port=8000] [--reload] [--open]
```

#### Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--host` | String | `0.0.0.0` | IP address to bind server |
| `--port` | Integer | `8000` | Port number to bind server |
| `--reload` | Flag | `False` | Enable auto-reload on code change (development mode) |

---

## 🔑 Environment Variables Reference

| Variable | Description | Default |
| :--- | :--- | :--- |
| `IDR_PLATFORM` | Default platform adapter (`duckdb`, `snowflake`, `bigquery`, `databricks`) | `duckdb` |
| `IDR_DATABASE` | Default DuckDB file path or cloud database name | None |
| `IDR_JSON_LOGS` | Output structured JSON logs instead of formatted console text (`1` or `true`) | `0` |
| `IDR_PII_ACCESS` | Controls MCP / API PII redaction: `masked` or `full` | `masked` |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | None |
| `SNOWFLAKE_USER` | Snowflake username | None |
| `SNOWFLAKE_PASSWORD` | Snowflake password | None |
| `SNOWFLAKE_WAREHOUSE` | Snowflake virtual warehouse name | None |
| `DATABRICKS_HOST` | Databricks workspace URL | None |
| `DATABRICKS_TOKEN` | Databricks personal access token | None |
| `DATABRICKS_HTTP_PATH` | Databricks SQL warehouse HTTP path | None |
