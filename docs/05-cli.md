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
| `idr run` | Execute full or incremental identity resolution pipelines |
| `idr init` | Provision or reset system schemas (`idr_meta`, `idr_work`, `idr_out`) |
| `idr config` | Validate, generate SQL DDL, or apply declarative YAML configurations |
| `idr quickstart` | Generate synthetic multi-channel retail data and run an automated local demo |
| `idr mcp` | Start the Model Context Protocol (MCP) server for AI assistants |
| `idr serve` | Launch the FastAPI REST service and Prometheus metrics endpoint |
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
| `--platform` | `-p` | String | **Required** | Target warehouse: `duckdb`, `snowflake`, `bigquery`, or `databricks` |
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

* **`idr config apply --file=<path> --platform=<platform> [connection-flags]`**:
  Executes generated SQL statements directly against your target warehouse to update metadata tables.

  ```bash
  idr config apply --file=rules.yaml --platform=duckdb --db=demo.duckdb
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

### 6. `idr serve`

Starts the FastAPI REST server with Swagger UI documentation (`/docs`) and Prometheus metrics scraper (`/metrics`).

```bash
idr serve [--host=0.0.0.0] [--port=8000] [--reload]
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
