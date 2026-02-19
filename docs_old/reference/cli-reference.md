# CLI Reference

The `idr` command-line interface is the primary way to interact with SQL Identity Resolution across all platforms.

## Global Options

| Option | Description |
|--------|-------------|
| `--help` | Show help message and exit |
| `version` | Show version info |

## Commands

### `idr quickstart`
Run an end-to-end demo on your local machine.

**Usage:**
```bash
idr quickstart [OPTIONS]
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--rows` | `1000` | Number of synthetic records to generate |
| `--output` | `qs_test.duckdb` | Path to output DuckDB file |
| `--seed` | `42` | Random seed for reproducible data |

**Example:**
```bash
idr quickstart --rows=50000 --output=demo.duckdb
```

---

### `idr run`
Execute the identity resolution pipeline.

**Usage:**
```bash
idr run --platform [duckdb|bigquery|snowflake|databricks] [CONNECTION_ARGS] [OPTIONS]
```

**Common Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | `FULL` | Run mode: `FULL` (rebuild all) or `INCR` (process updates) |
| `--max-iters` | `30` | Maximum iterations for label propagation |
| `--dry-run` | `False` | Preview mode: simulate run without committing changes |
| `--strict` | `False` | Deterministic mode: disable fuzzy matching |
| `--config` | | Optional YAML config file to apply before running |

**Platform Connection Arguments:**

*   **DuckDB**:
    *   `--db PATH`: Path to DuckDB database file.

*   **BigQuery**:
    *   `--project ID`: GCP Project ID.
    *   `--dataset NAME`: Output dataset (default: `idr_out`).
    *   `--meta-dataset NAME`: Metadata dataset (default: `idr_meta`).
    *   `--work-dataset NAME`: Working/staging dataset (default: `idr_work`).
    *   `--location REGION`: Dataset location (default: `US`).

*   **Snowflake**:
    *   Uses environment variables (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.) or `~/.snowflake/connections.toml`.

*   **Databricks**:
    *   Uses environment variables: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`.

**Examples:**

```bash
# DuckDB Full Run
idr run --platform=duckdb --db=idr.duckdb --mode=FULL

# BigQuery Incremental Dry Run
idr run --platform=bigquery --project=my-project --mode=INCR --dry-run
```

---

### `idr init`
Initialize the IDR metadata tables (`idr_meta` schema) on the target platform.

**Usage:**
```bash
idr init --platform [PLATFORM] [CONNECTION_ARGS] [--reset]
```

**Options:**
*   `--reset`: Drop and recreate existing metadata tables (WARNING: Destructive).

---

### `idr config`
Manage configuration files.

#### `validate`
Validate a YAML configuration file structure.
```bash
idr config validate --file=config.yaml
```

#### `generate`
Generate SQL statements from a YAML configuration.
```bash
idr config generate --file=config.yaml --dialect=[duckdb|bigquery|snowflake|databricks]
```

#### `apply`
Directly apply a YAML configuration to the metadata tables.
```bash
idr config apply --file=config.yaml --platform=[PLATFORM] [CONNECTION_ARGS]
```

---

### `idr serve`
Start the API server and Web UI.

**Usage:**
```bash
idr serve --host 0.0.0.0 --port 8000
```

---

### `idr mcp`
Start the Model Context Protocol (MCP) server for agentic integration.

**Usage:**
```bash
idr mcp
```
*Note: This command reads connection details from `IDR_PLATFORM` and related environment variables.*

---

## Deprecated Runners
> **Warning**: Direct usage of `python sql/*/idr_run.py` scripts is deprecated and will be removed in a future release. Please use the unified `idr` CLI.
