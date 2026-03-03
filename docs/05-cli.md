# CLI Reference

The `idr` command-line interface is the primary way to interact with SQL Identity Resolution.
**Source of Truth**: `idr_core/cli.py`.

## Global Options
| Option | Description |
|--------|-------------|
| `--help` | Show help message and exit |

## Commands

### `idr run`
Execute the identity resolution pipeline.

**Usage:**
```bash
idr run --platform [PLATFORM] [CONNECTION_ARGS] [OPTIONS]
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--platform` | Required | Target platform (`duckdb`, `bigquery`, `snowflake`, `databricks`) |
| `--mode` | `FULL` | `FULL` (rebuild) or `INCR` (process updates) |
| `--config` | None | YAML config file to apply before running |
| `--max-iters` | `30` | Max label propagation iterations |
| `--dry-run` | `False` | Preview changes without committing |
| `--strict` | `False` | Disable fuzzy matching (deterministic only) |

**Connection Arguments:**
*   **DuckDB**: `--db PATH`
*   **BigQuery**: `--project ID`, `--dataset` (default `idr_out`), `--meta-dataset` (`idr_meta`), `--work-dataset` (`idr_work`), `--location` (`US`)
*   **Snowflake**: Uses env vars or `~/.snowflake/connections.toml`
*   **Databricks**: Uses env vars `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`

---

### `idr init`
Initialize metadata tables (`idr_meta` schema).

**Usage:**
```bash
idr init --platform [PLATFORM] [--reset]
```

**Options:**
*   `--reset`: Drop and recreate existing metadata tables (Destructive).

---

### `idr config`
Manage configuration files.

*   `validate --file config.yaml`: Check YAML structure against schema.
*   `generate --file config.yaml --dialect [duckdb|...]`: Output SQL statements.
*   `apply --file config.yaml --platform [PLATFORM]`: Execute SQL statements directly to DB.

---

### `idr quickstart`
Run a zero-config demo on local DuckDB.

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--rows` | `10000` | Number of synthetic records |
| `--output` | `quickstart_demo.duckdb` | Output file path |
| `--seed` | `42` | Random seed |

---

### `idr mcp`
Start the Model Context Protocol (MCP) server.

**Options:**
*   `--transport`: `stdio` (default) or `sse` (requires uvicorn).

---

### `idr serve`
Start the API server.

**Options:**
*   `--host`: Bind host (default `0.0.0.0`)
*   `--port`: Bind port (default `8000`)
*   `--reload`: Enable auto-reload (dev mode)

Use `docker compose up` to run the standalone web UI with API.

---

### `idr doctor`
Run install/runtime diagnostics for supportability.

**Usage:**
```bash
idr doctor
idr doctor --json
idr doctor --strict
idr doctor --telemetry-url https://example.internal/idr-doctor
idr doctor --target cluster \
  --api-url https://idr.example.com/api/health \
  --metrics-url https://idr.example.com/metrics \
  --whoami-url https://idr.example.com/api/auth/whoami
```

**Behavior:**
- checks required runtime constraints (for example Python version and writable workspace)
- checks optional components (DuckDB/API modules, Docker/Compose availability)
- returns non-zero on required failures
- with `--strict`, warnings also return non-zero
- with `--telemetry-url` (or env `IDR_DOCTOR_TELEMETRY_URL`), report is POSTed as JSON for support workflows
- `--target cluster` enables endpoint probes (`--api-url`, `--metrics-url`, optional `--whoami-url`)
- `--token-env` controls which env var supplies bearer token for whoami probe (default `IDR_TOKEN`)

---

### `idr deploy`
Deploy enterprise Helm package using cloud presets.

**Usage:**
```bash
idr deploy --provider aws --mode plan
idr deploy --provider aws --mode apply --use-existing-secret idr-enterprise-secrets
```

**Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--provider` | Required | Cloud preset (`aws`, `gcp`, `azure`) |
| `--mode` | `apply` | `plan` (lint/template) or `apply` (helm upgrade/install) |
| `--namespace` | `idr` | Kubernetes namespace |
| `--release` | `idr-enterprise` | Helm release name |
| `--values` | None | Additional values files (repeatable) |
| `--set` | None | Additional helm set overrides (repeatable) |
| `--use-existing-secret` | None | Existing Kubernetes secret name for chart secret refs |
| `--no-doctor` | `False` | Skip post-deploy doctor probe |

## Deprecated Runners
> **Warning**: Direct usage of `python sql/*/idr_run.py` scripts is deprecated. Please use the unified `idr` CLI.
