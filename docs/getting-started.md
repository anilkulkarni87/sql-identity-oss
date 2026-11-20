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

Create a `config.yaml` file (or use `examples/config_retail.yaml`) and load it into `idr_meta.*` (and save a versioned snapshot in `idr_out.config_snapshot`):

```bash
idr config apply --platform duckdb --db demo.duckdb --file examples/config_retail.yaml
# Legacy alias also supported:
# idr apply-config --platform duckdb --db demo.duckdb --config examples/config_retail.yaml
```

### Step 3: Execute the Resolution Pipeline (or Ephemeral Dry Run)

Run a full graph build (`--mode FULL`), an incremental delta run (`--mode INCR`), or preview a candidate YAML without saving it to `idr_meta.*`:

```bash
# Preview a draft YAML ephemerally without saving config or mutating output tables
idr run --platform duckdb --db demo.duckdb --mode FULL --config candidate.yaml --dry-run

# First run: full graph resolution
idr run --platform duckdb --db demo.duckdb --mode FULL

# Subsequent runs: incremental watermark & state-diff resolution
idr run --platform duckdb --db demo.duckdb --mode INCR

# Inspect config version history or roll back to a previous snapshot
idr config history --platform duckdb --db demo.duckdb
idr config rollback --platform duckdb --db demo.duckdb
```

!!! tip "Dry Run Preview & Ephemeral Config Testing"
    Add `--dry-run` to preview moved/new entities, `Edges Delta`, `Clusters Delta`, and largest cluster changes in `idr_out.dry_run_results` and `idr_out.dry_run_summary` without mutating production tables. Passing `--config candidate.yaml --dry-run` snapshots `idr_meta.*` and automatically restores your saved configuration after the dry run finishes.

### Step 4: Launch the Interactive Web UI

Start the integrated FastAPI + React Web UI server with a single command:

```bash
# Launch UI + API on http://127.0.0.1:8000 (auto-detects local cloud/DuckDB credentials)
idr ui

# Or pre-connect a specific DuckDB file
idr ui --db demo.duckdb
```

Open **[http://127.0.0.1:8000](http://127.0.0.1:8000)** in your browser to:

- Configure sources, mappings, rules, exclusions, lenses, and survivorship in the persistent 4-tab **Identity Studio** (`1. Sources & Mappings` with merged multi-schema table browser & zero-PII statistical **`AI Suggest Config`**, `2. Matching Rules` with 4 sub-tabs for Match Rules / Multi-Lens Views / Blocklist Exclusions / Stewardship Overrides, `3. Survivorship`, `4. Run & Verify`).
- Trigger quick `INCR` or `FULL` runs from anywhere using the top-bar **`Run Pipeline`** slide-over drawer or open the **`AI Copilot (⌘K)`** drawer for natural-language graph Q&A, anomaly triage, and runtime diagnostics.
- Preview unsaved draft rule changes with non-saving **Start Dry Run** (`Draft Previewed — Saved Config Untouched`, `Edges Delta` & `Clusters Delta`), inspect **History**, or **Discard** drafts.
- Search entities, switch or diff **Identity Lenses**, inspect collapsible **Crosswalk Lineage** transition tables, run **AI Cluster Forensics (`Explain with AI`)**, create **Manual Overrides** (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`), or execute **GDPR Erasure** in the **7:5 Graph / Inspector Explorer Workbench**.
- Switch between **Crisp Daylight Engineering** (default) and **Dark Appearance** from the sidebar footer, or configure **Local Ollama** (`llama3.2`, `qwen2.5:14b`) and **Cloud BYOK** (`OpenAI`, `Anthropic`, `Gemini`) in **Settings**.

---

## 3. Zero-Config Credential Discovery

Both the CLI (`idr`) and the Web UI (`idr ui`) automatically discover local cloud credentials via `idr_core/credential_discovery.py` so you rarely need to type connection strings manually:

| Warehouse | Auto-Discovered Local Configs | Environment Variables |
| :--- | :--- | :--- |
| **DuckDB** | Local `*.duckdb` files in the current workspace | `IDR_DATABASE` (or `--db`) |
| **BigQuery** | `~/.config/gcloud/application_default_credentials.json` & active `gcloud` project | `IDR_PROJECT` / `GOOGLE_CLOUD_PROJECT`, `IDR_LOCATION` |
| **Snowflake** | `~/.snowflake/connections.toml` and `~/.snowflake/config` | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` (or RSA Key-Pair: `SNOWFLAKE_PRIVATE_KEY_PATH` / `SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, `SNOWFLAKE_AUTHENTICATOR`), `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_ROLE` |
| **Databricks** | `~/.databrickscfg` profiles (`HOST`, `token`, `http_path`) | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (or OAuth M2M Service Principal: `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`), `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG` |

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

---

## One-Command Local Demo (`idr demo` & `uvx` / `pipx`)

For personal exploration without creating a virtualenv or writing YAML:

```bash
# Run directly with uvx (zero install)
uvx --from "sql-identity-resolution[duckdb,api]" idr demo

# Or install globally with pipx
pipx install "sql-identity-resolution[duckdb,api,mcp]"
idr demo --rows 10000 --port 8000
```

`idr demo` generates a synthetic DuckDB dataset, runs the full identity resolution pipeline, and launches the bundled Explorer UI + `/mcp` server at `http://127.0.0.1:8000`.

---

## Docker & Self-Hosted Deployment

| File | Mode | What's Included |
| :--- | :--- | :--- |
| [`Dockerfile`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/Dockerfile) | **All-in-One** | Single container with React UI, FastAPI, `/mcp`, and all 4 warehouse drivers (`DuckDB`, `Snowflake`, `BigQuery`, `Databricks`). |
| [`docker-compose.yml`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/docker-compose.yml) | **Local Split Dev** | Separate `api` (`:8000`) and Nginx `ui` (`:3000`) containers with dev auth enabled. |
| [`docker-compose.prod.yml`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/docker-compose.prod.yml) | **Production (GHCR)** | Prebuilt `idr-api` and `idr-ui` images with OIDC auth enforced (`IDR_ALLOW_INSECURE_DEV_AUTH=false`). |
| [`docker-compose.enterprise.yml`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/docker-compose.enterprise.yml) | **Enterprise Stack** | `api`, `ui`, `keycloak` (`:8080` OIDC), `redis` (`:6379` session store), `prometheus` (`:9090`), and `grafana` (`:3001`). |

### All-in-One Quickstart

```bash
docker build -t sql-identity-oss .
docker run -p 8000:8000 \
  -e IDR_ALLOW_INSECURE_DEV_AUTH=true \
  -v $(pwd)/data:/data \
  sql-identity-oss
```

### Split Dev / Enterprise Stacks

```bash
# Local dev (all 4 warehouse connectors)
IDR_PIP_EXTRAS=all docker compose up --build

# Production GHCR images (with OIDC)
IDR_AUTH_ISSUER=https://your-idp/realms/idr \
IDR_AUTH_JWKS_URL=https://your-idp/realms/idr/protocol/openid-connect/certs \
docker compose -f docker-compose.prod.yml up -d

# Enterprise SSO + Redis + Prometheus + Grafana
IDR_PIP_EXTRAS=all docker compose -f docker-compose.enterprise.yml up --build
```

