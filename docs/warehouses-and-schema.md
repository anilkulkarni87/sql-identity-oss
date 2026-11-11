---
title: Supported Warehouses, Schema & Operations
---

# Supported Warehouses, Schema Reference & Operations

`sql-identity-resolution` supports **DuckDB**, **Snowflake**, **BigQuery**, and **Databricks** with 100% feature parity across all 8 pipeline stages.

---

## 1. Warehouse Connection & Tuning Guide

=== "DuckDB"

    ```bash
    # Local file database
    idr init --platform duckdb --db ./my_warehouse.duckdb
    idr apply-config --db ./my_warehouse.duckdb --config config.yaml
    idr run --db ./my_warehouse.duckdb --mode FULL
    ```

    - **Best For**: Local development, CI/CD testing, single-node analytics up to 50M+ rows.
    - **Adapter**: `idr_core/adapters/duckdb.py` (`DuckDBAdapter`).

=== "Snowflake"

    ```bash
    export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
    export SNOWFLAKE_USER="IDR_SERVICE_USER"
    export SNOWFLAKE_PASSWORD="..."
    export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
    export SNOWFLAKE_DATABASE="ANALYTICS"
    export SNOWFLAKE_ROLE="IDR_ADMIN"

    idr init --platform snowflake
    idr apply-config --platform snowflake --config config.yaml
    idr run --platform snowflake --mode INCR
    ```

    - **Credential Auto-Discovery**: Automatically reads `~/.snowflake/connections.toml` and `~/.snowflake/config`.
    - **Adapter**: `idr_core/adapters/snowflake.py` (`SnowflakeAdapter`).

=== "BigQuery"

    ```bash
    # Authenticate via gcloud Application Default Credentials
    gcloud auth application-default login

    idr init --platform bigquery --project my-gcp-project --location US
    idr apply-config --platform bigquery --project my-gcp-project --config config.yaml
    idr run --platform bigquery --project my-gcp-project --mode INCR
    ```

    - **Schema Mapping**: Maps `idr_meta`, `idr_work`, and `idr_out` to BigQuery datasets in your target GCP project.
    - **Adapter**: `idr_core/adapters/bigquery.py` (`BigQueryAdapter`).

=== "Databricks"

    ```bash
    export DATABRICKS_HOST="adb-123456789.12.azuredatabricks.net"
    export DATABRICKS_TOKEN="dapi..."
    export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abcdef123456"
    export DATABRICKS_CATALOG="main"

    idr init --platform databricks
    idr apply-config --platform databricks --config config.yaml
    idr run --platform databricks --mode INCR
    ```

    - **Credential Auto-Discovery**: Automatically reads `~/.databrickscfg` profiles (including headless INI and named profiles).
    - **Table Existence Checks**: Uses `SHOW TABLES IN <schema> LIKE '<table_name>'` so Unity Catalog / SQL Warehouses never log `[TABLE_OR_VIEW_NOT_FOUND]` errors during preflight checks.
    - **Adapter**: `idr_core/adapters/databricks.py` (`DatabricksAdapter`).

    ![Databricks Full IDR Run](assets/recordings/idr-databricks-run.gif)

---

## 2. Complete Warehouse Schema Reference

All tables are defined in `idr_core/schema_defs.py` (`SYSTEM_TABLES`) and mirrored in `sql/ddl/{duckdb,snowflake,bigquery,databricks}.sql`. During **Stage 1 (`preflight`)**, `PreflightStage._ensure_schema_upgrades()` automatically creates any missing tables on existing deployments.

### `idr_meta` Schema (Configuration, State & Governance)

| Table | Primary Key / Purpose | Key Columns |
| :--- | :--- | :--- |
| `idr_meta.source_table` | Registered source tables | `table_id`, `table_fqn`, `entity_type`, `entity_key_expr`, `watermark_column`, `watermark_lookback_minutes`, `is_active` |
| `idr_meta.source_identifier` | Identifier extraction mappings | `table_id`, `identifier_type`, `identifier_value_expr`, `is_hashed` |
| `idr_meta.entity_attribute_mapping` | Profile attribute mappings | `table_id`, `attribute_name`, `attribute_expr` |
| `idr_meta.rule` | Deterministic matching rules | `rule_id`, `identifier_type`, `canonicalize`, `max_group_size`, `priority`, `is_active` |
| `idr_meta.fuzzy_rule` | Blocking-based fuzzy rules | `rule_id`, `rule_name`, `blocking_key_expr`, `score_expr`, `threshold`, `priority`, `is_active` |
| `idr_meta.survivorship_rule` | Golden profile attribute rules | `attribute_name`, `strategy`, `source_priority_list`, `recency_field` |
| `idr_meta.identifier_exclusion` | Poison-pill blocklist | `identifier_type` (`EMAIL`, `PHONE`, `ANY`), `identifier_value_pattern`, `match_type` (`EXACT`, `LIKE`), `reason` |
| `idr_meta.identity_override` | Manual stewardship overrides | `override_id`, `override_type` (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`), `entity_key_a`, `entity_key_b`, `reason`, `created_by`, `is_active` |
| `idr_meta.privacy_audit_log` | GDPR / CCPA erasure ledger | `request_id`, `request_type`, `target_value`, `affected_entities_count`, `affected_clusters_count`, `tables_purged`, `requested_by`, `reason`, `executed_at`, `status` |
| `idr_meta.run_state` | Per-table incremental watermarks | `table_id`, `last_watermark_value`, `last_run_id`, `last_run_ts` |

### `idr_work` Schema (Ephemeral Pipeline Staging)

| Table | Purpose |
| :--- | :--- |
| `idr_work.entities_delta` | New/updated entities extracted in the current run (`watermark_value IS NOT NULL`) plus candidate-expanded historical entities (`watermark_value IS NULL`). |
| `idr_work.identifiers_extracted` | Canonicalized and exclusion-filtered identifiers for current delta entities. |
| `idr_work.identifiers_all` | Full working set of identifiers across impacted entities used for anchor edge building. |
| `idr_work.entity_attributes` | Extracted attributes for fuzzy blocking (skipped automatically when `0` active fuzzy rules exist). |
| `idr_work.edges_new` | Anchor-based star edges ($N-1$ edges per group) after `ISOLATE`, `DO_NOT_LINK`, and `FORCE_LINK` overrides. |
| `idr_work.lp_edges` / `idr_work.lp_labels` | Undirected graph edges and iterative Label Propagation labels (stabilized against historical `resolved_id`s). |

### `idr_out` Schema (Production Outputs & Telemetry)

| Table | Purpose | Key Columns |
| :--- | :--- | :--- |
| `idr_out.identity_resolved_membership_current` | Canonical mapping of every `entity_key` to its persistent `resolved_id`. | `entity_key`, `resolved_id`, `updated_at`, `super_cluster_id` |
| `idr_out.identity_identifiers_current` | Current normalized identifiers per `entity_key` (powers candidate expansion & no-watermark state-diffing). | `entity_key`, `identifier_type`, `identifier_value_norm`, `is_hashed`, `updated_at` |
| `idr_out.identity_clusters_current` | Cluster-level quality and confidence metrics. | `resolved_id`, `cluster_size`, `confidence_score`, `edge_diversity`, `match_density`, `primary_reason`, `updated_at` |
| `idr_out.golden_profile_current` | Consolidated 360° customer profile per `resolved_id`. | `resolved_id`, `email_primary`, `phone_primary`, `first_name`, `last_name`, `<dynamic_attributes>`, `updated_at` |
| `idr_out.resolved_id_crosswalk` | Historical ledger of cluster ID merges and splits across runs. | `run_id`, `old_resolved_id`, `new_resolved_id`, `entity_key`, `change_type`, `changed_at` |
| `idr_out.run_history` & `idr_out.stage_metrics` | Run-level and stage-level execution telemetry. | `run_id`, `run_mode`, `status`, `entities_processed`, `edges_created`, `clusters_Impacted`, `lp_iterations`, `duration_seconds` |
| `idr_out.skipped_identifier_groups` | Identifier groups skipped for exceeding `max_group_size`. | `run_id`, `identifier_type`, `identifier_value_norm`, `group_size`, `max_allowed`, `sample_entity_keys` |

---

## 3. Production Hardening & Troubleshooting

### Recommended Incremental Scheduling Pattern
- **Hourly / Intra-day Runs**: Run `idr run --mode INCR` every 15–60 minutes. Watermarked event/order tables only scan rows newer than `last_watermark_value`, while dimension tables without watermarks use fast identifier state-diffing against `idr_out.identity_identifiers_current` and short-circuit immediately if no identifiers changed.
- **Weekly Guardrail Audit**: Query `idr_out.skipped_identifier_groups` to spot new placeholder values that exceeded `max_group_size` and promote them to `idr_meta.identifier_exclusion`.

### Production Preflight Health Check (`idr doctor`)
Run automated preflight diagnostics across connectivity, IAM permissions, schema initialization, active rules, and guardrails:
```bash
idr doctor --platform bigquery --project my-project
```
Verifies:
1. **Connectivity & Latency**: Measures live warehouse ping.
2. **Schema & System Tables**: Checks presence of all 7 core tables across `idr_meta`, `idr_work`, and `idr_out`.
3. **DDL/DML Staging Probe**: Probes `idr_work` with non-destructive temporary table creation and drop to certify write permissions.
4. **Configuration & Watermarks**: Validates registered sources, watermarks vs. dimension tables, and matching rules.
5. **Poison-Pill Guardrails**: Verifies active exclusion lists and warns if dangerous supernode rules exist.

### Autonomous Configuration Bootstrapper (`idr bootstrap --auto`)
Profile warehouse tables, discover customer identifiers, and synthesize a production YAML manifest in seconds:
```bash
# Profile dataset and write to idr_config.yaml
idr bootstrap --platform bigquery --project my-project --schema ecommerce_synth --output idr_config.yaml

# Or profile and immediately apply to warehouse metadata tables
idr bootstrap --platform bigquery --project my-project --schema ecommerce_synth --apply
```

### Troubleshooting Common Scenarios

| Symptom | Root Cause & Resolution |
| :--- | :--- |
| **Authentication Error in Local Web UI** | Always launch via `idr ui` (which sets `IDR_ALLOW_INSECURE_DEV_AUTH=true` on loopback hosts) or set `VITE_ALLOW_INSECURE_DEV_AUTH=true` if running Vite dev server directly. |
| **Incremental Run Re-Processes Unchanged Rows** | Ensure event/fact tables have a `watermark_column` mapped in `sources:`. For tables without a `watermark_column`, Stage 3 automatically diffs extracted identifiers against `idr_out.identity_identifiers_current` and prunes unchanged rows. |
| **Oversized Cluster Formed (`cluster_size > 1000`)** | Enable `use_default_exclusions: true`, inspect `idr_out.skipped_identifier_groups`, add the offending shared identifier to `idr_meta.identifier_exclusion`, or apply an `ISOLATE` / `DO_NOT_LINK` override in `idr_meta.identity_override`. |

---

## 4. Orchestration, Deployment & Observability

### Apache Airflow (`deployment/airflow/`)
Automate hourly incremental and weekly full graph executions with pre-run safety gates:
- **Safety Gate**: Runs `idr run --mode INCR --dry-run` and inspects `idr_out.dry_run_summary` (`moved_entities`, `largest_proposed_cluster`). If changes stay within configured thresholds, branches to `live_incremental_run`; otherwise aborts and notifies on-call.
- Reference implementation: [`deployment/airflow/idr_dag.py`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/deployment/airflow/idr_dag.py).

### Snowflake Tasks & Streams (`deployment/snowflake/`)
Schedule IDR and automate retention cleanup natively in Snowflake:
- **Incremental & Full Tasks**: Runs hourly or weekly via Snowflake Tasks.
- **Daily Retention Cleanup**: Purges `idr_out.dry_run_results` (7-day retention), `idr_out.skipped_identifier_groups` (30-day retention), and truncates ephemeral `idr_work.*` staging tables when no run is active.
- **Failure Alert Stream**: An active stream on `idr_out.run_history` alerts immediately when `status = 'FAILED'`.
- Reference implementation: [`deployment/snowflake/tasks.sql`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/deployment/snowflake/tasks.sql).

### BigQuery / GCP Terraform (`deployment/gcp/`)
Provision serverless IDR pipelines on Google Cloud:
- Creates `idr_meta`, `idr_work`, and `idr_out` BigQuery datasets.
- Configures IAM service account with minimal `roles/bigquery.jobUser` and dataset permissions.
- Deploys Cloud Function (2nd Gen) and Cloud Scheduler triggers for hourly incremental runs.
- Reference implementation: [`deployment/gcp/main.tf`](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/deployment/gcp/main.tf).

### Enterprise Compose Stack with Keycloak SSO & Observability
Simulate or deploy production IDR with SSO, Redis session leasing, and metrics:
```bash
docker compose -f docker-compose.enterprise.yml up -d --build
```
- **Keycloak SSO**: Bootstrapped via `deployment/keycloak/idr-realm.json` (`test` / `test` user).
- **Prometheus Metrics**: Scrapes `http://api:8000/metrics` via `monitoring/prometheus/prometheus.yml`.
- **Grafana Dashboard**: Pre-provisioned 6-panel dashboard (`monitoring/grafana/dashboards/idr_dashboard.json`) tracking entities processed, edges created, clusters impacted, and last run duration.

### Scale Testing & Benchmarking Tools (`tools/scale_test/`)
- **End-to-End Benchmark**: `python tools/scale_test/run_scale_and_mcp_benchmark.py --rows 250000 --incr-rows 10000` (runs resolution, verifies 4 mathematical graph invariants, and benchmarks all MCP server tools).
- **Synthetic Omni-Channel Retail Generator**: `python tools/scale_test/generate_retail_idr_mimesis.py --config tools/scale_test/configs/retail_idr_5k.yaml` (generates Parquet files across CRM, POS, E-commerce, web events, app events, and support tickets).
- **Cloud Loaders**: Fast Parquet loaders for DuckDB (`load_duckdb.py`), BigQuery (`load_bigquery.py`), Snowflake (`load_snowflake.py`), and Databricks (`load_databricks.py`).

---

## 5. Production Benchmarks & Real-World Runtimes

The pipeline has been benchmarked on a realistic omni-channel e-commerce workload spanning **15 source tables** and **6,297,049 entity records** across customer profiles, loyalty transactions, POS accounts, online/in-store orders, addresses, email campaigns, AB-testing experiments, reviews, support tickets, web events, and fraud cases.

### Warehouse Benchmark Comparison (6.3M Entities, 15 Source Tables)

The table below summarizes full identity resolution execution metrics across supported warehouse engines (executed with `--mode FULL --strict`):

| Metric / Dimension | Google BigQuery | Databricks (Unity Catalog) | Snowflake | DuckDB (Local) |
| :--- | :--- | :--- | :--- | :--- |
| **Compute Engine** | Serverless BigQuery (US Multi-Region) | Serverless SQL Warehouse | *[Testing in progress — results being added shortly]* | Single-Node (10-core M-series ARM) |
| **Source Tables** | 15 tables | 15 tables | 15 tables | 15 tables |
| **Input Entities** | **6,297,049** | **~6,300,000** | *Upcoming* | 250,000 (scale harness) |
| **Edges Constructed** | **5,381,635** | **~5,380,000** | *Upcoming* | ~215,000 |
| **Resolved Clusters** | **1,544,042** | **~1,544,000** | *Upcoming* | ~62,000 |
| **Graph LP Iterations**| **6 iterations** | **6 iterations** | *Upcoming* | 4 iterations |
| **Total Pipeline Duration** | **306s (5.1 min)** | **296s (4.9 min)** | *[Testing in progress]* | 12.4s |
| **Graph Label Propagation** | **110s** | **~108s** | *Upcoming* | 4.8s |
| **Golden Profile Output** | **90s** | **~85s** | *Upcoming* | 3.2s |

> [!NOTE] Snowflake Testing in Progress
> Snowflake benchmarking on this exact 6.3M-entity omni-channel workload is currently underway and results will be added directly upon test verification.

---

### Detailed Stage-by-Stage Breakdown (BigQuery vs. Databricks)

Both warehouses demonstrate near-identical execution characteristics on identical workloads:

| Stage # | Pipeline Stage | BigQuery Duration | BigQuery Rows | Databricks Duration | Databricks Rows | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Stage 1** | `preflight` | 26.0s | 0 | ~24s | 0 | Metadata schema validation, column DDL checks, and run state initialization |
| **Stage 2** | `entity_extraction` | 5.0s | 6,297,049 | ~6s | ~6.3M | Scans 15 tables, generates composite entity keys, and stages into `idr_work.entities_delta` |
| **Stage 3** | `identifier_extraction`| 54.0s | 5,710,608 | ~52s | ~5.7M | Normalizes identifiers (`EMAIL_STANDARD`, `PHONE_DIGITS`, etc.) and applies poison-pill filters |
| **Stage 4** | `attribute_extraction` | 1.0s | 0 | ~1s | 0 | Extracts attributes for blocking rules (bypassed in `--strict` mode) |
| **Stage 5** | `edge_building` | 12.0s | 5,381,635 | ~14s | ~5.4M | Anchor-based star graph reduction ($N-1$ edges per group) avoiding $O(N^2)$ cliques |
| **Stage 6** | `label_propagation` | 110.0s | 1,544,042 | ~108s | ~1.54M | Iterative connected components (converged in 6 iterations) |
| **Stage 7** | `output_generation` | 90.0s | 1,544,042 | ~85s | ~1.54M | Wide consolidated golden profiles with survivorship across 15 source feeds, plus crosswalk |
| **Total** | **Full Pipeline Run** | **306s (5.1 min)** | — | **296s (4.9 min)** | — | Complete end-to-end warehouse-native execution |



