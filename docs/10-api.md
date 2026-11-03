# REST API Reference

The IDR backend provides a comprehensive FastAPI REST API powering the Web UI, automated CI/CD pipelines, and external integrations.

- **Base URL**: `http://localhost:8000`
- **Interactive Swagger UI**: `http://localhost:8000/docs`
- **OpenAPI Schema**: `http://localhost:8000/openapi.json`

---

## 1. Setup & Configuration (`/api/setup`)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/setup/connect` | Connect to a database adapter (`duckdb`, `snowflake`, `bigquery`, `databricks`). Initializes the session connection. |
| `GET` | `/api/setup/status` | Check current connection and metadata initialization status. |
| `GET` | `/api/setup/config` | Retrieve current saved configuration (sources, rules, survivorship, fuzzy rules, exclusions). |
| `POST` | `/api/setup/config/save` | Validate and persist configuration to metadata tables (`idr_meta`). Requires write access in enterprise RBAC. |
| `GET` | `/api/setup/discover/tables` | Discover tables in a given schema. Query param: `?schema=...`. |
| `GET` | `/api/setup/discover/columns` | Retrieve columns and data types for a table. Query param: `?table=...`. |
| `GET` | `/api/setup/fuzzy-templates` | Get supported fuzzy matching templates per dialect with parameter slots. |
| `POST` | `/api/setup/run` | Trigger an identity resolution pipeline run (`FULL`, `INCR`, or `dry_run=True`). |

### Example: Running a Pipeline via API
```bash
curl -X POST http://localhost:8000/api/setup/run \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "FULL",
    "strict": false,
    "max_iterations": 30,
    "dry_run": false
  }'
```

---

## 2. Metrics & Dashboard (`/api/metrics`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/metrics/summary` | Get high-level graph metrics: total entities, total clusters, total edges, and average confidence score. |
| `GET` | `/api/metrics/distribution` | Get cluster size distribution bucketed into singletons, pairs, small, and large groups. |
| `GET` | `/api/metrics/rules` | Get edge counts and active status broken down by matching rule. |
| `GET` | `/api/alerts` | List operational and data quality alerts (e.g. skipped identifier groups, un-converged runs). |

---

## 3. Identity Explorer (`/api/entities` & `/api/clusters`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/entities/search?q={query}` | Search entities by identifier (email, phone, loyalty ID, entity key). Supports both cluster members and singletons. |
| `GET` | `/api/clusters/{resolved_id}` | Retrieve comprehensive cluster details: member entities, connecting edges with rules, fuzzy `super_cluster_id`, and unified Golden Profile. |

---

## 4. Pipeline Runs & Telemetry (`/api/runs`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/runs` | List execution history with run mode, status, duration, entities processed, and warnings. |
| `GET` | `/api/runs/{run_id}` | Retrieve deep stage telemetry (`stage_metrics`) for a specific run, including per-stage durations, row counts, and error details. |

---

## 5. Enterprise Observability (`/metrics` & `/api/auth`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics scrape endpoint exporting `idr_entities_processed_total`, `idr_edges_created_total`, `idr_clusters_impacted_total`, and `idr_last_run_duration_seconds`. |
| `GET` | `/api/health` | Health check endpoint returning `{ "status": "healthy" }`. |
| `GET` | `/api/auth/me` | Current authenticated user identity and RBAC roles (when OIDC is enabled). |
