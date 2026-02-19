# Metrics Reference

Metrics are available in two forms:
- Warehouse-level run metrics in `idr_out.*` tables.
- API operational metrics exposed in Prometheus format at `/metrics`.

## Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `idr_run_duration_seconds` | Gauge | Total execution time |
| `idr_entities_processed` | Counter | Number of source records processed |
| `idr_edges_created` | Counter | Number of graph edges formed |
| `idr_clusters_count` | Gauge | Total unique clusters |
| `idr_lp_iterations` | Gauge | Iterations to convergence |
| `idr_groups_skipped` | Counter | "Supernodes" skipped due to size limit |

## Monitoring

### Prometheus Endpoint

The API exposes:
- `GET /metrics` (Prometheus/OpenMetrics text payload)

Core API metrics:
- `idr_http_requests_total{method,path,status}`
- `idr_http_request_duration_seconds{method,path}`
- `idr_api_db_connected`

### Alerts
*   **High Skipped Groups**: indicates `max_group_size` is too low or data quality issues (e.g., widespread default values).
*   **Max Iterations Reached**: indicates graph did not converge (infinite loop or very deep chains).
