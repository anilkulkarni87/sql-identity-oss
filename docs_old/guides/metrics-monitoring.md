# Metrics & Monitoring

Set up observability for your identity resolution pipeline.

---

## Built-in Metrics

Every run automatically records metrics to `idr_out.metrics_export`:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `idr_run_duration_seconds` | gauge | Total run duration |
| `idr_entities_processed` | gauge | Entities processed this run |
| `idr_edges_created` | counter | Edges created |
| `idr_clusters_impacted` | gauge | Clusters affected |
| `idr_lp_iterations` | gauge | Label propagation iterations |
| `idr_groups_skipped` | counter | Groups skipped (max_group_size) |
| `idr_large_clusters` | gauge | Clusters exceeding threshold |

---

## 1. Overview

Identity Resolution (IDR) provides comprehensive observability through:

1.  **UI Dashboard**: Real-time view of run history, cluster statistics, and rule performance. (Recommended for beginners).
2.  **System Tables**: `idr_out` schema tables for custom SQL analysis.
3.  **JSON Logs**: Structured logs for ingestion into Splunk/Datadog.
4.  **Metrics Export**: Plugin system for pushing metrics to external vendors.

This guide focuses on **System Tables** and **Custom Monitoring**.

## Structured Logging

For production observability, enable JSON structured logging. This allows log aggregation systems (Cloud Logging, Datadog, Splunk) to parse run details automatically.

**Enable via Environment Variable:**
```bash
export IDR_JSON_LOGS=1
```

**Log Output Format:**
```json
{
  "timestamp": "2024-03-20T10:00:00",
  "level": "INFO",
  "message": "Completed stage: entity_extraction",
  "run_id": "run_a1b2c3d4",
  "stage": "entity_extraction",
  "event": "stage_end",
  "rows_affected": 50000,
  "duration_seconds": 4.5
}
```

---

## Querying Metrics

### View Recent Metrics

```sql
SELECT
    run_id,
    metric_name,
    metric_value,
    metric_type,
    recorded_at
FROM idr_out.metrics_export
WHERE run_id = 'run_xyz'
ORDER BY recorded_at;
```

### Metrics Over Time

```sql
SELECT
    DATE(recorded_at) as date,
    metric_name,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value
FROM idr_out.metrics_export
WHERE metric_name = 'idr_run_duration_seconds'
  AND recorded_at >= CURRENT_DATE - 30
GROUP BY DATE(recorded_at), metric_name
ORDER BY date DESC;
```

---

## Metrics Exporter (DuckDB & Custom)

for DuckDB or self-hosted deployments, the `tools/metrics_exporter.py` script can push metrics to external systems.

### Usage (DuckDB)

```bash
# Export to Prometheus (scrapeable endpoint)
python tools/metrics_exporter.py \
    --db=idr.duckdb \
    --provider=prometheus \
    --port=9090

# Export to Webhook
python tools/metrics_exporter.py \
    --db=idr.duckdb \
    --provider=webhook \
    --url=https://hooks.slack.com/services/xxx
```

---

## Platform-Native Observability

For data warehouses, we recommend using the native monitoring tools.

### Snowflake

Use **Snowsight Dashboards** to visualize the `idr_out.metrics_export` table.

```sql
-- Example Snowsight Query
SELECT
    run_id,
    metric_value
FROM idr_out.metrics_export
WHERE metric_name = 'idr_entities_processed'
ORDER BY recorded_at DESC;
```

**Alerting:** Use Snowflake Alerts to email on failures.

```sql
CREATE OR REPLACE ALERT idr_failure_alert
  WAREHOUSE = compute_wh
  SCHEDULE = '1 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM idr_out.run_history
    WHERE status = 'FAILED'
      AND ended_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
  ))
  THEN CALL SYSTEM$SEND_EMAIL(...);
```

### BigQuery

Use **Cloud Monitoring** or **Looker Studio**.

1. Connect Looker Studio to `idr_out.metrics_export`.
2. Create time-series charts for `idr_run_duration_seconds` and `idr_entities_processed`.

### Databricks

Use **Databricks SQL Dashboards**.

1. Create a new Dashboard.
2. specific queries against `idr_out.metrics_export`.
3. Set up Alerts on the dashboard widgets (e.g., if `groups_skipped` > 0).

---

## Dashboards

### Key Metrics to Display

| Panel | Query |
|-------|-------|
| Run Success Rate | `COUNT(status='SUCCESS') / COUNT(*)` |
| Average Duration | `AVG(duration_seconds)` |
| Entities Per Run | `AVG(entities_processed)` |
| Cluster Growth | `MAX(cluster_size) over time` |
| Skipped Groups Trend | `SUM(groups_skipped) by date` |

### Sample SQL Dashboard Query

```sql
WITH run_stats AS (
    SELECT
        DATE(started_at) as run_date,
        COUNT(*) as runs,
        SUM(CASE WHEN status LIKE 'SUCCESS%' THEN 1 ELSE 0 END) as successful,
        AVG(duration_seconds) as avg_duration,
        SUM(entities_processed) as total_entities,
        SUM(groups_skipped) as total_skipped
    FROM idr_out.run_history
    WHERE started_at >= CURRENT_DATE - 30
    GROUP BY DATE(started_at)
)
SELECT
    run_date,
    runs,
    ROUND(100.0 * successful / runs, 1) as success_rate,
    ROUND(avg_duration, 0) as avg_duration_sec,
    total_entities,
    total_skipped
FROM run_stats
ORDER BY run_date DESC;
```

---

## Health Checks

### Pre-Run Health Check

```sql
-- Check sources are accessible
SELECT table_id, table_fqn
FROM idr_meta.source_table
WHERE is_active = TRUE;

-- Check for stale watermarks (no runs in 24h)
SELECT table_id, last_run_ts
FROM idr_meta.run_state
WHERE last_run_ts < CURRENT_TIMESTAMP - INTERVAL '24 hours';
```

### Post-Run Validation

```sql
-- Verify outputs populated
SELECT
    'membership' as table_name,
    COUNT(*) as row_count
FROM idr_out.identity_resolved_membership_current
UNION ALL
SELECT
    'clusters',
    COUNT(*)
FROM idr_out.identity_clusters_current;

-- Check for orphaned clusters
SELECT COUNT(*) as orphaned
FROM idr_out.identity_clusters_current c
LEFT JOIN idr_out.identity_resolved_membership_current m
    ON c.resolved_id = m.resolved_id
WHERE m.resolved_id IS NULL;
```

---

## Next Steps

- [Troubleshooting](troubleshooting.md)
- [CI/CD](../deployment/ci-cd.md)
- [Production Hardening](production-hardening.md)
