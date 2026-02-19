# Metrics Reference

Reference for all metrics exported by SQL Identity Resolution.

---

## Built-in Metrics

These metrics are automatically recorded during each run.

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `idr_run_duration_seconds` | gauge | seconds | Total run duration |
| `idr_entities_processed` | gauge | count | Entities processed this run |
| `idr_edges_created` | counter | count | Edges created |
| `idr_clusters_impacted` | gauge | count | Clusters affected |
| `idr_lp_iterations` | gauge | count | Label propagation iterations |
| `idr_groups_skipped` | counter | count | Groups skipped (max_group_size) |
| `idr_values_excluded` | counter | count | Values excluded (exclusion list) |
| `idr_large_clusters` | gauge | count | Clusters exceeding threshold |

---

## Metric Types

### Gauge

Current point-in-time value. Can go up or down.

**Examples:**
- `idr_run_duration_seconds`
- `idr_clusters_impacted`
- `idr_large_clusters`

### Counter

Cumulative count that only increases.

**Examples:**
- `idr_edges_created`
- `idr_groups_skipped`

---

## Dimensions

Metrics can include dimensions for filtering:

| Dimension | Values | Description |
|-----------|--------|-------------|
| `run_mode` | `FULL`, `INCR` | Processing mode |
| `platform` | `duckdb`, `snowflake`, `bigquery`, `databricks` | Platform |
| `status` | `SUCCESS`, `FAILED`, `DRY_RUN_COMPLETE` | Run status |

**Example with dimensions:**
```json
{
  "metric_name": "idr_run_duration_seconds",
  "metric_value": 42,
  "dimensions": {
    "run_mode": "INCR",
    "platform": "snowflake"
  }
}
```

---

## Querying Metrics

### Basic Query

```sql
SELECT
    run_id,
    metric_name,
    metric_value,
    recorded_at
FROM idr_out.metrics_export
WHERE run_id = 'run_abc123';
```

### Time Series

```sql
SELECT
    DATE(recorded_at) as date,
    metric_name,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value,
    COUNT(*) as samples
FROM idr_out.metrics_export
WHERE metric_name = 'idr_run_duration_seconds'
  AND recorded_at >= CURRENT_DATE - 30
GROUP BY DATE(recorded_at), metric_name
ORDER BY date DESC;
```

### Percentiles

```sql
SELECT
    metric_name,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY metric_value) as p50,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY metric_value) as p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) as p99
FROM idr_out.metrics_export
WHERE metric_name = 'idr_run_duration_seconds'
  AND recorded_at >= CURRENT_DATE - 30
GROUP BY metric_name;
```

---

## Prometheus Format

When using the Prometheus exporter:

```prometheus
# HELP idr_run_duration_seconds Total run duration
# TYPE idr_run_duration_seconds gauge
idr_run_duration_seconds{run_id="run_abc123",run_mode="INCR"} 42

# HELP idr_entities_processed Entities processed this run
# TYPE idr_entities_processed gauge
idr_entities_processed{run_id="run_abc123"} 1234

# HELP idr_edges_created Edges created
# TYPE idr_edges_created counter
idr_edges_created{run_id="run_abc123"} 5678
```

---

## DataDog Format

When using the DataDog exporter:

```json
{
  "series": [
    {
      "metric": "idr.run_duration_seconds",
      "type": "gauge",
      "points": [[1704067200, 42]],
      "tags": ["run_id:run_abc123", "run_mode:INCR"]
    },
    {
      "metric": "idr.entities_processed",
      "type": "gauge",
      "points": [[1704067200, 1234]],
      "tags": ["run_id:run_abc123"]
    }
  ]
}
```

---

## Alerting Thresholds

### Recommended Alerts

| Metric | Condition | Severity | Action |
|--------|-----------|----------|--------|
| `idr_run_duration_seconds` | > 3600 | Warning | Investigate performance |
| `idr_run_duration_seconds` | > 7200 | Critical | Immediate attention |
| `idr_groups_skipped` | > 100 | Warning | Review max_group_size |
| `idr_large_clusters` | > 10 | Warning | Investigate cluster growth |
| `idr_lp_iterations` | = max_iters | Warning | May not have converged |

### Prometheus Alert Rules

```yaml
groups:
  - name: idr_alerts
    rules:
      - alert: IDRRunTooLong
        expr: idr_run_duration_seconds > 3600
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "IDR run taking too long"
          description: "Run {{ $labels.run_id }} has been running for {{ $value }} seconds"

      - alert: IDRManySkippedGroups
        expr: idr_groups_skipped > 100
        labels:
          severity: warning
        annotations:
          summary: "Many identifier groups skipped"
          description: "{{ $value }} groups were skipped in run {{ $labels.run_id }}"
```

### DataDog Monitors

```yaml
name: "IDR - High Skipped Groups"
type: metric alert
query: "avg(last_5m):sum:idr.groups_skipped{*} > 100"
message: "More than 100 identifier groups skipped. Review max_group_size settings."
tags:
  - "service:identity-resolution"
  - "team:data"
```

---

## Custom Metrics

### Adding Custom Metrics

In Python runners:
```python
record_metric('my_custom_metric', value, {'dimension': 'value'}, 'gauge')
```

In Snowflake:
```javascript
recordMetric('my_custom_metric', value, {dimension: 'value'}, 'gauge');
```

### Metric Naming Conventions

- Use `idr_` prefix
- Use snake_case
- Include unit in name if applicable (`_seconds`, `_bytes`, `_count`)
- Be specific: `idr_email_edges_created` instead of just `edges`

---

## Retention

### Cleanup Old Metrics

```sql
-- Delete metrics older than 90 days
DELETE FROM idr_out.metrics_export
WHERE recorded_at < CURRENT_TIMESTAMP - INTERVAL '90 days';
```

### Partitioning (BigQuery)

```sql
CREATE TABLE idr_out.metrics_export
PARTITION BY DATE(recorded_at)
OPTIONS (
  partition_expiration_days = 90
);
```

---

## Next Steps

- [CLI Reference](cli-reference.md)
- [Schema Reference](schema-reference.md)
- [Metrics & Monitoring Guide](../guides/metrics-monitoring.md)
