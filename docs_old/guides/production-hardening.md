# Production Hardening

Best practices for running SQL Identity Resolution in production environments.

---

## Data Quality Controls

### max_group_size

Prevents generic identifiers from creating mega-clusters.

```sql
-- Set appropriate limits
UPDATE idr_meta.rule SET max_group_size = 10000 WHERE identifier_type = 'EMAIL';
UPDATE idr_meta.rule SET max_group_size = 5000 WHERE identifier_type = 'PHONE';
UPDATE idr_meta.rule SET max_group_size = 1 WHERE identifier_type = 'SSN';
```

**What happens when exceeded:**
1. Identifier group is skipped
2. Entities become singletons (resolved_id = entity_key)
3. Logged to `idr_out.skipped_identifier_groups`

**Review skipped groups:**
```sql
SELECT
    identifier_type,
    identifier_value_norm,
    group_size,
    max_allowed,
    sample_entity_keys
FROM idr_out.skipped_identifier_groups
WHERE run_id = 'run_xyz'
ORDER BY group_size DESC;
```

### Identifier Exclusions

Block known bad identifiers:

```sql
-- Exact matches
INSERT INTO idr_meta.identifier_exclusion VALUES
  ('EMAIL', 'test@test.com', FALSE, 'Generic test'),
  ('EMAIL', 'null@null.com', FALSE, 'Null placeholder'),
  ('PHONE', '0000000000', FALSE, 'Invalid');

-- Patterns (LIKE syntax)
INSERT INTO idr_meta.identifier_exclusion VALUES
  ('EMAIL', '%@example.com', TRUE, 'Example domain'),
  ('EMAIL', 'noreply@%', TRUE, 'No-reply'),
  ('EMAIL', '%@mailinator.%', TRUE, 'Disposable');
```

---

## Large Cluster Monitoring

### Configure Threshold

```sql
INSERT INTO idr_meta.config VALUES
  ('large_cluster_threshold', '5000', 'Warn on clusters larger than this', NOW());
```

### Monitor Large Clusters

```sql
-- Current large clusters
SELECT resolved_id, cluster_size
FROM idr_out.identity_clusters_current
WHERE cluster_size >= 5000
ORDER BY cluster_size DESC;

-- Growth over time
SELECT
    DATE(updated_ts) as date,
    COUNT(*) as large_cluster_count,
    MAX(cluster_size) as max_size
FROM idr_out.identity_clusters_current
WHERE cluster_size >= 5000
GROUP BY DATE(updated_ts)
ORDER BY date DESC;
```

### Alerting

Run warnings appear in `idr_out.run_history`:

```sql
SELECT
    run_id,
    status,
    large_clusters,
    groups_skipped,
    warnings
FROM idr_out.run_history
WHERE status = 'SUCCESS_WITH_WARNINGS'
ORDER BY started_at DESC;
```

---

## Incremental Processing

### Use INCR Mode

After initial FULL run, use INCR for efficiency:

```bash
# First time
idr run --platform [platform] --mode FULL

# Subsequent runs
idr run --platform [platform] --mode INCR
```

### Watermark Management

```sql
-- Check watermark status
SELECT
    table_id,
    last_watermark_value,
    last_run_id,
    last_run_ts
FROM idr_meta.run_state;

-- Reset watermark (force reprocess)
UPDATE idr_meta.run_state
SET last_watermark_value = '1900-01-01'::TIMESTAMP
WHERE table_id = 'customers';
```

### Lookback Buffer

For late-arriving data:

```sql
UPDATE idr_meta.source_table
SET watermark_lookback_minutes = 60  -- 1 hour buffer
WHERE table_id = 'customers';
```

---

## Performance Optimization

### Index Source Tables

```sql
-- DuckDB
CREATE INDEX idx_customers_updated ON customers(updated_at);
CREATE INDEX idx_customers_email ON customers(LOWER(email));

-- Snowflake (clustering)
ALTER TABLE customers CLUSTER BY (updated_at);

-- BigQuery (partitioning)
CREATE TABLE customers
PARTITION BY DATE(updated_at)
AS SELECT * FROM raw_customers;
```

### Limit LP Iterations

For very large graphs, reduce max iterations:

```bash
idr run --platform [platform] --max-iters 20  # Default: 30
```

### Parallel Processing

=== "Snowflake"
    ```sql
    -- Use larger warehouse
    ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';
    -- Invoke via CLI or Snowpark Container Service
    -- idr run --platform=snowflake ...
    ```

=== "BigQuery"
    ```bash
    # Slot reservations reduce processing time
    # Configure in BigQuery Console
    ```

=== "Databricks"
    ```python
    # Use larger cluster
    # Enable autoscaling
    ```

---

## Audit Trail

### Run History

Every run is logged:

```sql
SELECT
    run_id,
    run_mode,
    started_at,
    duration_seconds,
    entities_processed,
    edges_created,
    clusters_impacted,
    status,
    warnings
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 20;
```

### Skipped Groups Audit

```sql
SELECT *
FROM idr_out.skipped_identifier_groups
WHERE run_id = 'run_xyz';
```

### Stage Metrics

```sql
SELECT
    stage_name,
    rows_affected,
    duration_seconds
FROM idr_out.stage_metrics
WHERE run_id = 'run_xyz'
ORDER BY started_at;
```

---

## Disaster Recovery

### Backup Configuration

```sql
-- Export configuration
CREATE TABLE backup.source_table AS SELECT * FROM idr_meta.source_table;
CREATE TABLE backup.rule AS SELECT * FROM idr_meta.rule;
CREATE TABLE backup.identifier_mapping AS SELECT * FROM idr_meta.identifier_mapping;
```

### Rollback Procedure

If a bad run needs to be rolled back:

1. **Stop any scheduled jobs**
2. **Identify the bad run_id**
3. **Reset watermarks** (if needed):
   ```sql
   UPDATE idr_meta.run_state
   SET last_watermark_value = (
       SELECT last_watermark_value
       FROM idr_out.run_history
       WHERE run_id = 'previous_good_run'
   );
   ```
4. **Restore from backup** (if available)
5. **Re-run with corrected configuration**

---

## Security Best Practices

### Least Privilege

Create dedicated roles:

=== "Snowflake"
    ```sql
    CREATE ROLE IDR_EXECUTOR;
    GRANT USAGE ON WAREHOUSE compute_wh TO ROLE IDR_EXECUTOR;
    GRANT SELECT ON ALL TABLES IN SCHEMA crm TO ROLE IDR_EXECUTOR;
    GRANT ALL ON SCHEMA idr_meta TO ROLE IDR_EXECUTOR;
    GRANT ALL ON SCHEMA idr_work TO ROLE IDR_EXECUTOR;
    GRANT ALL ON SCHEMA idr_out TO ROLE IDR_EXECUTOR;
    ```

=== "BigQuery"
    ```bash
    # Create service account with minimal permissions
    gcloud iam service-accounts create idr-runner

    # Grant BigQuery Job User + specific dataset access
    bq query --use_legacy_sql=false \
      "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA idr_out TO 'serviceAccount:idr-runner@project.iam.gserviceaccount.com'"
    ```

### Secrets Management

- Never hardcode credentials in scripts
- Use environment variables or secret managers
- Rotate credentials regularly

---

## Pre-Production Checklist

- [ ] All source tables registered and active
- [ ] Identifier mappings complete
- [ ] max_group_size configured appropriately
- [ ] Known bad identifiers excluded
- [ ] Dry run completed successfully
- [ ] Large cluster threshold set
- [ ] Structured logging enabled (IDR_JSON_LOGS=1)
- [ ] Monitoring/alerting configured
- [ ] Backup procedures documented
- [ ] Access controls verified
- [ ] Scheduling configured

---

## Next Steps

- [Metrics & Monitoring](metrics-monitoring.md)
- [Troubleshooting](troubleshooting.md)
- [CI/CD](../deployment/ci-cd.md)
