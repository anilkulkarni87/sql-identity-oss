# Production Hardening

Best practices for running SQL Identity Resolution in production environments.

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
  ('EMAIL', 'noreply@%', TRUE, 'No-reply');
```

## Large Cluster Monitoring

### Alerting

Run warnings appear in `idr_out.run_history`:

```sql
SELECT run_id, status, large_clusters, groups_skipped, warnings
FROM idr_out.run_history
WHERE status = 'SUCCESS_WITH_WARNINGS'
ORDER BY started_at DESC;
```

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
-- Reset watermark (force reprocess)
UPDATE idr_meta.run_state
SET last_watermark_value = '1900-01-01'::TIMESTAMP
WHERE table_id = 'customers';
```

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

## Audit Trail

### Run History

Every run is logged:

```sql
SELECT * FROM idr_out.run_history ORDER BY started_at DESC LIMIT 20;
```

### Stage Metrics

```sql
SELECT * FROM idr_out.stage_metrics WHERE run_id = 'run_xyz' ORDER BY started_at;
```

## Disaster Recovery

### Rollback Procedure

If a bad run needs to be rolled back:

1. **Stop any scheduled jobs**
2. **Identify the bad run_id**
3. **Reset watermarks** (if needed)
4. **Re-run with corrected configuration**

## Security Best Practices

### Least Privilege

Create dedicated roles:

**Snowflake**
```sql
CREATE ROLE IDR_EXECUTOR;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE IDR_EXECUTOR;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_meta TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_work TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_out TO ROLE IDR_EXECUTOR;
```

**BigQuery**
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
