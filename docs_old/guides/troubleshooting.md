# Troubleshooting

Common issues and solutions for SQL Identity Resolution.

---

## Run Failures

### "No active source tables found"

**Symptom:** Run fails immediately with this error.

**Cause:** No tables registered or all disabled.

**Solution:**
```sql
-- Check what's registered
SELECT * FROM idr_meta.source_table;

-- Ensure at least one is active
UPDATE idr_meta.source_table SET is_active = TRUE WHERE table_id = 'customers';
```

---

### "Source table does not exist"

**Symptom:** Run fails during entity extraction.

**Cause:** `table_fqn` in source_table doesn't match actual table.

**Solution:**
```sql
-- Verify table exists
SELECT * FROM information_schema.tables
WHERE table_schema = 'crm' AND table_name = 'customers';

-- Update if incorrect
UPDATE idr_meta.source_table
SET table_fqn = 'crm.customers'
WHERE table_id = 'customers';
```

---

### "No edges created"

**Symptom:** Run completes but no matching happens.

**Causes:**
1. No identifier mappings defined
2. All identifiers are NULL
3. All groups exceed max_group_size

**Diagnosis:**
```sql
-- Check mappings exist
SELECT * FROM idr_meta.identifier_mapping;

-- Check if identifiers extracted
SELECT COUNT(*) FROM idr_work.identifiers;

-- Check if groups were skipped
SELECT COUNT(*) FROM idr_out.skipped_identifier_groups
WHERE run_id = 'run_xyz';
```

---

## Performance Issues

### Run takes too long

**Causes:**
1. Too many iterations
2. Large data volume
3. Missing indexes

**Solutions:**

```bash
# Reduce max iterations (example for DuckDB)
idr run --platform duckdb --db my.db --max-iters 20
```

```sql
# Snowflake: Update task
CREATE OR REPLACE TASK ... AS
  EXECUTE IMMEDIATE $$
    -- Use IDR CLI wrapper or generic SQL entry point if available
    -- For now, generic task structure
  $$;
```

```sql
-- Add indexes on source tables
CREATE INDEX idx_updated ON customers(updated_at);

-- Use incremental mode
-- (after first FULL run)
```

---

### Out of memory

**Causes:**
1. Giant clusters
2. Too much data in single run

**Solutions:**
```sql
-- Lower max_group_size
UPDATE idr_meta.rule SET max_group_size = 5000;

-- Process in batches (reduce lookback)
UPDATE idr_meta.source_table
SET watermark_lookback_minutes = 0;
```

---

## Data Quality Issues

### Giant cluster forming

**Symptom:** One cluster has thousands of entities.

**Diagnosis:**
```sql
-- Find the giant cluster
SELECT resolved_id, cluster_size
FROM idr_out.identity_clusters_current
ORDER BY cluster_size DESC
LIMIT 5;

-- Find what's connecting them
SELECT
    i.identifier_type,
    i.identifier_value_norm,
    COUNT(DISTINCT i.entity_key) as entity_count
FROM idr_out.identity_resolved_membership_current m
JOIN idr_work.identifiers i ON m.entity_key = i.entity_key
WHERE m.resolved_id = 'giant_cluster_id'
GROUP BY i.identifier_type, i.identifier_value_norm
ORDER BY entity_count DESC;
```

**Solution:**
1. Add the problematic identifier to exclusion list
2. Lower max_group_size for that identifier type

---

### Entities not matching

**Symptom:** Entities with same identifier are in different clusters.

**Diagnosis:**
```sql
-- Check if identifiers match
SELECT entity_key, identifier_value_norm
FROM idr_work.identifiers
WHERE entity_key IN ('entity_a', 'entity_b');

-- Check normalization
-- Are they exactly equal?
```

**Causes:**
1. Normalization issues (different whitespace, case)
2. Character encoding differences
3. max_group_size exceeded

---

### Wrong entities matching

**Symptom:** Unrelated entities are clustered together.

**Diagnosis:**
```sql
-- Find the connecting path
WITH cluster_members AS (
    SELECT entity_key
    FROM idr_out.identity_resolved_membership_current
    WHERE resolved_id = 'problematic_cluster'
)
SELECT
    i.entity_key,
    i.identifier_type,
    i.identifier_value_norm
FROM idr_work.identifiers i
JOIN cluster_members c ON i.entity_key = c.entity_key
ORDER BY i.identifier_value_norm, i.entity_key;
```

**Solution:**
1. Identify the bad identifier
2. Add to exclusion list
3. Lower max_group_size

---

## Platform-Specific Issues

### DuckDB

**"database is locked"**
```bash
# Only one connection at a time
# Close other DuckDB connections
```

### Snowflake

**"Warehouse is suspended"**
```sql
-- Resume warehouse
ALTER WAREHOUSE compute_wh RESUME;
```

**"Insufficient privileges"**
```sql
-- Grant required permissions
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE idr_executor;
GRANT ALL ON SCHEMA idr_meta TO ROLE idr_executor;
```

### BigQuery

**"Quota exceeded"**
```bash
# Reduce query frequency
# Request quota increase in GCP Console
```

**"Dataset not found"**
```bash
bq mk --dataset your_project:idr_meta
```

### Databricks

**"Cluster terminated"**
```
# Check cluster auto-termination settings
# Extend timeout or keep cluster running
```

---

## Dry Run Issues

### "dry_run_results is empty"

**Cause:** No changes would be made.

**Diagnosis:**
```sql
-- Check if any entities processed
SELECT COUNT(*) FROM idr_work.entities_delta;

-- Check if any edges created
SELECT COUNT(*) FROM idr_work.edges_new;
```

---

### Dry run shows unexpected changes

**Diagnosis:**
```sql
-- Review moved entities
SELECT *
FROM idr_out.dry_run_results
WHERE run_id = 'dry_run_xyz'
  AND change_type = 'MOVED';

-- Check what identifier caused the move
-- (cross-reference with edges_new)
```

---

## Recovery Procedures

### Rollback a bad run

```sql
-- 1. Identify the bad run
SELECT * FROM idr_out.run_history ORDER BY started_at DESC;

-- 2. Note the previous good watermark
SELECT * FROM idr_meta.run_state;

-- 3. If you have backups, restore from them

-- 4. Otherwise, re-run in FULL mode after fixing config
```

### Reset to clean state

!!! danger "This deletes all output data"
    Use only if you want to start from scratch.

```sql
-- Reset output tables
TRUNCATE TABLE idr_out.identity_resolved_membership_current;
TRUNCATE TABLE idr_out.identity_clusters_current;
TRUNCATE TABLE idr_out.golden_profile_current;

-- Reset watermarks
UPDATE idr_meta.run_state
SET last_watermark_value = '1900-01-01'::TIMESTAMP;
```

---

## Getting Help

1. **Check run history:** `SELECT * FROM idr_out.run_history ORDER BY started_at DESC`
2. **Check warnings:** Look at the `warnings` column
3. **Check skipped groups:** `SELECT * FROM idr_out.skipped_identifier_groups`
4. **Enable verbose logging:** Platform-specific debug options
5. **Open an issue:** [GitHub Issues](https://github.com/anilkulkarni87/sql-identity-resolution/issues)

---

## Next Steps

- [Defining Identity](configuration.md)
- [Production Hardening](production-hardening.md)
- [Observability](metrics-monitoring.md)
