# Cluster Sizing Guide

Performance recommendations for running sql-identity-resolution at scale.

## Quick Reference

| Rows per Table | Total Rows | Max Edge Pairs | Expected Clusters |
|---------------|------------|----------------|-------------------|
| 100K | 500K | ~1M | ~40K-60K |
| 500K | 2.5M | ~5M | ~200K-300K |
| 1M | 5M | ~10M | ~400K-600K |
| 5M | 25M | ~50M | ~2M-3M |
| 10M | 50M | ~100M | ~4M-6M |

## Platform-Specific Sizing

### Databricks

| Scale | Cluster Type | Workers | Node Size | Runtime |
|-------|-------------|---------|-----------|---------|
| 100K | Standard | 2-4 | m5.xlarge | 13.3 LTS |
| 500K | Standard | 4-8 | m5.xlarge | 13.3 LTS |
| 1M | Standard | 8-16 | m5.2xlarge | 13.3 LTS |
| 5M | Photon | 16-32 | m5.4xlarge | 14.3 LTS |
| 10M | Photon | 32-64 | m5.4xlarge | 14.3 LTS |

**Spark Config for Scale:**
```python
# 1M+ rows
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 5M+ rows  
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

### Snowflake

| Scale | Warehouse Size | Expected Duration |
|-------|---------------|-------------------|
| 100K | X-Small | 2-5 min |
| 500K | Small | 5-15 min |
| 1M | Medium | 15-30 min |
| 5M | Large | 30-60 min |
| 10M | X-Large | 1-2 hours |

**Tips:**
```sql
-- Scale up warehouse before running
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'LARGE';

-- Enable clustering for large tables
ALTER TABLE idr_out.identity_edges_current CLUSTER BY (identifier_type);
```

### BigQuery

| Scale | Pricing Model | Slots | Expected Duration |
|-------|--------------|-------|-------------------|
| 100K | On-Demand | Auto | 2-5 min |
| 500K | On-Demand | Auto | 5-15 min |
| 1M | On-Demand | Auto | 15-30 min |
| 5M | Flat-Rate | 2000+ | 30-60 min |
| 10M | Flat-Rate | 4000+ | 1-2 hours |

**Tips:**
- Use partitioned tables for > 1M rows
- Enable BI Engine for faster aggregations
- Monitor slot usage in BigQuery Admin

### DuckDB

| Scale | RAM Required | Expected Duration |
|-------|-------------|-------------------|
| 100K | 2 GB | 30s - 2 min |
| 500K | 4 GB | 2-5 min |
| 1M | 8 GB | 5-15 min |
| 2M | 16 GB | 15-30 min |

**Note:** DuckDB is best for testing < 10M rows. For larger scale, use cloud platforms.

## Overlap Rate Impact

The `OVERLAP_RATE` parameter controls identity density:

| Overlap Rate | Effect | Use Case |
|-------------|--------|----------|
| 0.3 | Many unique identities, few matches | Sparse data |
| 0.5 | Moderate matching | Typical B2C |
| 0.6 | Good matching (default) | Retail/CDP |
| 0.7 | High matching | Loyalty-heavy |

Higher overlap = larger clusters = more LP iterations needed.

## Label Propagation Iterations

| Max Cluster Size | Iterations Needed |
|-----------------|-------------------|
| < 10 | 5-10 |
| 10-50 | 10-20 |
| 50-200 | 20-30 |
| 200+ | 30-50 |

**Warning signs:**
- Not converging at 30 iterations → check for "super clusters" (bad identifiers)
- Giant clusters (1000+) → likely data quality issue (generic identifiers)

## Memory Considerations

The label propagation step is memory-intensive:

```
Memory ≈ (nodes × 2 + edges × 3) × 8 bytes
```

For 1M entities with 2M edges:
- ~48 MB for labels
- Spark/Snowflake handle this automatically
- DuckDB needs sufficient RAM

## Monitoring During Scale Runs

### Key Metrics to Watch

1. **LP Iterations** — Should converge before max
2. **Stage Timing** — Identify bottlenecks
3. **Cluster Size Distribution** — Giant clusters indicate issues

### Query to Check Health

```sql
SELECT
  run_id,
  status,
  duration_seconds,
  lp_iterations,
  entities_processed,
  edges_created
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 5;
```

### Cluster Health Check

```sql
-- Check for giant clusters (potential data issues)
SELECT
  CASE
    WHEN cluster_size = 1 THEN 'singleton'
    WHEN cluster_size <= 10 THEN 'small'
    WHEN cluster_size <= 100 THEN 'medium'
    WHEN cluster_size <= 1000 THEN 'large'
    ELSE 'GIANT ⚠️'
  END AS bucket,
  COUNT(*) AS count,
  SUM(cluster_size) AS entities
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1;
```

## Running Scale Tests

### Databricks
```bash
# 1. Generate Data (Local or Driver)
python tools/scale_test/generate_global_retail_idr.py --rows=1000000 --output=data/

# 2. Upload to DBFS/Volume
# ...

# 3. Run IDR via CLI
idr run --platform databricks --mode FULL
```

### Snowflake
```bash
# 1. Generate & Upload Data
# ...

# 2. Run IDR via CLI
idr run --platform snowflake --mode FULL
```

### BigQuery
```bash
idr run --platform bigquery --project=my-project --mode FULL
```

### DuckDB
```bash
# 1. Generate Data
python tools/scale_test/generate_global_retail_idr.py --rows=1000000 --db=scale_test.duckdb

# 2. Run
idr run --platform duckdb --db=scale_test.duckdb --mode FULL
```
