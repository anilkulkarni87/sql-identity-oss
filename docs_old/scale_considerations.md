# Scale Considerations

This document covers performance characteristics and optimization strategies for processing **millions of rows** across **multiple sources**.

---

## Performance Characteristics

### Complexity Analysis

| Component | Complexity | Notes |
|-----------|------------|-------|
| Entity extraction | O(N) | Linear scan per source table |
| Identifier extraction | O(N × M) | N entities, M identifier types per entity |
| Edge building | O(N) | Anchor-based, not O(N²) |
| Label propagation | O(E × D) | E edges, D graph diameter (iterations) |
| Golden profile | O(C × A) | C clusters, A attributes per cluster |

### Expected Scale

| Metric | 1M rows | 10M rows | 100M rows |
|--------|---------|----------|-----------|
| Entities delta (incremental) | ~10K-100K | ~100K-1M | ~1M-10M |
| Edges created per run | ~5K-50K | ~50K-500K | ~500K-5M |
| LP iterations | 3-5 | 5-8 | 5-10 |
| Typical run time | 2-10 min | 10-30 min | 30-120 min |

> **Note**: Times assume Databricks with 8-16 workers. Actual performance depends on cluster size, data skew, and cluster connectivity.

---

## Bottleneck Analysis

### 1. Large Identifier Groups

**Symptom**: One identifier (e.g., shared email domain) matches millions of entities.

**Impact**:
- Huge edge tables for that identifier
- LP iterations slow to converge
- Giant clusters (data quality issue)

**Mitigation**:
```sql
-- Add to rule table: max_group_size
-- In edge building, skip groups larger than threshold
WHERE group_size <= COALESCE(r.max_group_size, 10000)
```

### 2. Deeply Connected Graphs

**Symptom**: LP takes 20+ iterations to converge.

**Impact**: Each iteration creates/drops large tables.

**Mitigation**:
- Set reasonable `MAX_ITERS` (default: 30)
- Investigate data quality—deep chains often indicate bad identifiers
- Consider breaking chains at low-confidence edges

### 3. Identifier Table Scans

**Symptom**: `identifiers_all` table scan is slow.

**Impact**: Edge building step dominates runtime.

**Mitigation**:
```sql
-- Partition by identifier_type
ALTER TABLE idr_work.identifiers_all
CLUSTER BY (identifier_type, identifier_value_norm)

-- Or use Delta Z-ORDER
OPTIMIZE idr_work.identifiers_all
ZORDER BY (identifier_type, identifier_value_norm)
```

### 4. Golden Profile Correlated Subqueries

**Symptom**: Golden profile step is slow for large clusters.

**Impact**: One subquery per attribute per resolved_id.

**Mitigation**: Refactor to window functions:
```sql
-- Instead of correlated subquery per attribute
SELECT resolved_id,
  FIRST_VALUE(email_raw) OVER (
    PARTITION BY resolved_id
    ORDER BY trust_rank, record_updated_at DESC
  ) AS email_primary
FROM ...
```

---

## Cluster Sizing Guidelines

### Databricks

| Data Volume | Recommended Cluster |
|-------------|---------------------|
| < 1M rows | 4 workers, Standard_DS3_v2 |
| 1-10M rows | 8 workers, Standard_DS4_v2 |
| 10-100M rows | 16+ workers, Standard_DS5_v2 |
| > 100M rows | 32+ workers, enable Photon |

### Key Settings

```python
# Spark configs for large jobs
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Increase for > 10M rows
spark.conf.set("spark.sql.adaptive.enabled", "true")   # Auto-optimize
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

---

## Incremental vs Full Processing

To run the Identity Resolution process:

```bash
# First time
idr run --platform [platform] --mode FULL

# Subsequent runs
idr run --platform [platform] --mode INCR
```

### When to Use Incremental (`RUN_MODE=INCR`)

- ✅ Daily/hourly runs with delta changes
- ✅ Source tables have reliable watermark columns
- ✅ Processing window is < 10% of total volume

### When to Use Full (`RUN_MODE=FULL`)

- ✅ Initial load / first-time setup
- ✅ After schema changes or rule updates
- ✅ Monthly full refresh for data hygiene
- ✅ When watermarks are unreliable

### Hybrid Strategy

```
Weekly: RUN_MODE=INCR (process deltas)
Monthly: RUN_MODE=FULL (full refresh, verify consistency)
```

---

## Partitioning Strategy

### Recommended Partitioning

| Table | Partition By | Cluster By |
|-------|--------------|------------|
| `identity_edges_current` | None | `(identifier_type, identifier_value_norm)` |
| `identity_resolved_membership_current` | None | `(resolved_id)` |
| `golden_profile_current` | None | `(resolved_id)` |

### For Very Large Deployments (> 100M)

```sql
-- Partition edges by month for easier maintenance
CREATE TABLE idr_out.identity_edges_current (
  ...
) PARTITIONED BY (first_seen_month STRING)
```

---

## Monitoring Queries

### Check Cluster Size Distribution

```sql
SELECT
  CASE
    WHEN cluster_size = 1 THEN '1 (singletons)'
    WHEN cluster_size <= 5 THEN '2-5'
    WHEN cluster_size <= 20 THEN '6-20'
    WHEN cluster_size <= 100 THEN '21-100'
    ELSE '100+'
  END AS size_bucket,
  COUNT(*) AS cluster_count,
  SUM(cluster_size) AS total_entities
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1
```

### Identify Problematic Identifiers

```sql
-- Find identifiers linking too many entities
SELECT identifier_type, identifier_value_norm, COUNT(*) AS entity_count
FROM idr_work.identifiers_all
GROUP BY 1, 2
HAVING COUNT(*) > 1000
ORDER BY entity_count DESC
LIMIT 20
```

### Track Run Performance

```sql
SELECT
  run_id,
  SUM(edges_created) AS total_edges,
  MIN(started_at) AS started,
  MAX(ended_at) AS ended,
  TIMESTAMPDIFF(MINUTE, MIN(started_at), MAX(ended_at)) AS duration_min
FROM idr_out.rule_match_audit_current
GROUP BY run_id
ORDER BY started DESC
LIMIT 10
```

---

## Common Scale Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Cluster timeout | Job killed after 2+ hours | Increase workers, check for data skew |
| OOM errors | Executor lost, memory exceeded | Increase executor memory, reduce shuffle partitions |
| Slow LP convergence | 20+ iterations | Check for giant clusters, add identifier quality rules |
| Giant clusters | 10K+ entities per cluster | Likely bad identifier (e.g., NULL, default value) |
| Edge explosion | Billions of edges | Missing `require_non_null`, bad canonicalization |

---

## Recommended Testing Path

1. **Start small**: 10K rows per source, verify correctness
2. **Scale to 100K**: Check performance, identify bottlenecks
3. **Scale to 1M**: Tune cluster sizing, validate incremental
4. **Production load**: Full volume with monitoring

---

## Further Reading

- [Architecture](concepts/architecture.md) — Core algorithms and data model
- [Troubleshooting Guides](guides/troubleshooting.md) — Operational guide
