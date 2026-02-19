# IDR Performance Benchmark Results

> **Status**: DuckDB tested, cloud platforms pending  
> **Dataset**: Retail customer data (deterministic seed: 42)  
> **Last Updated**: 2026-01-04

---

## Test Environment Summary

> **Deep Dive**: See the [Detailed 7M Benchmark Report](benchmark-7m-detailed.md) for comprehensive analysis of the 7 million entity test.

| Platform | Configuration | Instance Type | Notes |
|----------|--------------|---------------|-------|
| DuckDB | Local / Docker | MacBook Pro M1/M2 | Single-node, 16GB RAM |
| Snowflake | _TBD_ | _Warehouse size_ | |
| BigQuery | On-demand | Serverless | Auto-scaling, pay-per-query |
| Databricks | _TBD_ | _Cluster config_ | |

---

## 10 Million Rows (DuckDB Baseline)

### Timing Results

| Platform | Data Load | Entity Extract | Edge Build | Label Prop | Output Gen | **Total** |
|----------|-----------|----------------|------------|------------|------------|-----------|
| DuckDB | 1s | 7s | 33s | 81s | 12s | **143s** |
| Snowflake | 5s | 17s | 58s | 53s | 26s | **168s** |
| BigQuery* | N/A | N/A | N/A | N/A | N/A | **383s** |
| Databricks | 14s | 36s | 77s | 115s | ~75s | **317s** |

*> BigQuery run was on 7M records due to sampling, scaled to comparable time.*

### Metrics Results

| Platform | Entities | Edges | Clusters | Largest | Singletons | LP Iters |
|----------|----------|-------|----------|---------|------------|----------|
| DuckDB | 10,000,000 | 16,124,751 | 1,839,324 | _TBD_ | _TBD_ | 6 |
| Snowflake | 10,000,000 | 16,124,751 | 1,839,324 | _TBD_ | _TBD_ | 6 |
| BigQuery*| 7,000,000 | 6,551,391 | 2,593,385 | _TBD_ | _TBD_ | _TBD_ |
| Databricks | 10,000,000 | 16,124,751 | 1,839,324 | _TBD_ | _TBD_ | 6 |

### Consistency Check  
- [ ] All platforms produced same cluster count
- [ ] Largest cluster size matches across platforms
- [ ] Singleton count matches across platforms

---

## 50 Million Rows (Planned)

### Timing Results

| Platform | Data Load | Entity Extract | Edge Build | Label Prop | Output Gen | **Total** |
|----------|-----------|----------------|------------|------------|------------|-----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |

### Metrics Results

| Platform | Entities | Edges | Clusters | Largest | Singletons | LP Iters |
|----------|----------|-------|----------|---------|------------|----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

### Consistency Check  
- [ ] All platforms produced same cluster count
- [ ] Largest cluster size matches across platforms
- [ ] Singleton count matches across platforms

---

## 100 Million Rows (Planned)

### Timing Results

| Platform | Data Load | Entity Extract | Edge Build | Label Prop | Output Gen | **Total** |
|----------|-----------|----------------|------------|------------|------------|-----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |

### Metrics Results

| Platform | Entities | Edges | Clusters | Largest | Singletons | LP Iters |
|----------|----------|-------|----------|---------|------------|----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

### Consistency Check  
- [ ] All platforms produced same cluster count
- [ ] Largest cluster size matches across platforms
- [ ] Singleton count matches across platforms

---

## Performance Charts

### Total Duration by Platform & Scale

```
Platform     | 10M       | 50M      | 100M
-------------|-----------|----------|----------
DuckDB       | ███ 143s  |          |  
Snowflake    | ████ 168s |          |  
BigQuery     | ██████ 295s|         |  
Databricks   | ██████ 317s|         |  
```
_(To be replaced with actual Chart.js visualization)_

### Cost Analysis (Cloud Only)

| Platform | 10M Cost | 50M Cost | 100M Cost | Notes |
|----------|----------|----------|-----------|-------|
| DuckDB | Free | Free | Free | Local/Docker |
| Snowflake | ~$0.25 | _TBD_ | _TBD_ | XS Warehouse: 0.1 credits |
| BigQuery | ~$0.50 | _TBD_ | _TBD_ | On-demand: $6.25/TB scanned |
| Databricks | $_TBD_ | $_TBD_ | $_TBD_ | Serverless SQL Warehouse |

---

## Observations & Insights

### DuckDB
- **10M rows in 143 seconds** (~2.4 min) - excellent for local/dev workloads
- **Label Propagation dominates**: 81s (57% of total) - bottleneck on both platforms
- Edge Building: 33s (23%), fast single-node execution
- Output Gen: 12s (8%), efficient local writes
- Throughput: ~70,000 entities/second
- Created 16.1M edges from 10M entities
- Resolved into 1.84M clusters (~5.4 entities/cluster average)
- RAM usage: ~8-12GB peak for 10M entities

### Snowflake
- **10M rows in 168 seconds** (~2.8 min) - **fastest cloud platform!**
- Only 1.17x slower than local DuckDB
- **Label Propagation**: 53s (32%) - fastest LP of all cloud platforms
- Edge Building: 58s (35%), excellent parallel execution
- Entity + Identifier Extraction: 22s combined (13%)
- Output Gen: 26s (15%), efficient MERGE operations
- Identical metrics: 16.1M edges, 1.84M clusters, 6 LP iterations
- Throughput: ~59,500 entities/second
- Warehouse: IDR_WH (standard size)

### BigQuery
- **7M rows in 383 seconds** (~6.4 min)
- **Fuzzy Matching Enabled**: Includes expensive Levenshtein ops (optimized from UDF)
- **Performance**: ~18,000 entities/second
- **Optimization**: Reduced from 25+ minutes by switching to native SQL functions
- Throughput is lower than DuckDB/Snowflake but scalable to petabytes
- Cost: On-demand model makes this run cost ~$0.50-$1.00

### Databricks
- **10M rows in 317 seconds** (~5.3 min) - 2.2x slower than DuckDB
- Similar to BigQuery performance
- Identical metrics: 16.1M edges, 1.84M clusters, 6 LP iterations
- Throughput: ~31,500 entities/second
- Unity Catalog adds overhead for table operations
- Better suited for larger datasets with Spark parallelism

---

## Recommendations

### When to Use Each Platform

| Use Case | Recommended Platform | Rationale |
|----------|---------------------|-----------|
| < 10M rows, local dev | DuckDB | Fast, free, no infra |
| 10-50M rows, batch | _TBD_ | Based on testing |
| 50-100M rows | _TBD_ | Based on testing |
| > 100M rows | _TBD_ | Based on testing |
| Real-time/streaming | _TBD_ | Based on latency needs |

---

## Test Commands

```bash
# Generate 10M dataset (Mimesis)
python tools/scale_test/generate_retail_idr_mimesis.py --config tools/scale_test/configs/retail_idr_10m.yaml

# Load into DuckDB
python tools/scale_test/load_duckdb.py \
    --input-dir=tools/scale_test/output/retail_idr_10m/ \
    --db=idr_benchmark.duckdb

# Run DuckDB benchmark
# Note: Generator copies config to output folder; use it for consistency.
./idr init --platform duckdb --db idr_benchmark.duckdb --reset
./idr config apply --platform duckdb --db idr_benchmark.duckdb --file tools/scale_test/output/retail_idr_10m/retail_idr_10m.yaml
./idr run --platform duckdb --db idr_benchmark.duckdb
```

---

## Appendix: Test Data Distribution

Target distribution (retail industry standard):

| Cluster Size | Percentage | @ 20M | @ 50M | @ 100M |
|--------------|------------|-------|-------|--------|
| 1 (singleton) | 35% | 7M | 17.5M | 35M |
| 2 (pairs) | 25% | 5M | 12.5M | 25M |
| 3-5 (small) | 20% | 4M | 10M | 20M |
| 6-15 (medium) | 12% | 2.4M | 6M | 12M |
| 16-50 (large) | 5% | 1M | 2.5M | 5M |
| 51-200 (v.large) | 2% | 400K | 1M | 2M |
| 201-1000 (massive) | 1% | 200K | 500K | 1M |

Identifier match rates:
- Email: 55%
- Phone: 25%
- Loyalty ID: 10%
- Address: 10%
- Chain patterns: 15%
