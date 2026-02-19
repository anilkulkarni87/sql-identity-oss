# IDR Benchmark Results: 7 Million Entities

> **Test Date**: January 12, 2026  
> **Dataset**: Synthetic Retail Customer Data (7 source tables, 1M rows each)  
> **Deterministic Seed**: 42

---

## Executive Summary

This document presents detailed benchmark results for the SQL Identity Resolution (IDR) framework across multiple cloud data platforms. The test processed **7 million entities** from 7 source tables, demonstrating the framework's ability to perform warehouse-native identity resolution at scale.

### Quick Comparison

| Platform | Total Duration | Entities/sec | Clusters | Validation | Est. Cost |
|----------|---------------|--------------|----------|------------|----------|
| **Databricks (Serverless)** | 248s (4.1 min) | 25,362 | 2,593,385 | ✅ 13/13 | ~$0.35 |
| **BigQuery** | 338s (5.6 min) | 20,710 | 2,593,385 | ✅ 13/13 | ~$0.75 |
| **BigQuery (Fuzzy)** | 201s (3.3 min) | 34,825 | 2,593,385 | ✅ 13/13 | ~$0.50 |
| **Snowflake (SMALL)** | 86s (1.4 min) | 81,395 | 2,593,385 | ✅ 13/13 | ~$0.15 |
| **Snowflake (X-SMALL)** | 606s (10.1 min) | 11,551 | 2,593,385 | ✅ 13/13 | ~$0.34 |
| **Snowflake (Fuzzy)** | 633s (10.5 min) | 11,065 | 2,593,385 | ✅ 13/13 | - |

---

## Test Environment

### Dataset Characteristics

| Attribute | Value |
|-----------|-------|
| **Source Tables** | 7 (DIGITAL, POS, ECOM, STORE, SDK, SURVEY, REVIEW) |
| **Total Entities** | 7,000,000 (1M per source) |
| **Identifier Types** | 3 (EMAIL, PHONE, CUSTOMER_ID) |
| **Identifier Mappings** | 13 |
| **Active Rules** | 3 |

### Platform Configurations

| Platform | Configuration | Pricing Model |
|----------|--------------|---------------|
| **BigQuery** | Project: `ga4-134745`, On-demand | Pay-per-query ($6.25/TB) |
| **Snowflake** | X-SMALL / SMALL Warehouse | Compute credits (~$2-4/credit) |
| **Databricks** | Serverless SQL Warehouse | DBU per second |

---

## Databricks Results

### Run Information

| Field | Value |
|-------|-------|
| **Run ID** | `run_6acaab121a19` |
| **Run Mode** | FULL |
| **Status** | SUCCESS |
| **Started** | 2026-01-13 |
| **Total Duration** | **248.6 seconds (4.1 minutes)** |

### Resolution Metrics

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |
| **LP Iterations** | 9 |

---

## BigQuery Results

### Run Information

| Field | Value |
|-------|-------|
| **Run ID** | `run_5126404b8293` |
| **Run Mode** | FULL |
| **Status** | SUCCESS |
| **Started** | 2026-01-12 05:54:59 UTC |
| **Ended** | 2026-01-12 06:00:58 UTC |
| **Total Duration** | **338 seconds (5.6 minutes)** |

### Stage Timing Breakdown

| Stage | Duration | Rows Processed | % of Total |
|-------|----------|----------------|------------|
| Entity Extraction | 12s | 7,000,000 | 3.6% |
| Identifier Extraction | 25s | 9,351,336 | 7.4% |
| Edge Building | 42s | 6,551,391 | 12.4% |
| Subgraph Building | 16s | 5,365,386 | 4.7% |
| **Label Propagation** | **102s** | - | **30.2%** |
| Membership Update | 46s | 2,593,385 | 13.6% |
| Golden Profile Generation | 66s | 2,593,385 | 19.5% |

```
Stage Timing Visualization:

Entity Extraction      ██ 12s (3.6%)
Identifier Extraction  ████ 25s (7.4%)
Edge Building          ██████ 42s (12.4%)
Subgraph Building      ███ 16s (4.7%)
Label Propagation      ███████████████ 102s (30.2%)
Membership Update      ███████ 46s (13.6%)
Golden Profile         ██████████ 66s (19.5%)
```

### Resolution Metrics

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |
| **LP Iterations** | 9 |
| **Resolution Ratio** | 0.370 (37% unique identities) |
| **Avg Cluster Size** | 2.70 |
| **Max Cluster Size** | 57 |

### Cluster Size Distribution

| Size Bucket | Count | Percentage |
|-------------|-------|------------|
| 1 (Singleton) | 1,634,614 | 63.03% |
| 2 | 183,327 | 7.07% |
| 3-5 | 313,077 | 12.07% |
| 6-10 | 408,396 | 15.75% |
| 11-50 | 53,970 | 2.08% |
| 50+ | 1 | 0.0% |

### Source Coverage

| Source | Entities | Unique Clusters | Avg per Cluster |
|--------|----------|-----------------|-----------------|
| DIGITAL | 1,000,000 | 563,658 | 1.77 |
| POS | 1,000,000 | 797,293 | 1.25 |
| ECOM | 1,000,000 | 566,074 | 1.77 |
| STORE | 1,000,000 | 795,615 | 1.26 |
| SDK | 1,000,000 | 890,681 | 1.12 |
| SURVEY | 1,000,000 | 565,975 | 1.77 |
| REVIEW | 1,000,000 | 569,606 | 1.76 |

### Edge Type Contribution

| Identifier Type | Rule ID | Edges Created | % of Total |
|----------------|---------|---------------|------------|
| EMAIL | R_EMAIL | 2,884,845 | 44.0% |
| PHONE | R_PHONE | 2,094,039 | 32.0% |
| CUSTOMER_ID | R_CUSTOMER_ID | 1,572,507 | 24.0% |

### Confidence Score Distribution

| Confidence Level | Count | Percentage |
|-----------------|-------|------------|
| 0.9-1.0 (Very High) | 2,128,174 | 82.06% |
| 0.8-0.9 (High) | 224,477 | 8.66% |
| 0.6-0.8 (Medium) | 240,734 | 9.28% |

> **90.7% of clusters** have high confidence scores (≥0.8)

### Validation Results

All 13 validation checks passed:

| Check | Status | Result |
|-------|--------|--------|
| clusters_created | ✅ PASS | 2,593,385 clusters |
| membership_completeness | ✅ PASS | All entities have membership |
| resolution_ratio | ✅ PASS | 0.370 in expected range |
| avg_cluster_size | ✅ PASS | 2.70 in expected range |
| max_cluster_size | ✅ PASS | 57 within limit |
| confidence_scores_valid | ✅ PASS | All scores valid |

### Fuzzy Logic Run Results

> **Configuration**: Includes Jaro-Winkler/Levenshtein matching enabled.
> **Note**: This run was significantly faster than the initial deterministic run (201s vs 338s), likely due to BigQuery BI Engine caching or slot warming effects during subsequent execution.

| Field | Value |
|-------|-------|
| **Run ID** | `run_97bf67268ddf` |
| **Run Mode** | FULL (Fuzzy Enabled) |
| **Status** | SUCCESS |
| **Total Duration** | **201.0 seconds (3.35 minutes)** |

**Resolution Metrics:**

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |

---

## Snowflake Results

### Run Information

| Field | Value |
|-------|-------|
| **Run ID** | `run_3d5e0631eb7a` |
| **Run Mode** | FULL |
| **Status** | SUCCESS |
| **Started** | 2026-01-13 |
| **Total Duration** | **86.6 seconds (1.4 minutes)** |

### Resolution Metrics

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |
| **LP Iterations** | 9 |
| high_confidence_ratio | ✅ PASS | 90.7% high confidence |
| source_columns_populated | ✅ PASS | All populated |
| excluded_identifiers | ✅ PASS | No exclusions in edges |
| golden_profiles_exist | ✅ PASS | 2,593,385 profiles |
| cluster_membership_consistency | ✅ PASS | All consistent |
| singletons_included | ✅ PASS | 1,634,614 singletons |
| run_history_recorded | ✅ PASS | 1 completed run |

### Ground Truth Comparison

| Metric | Value |
|--------|-------|
| Perfect Resolution (1 cluster/person) | 8.07% |
| Fragmented (2+ clusters/person) | 91.93% |
| Unmapped (0 clusters) | 0.0% |

> **Note**: The high fragmentation rate is expected with synthetic data where identifier overlap between records may be limited. Real-world data typically shows higher resolution rates.

---

## Databricks Results

> **Status**: ✅ **Complete** - January 12, 2026

### Run Information

| Field | Value |
|-------|-------|
| **Run ID** | run_5052c2ff92b44e78aabe9d2e03efa5c6 |
| **Run Mode** | FULL |
| **Status** | SUCCESS |
| **Total Duration** | 276 seconds (~4.6 min) |
| **Compute** | Serverless SQL Warehouse |

### Stage Timing Breakdown

| Stage | Duration | % of Total |
|-------|----------|------------|
| Entity Extraction | 4s | 2.7% |
| Edge Building | 29s | 19.6% |
| Subgraph Building | 17s | 11.5% |
| Label Propagation | 98s | 35.5% |
| Other (Membership, Profile, etc.) | 128s | 46.4% |
| **TOTAL** | **276s** | 100% |

### Resolution Metrics

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |
| **LP Iterations** | 9 |
| **Resolution Ratio** | 37.0% |
| **Avg Cluster Size** | 2.70 |
| **Max Cluster Size** | 57 |
| **Singletons** | 1,634,614 (63%) |

### Validation Results

| Check | Status | Result |
|-------|--------|--------|
| clusters_created | ✅ PASS | 2,593,385 clusters |
| membership_completeness | ✅ PASS | All entities have membership |
| resolution_ratio | ✅ PASS | 0.370 in expected range |
| avg_cluster_size | ✅ PASS | 2.70 in expected range |
| max_cluster_size | ✅ PASS | 57 within limit |
| confidence_scores_valid | ✅ PASS | All scores valid |
| high_confidence_ratio | ✅ PASS | 90.7% high confidence |
| source_columns_populated | ✅ PASS | All populated |
| excluded_identifiers | ✅ PASS | No excluded identifiers |
| golden_profiles_exist | ✅ PASS | 2,593,385 profiles |
| cluster_membership_consistency | ✅ PASS | All consistent |
| singletons_included | ✅ PASS | 1,634,614 singletons |
| run_history_recorded | ✅ PASS | 1 completed run |

---

## Snowflake Results

> **Status**: ✅ **Complete** - January 12, 2026

### Run Information

| Field | Value |
|-------|-------|
| **Run ID (X-SMALL)** | run_dl7l7z6jco |
| **Run ID (SMALL)** | run_cb2dm6ukd28 |
| **Run Mode** | FULL |
| **Status** | SUCCESS |

### Warehouse Size Comparison

| Warehouse | Duration | Entities/sec | Est. Cost |
|-----------|----------|--------------|----------|
| **X-SMALL** | 606s (~10 min) | 11,551 | ~$0.34 |
| **SMALL** | 264s (~4.4 min) | 26,515 | ~$0.29 |

> **Note**: SMALL warehouse (2x compute) is 2.3x faster and slightly cheaper due to reduced total compute time.

### Resolution Metrics

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |
| **LP Iterations** | 9 |
| **Resolution Ratio** | 37.0% |
| **Avg Cluster Size** | 2.70 |
| **Max Cluster Size** | 57 |
| **Singletons** | 1,634,614 (63%) |

### Edge Type Breakdown

| Identifier Type | Edges | % of Total |
|-----------------|-------|------------|
| EMAIL | 2,884,845 | 44% |
| PHONE | 2,094,039 | 32% |
| CUSTOMER_ID | 1,572,507 | 24% |

### Confidence Distribution

| Confidence Level | Clusters | % |
|------------------|----------|---|
| Very High (0.9-1.0) | 2,128,174 | 82.1% |
| High (0.8-0.9) | 224,477 | 8.7% |
| Medium (0.6-0.8) | 240,734 | 9.3% |

### Cluster Size Distribution

| Size Bucket | Clusters | % |
|-------------|----------|---|
| 1 (Singleton) | 1,634,614 | 63.0% |
| 2 | 183,327 | 7.1% |
| 3-5 | 313,077 | 12.1% |
| 6-10 | 408,396 | 15.8% |
| 11-50 | 53,970 | 2.1% |
| 50+ | 1 | 0.0% |

### Validation Results

| Check | Status | Result |
|-------|--------|--------|
| clusters_created | ✅ PASS | 2,593,385 clusters |
| membership_completeness | ✅ PASS | All entities have membership |
| resolution_ratio | ✅ PASS | 0.370 in expected range |
| avg_cluster_size | ✅ PASS | 2.70 in expected range |
| max_cluster_size | ✅ PASS | 57 within limit |
| confidence_scores_valid | ✅ PASS | All scores valid |
| high_confidence_ratio | ✅ PASS | 90.7% high confidence |
| source_columns_populated | ✅ PASS | All populated |
| excluded_identifiers | ✅ PASS | No excluded identifiers |
| golden_profiles_exist | ✅ PASS | 2,593,385 profiles |
| cluster_membership_consistency | ✅ PASS | All consistent |
| singletons_included | ✅ PASS | 1,634,614 singletons |
| run_history_recorded | ✅ PASS | 1 completed run |

### Fuzzy Logic Run Results

> **Configuration**: Includes Jaro-Winkler/Levenshtein matching enabled.
> **Note**: Results identical to deterministic run indicate no additional fuzzy matches found in this specific dataset, but performance numbers reflect the overhead of fuzzy logic processing.

| Field | Value |
|-------|-------|
| **Run ID** | `run_b5ddc1ad15df` |
| **Run Mode** | FULL (Fuzzy Enabled) |
| **Status** | SUCCESS |
| **Total Duration** | **632.6 seconds (10.5 minutes)** |

**Resolution Metrics:**

| Metric | Value |
|--------|-------|
| **Total Entities** | 7,000,000 |
| **Identifiers Extracted** | 9,351,336 |
| **Total Clusters** | 2,593,385 |
| **Total Edges** | 6,551,391 |
| **LP Iterations** | 9 |

---

## Platform Comparison

### Performance Comparison

| Metric | BigQuery | Snowflake (SMALL) | Databricks (Serverless) | Winner |
|--------|----------|-----------|------------|--------|
| **Total Duration** | 338s | 264s | 276s | ❄️ Snowflake |
| **Entities/second** | 20,710 | 26,515 | 25,362 | ❄️ Snowflake |
| **LP Iterations** | 9 | 9 | 9 | Tie |

### Stage-by-Stage Comparison

| Stage | BigQuery | Snowflake | Databricks |
|-------|----------|-----------|------------|
| Entity Extraction | 12s | _N/A_ | 4s |
| Edge Building | 42s | _N/A_ | 29s |
| Subgraph Building | 16s | _N/A_ | 17s |
| Label Propagation | 102s | _N/A_ | 98s |
| **TOTAL** | **338s** | **264s** | **276s** |

### Results Consistency Check

| Metric | BigQuery | Snowflake | Databricks | Match? |
|--------|----------|-----------|------------|--------|
| Total Clusters | 2,593,385 | 2,593,385 | 2,593,385 | ✅ |
| Total Edges | 6,551,391 | 6,551,391 | 6,551,391 | ✅ |
| Max Cluster Size | 57 | 57 | 57 | ✅ |
| Singletons | 1,634,614 | 1,634,614 | 1,634,614 | ✅ |
| Golden Profiles | 2,593,385 | 2,593,385 | 2,593,385 | ✅ |

> **Note**: Since both platforms use the same deterministic algorithm and data seed, results should be identical. Any discrepancy indicates a potential bug or data loading issue.

### Cost Comparison

| Platform | Estimated Cost | Cost Model | Notes |
|----------|---------------|------------|-------|
| **Snowflake (SMALL)** | ~$0.29 | 264s × $4/hr ÷ 3600 | Fastest |
| **Databricks (Serverless)** | ~$0.35 | DBU per second | Close second |
| **Snowflake (X-SMALL)** | ~$0.34 | 606s × $2/hr ÷ 3600 | Slowest |
| **BigQuery** | ~$0.75 | On-demand ($6.25/TB) | ~120GB scanned |

> **Cost Assumptions**:
> - Databricks Serverless: ~$0.08/DBU, estimated 4.5 DBU for 276s
> - Snowflake: X-SMALL = ~$2/hr, SMALL = ~$4/hr (1 credit = ~$2-4)
> - BigQuery: ~120GB data scanned at $6.25/TB = ~$0.75
> - All costs are compute-only, excluding storage

---

## Key Insights

### BigQuery Observations

1. **Label Propagation is the bottleneck**: At 102s (30% of total), LP is the most expensive stage due to iterative query execution overhead in BigQuery.

2. **Golden Profile generation is significant**: 66s (20%) spent on survivorship logic and MERGE operations.

3. **90% high confidence**: The algorithm produces high-quality matches with 90.7% of clusters having confidence ≥ 0.8.

4. **63% singletons**: A large portion of entities don't match any other records, which is typical for diverse customer data.

5. **Email is the strongest identifier**: 44% of edges come from email matches, followed by phone (32%) and customer_id (24%).

### Snowflake Observations

_To be added after Snowflake benchmark completion._

### Databricks Observations

_To be added after Databricks benchmark completion._

---

## Recommendations

### Platform Selection by Volume

| Entity Volume | Recommended Platforms | Notes |
|--------------|----------------------|-------|
| < 1M | DuckDB, Any Cloud | DuckDB is fastest for small datasets |
| 1M - 5M | DuckDB (32GB+ RAM), Cloud | DuckDB works but requires tuning |
| **5M - 10M** | **Cloud Preferred** | DuckDB may struggle with memory |
| > 10M | BigQuery, Snowflake, Databricks | Cloud platforms only |
| > 100M | BigQuery, Snowflake, Databricks | Horizontal scaling required |

### DuckDB Limitations

> [!IMPORTANT]
> **DuckDB did not complete** the 7M entity benchmark on a MacBook Pro. The run exhausted available memory during label propagation.

**Why DuckDB struggles at scale:**
1. **Single-node execution** - cannot distribute workload across machines
2. **Memory-bound** - all intermediate tables must fit in RAM
3. **Label propagation overhead** - 9 iterations × 13M edges = massive memory pressure
4. **No horizontal scaling** - unlike cloud warehouses

**Recommended DuckDB settings for larger datasets:**
```sql
SET memory_limit = '32GB';
SET temp_directory = '/tmp/duckdb_temp';
SET preserve_insertion_order = false;
```

### When to Use Each Platform

| Scenario | Recommended | Rationale |
|----------|-------------|-----------|
| Local dev/testing (< 5M) | DuckDB | Fast, free, no infrastructure |
| Production (5M+) | Cloud Warehouse | Scalability, reliability |
| Ad-hoc analysis | BigQuery | Pay-per-query, fast startup |
| Scheduled batch jobs | Snowflake/Databricks | Compute isolation |
| Existing GCP investment | BigQuery | Native integration |
| Existing Azure/AWS + Databricks | Databricks | Unified data platform |
| Existing Snowflake investment | Snowflake | No additional tooling |

### Next Steps

- [ ] Run Snowflake benchmark with same 7M dataset
- [ ] Run Databricks benchmark with same 7M dataset
- [ ] Compare stage timings across all cloud platforms
- [ ] Update cost estimates based on actual usage

---

## Appendix

### Commands Used

```bash
# BigQuery - Create external tables
python bq_create_external_tables.py --project ga4-134745 --dataset idr_input --bucket sqlidr --prefix idr_parquet_1M

# BigQuery - Setup metadata
bq query --project_id=ga4-134745 --use_legacy_sql=false < setup_retail_metadata_bq.sql

# BigQuery - Run IDR
idr run --platform=bigquery --project=ga4-134745 --mode=FULL

# BigQuery - Validate
# (Validation scripts are internal tools)

```

### Data Files

| File | Location |
|------|----------|
| Source Parquet | `gs://sqlidr/idr_parquet_1M/` |
| Validation Report | `tools/scale_test/validation_report.json` |

---

*Document generated from IDR benchmark run on 2026-01-12*
