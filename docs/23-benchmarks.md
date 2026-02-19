# Benchmarks

Performance tests on standard datasets.

## 10 Million Entities

| Platform | Rows | Total Time | Notes |
|----------|------|------------|-------|
| **DuckDB** | 10M | **143s** | M1 Max, Local NVMe |
| **Snowflake** | 10M | **168s** | Standard Warehouse |
| **Databricks** | 10M | **317s** | SQL Warehouse (Serverless) |
| **BigQuery** | 7M* | **383s** | Scaled estimate |

## Throughput
*   **DuckDB**: ~70k entities/sec
*   **Snowflake**: ~60k entities/sec

## Observations
*   **Label Propagation** is the most expensive step (30-50% of time).
*   **Edge Building** scales linearly with `max_group_size` limits.
