# Cluster Sizing

Recommended resources for different data scales.

## DuckDB (Local)
| Rows | RAM | Storage |
|------|-----|---------|
| < 1M | 4GB | 1GB |
| 10M | 16GB | 5GB |
| 50M+ | 64GB | 20GB |

## Snowflake
| Rows | Warehouse Size |
|------|----------------|
| < 1M | X-Small |
| 10M | Medium |
| 100M | 2X-Large |

## Databricks
| Rows | Cluster |
|------|---------|
| < 10M | Standard (4 workers) |
| 100M+ | Photon (16+ workers) |

## Spark Configs
For large jobs (>10M rows):
```python
spark.conf.set("spark.sql.shuffle.partitions", "2000")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
