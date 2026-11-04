# Warehouse Sizing & Capacity Planning

SQL Identity Resolution executes graph algorithms and table merges directly inside your data warehouse engine. This guide provides concrete hardware and warehouse sizing recommendations across all 4 supported platforms based on production benchmarks.

---

## 📊 Sizing Matrix by Platform & Record Volume

| Platform | < 100K Records | 1M Records | 10M Records | 50M - 100M+ Records |
| :--- | :--- | :--- | :--- | :--- |
| **DuckDB** | In-Memory / 2GB RAM | 4 Cores, 8GB RAM | 8 Cores, 32GB RAM | 16 Cores, 64GB-128GB RAM (NVMe disk) |
| **Snowflake** | `X-Small` | `X-Small` - `Small` | `Medium` - `Large` | `X-Large` - `2X-Large` |
| **BigQuery** | On-demand (default) | On-demand (default) | 500-1,000 slots | 2,000+ slots (Reservations) |
| **Databricks** | Serverless SQL / Small | 2-4 Workers (Standard) | 4-8 Workers (Standard) | 8-16 Workers (Photon-enabled) |

---

## 🦆 DuckDB (Local / Single Node)

DuckDB handles millions of records on standard developer laptops and single cloud compute instances (e.g., AWS `c6i.4xlarge` or GCP `c2-standard-16`).

### Memory & Storage Recommendations:
* **Memory**: Reserve roughly 1.5GB RAM per million entity records.
* **Storage**: DuckDB uses columnar compression. A database storing 10M customer rows and 30M extracted identifiers typically occupies 3GB - 6GB of disk space.
* **Threads**: DuckDB defaults to utilizing all available CPU cores. For batch CLI workloads, keep default threading enabled.

---

## ❄️ Snowflake

Snowflake's columnar engine excels at join-heavy graph edge generation and recursive label propagation.

### Warehouse Recommendations:
* **Initial Setup / POC (< 1M records)**: `X-Small` warehouse provides ample capacity and minimizes credit consumption.
* **Production Daily Batch (5M - 20M records)**: `Medium` warehouse typically completes full resolution in 4 to 8 minutes.
* **Large Scale (50M - 100M+ records)**: Use `X-Large` or `2X-Large`.
* **Auto-Suspend**: Set auto-suspend to 60 seconds so compute costs cease immediately after pipeline completion:
  ```sql
  ALTER WAREHOUSE idr_wh SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
  ```

---

## 🔍 Google Cloud BigQuery

BigQuery provides serverless execution where compute automatically scales based on query complexity.

### Configuration Best Practices:
* **On-Demand vs Reservations**:
  * For datasets < 10M rows, BigQuery's On-Demand pricing ($6.25 per TB scanned) is cost-effective because query filtering and column pruning keep scanned bytes low.
  * For recurring enterprise production runs (> 50M records), BigQuery Slots (Capacity Reservations) provide predictable pricing and prevent concurrency throttling.
* **Partitioning & Clustering**:
  * Cluster `idr_work.identifiers_work` by `(identifier_type, identifier_hash)`.
  * Cluster `idr_work.edges_work` by `(identifier_type, src_key)`.
  * This reduces slot runtime during join operations by up to 60%.

---

## 🧱 Databricks (Spark & Delta Lake)

Databricks leverages Apache Spark and Photon to distribute graph resolution across multi-node clusters.

### Spark Configuration for Large Datasets (> 10M records):

When running on Databricks clusters or Lakeflow jobs, apply the following Spark configurations:

```python
# Enable Adaptive Query Execution for dynamic shuffle partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Tune shuffle partitions based on cluster worker cores
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Delta Lake optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### Table Properties:
For Delta tables in Databricks Unity Catalog, enable column defaults and auto-compaction:
```sql
ALTER TABLE idr_out.identity_clusters_current
SET TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```
