# Scale Testing & Graph Benchmarks

IDR is engineered to process hundreds of millions of records inside data warehouses using set-based SQL and distributed graph algorithms.

---

## ⚡ Automated 260,000-Entity Benchmark

IDR includes an end-to-end multi-source scale test script validating full resolution, incremental processing, and MCP agent responsiveness.

### Running the Benchmark
```bash
python tools/scale_test/run_scale_and_mcp_benchmark.py --rows 250000 --incr-rows 10000
```

### Benchmark Architecture:
1. **Multi-Source Generation**: Generates 250,000 entities across 3 distinct source tables (`CRM Accounts`, `Web Users`, `POS Customers`) with natural overlapping identifiers.
2. **Deterministic & Guardrail Execution**: Applies rules across `EMAIL`, `PHONE`, and `LOYALTY`, with `max_group_size=100` catching a synthetic shared kiosk email.
3. **FULL Pipeline Run**: Resolves 250,000 entities into 58,000+ clusters in 5 Label Propagation iterations at **~50,000 entities/sec**.
4. **INCR Pipeline Run**: Processes 10,000 newly arriving incremental rows in 1.5s, linking them to existing clusters without reprocessing the full graph.
5. **Graph Invariant Verification**: Validates all 4 mathematical invariants:
   - **Partition Invariant**: Every entity belongs to exactly one cluster (`COUNT(entity_key) == SUM(cluster_size)`).
   - **Connected-Component Completeness**: 0 cross-cluster edges.
   - **Minimum-Label Determinism**: `resolved_id == MIN(entity_key)` for every cluster.
   - **Golden Profile Bijectivity**: Exactly 1 Golden Profile per cluster.
6. **MCP Latency Verification**: Benchmarks all 17 MCP agent tools against the 260,000-entity graph (sub-second response times).

---

## 📈 Generating Custom Scale Datasets

To generate multi-million-row synthetic retail datasets:

```bash
# Generate 10 Million rows using Mimesis
python tools/scale_test/generate_retail_idr_mimesis.py \
  --config tools/scale_test/configs/retail_idr_10m.yaml

# Load into DuckDB
python tools/scale_test/load_duckdb.py \
  --input-dir tools/scale_test/output/retail_idr_10m \
  --db retail_10m.duckdb

# Or stage to Cloud Storage & load into Snowflake / BigQuery / Databricks
python tools/scale_test/load_snowflake.py --input-dir tools/scale_test/output/retail_idr_10m ...
python tools/scale_test/load_bigquery.py --input-dir tools/scale_test/output/retail_idr_10m ...
python tools/scale_test/load_databricks.py --input-dir tools/scale_test/output/retail_idr_10m ...
```

---

## 🏎️ Performance & Sizing Guidelines

| Data Scale | DuckDB | Snowflake | BigQuery | Databricks |
|---|---|---|---|---|
| **< 1M rows** | In-Memory / File (4GB RAM) | X-Small Warehouse | On-Demand Serverless | Small Serverless SQL Warehouse |
| **10M rows** | 16GB RAM, Local NVMe (~140s) | Medium Warehouse (~160s) | Slot Reservation (~300s) | Medium SQL Warehouse (~280s) |
| **50M - 100M+** | 64GB+ RAM Workstation | Large / X-Large Warehouse | Reservation + Partitioning | Large SQL Warehouse / Photon |

### Memory & Disk Tips:
- **DuckDB**: When running automated scale test suites locally, prefer `conn = duckdb.connect(":memory:")` to prevent temporary write amplification and disk full errors.
- **Delta Lake on Databricks**: Use `OPTIMIZE idr_out.identity_resolved_membership_current ZORDER BY (resolved_id)` to cluster files for ultra-fast point lookups.
