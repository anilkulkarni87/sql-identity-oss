# IDR Scale Testing & Benchmarks

Synthetic multi-source retail IDR data generation, benchmark suites, and warehouse loaders.

---

## 1. Automated Scale & FastMCP Benchmark

Run an end-to-end benchmark (generates synthetic retail data, runs `FULL` + `INCR` resolution, verifies mathematical graph invariants, and benchmarks all MCP server tools):

```bash
# Default: 250,000 initial entities + 10,000 incremental entities
python tools/scale_test/run_scale_and_mcp_benchmark.py --rows 250000 --incr-rows 10000

# Fast local test (10k rows)
python tools/scale_test/run_scale_and_mcp_benchmark.py --rows 10000 --incr-rows 1000
```

---

## 2. Generate Synthetic Retail Datasets (Mimesis)

Generate realistic omni-channel retail datasets across customer accounts, POS transactions, e-commerce orders, web clickstream, app events, and support tickets:

```bash
pip install mimesis pyarrow pyyaml

python tools/scale_test/generate_retail_idr_mimesis.py \
  --config tools/scale_test/configs/retail_idr_500k.yaml
```

Available config presets in `tools/scale_test/configs/`:
- `retail_idr_5k.yaml` (5,000 customers — instant local smoke tests)
- `retail_idr_500k.yaml` (500,000 customers)
- `retail_idr_1m.yaml` (1,000,000 customers)
- `retail_idr_10m.yaml` (10,000,000 customers)
- `retail_idr_20m.yaml` (20,000,000 customers)
- `retail_idr_30m.yaml` (30,000,000 customers)

Outputs partitioned Parquet files to `tools/scale_test/output/retail_idr_<config_name>/`.

---

## 3. Warehouse Loaders

Load generated Parquet datasets into your target database:

### DuckDB
```bash
python tools/scale_test/load_duckdb.py \
  --input-dir tools/scale_test/output/retail_idr_5k \
  --db retail_idr.duckdb
```

### BigQuery
```bash
python tools/scale_test/load_bigquery.py \
  --input-dir tools/scale_test/output/retail_idr_5k \
  --project my-gcp-project \
  --dataset retail_raw \
  --create-dataset
```

### Snowflake
```bash
python tools/scale_test/load_snowflake.py \
  --input-dir tools/scale_test/output/retail_idr_5k \
  --account <account_id> --user <user> --password <password> \
  --warehouse COMPUTE_WH --database ANALYTICS --schema RAW
```

### Databricks
```bash
python tools/scale_test/load_databricks.py \
  --input-dir tools/scale_test/output/retail_idr_5k \
  --catalog main --schema retail_raw
```
