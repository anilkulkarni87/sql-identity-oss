# Scale Testing

Validate performance with large synthetic datasets.

## Data Generation
Use the `generate_global_retail_idr.py` script to create realistic test data at scale.

```bash
# Generate 1 Million rows
python tools/scale_test/generate_global_retail_idr.py \
  --rows=1000000 \
  --output=data/1m_test
```

## Running the Benchmark

1.  **Load Data**: Use the platform-specific loader.
    ```bash
    python tools/scale_test/load_duckdb.py --db=scale.duckdb --input=data/1m_test
    ```

2.  **Initialize**:
    ```bash
    idr init --platform duckdb --db scale.duckdb --reset
    ```

3.  **Run IDR**:
    ```bash
    idr run --platform duckdb --db scale.duckdb --mode FULL
    ```

## Sizing Reference
See `docs/cluster_sizing.md` (to be migrated) for expected resource usage.
