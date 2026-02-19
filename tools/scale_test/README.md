# IDR Scale Testing

Synthetic retail IDR data generation + loading helpers.

## Generate Data (Mimesis)

```bash
pip install mimesis pyarrow

python tools/scale_test/generate_retail_idr_mimesis.py \
  --config tools/scale_test/configs/retail_idr_500k.yaml
```

Outputs Parquet files to:
`tools/scale_test/output/retail_idr_<config_name>/`

## Loaders

```bash
# DuckDB
python tools/scale_test/load_duckdb.py \
  --input-dir tools/scale_test/output/retail_idr_500k \
  --db retail_idr.duckdb

# BigQuery (local Parquet -> table)
python tools/scale_test/load_bigquery.py \
  --input-dir tools/scale_test/output/retail_idr_500k \
  --project your-project \
  --dataset idr_test \
  --create-dataset

# Snowflake (PUT/COPY)
python tools/scale_test/load_snowflake.py \
  --input-dir tools/scale_test/output/retail_idr_500k \
  --account <acct> --user <user> --password <pwd> \
  --warehouse <wh> --database <db> --schema <schema>

# Databricks (run inside Databricks or with databricks-connect)
python tools/scale_test/load_databricks.py \
  --input-dir tools/scale_test/output/retail_idr_500k \
  --schema idr_test
```

Notes:
- For very large loads, prefer staging Parquet files in cloud storage (GCS/S3/ADLS) and using native load utilities.
- All tables include `updated_at`. Orders and POS include `product_id`.
