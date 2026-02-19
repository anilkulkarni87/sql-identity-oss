# dbt Integration Tests

This directory contains a complete integration test for the dbt_idr package
using DuckDB (local, no cloud account required).

## Prerequisites

```bash
pip install dbt-duckdb
```

## Quick Test

```bash
cd dbt_idr/integration_tests
dbt deps
dbt seed
dbt run
dbt test
```

## What It Does

1. **Creates sample source tables** (via seeds):
   - `sample_crm_customers` - 10 CRM records
   - `sample_orders` - 10 order records
   - `sample_pos_transactions` - 10 POS records

2. **Configures IDR metadata** (seeds already configured):
   - Sources point to sample tables
   - Matching rules for EMAIL, PHONE, LOYALTY
   - Survivorship rules for golden profiles

3. **Runs full IDR pipeline**

4. **Validates results** (dbt tests):
   - Clusters are created
   - Confidence scores calculated
   - Expected cluster count matches
