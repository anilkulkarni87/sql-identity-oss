# Legacy Tests (Archived)

These test files predate the unified `idr_core` package and pytest-based test suite.
They are kept for reference but are **not executed** by CI or `make test`.

| File | Description |
|------|-------------|
| `run_tests_duckdb.py` | Standalone DuckDB integration tests with an inline IDR pipeline reimplementation |
| `run_tests.py` | Databricks notebook test runner (requires `dbutils` / `spark` globals) |
| `base_test.py` | Abstract base class for cross-platform integration tests |
| `bigquery/test_integration.py` | BigQuery integration tests (requires GCP credentials) |
| `snowflake/test_integration.py` | Snowflake integration tests (requires Snowflake credentials) |

## Current Test Suite

Use `pytest` from the repo root:

```bash
python -m pytest tests/ --ignore=tests/legacy -v
```
