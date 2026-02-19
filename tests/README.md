# Tests

Unit and integration tests for sql-identity-resolution using **pytest**.

## Running Tests

```bash
# Run all tests
python -m pytest tests/ --ignore=tests/legacy -v

# Run a specific test file
python -m pytest tests/test_config.py -v

# Via Makefile
make test
```

## Test Files

| File | Tests | Covers |
|------|-------|--------|
| `test_core.py` | 63 | Validation functions, `RunConfig`, `DuckDBAdapter` basics |
| `test_config.py` | 27 | `config_to_sql()` round-trip across DuckDB/Snowflake/BigQuery, `validate_config()` |
| `test_profile_builder.py` | 16 | `ProfileBuilder` survivorship strategies (RECENCY, PRIORITY, FREQUENCY) |
| `test_api.py` | 22 | API endpoints via FastAPI TestClient — health, schema, connect, error handling |
| `test_stages.py` | 46 | `StageContext`, `BaseStage` validation, multi-dialect support |
| `test_new_runner.py` | — | Runner integration tests with DuckDB |

## Test Design Principles

1. **Deterministic**: Same inputs always produce same outputs
2. **Isolated**: Pure unit tests with mocks where possible
3. **Fast**: No external databases required (except DuckDB in-memory)
4. **Multi-dialect**: Key tests run across DuckDB, Snowflake, BigQuery, Databricks

## Adding New Tests

1. Create a test file matching `tests/test_*.py`
2. Use pytest conventions (`TestClassName`, `test_method_name`)
3. Use `conftest.py` fixtures for shared setup
4. Run `python -m pytest tests/ --ignore=tests/legacy -v` to verify

## Legacy Tests

Archived tests from the pre-`idr_core` era are in `tests/legacy/`.
See [legacy/README.md](legacy/README.md) for details.
