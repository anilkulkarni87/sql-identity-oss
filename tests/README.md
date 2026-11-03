# Tests

Unit and integration tests for sql-identity-resolution using **pytest**.

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run a specific test file
pytest tests/test_config.py -v

# Via Makefile
make test
```

## Test Files

| File | Covers |
|------|--------|
| `test_core.py` | Validation functions, `RunConfig`, `DuckDBAdapter` basics |
| `test_config.py` | `config_to_sql()` round-trip across DuckDB/Snowflake/BigQuery, `validate_config()` |
| `test_profile_builder.py` | `ProfileBuilder` survivorship strategies (`RECENCY`, `PRIORITY`, `FREQUENCY`) |
| `test_api.py` | FastAPI endpoints, Setup Wizard round-trip, Golden Profile, Fuzzy Super-Cluster, RBAC |
| `test_api_and_cli_deep.py` | Connected Dashboard metrics/distribution/rules/alerts, discovery, 4-dialect fuzzy templates, full CLI (`init`, `config`, `run`, `quickstart`) |
| `test_cloud_adapters_unit.py` | Unit tests for `BigQueryAdapter`, `SnowflakeAdapter` (Snowpark + DB-API), `DatabricksAdapter` (Spark + DB-API), and 4-dialect stage SQL verification |
| `test_pipeline_e2e.py` | Full & incremental pipeline runs, candidate expansion, skipped identifier groups, sentinel null filtering, PII hashing |
| `test_pipeline_deep_scenarios.py` | Dry-run diffing (`NEW`/`MOVED`/`UNCHANGED`), strict mode vs fuzzy mode, 4-hop transitive chains across 3 sources, `EXACT`/`LIKE` exclusions, Preflight concurrency lock & stale run recovery |
| `test_scale_and_invariants.py` | 1,000-row synthetic scale benchmark, mathematical graph invariants (partition completeness, cluster conservation, edge transitivity, golden profile bijectivity), FULL/INCR idempotency, `explain_cluster`, and API dry-run breakdown |
| `test_mcp_server.py` | MCP server tools (`search_identifier`, `get_cluster`, `get_golden_profile`, `list_skipped_groups`, `get_metrics_summary`, `explain_cluster`, `dry_run_pipeline`) and PII masking |
| `test_session_store.py` | In-memory and Redis multi-worker session stores (`RedisConnectionSessionStore`) |
| `test_auth.py` | OIDC / JWKS rotation and dev-mode authentication |
| `test_output_stage.py` | Multi-dialect `OutputStage`, `GraphStage._swap_tables`, and `PreflightStage` interval syntax |
| `test_schema_ddl_sync.py` | `SchemaManager.generate_ddl()` and static `sql/ddl/{duckdb,snowflake,bigquery,databricks}.sql` parity |
| `test_cloud_dialects_live.py` | Opt-in live cloud smoke tests for Snowflake, BigQuery, and Databricks |
| `test_stages.py` | `StageContext`, `BaseStage` validation, multi-dialect support |
| `test_cli_smoke.py` | CLI dispatch smoke tests |

## Test Design Principles

1. **Deterministic**: Same inputs always produce same outputs
2. **Isolated**: Pure unit tests with mocks where possible
3. **Fast**: No external databases required (except DuckDB in-memory)
4. **Multi-dialect**: Key tests run across DuckDB, Snowflake, BigQuery, Databricks

## Adding New Tests

1. Create a test file matching `tests/test_*.py`
2. Use pytest conventions (`TestClassName`, `test_method_name`)
3. Use `conftest.py` fixtures for shared setup
4. Run `pytest tests/ -v` to verify
