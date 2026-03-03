# MCP Server

The MCP server exposes read-only tools for agents (clusters, profiles, edges, run history).

## Prerequisites

- Python 3.11+
- IDR data source configured (DuckDB or warehouse adapter)
- Environment variables for selected platform connection

DuckDB example:
```bash
export IDR_PLATFORM=duckdb
export IDR_DATABASE=./demo.duckdb
```

## Start Server

Start:
```bash
idr mcp
```

Secrets:
- Supports `*_FILE` secret loading for sensitive credentials (for example `SNOWFLAKE_PASSWORD_FILE`, `DATABRICKS_TOKEN_FILE`).

PII masking:
- Default is masked
- Set `IDR_PII_ACCESS=full` for unmasked values

Key tools:
- `get_cluster(resolved_id, include_edges, include_entities)`
- `get_golden_profile(resolved_id)`
- `search_identifier(value, identifier_type, limit)`
- `list_edges_for_cluster(resolved_id)`
- `explain_edge(entity_key_a, entity_key_b)`
- `run_history(limit)`
- `latest_run()`
- `config_snapshot(config_hash)`
- `list_rules()`
- `list_sources()`

Error envelope:
- Tool failures return `{ "error": { "schema": "mcp_error_v1", "code", "message", "retryable", "context" } }`.
- Supported error codes: `MCP_NOT_CONNECTED`, `MCP_NOT_FOUND`, `MCP_INVALID_ARGUMENT`, `MCP_QUERY_FAILED`.
- Messages are safe for operators and do not include raw exception traces.

Connection is established from environment variables on startup.

## Verification Commands

From repo root:

```bash
python -m pip install -r requirements/ci.lock
python -m pytest tests/test_mcp_contract.py tests/test_mcp_secrets.py tests/test_mcp_errors.py -q
```

Expected outcome:
- all MCP tests pass
- contract shape is stable for profile/cluster/history/config tools
- secrets behavior (`*_FILE`) passes on covered connectors
- error envelopes remain deterministic and safe

## Troubleshooting

- General operator issues: `14-troubleshooting.md`
- Auth and endpoint context: `10-api.md`
- Golden path and CI mapping: `33-golden-paths.md`
- UI auth and MCP operator runbook (`401`/`403`/`MCP_NOT_CONNECTED`): `38-ui-auth-mcp-operator-runbook.md`
