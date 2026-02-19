# MCP Server

The MCP server exposes read-only tools for agents (clusters, profiles, edges, run history).

Start:
```bash
export IDR_PLATFORM=duckdb
export IDR_DATABASE=./demo.duckdb
idr mcp
```

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

Connection is established from environment variables on startup.
