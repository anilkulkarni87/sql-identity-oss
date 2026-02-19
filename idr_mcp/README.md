# IDR MCP Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server that provides read-only access to the IDR identity graph. This allows AI agents (like Claude or Cursor) to search for identities, retrieve clusters, and query golden profiles directly from your SQL warehouse.

## Features

- **Cross-Platform**: Supports DuckDB, Snowflake, BigQuery, and Databricks.
- **PII-Safe**: All PII is masked by default — explicit opt-in for full access.
- **Tools Included**:
  - `search_identifier` — Find clusters by name, email, phone, etc.
  - `get_cluster` — Retrieve full cluster details (entities + edges).
  - `get_golden_profile` — Get the best/surviving attributes for an identity.
  - `explain_edge` — View evidence for why two records matched.
  - `config_snapshot` — Retrieve saved configuration snapshots.
  - `run_history` / `latest_run` — Check recent job status.
  - `list_rules` / `list_sources` — View active configuration.

## Quick Start

```bash
# Install with MCP support
pip install 'sql-identity-resolution[mcp,duckdb]'

# Set environment variables
export IDR_PLATFORM=duckdb
export IDR_DATABASE=/path/to/your.duckdb

# Run via CLI
idr mcp
```

## Configuration

The server connects to your data warehouse via environment variables.

### DuckDB
```bash
export IDR_PLATFORM=duckdb
export IDR_DATABASE=/path/to/your.duckdb
```

### BigQuery
```bash
export IDR_PLATFORM=bigquery
export IDR_PROJECT=your-gcp-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

### Snowflake
```bash
export IDR_PLATFORM=snowflake
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=username
export SNOWFLAKE_PASSWORD=password
export SNOWFLAKE_WAREHOUSE=compute_wh
export SNOWFLAKE_DATABASE=idr_db
export SNOWFLAKE_SCHEMA=public
```

### Databricks
```bash
export IDR_PLATFORM=databricks
export DATABRICKS_HOST=hostname
export DATABRICKS_HTTP_PATH=path
export DATABRICKS_TOKEN=token
export DATABRICKS_CATALOG=hive_metastore
```

## Security & Privacy (PII)

By default, all PII (emails, phone numbers, exact match values) is **masked** in tool output.

- **Default (Masked)**: `IDR_PII_ACCESS=masked` (or unset). Returns partial strings like `jo***hn`.
- **Full Access**: `IDR_PII_ACCESS=full` — requires explicit opt-in.

```bash
export IDR_PII_ACCESS=full
```

## Agent Configuration

### Claude Desktop

Add to your `~/.config/claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "idr-graph": {
      "command": "idr",
      "args": ["mcp"],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DATABASE": "/absolute/path/to/retail.duckdb"
      }
    }
  }
}
```

### Cursor

Add to your `.cursor/mcp.json` in the project root:

```json
{
  "mcpServers": {
    "idr-graph": {
      "command": "idr",
      "args": ["mcp"],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DATABASE": "/absolute/path/to/retail.duckdb"
      }
    }
  }
}
```

### VS Code (Copilot)

Add to your `.vscode/settings.json`:

```json
{
  "mcp": {
    "servers": {
      "idr-graph": {
        "command": "idr",
        "args": ["mcp"],
        "env": {
          "IDR_PLATFORM": "duckdb",
          "IDR_DATABASE": "/absolute/path/to/retail.duckdb"
        }
      }
    }
  }
}
```

## Running Directly (without CLI)

```bash
# Using uv
uv run idr_mcp/server.py

# Using python
python -m idr_mcp.server
```
