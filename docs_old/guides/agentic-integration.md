# Agentic Integration (MCP)

SQL Identity Resolution includes a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server. This allows AI agents (like Claude Desktop, Cursor, or custom agents) to "talk" to your identity graph.

---

## What is it for?

The MCP server acts as a **Read Layer** on top of your unified data. It runs **after** the unification process (`idr run`) is complete.

**Top Use Cases:**
1.  **"Who is this customer?"**: Give the agent an email, and it returns the full Golden Profile and all linked IDs.
2.  **"Why do these link?"**: The agent can call `explain_edge` to see exactly which rule linked two records.
3.  **"Show me the history"**: Ask the agent about the latest run status or data quality warnings.

---

## Installation

The server is included in the `idr_mcp` package.

```bash
# Install dependencies
pip install mcp
```

## Running with Claude Desktop

To use IDR with Claude Desktop, add this to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "identity-resolution": {
      "command": "python",
      "args": [
        "/absolute/path/to/sql-identity-resolution/idr_mcp/server.py"
      ],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DATABASE": "/absolute/path/to/local.duckdb",
        "IDR_PII_ACCESS": "masked"
      }
    }
  }
}
```

### Configuration Examples

<details>
<summary><b>BigQuery</b></summary>

```json
{
  "mcpServers": {
    "identity-resolution": {
      "command": "python",
      "args": ["/path/to/idr_mcp/server.py"],
      "env": {
        "IDR_PLATFORM": "bigquery",
        "IDR_PROJECT": "your-gcp-project-id",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/key.json"
      }
    }
  }
}
```
</details>

<details>
<summary><b>Snowflake</b></summary>

```json
{
  "mcpServers": {
    "identity-resolution": {
      "command": "python",
      "args": ["/path/to/idr_mcp/server.py"],
      "env": {
        "IDR_PLATFORM": "snowflake",
        "SNOWFLAKE_ACCOUNT": "xy12345.us-east-1",
        "SNOWFLAKE_USER": "IDR_USER",
        "SNOWFLAKE_PASSWORD": "your-password",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "IDR_PROD",
        "SNOWFLAKE_SCHEMA": "PUBLIC"
      }
    }
  }
}
```
</details>

<details>
<summary><b>Databricks</b></summary>

```json
{
  "mcpServers": {
    "identity-resolution": {
      "command": "python",
      "args": ["/path/to/idr_mcp/server.py"],
      "env": {
        "IDR_PLATFORM": "databricks",
        "DATABRICKS_HOST": "adb-123456789.1.azuredatabricks.net",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abcdef123",
        "DATABRICKS_TOKEN": "dapi...",
        "DATABRICKS_CATALOG": "hive_metastore"
      }
    }
  }
}
```
</details>

---

## Available Tools

Once connected, Claude has access to these tools:

| Tool | Description |
|------|-------------|
| `search_identifier` | Find clusters by partial email, phone, or name. |
| `get_golden_profile` | Retrieve the consolidated "Best Record" for a person. |
| `get_cluster` | Get raw cluster members and graph structure. |
| `explain_edge` | Debug *why* Entity A is linked to Entity B. |
| `run_history` | Check the status of recent ETL jobs. |

## Example Conversation

**User**: "Can you check if we have a customer named 'John Doe'?"

**Claude** (calls `search_identifier`):
> I found a cluster `R-12345` matching "John Doe".

**User**: "What's his phone number?"

**Claude** (calls `get_golden_profile`):
> According to the Golden Profile, his primary phone is **555-0199**.

**User**: "Why is he linked to 'j.doe@test.com'?"

**Claude** (calls `explain_edge`):
> They are linked by an **Exact Match** on the `customer_id` "C-987" from the Support System.
