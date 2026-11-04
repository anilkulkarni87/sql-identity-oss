# Model Context Protocol (MCP) Server

The IDR MCP Server connects your identity graph directly to AI assistants such as **Claude Desktop**, **Cursor**, or custom LLM agent workflows using the open [Model Context Protocol (MCP)](https://modelcontextprotocol.io/).

Agents can inspect clusters, trace edge evidence, query Golden Profiles, review run history, and dry-run pipeline executions using natural language.

---

## 🚀 Starting the MCP Server

```bash
# Environment Configuration
export IDR_PLATFORM=duckdb              # duckdb | snowflake | bigquery | databricks
export IDR_DATABASE=./demo.duckdb        # or Snowflake/BigQuery/Databricks credentials
export IDR_PII_ACCESS=masked            # "masked" (default) or "full"

# Start in stdio mode (for Claude Desktop / Cursor)
idr mcp
```

---

## 🔒 PII Masking & Privacy Controls

By default (`IDR_PII_ACCESS=masked` or unset), the MCP server automatically redacts sensitive attributes (emails, phone numbers, names):

- `john.doe@example.com` becomes `jo***om`
- `555-0199` becomes `55***99`

To grant agents full unmasked access for internal audited workflows:
```bash
export IDR_PII_ACCESS=full
```

---

## 🛠️ Complete Tool Catalog (17 Tools)

### 1. Agentic Identity Intelligence & Customer 360 (New in v0.6.0)

* **`resolve_customer_360(identifier_value: str, identifier_type: str = None)`**
  * **Primary context-grounding tool for AI agents and RAG pipelines.** Given any fragmented customer identifier (email, phone, loyalty ID), resolves the person's canonical identity in a single call. Returns their unified **Golden Profile** attributes, all linked source accounts (e.g. CRM, Shopify, Zendesk), complete cross-channel identifier graph, and cluster confidence score.
* **`discover_and_suggest_rules(table_fqn: str = None, sample_size: int = 500)`**
  * **Autonomous data engineering tool.** Inspects warehouse schemas, samples column data, semantically detects PII candidates (`email`, `phone`, `account_id`, `loyalty_id`, `device_id`), and automatically generates a production-ready declarative IDR YAML configuration with exact matching rules, `max_group_size` safeguards, and survivorship policies.
* **`simulate_rule_impact(rule: Dict[str, Any], dialect: str = None)`**
  * **Safe rule tuning & risk analysis.** Takes a candidate matching rule and tests it against existing warehouse records without mutating production tables. Evaluates simulated anchor edge volume, predicted cluster merges, and flags supernode risks before deployment.

### 2. Cluster Investigation & Explanation
* **`explain_cluster(resolved_id: str)`**
  * Generates a comprehensive 360° explanation for a resolved cluster: cluster metadata (size, confidence score, primary reason, fuzzy `super_cluster_id`), member entities, connecting edges with rules, and the unified Golden Profile in a single call.
* **`get_cluster(resolved_id: str, include_edges: bool = False, include_entities: bool = False)`**
  * Retrieves raw cluster metadata with optional inclusion of member entities and connecting edges.
* **`get_golden_profile(resolved_id: str)`**
  * Retrieves the consolidated 360° customer profile for a cluster resulting from survivorship rules.
* **`search_identifier(value: str, identifier_type: str = None, limit: int = 10)`**
  * Case-insensitive search for clusters matching an identifier (email, phone, loyalty ID). Finds both multi-entity clusters and singleton entities.

### 3. Edge & Evidence Reasoning
* **`explain_edge(entity_key_a: str, entity_key_b: str)`**
  * Discovers the exact evidence trail explaining why two entities are linked. Detects both **direct connections** (shared identifier and rule) and **transitive connections** (connected via intermediate entities in the same cluster).
* **`list_edges_for_cluster(resolved_id: str, limit: int = 50)`**
  * Lists all internal graph edges connecting members within a resolved cluster.

### 4. Operational & Health Metrics
* **`get_metrics_summary()`**
  * High-level identity resolution graph telemetry: total clusters, total entities, total edges, and average cluster confidence score.
* **`list_skipped_groups(limit: int = 20)`**
  * Audits high-cardinality identifier groups that were skipped by `max_group_size` guardrails to prevent hub flattening.
* **`latest_run()`**
  * Returns status, duration, and metrics for the most recent pipeline run.
* **`run_history(limit: int = 10)`**
  * Retrieves recent execution history across `FULL`, `INCR`, and `DRY_RUN` runs.

### 5. Configuration & Dry-Run Planning
* **`dry_run_pipeline(run_mode: str = "FULL", max_iters: int = 30)`**
  * Executes a dry-run analysis to preview proposed entity additions and cluster moves without mutating production tables.
* **`list_rules()`**
  * Lists active deterministic matching rules and their priorities.
* **`list_sources()`**
  * Lists active source tables, entity keys, and watermark columns.
* **`config_snapshot(config_hash: str = None)`**
  * Retrieves the saved configuration JSON snapshot for historical auditing and reproducibility.

---

## 💻 1-Click Agent Configuration

### Claude Desktop

Add the server to your `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "sql-identity-resolution": {
      "command": "idr",
      "args": ["mcp"],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DATABASE": "/absolute/path/to/demo.duckdb",
        "IDR_PII_ACCESS": "masked"
      }
    }
  }
}
```

### Cursor

Add to your `.cursor/mcp.json` in your workspace root:

```json
{
  "mcpServers": {
    "idr-graph": {
      "command": "idr",
      "args": ["mcp"],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DATABASE": "/absolute/path/to/demo.duckdb",
        "IDR_PII_ACCESS": "masked"
      }
    }
  }
}
```
