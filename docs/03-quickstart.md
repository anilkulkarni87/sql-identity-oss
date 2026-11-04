# Quickstart & Onboarding

Get up and running with SQL Identity Resolution in under 2 minutes.

---

## ⚡ 1. The 60-Second Demo (Local DuckDB)

The quickest way to evaluate IDR is through the built-in zero-config quickstart.

```bash
# 1. Install IDR with DuckDB and MCP extras
pip install "sql-identity-resolution[duckdb,api,mcp]"

# 2. Run the quickstart command
idr quickstart
```

### What Happens Behind the Scenes:
1. **Generates Synthetic Retail Data**: Creates 10,000 realistic customer rows across 4 source systems (`web`, `store`, `mobile`, `call_center`) with realistic overlap in email, phone, and loyalty IDs.
2. **Initializes Schema**: Provisions `idr_meta`, `idr_work`, and `idr_out` schemas in a local DuckDB file (`quickstart_demo.duckdb`).
3. **Applies Metadata Rules**: Configures exact matching rules for `EMAIL`, `PHONE`, and `LOYALTY`, sets attribute mappings, and defines Golden Profile survivorship rules.
4. **Executes Identity Resolution**: Runs `FULL` mode resolution with Label Propagation and materializes final identity tables.
5. **Prints Summary**: Outputs duration, entities processed, edges formed, and cluster size distributions.

Customizing the quickstart:
```bash
idr quickstart --rows=50000 --output=my_demo.duckdb --seed=123
```

---

## 🔍 2. Inspecting Results

### Querying with SQL
You can query the output tables directly using the DuckDB CLI or Python:

```bash
duckdb quickstart_demo.duckdb
```

```sql
-- See cluster size distribution
SELECT
    cluster_size,
    COUNT(*) as total_clusters,
    SUM(cluster_size) as total_entities
FROM idr_out.identity_clusters_current
GROUP BY cluster_size
ORDER BY cluster_size;

-- View resolved customer memberships
SELECT
    m.entity_key,
    m.resolved_id,
    c.cluster_size,
    c.confidence_score
FROM idr_out.identity_resolved_membership_current m
JOIN idr_out.identity_clusters_current c ON m.resolved_id = c.resolved_id
LIMIT 10;

-- Inspect Unified Golden Profiles
SELECT * FROM idr_out.golden_profiles LIMIT 5;
```

---

## 🖥️ 3. Launching the Interactive Web UI

IDR ships with a React SPA for visual cluster exploration, run telemetry, and an interactive 6-step Setup Wizard.

### Option A: Local Dev Server
```bash
# Start API backend
idr serve --host 0.0.0.0 --port 8000
```
Open [http://localhost:8000/docs](http://localhost:8000/docs) for the interactive Swagger documentation.

To run the frontend UI alongside the API:
```bash
cd idr_ui
npm install
npm run dev
```
Open [http://localhost:5173](http://localhost:5173).

### Option B: Docker Compose
```bash
docker compose up --build
```
Open [http://localhost:3000](http://localhost:3000) for the UI and [http://localhost:8000/docs](http://localhost:8000/docs) for the API.

---

## 🤖 4. AI Agent Integration via MCP

IDR includes a native **FastMCP** server providing 17 agent tools for Cursor, Claude Desktop, and LLM orchestration frameworks.

### Running the MCP Server
```bash
export IDR_PLATFORM=duckdb
export IDR_DATABASE=quickstart_demo.duckdb
export IDR_PII_ACCESS=masked   # or "full"

idr mcp
```

### Adding to Claude Desktop / Cursor (`claude_desktop_config.json`)
```json
{
  "mcpServers": {
    "sql-identity-resolution": {
      "command": "idr",
      "args": ["mcp"],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DATABASE": "/absolute/path/to/quickstart_demo.duckdb",
        "IDR_PII_ACCESS": "masked"
      }
    }
  }
}
```

Now ask your agent:
> *"Explain cluster for customer email alice@example.com, and show me the confidence score, member records, and Golden Profile."*

---

## 🚀 5. Next Steps

- **Configure Your Data**: See [Configuration Guide](04-configuration.md) to define sources, rules, exclusions, and survivorship.
- **Deploy to Your Warehouse**: See [Platforms & Connectivity](06-platforms.md) for Snowflake, BigQuery, and Databricks.
- **Run at Scale**: Check [Scale Testing Guide](13-scale-testing.md) to benchmark multi-million-row datasets.
