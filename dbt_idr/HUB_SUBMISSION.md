# Distribution & Registry Submission Guide

This guide details how `dbt_idr` and `idr-mcp` are submitted to their respective global registries.

---

## 1. dbt Package Hub Registration (`hub.getdbt.com`)

### Architecture & Requirement
The official dbt package indexing bot ([`dbt-labs/hubcap`](https://github.com/dbt-labs/hubcap)) scans public GitHub repositories listed in its registry (`hub.json`) every hour.
- **Requirement**: `hubcap` expects `dbt_project.yml` to reside at the **root level** of the repository (not in a subfolder).
- **Package Name**: `dbt_idr`
- **Namespace**: `anilkulkarni87`

### Step A: Create Dedicated Mirror Repository (`anilkulkarni87/dbt-idr`)
1. Create a public GitHub repository named:
   `https://github.com/anilkulkarni87/dbt-idr`
2. Push the contents of the `dbt_idr/` directory to this repository root:
   ```bash
   cd /tmp
   git clone https://github.com/anilkulkarni87/dbt-idr.git
   cp -r /path/to/sql-identity-oss/dbt_idr/* dbt-idr/
   cd dbt-idr
   git add .
   git commit -m "feat: initial dbt-idr release v0.6.1"
   git tag -a v0.6.1 -m "Release v0.6.1"
   git push origin main --tags
   ```

### Step B: Submit Pull Request to `dbt-labs/hubcap`
1. Fork [`https://github.com/dbt-labs/hubcap`](https://github.com/dbt-labs/hubcap).
2. Edit `hub.json` to add your repository:
   ```json
   {
     "anilkulkarni87": [
       "dbt-idr"
     ]
   }
   ```
3. Open a Pull Request on `dbt-labs/hubcap`:
   - **Title**: `Add anilkulkarni87/dbt-idr to package hub`
   - **Description**:
     > Adds `dbt_idr` by `@anilkulkarni87`: in-warehouse deterministic identity resolution and customer 360 golden profile generation for Snowflake, Databricks, BigQuery, and DuckDB.

4. Once merged, `hubcap` automatically generates the registry entries on `hub.getdbt.com`, enabling users worldwide to install via:
   ```yaml
   packages:
     - package: anilkulkarni87/dbt_idr
       version: 0.6.1
   ```

### Immediate Installation (Before Hub Merge)
Users can immediately install `dbt_idr` today directly from GitHub without waiting for the hub bot:
```yaml
packages:
  - git: "https://github.com/anilkulkarni87/sql-identity-oss.git"
    subdirectory: "dbt_idr"
    version: "v0.6.1"
```

---

## 2. Model Context Protocol (MCP) Registries & Distribution

With recent ecosystem shifts (Smithery transitioning to **Arcade** and `mcp.so` becoming paid), the authoritative, free, high-traffic directories for developer & agent adoption are:

### A. Cursor Native Workspace Preset (`.cursor/mcp.json`)
We have pre-configured [`.cursor/mcp.json`](../.cursor/mcp.json) in the repository root. Anyone opening `sql-identity-oss` in Cursor immediately has all 17 identity resolution agent tools available out of the box with zero manual configuration.

```json
{
  "mcpServers": {
    "sql-identity-resolution": {
      "command": "python",
      "args": ["-m", "idr_mcp"],
      "env": {
        "IDR_PLATFORM": "duckdb",
        "IDR_DB_PATH": "retail.duckdb",
        "IDR_READ_PII": "false"
      }
    }
  }
}
```

---

### B. Awesome-MCP-Servers (`punkpeye/awesome-mcp-servers`)
The most heavily starred MCP index on GitHub (40k+ stars). Every MCP client developer and agent platform indexes this list.

1. Fork [`https://github.com/punkpeye/awesome-mcp-servers`](https://github.com/punkpeye/awesome-mcp-servers).
2. Edit `README.md` under the **Data & Analytics** or **Database** section:
   ```markdown
   - [SQL Identity Resolution (idr-mcp)](https://github.com/anilkulkarni87/sql-identity-oss) - In-warehouse deterministic identity resolution, entity graph traversal, and Customer 360 golden profile generation with zero data movement across Snowflake, Databricks, BigQuery, and DuckDB.
   ```
3. Submit a Pull Request.

---

### C. Anthropic Official MCP Servers (`modelcontextprotocol/servers`)
The official reference repository maintained by Anthropic:

1. Fork [`https://github.com/modelcontextprotocol/servers`](https://github.com/modelcontextprotocol/servers).
2. Open a PR referencing `anilkulkarni87/sql-identity-oss` as a community server under `Database & Data Warehousing`.

---

### D. Glama.ai Registry (`glama.ai/mcp/servers`)
Free, community-driven directory indexing MCP servers directly from GitHub:
1. Navigate to [https://glama.ai/mcp/servers](https://glama.ai/mcp/servers).
2. Click **Submit a Server** and paste `https://github.com/anilkulkarni87/sql-identity-oss`.
3. Tags: `Customer 360`, `Database`, `Data Engineering`, `FastMCP`.

---

### E. Arcade (formerly Smithery)
[`smithery.yaml`](../smithery.yaml) remains committed at the repository root and is compatible with Arcade's CLI for `uvx` execution:
```bash
uvx --from sql-identity-resolution[all] idr-mcp
```

