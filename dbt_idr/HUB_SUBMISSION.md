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

## 2. Model Context Protocol (MCP) Registries

### A. Smithery.ai Registration
We have committed and pushed [`smithery.yaml`](../smithery.yaml) to the repository root.

1. Navigate to [https://smithery.ai](https://smithery.ai).
2. Click **"Publish a Server"** (or connect your GitHub account).
3. Paste the repository URL:
   `https://github.com/anilkulkarni87/sql-identity-oss`
4. Set the Tags:
   - `Customer 360`
   - `Data Engineering`
   - `Database Tools`
   - `Identity Resolution`
5. Smithery automatically parses `smithery.yaml`, validates the 17 FastMCP tools, and enables 1-click installation:
   ```bash
   npx -y @smithery/cli install sql-identity-resolution --client claude
   ```

---

### B. mcp.so & Cursor Directory Registration

1. Navigate to [https://mcp.so/submit](https://mcp.so/submit) or [https://cursor.directory](https://cursor.directory).
2. Submit the server metadata:
   - **Name**: `SQL Identity Resolution (idr-mcp)`
   - **Repository**: `https://github.com/anilkulkarni87/sql-identity-oss`
   - **Category**: `Data Engineering & Analytics` / `Customer 360`
   - **Tags**: `Customer 360`, `Data Engineering`, `Database Tools`, `DuckDB`, `Snowflake`, `Databricks`, `BigQuery`
   - **Install Command**:
     ```json
     {
       "mcpServers": {
         "sql-identity-resolution": {
           "command": "uvx",
           "args": ["--from", "sql-identity-resolution[all]", "idr-mcp"],
           "env": {
             "IDR_PLATFORM": "duckdb",
             "IDR_DB_PATH": "retail.duckdb",
             "IDR_READ_PII": "false"
           }
         }
       }
     }
     ```
