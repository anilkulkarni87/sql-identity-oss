# Installation & Setup Guide

Install **SQL Identity Resolution (`sql-identity-resolution`)** using `pip` extras tailored to your target data warehouse and operational environment.

---

## 🗺️ Deployment Paths

<div class="grid cards" markdown>

-   **🐍 Python Package & CLI**
    ---
    Install via `pip` with warehouse-specific adapters for pipelines, jobs, and local exploration.
    
    [:octicons-arrow-right-24: Pip Installation](#-pip-installation-by-warehouse)

-   **🐳 Local Docker Compose**
    ---
    Spin up the React Web UI, FastAPI backend, and embedded DuckDB with a single command.
    
    [:octicons-arrow-right-24: Docker Setup](#-docker-compose-environments)

-   **🤖 AI Agent MCP Server**
    ---
    Connect Claude Desktop, Cursor, or custom LLM agents directly to your identity graph.
    
    [:octicons-arrow-right-24: MCP Setup](#-mcp-server-for-ai-assistants)

-   **🏭 Production & Enterprise**
    ---
    Deploy pinned Docker images with Prometheus observability, Redis caching, or SSO/OIDC.
    
    [:octicons-arrow-right-24: Production Config](#-docker-compose-environments)

</div>

---

## 📦 Pip Installation by Warehouse

Select your target warehouse platform below to install the minimal required dependencies:

=== "DuckDB (Local / Dev / CI)"
    ```bash
    pip install "sql-identity-resolution[duckdb]"
    ```

    * Zero cloud dependencies. Ideal for unit tests, local development, and embedded workloads.

=== "Snowflake"
    ```bash
    pip install "sql-identity-resolution[snowflake]"
    ```

    * Includes `snowflake-connector-python` with native query execution.

=== "Google Cloud BigQuery"
    ```bash
    pip install "sql-identity-resolution[bigquery]"
    ```

    * Includes `google-cloud-bigquery` for serverless slot execution and dataset management.

=== "Databricks"
    ```bash
    pip install "sql-identity-resolution[databricks]"
    ```

    * Includes `databricks-sql-connector` for Serverless SQL Warehouses and Delta Lake on Unity Catalog.

=== "All Warehouses"
    ```bash
    pip install "sql-identity-resolution[all]"
    ```

    * Installs all cloud connectors, the FastAPI backend, and the FastMCP agent server.

---

## 🐳 Docker Compose Environments

=== "Local Development (UI + API)"
    Launches the React Web UI on port `5173` and the FastAPI service on port `8000`:
    ```bash
    docker compose up --build
    ```

    * **Web UI**: `http://localhost:5173`
    * **Swagger API Docs**: `http://localhost:8000/docs`

=== "Pinned Production Images"
    Deploys deterministic, reproducible images pinned to immutable digest hashes:
    ```bash
    cp .env.example .env
    docker compose -f docker-compose.prod.yml --env-file .env up -d
    ```

=== "Enterprise (SSO + Observability)"
    Includes OIDC authentication, Redis session persistence, and Prometheus metrics:
    ```bash
    docker compose -f docker-compose.enterprise.yml up -d --build
    ```

---

## 🤖 MCP Server for AI Assistants

To enable AI agents (Claude Desktop, Cursor) to interact with identity resolution:

```bash
pip install "sql-identity-resolution[mcp]"
```

Start the MCP server in stdio mode:
```bash
idr mcp
```

---

## 🔍 Verification

Verify that the CLI is installed and check the engine version:

```bash
idr version
```

Expected output:
```text
sql-identity-resolution v0.6.0 (engine: deterministic-v2, dialect: duckdb)
```

For platform sizing, compatibility matrices, and hardware recommendations, see [Platforms Overview](06-platforms.md) and [Distribution & Deployment](27-distribution.md).
