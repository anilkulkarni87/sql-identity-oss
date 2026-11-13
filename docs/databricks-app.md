# Deploying SQL-IDR as a Databricks App

SQL-IDR includes first-class support for **Databricks Apps** (`app.yaml` + FastAPI backend + pre-built React SPA + automatic SQL Warehouse OAuth connectivity).

## How It Works

1. **Zero-Config Authentication (`idr_api/dependencies.py`):**
   When `DATABRICKS_APP_NAME` is set by the Databricks Apps runtime, workspace SSO is handled at the platform proxy level (`x-forwarded-email` / `x-forwarded-user` headers) with no separate OIDC setup required.
2. **Automatic SQL Warehouse Connection (`idr_api/main.py`):**
   On startup, if `DATABRICKS_WAREHOUSE_ID` is injected (via `app.yaml` `valueFrom: sql-warehouse`), the API automatically connects using the app's Service Principal OAuth (`databricks.sdk.core.Config()`) and registers a `DatabricksAdapter`.
   Set `IDR_DATABRICKS_CATALOG` in `app.yaml` if you want to pin a default Unity Catalog.
3. **Static UI Serving (`idr_api/static/`):**
   Because Databricks Apps directory uploads skip `dist/` folders by default, the deploy script copies `idr_ui/dist/` into `idr_api/static/`, which FastAPI serves automatically at `/`.

## Quick Deploy

```bash
# 1. Authenticate Databricks CLI
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 2. Build UI, sync static assets, upload, and deploy
chmod +x scripts/deploy_databricks_app.sh
./scripts/deploy_databricks_app.sh sql-idr
```

## Attach the SQL Warehouse Resource

In the Databricks UI (**Compute → Apps → `sql-idr` → Edit → App resources**):
1. Click **+ Add resource** → **SQL warehouse**
2. Select your Serverless SQL Warehouse and grant **Can use**
3. Set the resource key to **`sql-warehouse`** (matches `valueFrom: sql-warehouse` in `app.yaml`)
4. Grant the app's Service Principal `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on your source and `idr_*` schemas in Unity Catalog.

