# Production Deployment: Databricks

This guide details the exact steps to deploy SQL Identity Resolution (IDR) to a production Databricks environment.

---

## Prerequisites

- **Databricks Workspace**: Azure Databricks, AWS Databricks, or GCP Databricks.
- **Unity Catalog (Recommended)**: For best governance, though Hive Metastore is supported.
- **SQL Warehouse**: Required for running SQL workloads.
- **Python Environment**: For running the CLI.

---

## Step 1: Schema Setup

### 1.2 Initialize Schema
Run the IDR CLI to initialize the Unity Catalog schemas and tables.

```bash
idr config apply --platform databricks \
    --file production.yaml \
    --host "adb-123.1.azuredatabricks.net" \
    --token "dapi..." \
    --http-path "/sql/1.0/..." \
    --catalog "hive_metastore"
```

---

## Step 2: Configuration

Create a `production.yaml` file defining your rules and sources.

**Example `production.yaml`:**
```yaml
rules:
  - id: email_exact
    type: EXACT
    match_keys: [EMAIL]
    priority: 1
    canonicalize: LOWERCASE

sources:
  - id: delta_customers
    table: catalog.schema.customers
    entity_key: id
    identifiers:
      - type: EMAIL
        expr: email
```

---

## Step 3: Metadata Loading

To update metadata (add new rules or sources), simply run `config apply` again.

```bash
# Update configuration in place
idr config apply --platform databricks --file production_v2.yaml
```

---

## Step 4: Execution & Scheduling

The IDR process runs as a client application that interacts with Databricks SQL.

### Option A: Databricks Workflows (Python Wheel)

1.  Package the repo (`python setup.py bdist_wheel`).
2.  Create a Databricks Job using a **Python Wheel** task.
3.  Entry point: `idr_core.cli`.
4.  Parameters: `["run", "--platform=databricks", "--mode=FULL"]`.

### Option B: External Orchestrator (Airflow/Dagster)

Run the CLI from your orchestrator's worker, connecting via the Databricks SQL Connector.

```python
# Airflow BashOperator
run_idr = BashOperator(
    task_id='run_idr',
    bash_command='idr run --platform databricks --mode FULL',
    env={
        'DATABRICKS_HOST': '...',
        'DATABRICKS_TOKEN': '...',
        'DATABRICKS_HTTP_PATH': '...'
    }
)
```

---

## Step 5: Monitoring

Monitor the pipeline using the `idr_out` tables.

**Check Run History:**
```sql
SELECT run_id, status, duration_seconds
FROM idr_out.run_history
ORDER BY started_at DESC;
```
