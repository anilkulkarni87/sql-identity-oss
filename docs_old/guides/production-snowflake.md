# Production Deployment: Snowflake

This guide details the exact steps to deploy SQL Identity Resolution (IDR) to a production Snowflake environment.

---

## Prerequisites

- **Snowflake Account**: Usage of `ACCOUNTADMIN` or a role with `CREATE DATABASE/SCHEMA` privileges.
- **Python Environment**: For running the metadata loader (CI/CD or orchestration server).
- **Source Data**: Read access to the tables you wish to resolve.

---

## Step 1: Schema Setup

You need to create the tables and the stored procedure runner.

### 1.2 Initialize Schema
Run the IDR CLI to initialize the warehouse tables.

```bash
idr config apply --platform snowflake --file production.yaml
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
  - id: crm_customers
    table: RAW.CRM.CUSTOMERS
    entity_key: customer_id
    trust_rank: 1
    identifiers:
      - type: EMAIL
        expr: email_address
    attributes:
      - name: first_name
        expr: fname
      - name: last_name
        expr: lname
```

---

## Step 3: Metadata Loading



Use `idr config apply` to push your configuration to the Snowflake metadata tables.

```bash
./idr config apply --platform snowflake --file production.yaml
```


---

## Step 4: Execution & Scheduling

## Step 4: Execution & Scheduling

The IDR process runs as a client application that issues SQL commands to Snowflake. It does not require a Stored Procedure.

### Option A: Snowflake Tasks (Container)

Run the IDR CLI in a container (Snowpark Container Services) or external orchestrator.

### Option B: dbt / Airflow

You can invoke the CLI from Airflow or Prefect.

```python
# Airflow Example
run_idr = BashOperator(
    task_id='run_idr',
    bash_command='idr run --platform snowflake --mode FULL',
    dag=dag
)
```

### Option C: Local CLI (Dev/Debugging)

You can run IDR from your local machine using the CLI (requires `snowflake-snowpark-python`).
Ensure your `SNOWFLAKE_*` env variables or `connections.toml` specify the target database/schema.

```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
# ... other env vars

./idr run --platform=snowflake --mode=FULL
```

---

## Step 5: Monitoring

Monitor the pipeline using the `idr_out` tables.

**Check Run Status:**
```sql
SELECT run_id, status, duration_seconds, entities_processed, clusters_impacted
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 10;
```

**Check for Warnings:**
```sql
SELECT * FROM idr_out.run_history
WHERE warnings IS NOT NULL
ORDER BY started_at DESC;
```
