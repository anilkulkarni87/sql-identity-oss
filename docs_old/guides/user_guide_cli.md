# User Guide: CLI Mode

This guide explains how to install, configure, and run the Identity Resolution (IDR) engine using the Command Line Interface (CLI). This mode is ideal for automated workflows, production pipelines, and data engineers.

## 1. Installation

Install the package using pip. We recommend installing with `[all]` to support all platforms.

```bash
pip install sql-identity-resolution[all]
```

Verify installation:

```bash
idr version
# Output: sql-identity-resolution v0.5.0
```

## 2. Initialization

Before running any jobs, you must initialize the metadata schemas (`idr_meta`, `idr_work`, `idr_out`).

**Example (DuckDB):**
```bash
idr init --platform duckdb --db retail.duckdb
```

**Example (BigQuery):**
```bash
idr init --platform bigquery --project my-gcp-project
```

**Example (Snowflake):**
```bash
# Assumes env vars SNOWFLAKE_ACCOUNT, etc. are set
idr init --platform snowflake
```

## 3. Configuration

The CLI uses a YAML configuration file to define data sources and rules.

Create a file named `config.yaml`:

```yaml
sources:
  - id: "web_users"
    table: "raw.web_users"
    entity_key: "user_hash"
    attributes:
      - name: "email"
        type: "IDENTIFIER"
        identifier_type: "EMAIL"
      - name: "phone"
        type: "IDENTIFIER"
        identifier_type: "PHONE"
      - name: "first_name"
        type: "ATTRIBUTE"

rules:
  - id: "email_exact"
    match_keys: ["email"]
    priority: 1

survivorship:
  - attribute: "first_name"
    strategy: "RECENCY"
```

Apply the configuration to the database:

```bash
idr config apply --file config.yaml --platform duckdb --db retail.duckdb
```

## 4. Execution

Run the identity resolution pipeline.

**Full Run (Rebuilds Identity Graph):**
```bash
idr run --platform duckdb --db retail.duckdb --mode FULL
```

**Incremental Run (Processes new/changed records only):**
```bash
idr run --platform duckdb --db retail.duckdb --mode INCR
```

### Options
- `--dry-run`: Preview changes without committing.
- `--strict`: Run in deterministic mode (Exact Match only), disabling fuzzy blocking.
- `--max-iters N`: Set maximum Label Propagation iterations (default: 30).

## 5. Understanding Logging

The CLI outputs logs to **stderr** by default.

To enable JSON logging (for Splunk/Datadog):
```bash
export IDR_JSON_LOGS=1
idr run ...
```

**Log Levels:**
- **INFO**: Progress of stages (Blocking, scoring, clustering).
- **WARNING**: Data quality issues (e.g., skipped groups due to size).
- **ERROR**: Pipeline failures.

## 6. Execution & Verification

After the run, verify results in the database:

```sql
-- Check resolved identities
SELECT * FROM idr_out.identity_resolved_membership_current LIMIT 10;

-- Check clusters
SELECT * FROM idr_out.identity_clusters_current WHERE cluster_size > 1;

-- Check run history
SELECT * FROM idr_out.run_history ORDER BY started_at DESC LIMIT 5;
```

## 7. Production Deployment (Airflow)

For production, wrap the CLI command in an orchestrator like Airflow using `BashOperator`.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("idr_pipeline", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:

    run_idr = BashOperator(
        task_id="run_identity_resolution",
        bash_command="""
            export GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
            idr run --platform bigquery --project my-project --mode INCR
        """
    )
```
