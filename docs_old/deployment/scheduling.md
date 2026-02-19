# Scheduling

Set up automated, recurring IDR runs on each platform using the unified CLI.

---

## 1. Platform-Native Orchestration (Recommended)

IDR is designed to run using your data warehouse's native scheduling tools or standard orchestrators.

| Platform | Scheduler | Method |
|----------|-----------|--------|
| **All** | Airflow / Dagster | `idr run --platform=...` via Docker/BashOperator |
| **DuckDB** | Crontab | `idr run --platform=duckdb ...` |
| **BigQuery** | Cloud Scheduler | Cloud Run invoking `idr run` |
| **Snowflake** | Snowflake Tasks | Container / External Orchestrator |
| **Databricks** | Workflows | Job using `idr` CLI or Wheel |

---

## 2. General Query / Crontab

For local deployments or simple VMs, use `cron`.

```bash
# Edit crontab
crontab -e

# Run every hour
30 * * * * cd /path/to/repo && ./idr run --platform=duckdb --db=idr.duckdb --mode=INCR >> /var/log/idr.log 2>&1
```

---

## 3. Airflow

```python
# dags/idr_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'idr_hourly',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Optional: Dry Run
    dry_run = BashOperator(
        task_id='dry_run',
        bash_command='./idr run --platform=bigquery --project=my-proj --mode=INCR --dry-run',
    )

    # Official Run
    live_run = BashOperator(
        task_id='live_run',
        bash_command='./idr run --platform=bigquery --project=my-proj --mode=INCR',
    )

    dry_run >> live_run
```

---

## 4. Best Practices

*   **Start with FULL, then INCR**: Always run `FULL` mode once manually to initialize. Schedule `INCR` for ongoing updates.
*   **Weekly FULL Re-sync**: Consider scheduling a `FULL` run once a week (e.g., Sunday 2 AM) to correct any drift.
*   **Monitoring**: Ensure you are alerting on failures (see [Observability](../guides/metrics-monitoring.md)).
