"""
Airflow DAG for SQL Identity Resolution

This DAG runs the IDR pipeline on a schedule.
Customize for your environment.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Configuration
IDR_HOME = "/opt/idr"  # Path to sql-identity-resolution repo
DB_PATH = "/data/idr.duckdb"  # Path to DuckDB database
ALERT_EMAIL = "alerts@company.com"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def check_dry_run_results(**context):
    """Check dry run results and decide whether to proceed."""
    import duckdb

    conn = duckdb.connect(DB_PATH, read_only=True)

    # Get latest dry run
    result = conn.execute("""
        SELECT run_id, moved_entities, largest_proposed_cluster
        FROM idr_out.dry_run_summary
        ORDER BY created_at DESC
        LIMIT 1
    """).fetchone()

    if not result:
        return "skip_live_run"

    run_id, moved, largest = result

    # Thresholds - customize these
    MAX_MOVED = 10000
    MAX_CLUSTER = 50000

    if moved > MAX_MOVED:
        print(f"Too many moved entities: {moved} > {MAX_MOVED}")
        return "skip_live_run"

    if largest > MAX_CLUSTER:
        print(f"Largest cluster too big: {largest} > {MAX_CLUSTER}")
        return "skip_live_run"

    return "live_run"


with DAG(
    "idr_hourly",
    default_args=default_args,
    description="Hourly Identity Resolution Pipeline",
    schedule_interval="0 * * * *",  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["idr", "identity-resolution"],
) as dag:
    # Task 1: Dry run
    dry_run = BashOperator(
        task_id="dry_run",
        bash_command=f"""
            idr run \\
                --platform=duckdb \\
                --db={DB_PATH} \\
                --mode=INCR \\
                --dry-run
        """,
    )

    # Task 2: Check dry run results
    check_results = BranchPythonOperator(
        task_id="check_dry_run_results",
        python_callable=check_dry_run_results,
    )

    # Task 3a: Live run (if dry run looks good)
    live_run = BashOperator(
        task_id="live_run",
        bash_command=f"""
            idr run \\
                --platform=duckdb \\
                --db={DB_PATH} \\
                --mode=INCR
        """,
    )

    # Task 3b: Skip (if dry run looks bad)
    skip_live_run = BashOperator(
        task_id="skip_live_run",
        bash_command='echo "Skipping live run due to dry run thresholds"',
    )

    # Task 4: Export metrics
    # Note: metrics_exporter logic should also be moved to CLI eventually
    # For now assuming legacy tools still exist or replacing with generic check
    export_metrics = BashOperator(
        task_id="export_metrics",
        bash_command="""
            # Example metric check (custom script or SQL check)
            echo "Metrics export placeholder"
        """,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Task 5: Generate dashboard
    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command="""
             # Dashboard is now unified in the UI/API
             # Could trigger a report build here if needed
             echo "Dashboard generation placeholder"
        """,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Dependencies
    dry_run >> check_results
    check_results >> [live_run, skip_live_run]
    [live_run, skip_live_run] >> export_metrics >> generate_dashboard


# Second DAG for weekly full run
with DAG(
    "idr_weekly_full",
    default_args=default_args,
    description="Weekly Full Identity Resolution",
    schedule_interval="0 2 * * 0",  # Sunday 2am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["idr", "identity-resolution"],
) as weekly_dag:
    full_dry_run = BashOperator(
        task_id="full_dry_run",
        bash_command=f"""
            idr run \\
                --platform=duckdb \\
                --db={DB_PATH} \\
                --mode=FULL \\
                --dry-run
        """,
    )

    full_live_run = BashOperator(
        task_id="full_live_run",
        bash_command=f"""
            idr run \\
                --platform=duckdb \\
                --db={DB_PATH} \\
                --mode=FULL
        """,
    )

    full_dry_run >> full_live_run
