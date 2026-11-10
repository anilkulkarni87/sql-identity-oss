"""
Airflow DAG for SQL Identity Resolution (idr)

Automates scheduled execution of the IDR pipeline with:
- Pre-execution dry-run threshold validation (safety gate)
- Hourly incremental watermark resolution (INCR)
- Weekly full graph refresh (FULL)
- Automated failure notification
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

# Environment configuration
DB_PLATFORM = "duckdb"  # Or 'snowflake', 'bigquery', 'databricks'
DB_PATH = "/data/idr.duckdb"  # DuckDB file or cloud connection flag
ALERT_EMAIL = "alerts@company.com"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def evaluate_dry_run_safety(**context):
    """
    Inspects dry-run preview metrics to verify changes stay within safety thresholds
    before proceeding to live pipeline execution.
    """
    import duckdb

    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        row = conn.execute("""
            SELECT run_id, moved_entities, largest_proposed_cluster
            FROM idr_out.dry_run_summary
            ORDER BY created_at DESC
            LIMIT 1
        """).fetchone()

        if not row:
            print("[WARN] No dry-run summary found; proceeding with caution.")
            return "live_incremental_run"

        run_id, moved, largest = row

        # Configurable guardrail thresholds
        MAX_MOVED_THRESHOLD = 50000
        MAX_CLUSTER_THRESHOLD = 100000

        if moved > MAX_MOVED_THRESHOLD:
            print(f"[ALERT] Excessive entity movements in dry-run: {moved} > {MAX_MOVED_THRESHOLD}")
            return "abort_live_run"

        if largest > MAX_CLUSTER_THRESHOLD:
            print(f"[ALERT] Largest proposed cluster exceeds threshold: {largest} > {MAX_CLUSTER_THRESHOLD}")
            return "abort_live_run"

        print(f"[INFO] Dry-run passed guardrails (moved={moved}, largest={largest}).")
        return "live_incremental_run"
    finally:
        conn.close()


# ============================================
# 1. HOURLY INCREMENTAL DAG
# ============================================
with DAG(
    "idr_hourly_incremental",
    default_args=default_args,
    description="Hourly Incremental SQL Identity Resolution with Dry-Run Gating",
    schedule_interval="0 * * * *",  # Every hour at :00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["idr", "identity-resolution", "incremental"],
) as incremental_dag:

    # Step 1: Run dry-run to preview proposed cluster changes
    dry_run = BashOperator(
        task_id="dry_run_preview",
        bash_command=f"idr run --platform={DB_PLATFORM} --db={DB_PATH} --mode=INCR --dry-run",
    )

    # Step 2: Evaluate safety guardrails
    check_guardrails = BranchPythonOperator(
        task_id="check_guardrails",
        python_callable=evaluate_dry_run_safety,
    )

    # Step 3a: Live incremental run
    live_run = BashOperator(
        task_id="live_incremental_run",
        bash_command=f"idr run --platform={DB_PLATFORM} --db={DB_PATH} --mode=INCR",
    )

    # Step 3b: Abort branch if guardrails fail
    abort_run = BashOperator(
        task_id="abort_live_run",
        bash_command='echo "ABORT: Dry-run exceeded safety thresholds. Inspect idr_out.dry_run_results." && exit 1',
    )

    # Flow
    dry_run >> check_guardrails >> [live_run, abort_run]


# ============================================
# 2. WEEKLY FULL REFRESH DAG
# ============================================
with DAG(
    "idr_weekly_full",
    default_args=default_args,
    description="Weekly Full Refresh SQL Identity Resolution",
    schedule_interval="0 2 * * 0",  # Sunday 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["idr", "identity-resolution", "full"],
) as full_dag:

    full_dry_run = BashOperator(
        task_id="full_dry_run",
        bash_command=f"idr run --platform={DB_PLATFORM} --db={DB_PATH} --mode=FULL --dry-run",
    )

    full_live_run = BashOperator(
        task_id="full_live_run",
        bash_command=f"idr run --platform={DB_PLATFORM} --db={DB_PATH} --mode=FULL",
    )

    full_dry_run >> full_live_run
