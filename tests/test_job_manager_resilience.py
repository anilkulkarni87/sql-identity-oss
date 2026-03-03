import time
from pathlib import Path

from idr_api.job_manager import SQLiteRunJobManager


def _manager(db_path: Path, max_attempts: int = 3, retry_backoff_seconds: float = 0.01):
    return SQLiteRunJobManager(
        db_path=str(db_path),
        max_attempts=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
    )


def test_transient_failure_retries_without_terminal_corruption(tmp_path):
    db_path = tmp_path / "run_jobs.sqlite3"
    manager = _manager(db_path, max_attempts=3)
    manager.reset_for_tests()

    job = manager.create_job(user_key="u-resilience", request={"mode": "FULL"})
    claimed = manager.claim_next_job()
    assert claimed is not None
    assert claimed["job_id"] == job["job_id"]

    manager.mark_failed_or_retry(job["job_id"], "transient db error")
    retrying = manager.get_job(job["job_id"], user_key="u-resilience")
    assert retrying["status"] == "RETRYING"
    assert retrying["attempt_count"] == 1

    time.sleep(0.02)
    claimed_retry = manager.claim_next_job()
    assert claimed_retry is not None
    assert claimed_retry["job_id"] == job["job_id"]
    assert claimed_retry["attempt_count"] == 2

    manager.mark_succeeded(job["job_id"], {"run_id": "run_ok", "status": "SUCCESS"})
    final = manager.get_job(job["job_id"], user_key="u-resilience")
    assert final["status"] == "SUCCEEDED"
    assert final["result"]["run_id"] == "run_ok"

    events = manager.list_job_events(job["job_id"], user_key="u-resilience", limit=50)
    event_types = [event["event_type"] for event in events]
    assert event_types.count("JOB_RETRY_SCHEDULED") == 1
    assert "JOB_SUCCEEDED" in event_types
    assert "JOB_FAILED" not in event_types

    with manager._connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM run_jobs WHERE job_id = ?",
            (job["job_id"],),
        ).fetchone()
    assert int(row["cnt"]) == 1


def test_worker_kill_recovery_moves_running_job_to_retry(tmp_path):
    db_path = tmp_path / "run_jobs_recover.sqlite3"
    manager = _manager(db_path, max_attempts=3)
    manager.reset_for_tests()

    job = manager.create_job(user_key="u-recover", request={"mode": "FULL"})
    claimed = manager.claim_next_job()
    assert claimed is not None
    assert claimed["status"] == "RUNNING"
    assert claimed["attempt_count"] == 1

    recovered_manager = _manager(db_path, max_attempts=3)
    recovered = recovered_manager.get_job(job["job_id"], user_key="u-recover")
    assert recovered["status"] == "RETRYING"
    assert recovered["attempt_count"] == 1
    assert "Recovered after API restart" in (recovered.get("error") or "")

    claimed_after_recovery = recovered_manager.claim_next_job()
    assert claimed_after_recovery is not None
    assert claimed_after_recovery["job_id"] == job["job_id"]
    assert claimed_after_recovery["status"] == "RUNNING"
    assert claimed_after_recovery["attempt_count"] == 2

    recovered_manager.mark_succeeded(job["job_id"], {"run_id": "run_recovered", "status": "SUCCESS"})
    final = recovered_manager.get_job(job["job_id"], user_key="u-recover")
    assert final["status"] == "SUCCEEDED"

    events = recovered_manager.list_job_events(job["job_id"], user_key="u-recover", limit=50)
    event_types = [event["event_type"] for event in events]
    assert "JOB_RECOVERED_RETRYING" in event_types
    assert "JOB_SUCCEEDED" in event_types


def test_restart_recovery_fails_job_when_attempts_exhausted(tmp_path):
    db_path = tmp_path / "run_jobs_exhausted.sqlite3"
    manager = _manager(db_path, max_attempts=1)
    manager.reset_for_tests()

    job = manager.create_job(user_key="u-exhausted", request={"mode": "FULL"})
    claimed = manager.claim_next_job()
    assert claimed is not None
    assert claimed["status"] == "RUNNING"
    assert claimed["attempt_count"] == 1

    recovered_manager = _manager(db_path, max_attempts=1)
    recovered = recovered_manager.get_job(job["job_id"], user_key="u-exhausted")
    assert recovered["status"] == "FAILED"
    assert recovered["attempt_count"] == 1
    assert recovered["finished_at"] is not None
    assert "max attempts exhausted" in (recovered.get("error") or "").lower()

    assert recovered_manager.claim_next_job() is None

    events = recovered_manager.list_job_events(job["job_id"], user_key="u-exhausted", limit=50)
    event_types = [event["event_type"] for event in events]
    assert "JOB_RECOVERED_FAILED" in event_types


def test_cancel_requested_job_recovers_to_cancelled(tmp_path):
    db_path = tmp_path / "run_jobs_cancelled.sqlite3"
    manager = _manager(db_path, max_attempts=3)
    manager.reset_for_tests()

    job = manager.create_job(user_key="u-cancel", request={"mode": "FULL"})
    claimed = manager.claim_next_job()
    assert claimed is not None
    assert claimed["status"] == "RUNNING"

    cancel_resp = manager.cancel_job(job_id=job["job_id"], user_key="u-cancel")
    assert cancel_resp is not None
    assert cancel_resp["status"] == "CANCEL_REQUESTED"

    recovered_manager = _manager(db_path, max_attempts=3)
    recovered = recovered_manager.get_job(job["job_id"], user_key="u-cancel")
    assert recovered["status"] == "CANCELLED"
    assert recovered["finished_at"] is not None

    events = recovered_manager.list_job_events(job["job_id"], user_key="u-cancel", limit=50)
    event_types = [event["event_type"] for event in events]
    assert "JOB_CANCEL_REQUESTED" in event_types
    assert "JOB_RECOVERED_CANCELLED" in event_types
