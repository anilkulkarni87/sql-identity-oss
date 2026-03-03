import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def _create_sqlite(path: Path, table: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute(f"INSERT INTO {table} (value) VALUES ('ok')")
        conn.commit()
    finally:
        conn.close()


def test_backup_restore_drill_emits_evidence_and_restores_integrity(tmp_path: Path):
    data_dir = tmp_path / "state"
    backup_dir = tmp_path / "backup"
    data_dir.mkdir(parents=True, exist_ok=True)

    idr_db = data_dir / "idr.duckdb"
    idr_db.write_bytes(b"duckdb-demo-bytes-v1")

    run_jobs = data_dir / "idr_run_jobs.sqlite3"
    service_auth = data_dir / "idr_service_auth.sqlite3"
    audit_db = data_dir / "idr_audit.sqlite3"
    _create_sqlite(run_jobs, "run_jobs")
    _create_sqlite(service_auth, "service_auth")
    _create_sqlite(audit_db, "audit")

    evidence_file = backup_dir / "drill_report.json"
    cmd = [
        sys.executable,
        "tools/ci/run_backup_restore_drill.py",
        "--file",
        f"idr_database={idr_db}",
        "--file",
        f"run_jobs={run_jobs}",
        "--file",
        f"service_auth={service_auth}",
        "--file",
        f"audit={audit_db}",
        "--backup-dir",
        str(backup_dir),
        "--evidence-file",
        str(evidence_file),
        "--simulate-incident",
        "--max-rto-seconds",
        "60",
        "--max-rpo-bytes",
        "0",
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert evidence_file.exists()

    report = json.loads(evidence_file.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["restore"]["verified"] is True
    assert report["objectives"]["observed_rpo_bytes"] == 0
    assert report["incident"]["simulated"] is True
    assert len(report["managed_files"]) == 4
