import json
import subprocess
import sys
from pathlib import Path


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_benchmark_slo_gate_passes_when_thresholds_met(tmp_path: Path):
    artifact = {
        "summary": {"failed": 0, "success": 2, "skipped": 0, "total": 2},
        "profiles": [
            {"profile_id": "duckdb_ci_small", "metrics": {"run_success_rate": 1.0}},
            {"profile_id": "api_latency_ci", "metrics": {"request_latency_seconds": {"p95": 0.1}}},
        ],
    }
    thresholds = {
        "artifact_version": "1.0",
        "rules": [
            {
                "id": "summary_failed_zero",
                "type": "summary_max",
                "path": "failed",
                "op": "<=",
                "value": 0,
            },
            {
                "id": "duckdb_success_rate",
                "type": "profile_metric",
                "profile_id": "duckdb_ci_small",
                "path": "metrics.run_success_rate",
                "op": ">=",
                "value": 1.0,
            },
            {
                "id": "api_p95",
                "type": "profile_metric",
                "profile_id": "api_latency_ci",
                "path": "metrics.request_latency_seconds.p95",
                "op": "<=",
                "value": 0.2,
            },
        ],
    }

    artifact_path = tmp_path / "artifact.json"
    thresholds_path = tmp_path / "thresholds.json"
    report_path = tmp_path / "report.json"
    _write_json(artifact_path, artifact)
    _write_json(thresholds_path, thresholds)

    cmd = [
        sys.executable,
        "tools/ci/check_benchmark_slos.py",
        "--benchmark-json",
        str(artifact_path),
        "--thresholds",
        str(thresholds_path),
        "--report",
        str(report_path),
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["failed_rules"] == 0
    assert report["passed_rules"] == 3


def test_benchmark_slo_gate_fails_when_threshold_breached(tmp_path: Path):
    artifact = {
        "summary": {"failed": 0, "success": 1, "skipped": 0, "total": 1},
        "profiles": [
            {"profile_id": "api_latency_ci", "metrics": {"request_latency_seconds": {"p95": 0.35}}}
        ],
    }
    thresholds = {
        "artifact_version": "1.0",
        "rules": [
            {
                "id": "api_p95",
                "type": "profile_metric",
                "profile_id": "api_latency_ci",
                "path": "metrics.request_latency_seconds.p95",
                "op": "<=",
                "value": 0.2,
            }
        ],
    }

    artifact_path = tmp_path / "artifact.json"
    thresholds_path = tmp_path / "thresholds.json"
    _write_json(artifact_path, artifact)
    _write_json(thresholds_path, thresholds)

    cmd = [
        sys.executable,
        "tools/ci/check_benchmark_slos.py",
        "--benchmark-json",
        str(artifact_path),
        "--thresholds",
        str(thresholds_path),
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True)
    assert completed.returncode == 1, completed.stdout + "\n" + completed.stderr
