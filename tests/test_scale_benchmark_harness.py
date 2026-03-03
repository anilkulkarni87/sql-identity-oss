import json
import subprocess
import sys
from pathlib import Path


def test_scale_benchmark_harness_emits_versioned_artifacts(tmp_path: Path):
    profiles = {
        "profiles": [
            {
                "id": "duckdb_test_profile",
                "platform": "duckdb",
                "mode": "pipeline",
                "rows": 500,
                "repetitions": 1,
                "seed": 7,
                "strict": False,
                "max_iters": 20,
            },
            {
                "id": "bigquery_compile_profile",
                "platform": "bigquery",
                "mode": "sql_compile",
                "repetitions": 3,
            },
            {
                "id": "api_latency_profile",
                "platform": "api",
                "mode": "api_latency",
                "endpoints": ["/api/health"],
                "warmup_requests": 0,
                "requests_per_endpoint": 3,
            },
        ]
    }
    profiles_path = tmp_path / "profiles.json"
    output_dir = tmp_path / "artifacts"
    profiles_path.write_text(json.dumps(profiles), encoding="utf-8")

    cmd = [
        sys.executable,
        "tools/ci/run_scale_benchmarks.py",
        "--profiles",
        str(profiles_path),
        "--output-dir",
        str(output_dir),
        "--run-label",
        "pytest",
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr

    latest_path = output_dir / "benchmark_metrics_latest.json"
    assert latest_path.exists()
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert payload["artifact_version"] == "1.0"
    assert payload["run_label"] == "pytest"
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["total"] == 3

    profile_ids = {p["profile_id"] for p in payload["profiles"]}
    assert profile_ids == {
        "duckdb_test_profile",
        "bigquery_compile_profile",
        "api_latency_profile",
    }

    for profile_id in profile_ids:
        profile_path = output_dir / f"benchmark_profile_{profile_id}.json"
        assert profile_path.exists()

    versioned_files = list(output_dir.glob("benchmark_metrics_v*.json"))
    assert versioned_files
