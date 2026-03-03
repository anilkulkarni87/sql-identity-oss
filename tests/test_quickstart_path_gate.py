import json
from pathlib import Path

from tools.ci import validate_quickstart_path


def test_quickstart_gate_pass(monkeypatch, tmp_path):
    db_path = tmp_path / "quickstart.duckdb"
    report_path = tmp_path / "report.json"

    def _fake_run_quickstart(output, rows, seed, verbose):
        del rows, seed, verbose
        import duckdb

        conn = duckdb.connect(output)
        conn.execute("CREATE SCHEMA IF NOT EXISTS idr_out")
        conn.execute("CREATE TABLE idr_out.golden_profile_current (id INTEGER)")
        conn.execute("INSERT INTO idr_out.golden_profile_current VALUES (1)")
        conn.execute(
            "CREATE TABLE idr_out.identity_clusters_current (resolved_id VARCHAR, cluster_size INTEGER)"
        )
        conn.execute("INSERT INTO idr_out.identity_clusters_current VALUES ('a', 1)")
        conn.close()
        return 0

    monkeypatch.setattr(validate_quickstart_path, "run_quickstart", _fake_run_quickstart)
    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_quickstart_path.py",
            "--output",
            str(db_path),
            "--report",
            str(report_path),
            "--max-seconds",
            "600",
        ],
    )

    rc = validate_quickstart_path.main()
    assert rc == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "pass"


def test_quickstart_gate_fails_on_error(monkeypatch, tmp_path):
    report_path = tmp_path / "report.json"

    monkeypatch.setattr(validate_quickstart_path, "run_quickstart", lambda **_kwargs: 1)
    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_quickstart_path.py",
            "--output",
            str(tmp_path / "quickstart.duckdb"),
            "--report",
            str(report_path),
        ],
    )

    rc = validate_quickstart_path.main()
    assert rc == 1
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["failure_reason"] == "quickstart_run_failed"
