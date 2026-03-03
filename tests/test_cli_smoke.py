"""
CLI smoke tests for high-level install/run commands.
"""

import os
import sys
from types import SimpleNamespace

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import idr_core
from idr_core.cli import main


def test_cli_version(capsys):
    rc = main(["version"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "sql-identity-resolution" in out


def test_cli_quickstart_dispatch(monkeypatch):
    called = {}

    def fake_handle_quickstart(args):
        called["rows"] = args.rows
        called["output"] = args.output
        called["seed"] = args.seed
        return 0

    monkeypatch.setattr("idr_core.cli.handle_quickstart", fake_handle_quickstart)
    rc = main(["quickstart", "--rows", "123", "--output", "demo.duckdb", "--seed", "7"])

    assert rc == 0
    assert called == {"rows": 123, "output": "demo.duckdb", "seed": 7}


def test_cli_serve_dispatch(monkeypatch):
    called = {}

    def fake_run(app_path, **kwargs):
        called["app_path"] = app_path
        called["host"] = kwargs.get("host")
        called["port"] = kwargs.get("port")
        called["reload"] = kwargs.get("reload")

    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=fake_run))
    rc = main(["serve", "--host", "127.0.0.1", "--port", "9001"])

    assert rc == 0
    assert called["app_path"] == "idr_api.main:app"
    assert called["host"] == "127.0.0.1"
    assert called["port"] == 9001
    assert called["reload"] is False


def test_cli_run_incr_mode(monkeypatch):
    class FakeAdapter:
        dialect = "duckdb"

        def execute(self, _sql):
            return None

        def close(self):
            return None

    class FakeResult:
        status = "SUCCESS"
        run_id = "run_test"
        entities_processed = 1
        edges_created = 1
        clusters_impacted = 1
        duration_seconds = 0.1
        error = None

    class FakeRunner:
        last_config = None

        def __init__(self, adapter):
            self.adapter = adapter

        def run(self, config):
            FakeRunner.last_config = config
            return FakeResult()

    fake_adapter = FakeAdapter()
    monkeypatch.setattr("idr_core.cli.get_adapter", lambda _args: fake_adapter)
    monkeypatch.setattr(idr_core, "IDRRunner", FakeRunner)

    rc = main(["run", "--platform", "duckdb", "--db", "demo.duckdb", "--mode", "INCR"])

    assert rc == 0
    assert FakeRunner.last_config.run_mode == "INCR"


def test_cli_doctor_dispatch(monkeypatch):
    called = {}

    def fake_handle_doctor(args):
        called["strict"] = args.strict
        called["json"] = args.json
        called["target"] = args.target
        called["api_url"] = args.api_url
        called["metrics_url"] = args.metrics_url
        called["whoami_url"] = args.whoami_url
        called["token_env"] = args.token_env
        return 0

    monkeypatch.setattr("idr_core.cli.handle_doctor", fake_handle_doctor)
    rc = main(
        [
            "doctor",
            "--strict",
            "--json",
            "--target",
            "cluster",
            "--api-url",
            "https://idr.example.com/api/health",
            "--metrics-url",
            "https://idr.example.com/metrics",
            "--whoami-url",
            "https://idr.example.com/api/auth/whoami",
            "--token-env",
            "IDR_TOKEN",
        ]
    )
    assert rc == 0
    assert called == {
        "strict": True,
        "json": True,
        "target": "cluster",
        "api_url": "https://idr.example.com/api/health",
        "metrics_url": "https://idr.example.com/metrics",
        "whoami_url": "https://idr.example.com/api/auth/whoami",
        "token_env": "IDR_TOKEN",
    }


def test_cli_deploy_dispatch(monkeypatch):
    called = {}

    def fake_exists(path):
        called["script_path"] = path
        return True

    class _Completed:
        returncode = 0

    def fake_run(cmd, check=False):
        called["cmd"] = cmd
        called["check"] = check
        return _Completed()

    monkeypatch.setattr("idr_core.cli.os.path.exists", fake_exists)
    monkeypatch.setattr("idr_core.cli.subprocess.run", fake_run)

    rc = main(
        [
            "deploy",
            "--provider",
            "aws",
            "--mode",
            "plan",
            "--namespace",
            "idr",
            "--release",
            "idr-enterprise",
            "--values",
            "/tmp/custom.yaml",
            "--set",
            "api.replicaCount=2",
            "--use-existing-secret",
            "idr-enterprise-secrets",
            "--no-doctor",
        ]
    )

    assert rc == 0
    assert called["check"] is False
    assert called["cmd"][0] == "bash"
    assert "--provider" in called["cmd"]
    assert "aws" in called["cmd"]
