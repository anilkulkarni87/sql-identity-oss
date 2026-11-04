"""
Deep API & CLI integration tests covering:
1. Connected Dashboard endpoints (/api/metrics/summary, /api/metrics/distribution, /api/metrics/rules, /api/alerts)
2. Setup discovery endpoints (/api/setup/discover/tables, /api/setup/discover/columns)
3. Dialect-specific fuzzy templates (/api/setup/fuzzy-templates) across DuckDB, Snowflake, BigQuery, Databricks
4. Full CLI command execution (idr init, idr config validate/generate/apply, idr run FULL/INCR/dry-run/strict, idr quickstart)
"""

from unittest.mock import MagicMock

import duckdb
import pytest
import yaml
from fastapi.testclient import TestClient

import idr_api.dependencies as deps
from idr_api.dependencies import get_manager
from idr_api.main import app
from idr_core.cli import main as cli_main


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    return TestClient(app, raise_server_exceptions=False)


class TestConnectedDashboardAndDiscoveryAPI:
    def test_dashboard_metrics_distribution_rules_alerts_and_discovery(self, client, tmp_path):
        db_path = str(tmp_path / "dashboard_test.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("""
            CREATE TABLE raw.customers (
                id VARCHAR, email VARCHAR, phone VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO raw.customers VALUES
                ('c1', 'shared@example.com', '111-1111', 'Alice', '2026-01-01 00:00:00'),
                ('c2', 'shared@example.com', '222-2222', 'Alyce', '2026-01-02 00:00:00'),
                ('c3', 'solo@example.com', '333-3333', 'Bob', '2026-01-03 00:00:00'),
                ('c4', 'oversized@example.com', '444-4444', 'Spam1', '2026-01-04 00:00:00'),
                ('c5', 'oversized@example.com', '555-5555', 'Spam2', '2026-01-04 00:00:00'),
                ('c6', 'oversized@example.com', '666-6666', 'Spam3', '2026-01-04 00:00:00')
        """)
        conn.close()

        # 1. Connect via Setup API
        resp = client.post("/api/setup/connect", json={"platform": "duckdb", "params": {"path": db_path}})
        assert resp.status_code == 200

        # 2. Test Table & Column Discovery
        tables_resp = client.get("/api/setup/discover/tables", params={"schema": "raw"})
        assert tables_resp.status_code == 200
        assert any("customers" in t for t in tables_resp.json()["tables"])

        cols_resp = client.get("/api/setup/discover/columns", params={"table": "raw.customers"})
        assert cols_resp.status_code == 200
        col_names = {c["name"] for c in cols_resp.json()["columns"]}
        assert {"id", "email", "phone", "first_name", "updated_at"}.issubset(col_names)

        # 3. Configure and run pipeline (set max_group_size=2 so 'oversized@example.com' triggers a skipped group alert)
        cfg = {
            "sources": [
                {
                    "id": "raw_customers",
                    "table": "raw.customers",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [
                        {"type": "email", "column": "email", "expr": "email"},
                        {"type": "phone", "column": "phone", "expr": "phone"},
                    ],
                    "attributes": [{"name": "first_name", "column": "first_name", "expr": "first_name"}],
                }
            ],
            "rules": [
                {
                    "id": "rule_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "priority": 1,
                    "canonicalize": "LOWERCASE",
                    "max_group_size": 2,
                },
                {
                    "id": "rule_phone",
                    "type": "EXACT",
                    "match_keys": ["phone"],
                    "priority": 2,
                    "canonicalize": "EXACT",
                    "max_group_size": 100,
                },
            ],
            "fuzzy_rules": [],
            "survivorship": [],
        }
        assert client.post("/api/setup/config/save", json={"config": cfg}).status_code == 200
        run_resp = client.post(
            "/api/setup/run",
            json={"mode": "FULL", "strict": False, "max_iterations": 10, "dry_run": False},
        )
        assert run_resp.status_code == 200

        # 4. Verify /api/metrics/summary
        summary = client.get("/api/metrics/summary").json()
        assert summary["total_entities"] == 6
        assert summary["total_clusters"] == 5  # (c1+c2) + c3 + c4 + c5 + c6
        assert summary["total_edges"] == 1
        assert summary["last_run_id"] is not None

        # 5. Verify /api/metrics/distribution
        dist = client.get("/api/metrics/distribution").json()
        bucket_map = {d["bucket"]: d["count"] for d in dist}
        assert bucket_map.get("singleton") == 4
        assert bucket_map.get("2-5") == 1

        # 6. Verify /api/metrics/rules
        rules_stats = client.get("/api/metrics/rules").json()
        assert len(rules_stats) >= 1
        email_stat = [r for r in rules_stats if r["rule_id"] == "rule_email"][0]
        assert email_stat["edges_created"] == 1
        assert email_stat["percentage"] == 100.0

        # 7. Verify /api/alerts surfaces the skipped identifier group warning
        alerts = client.get("/api/alerts").json()
        assert len(alerts) >= 1
        assert any("skipped" in a["message"].lower() for a in alerts)

    @pytest.mark.parametrize(
        "dialect,expected_snippet",
        [
            ("duckdb", "jaro_winkler_similarity(<a>.{col}, <b>.{col})"),
            ("snowflake", "(JAROWINKLER_SIMILARITY(<a>.{col}, <b>.{col}) / 100.0)"),
            ("bigquery", "LEVENSHTEIN(<a>.{col}, <b>.{col})"),
            ("databricks", "levenshtein(<a>.{col}, <b>.{col})"),
        ],
    )
    def test_fuzzy_templates_for_all_four_dialects(self, client, dialect, expected_snippet):
        fake_adapter = MagicMock()
        fake_adapter.dialect = dialect
        get_manager().set_adapter_for_user("dev-user", fake_adapter, {"platform": dialect})

        resp = client.get("/api/setup/fuzzy-templates")
        assert resp.status_code == 200
        templates = resp.json()["templates"]
        assert len(templates) >= 2
        assert any(expected_snippet in t["sql_template"] for t in templates)
        for t in templates:
            assert 0.0 < t["default_threshold"] <= 1.0


class TestCLIEndToEnd:
    def test_cli_init_config_generate_apply_run_and_quickstart(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli_e2e.duckdb")
        cfg_path = tmp_path / "test_config.yaml"

        # Seed a source table in DuckDB
        conn = duckdb.connect(db_path)
        conn.execute("""
            CREATE TABLE raw_users (id VARCHAR, email VARCHAR, first_name VARCHAR, updated_at TIMESTAMP);
            INSERT INTO raw_users VALUES
                ('1', 'a@example.com', 'Alice', '2026-01-01 00:00:00'),
                ('2', 'A@example.com', 'Alicia', '2026-01-02 00:00:00');
        """)
        conn.close()

        config_dict = {
            "sources": [
                {
                    "id": "users",
                    "table": "raw_users",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [{"type": "email", "expr": "email"}],
                    "attributes": [{"name": "first_name", "expr": "first_name"}],
                }
            ],
            "rules": [
                {
                    "id": "r_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "priority": 1,
                    "canonicalize": "LOWERCASE",
                }
            ],
            "survivorship": [{"attribute": "first_name", "strategy": "RECENCY", "recency_field": "updated_at"}],
        }
        cfg_path.write_text(yaml.safe_dump(config_dict))

        # 1. idr init
        assert cli_main(["init", "--platform=duckdb", f"--db={db_path}", "--reset"]) == 0

        # 2. idr config validate
        assert cli_main(["config", "validate", f"--file={cfg_path}"]) == 0

        # 3. idr config generate across dialects
        for dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            capsys.readouterr()
            assert (
                cli_main(["config", "generate", f"--file={cfg_path}", f"--dialect={dialect}"]) == 0
            )
            out = capsys.readouterr().out
            assert "idr_meta.source_table" in out

        # 4. idr config apply
        assert (
            cli_main(["config", "apply", f"--file={cfg_path}", "--platform=duckdb", f"--db={db_path}"])
            == 0
        )

        # 5. idr run --mode=FULL --dry-run
        assert (
            cli_main(["run", "--platform=duckdb", f"--db={db_path}", "--mode=FULL", "--dry-run"])
            == 0
        )

        # 6. idr run --mode=FULL --strict
        assert (
            cli_main(["run", "--platform=duckdb", f"--db={db_path}", "--mode=FULL", "--strict"])
            == 0
        )

        # 7. idr quickstart with small row count
        qs_db = str(tmp_path / "qs_demo.duckdb")
        assert cli_main(["quickstart", "--rows=100", f"--output={qs_db}", "--seed=7"]) == 0
