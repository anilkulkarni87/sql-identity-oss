"""Tests for Task 6: Multi-Lens Identity (`lenses:`) from one edge graph."""

from __future__ import annotations

import json

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.cli import main as cli_main
from idr_core.config import (
    apply_config_to_warehouse,
    compute_lens_diff,
    config_to_sql,
    parse_config_snapshot_row,
    sql_to_config,
)
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


def _seed_two_source_dataset(adapter: DuckDBAdapter) -> None:
    """Create two source tables with a household phone bridge and separate emails."""
    adapter.execute("CREATE SCHEMA IF NOT EXISTS src")
    adapter.execute("""
        CREATE OR REPLACE TABLE src.crm_customers (
            customer_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        CREATE OR REPLACE TABLE src.pos_orders (
            order_user_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO src.crm_customers VALUES
            ('c1', 'alice@example.com', '555-0100', TIMESTAMP '2026-01-01 00:00:00'),
            ('c2', 'bob@example.com',   '555-0100', TIMESTAMP '2026-01-01 00:00:00'),
            ('c3', 'carol@example.com', '555-0200', TIMESTAMP '2026-01-01 00:00:00')
    """)
    adapter.execute("""
        INSERT INTO src.pos_orders VALUES
            ('p1', 'alice@example.com', '555-9999', TIMESTAMP '2026-01-01 00:00:00'),
            ('p2', 'dave@example.com',  '555-0200', TIMESTAMP '2026-01-01 00:00:00')
    """)


def _base_config_with_lenses() -> dict:
    return {
        "sources": [
            {
                "id": "crm_customers",
                "table": "src.crm_customers",
                "entity_key": "customer_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "EMAIL", "expr": "email"},
                    {"type": "PHONE", "expr": "phone"},
                ],
            },
            {
                "id": "pos_orders",
                "table": "src.pos_orders",
                "entity_key": "order_user_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "EMAIL", "expr": "email"},
                    {"type": "PHONE", "expr": "phone"},
                ],
            },
        ],
        "rules": [
            {"id": "EMAIL", "match_key": "EMAIL", "canonicalize": "LOWERCASE", "priority": 1},
            {"id": "PHONE", "match_key": "PHONE", "canonicalize": "NONE", "priority": 2},
        ],
        "lenses": [
            {
                "lens_id": "strict_person",
                "description": "Strict person resolution on email only",
                "include_types": ["EMAIL"],
                "max_iters": 15,
            },
            {
                "lens_id": "household",
                "description": "Household graph across email and phone",
                "include_types": ["EMAIL", "PHONE"],
                "max_iters": 15,
            },
            {
                "lens_id": "crm_only",
                "description": "CRM-only isolated lens",
                "include_types": ["EMAIL", "PHONE"],
                "source_filter": ["crm*"],
                "max_iters": 15,
            },
        ],
    }


def _membership_map(adapter: DuckDBAdapter, lens_id: str) -> dict[str, str]:
    rows = adapter.query(
        """
        SELECT entity_key, resolved_id
        FROM idr_out.identity_lens_membership_current
        WHERE lens_id = ?
        """,
        [lens_id],
    )
    return {r["entity_key"]: r["resolved_id"] for r in rows}


def test_lenses_config_roundtrip_and_snapshot(tmp_path) -> None:
    db_path = str(tmp_path / "lens_cfg.duckdb")
    adapter = DuckDBAdapter(db_path)
    try:
        SchemaManager(adapter).initialize(reset=True)
        cfg = _base_config_with_lenses()

        sql_stmts = config_to_sql(cfg, dialect="duckdb")
        assert any("idr_meta.lens_definition" in s for s in sql_stmts)

        apply_config_to_warehouse(adapter, cfg)
        loaded = sql_to_config(adapter)
        assert "lenses" in loaded
        by_id = {lens_item["id"]: lens_item for lens_item in loaded["lenses"]}
        assert set(by_id.keys()) == {"strict_person", "household", "crm_only"}
        assert by_id["strict_person"]["include_types"] == ["EMAIL"]
        assert by_id["crm_only"]["source_filter"] == ["crm*"]

        snap_row = {
            "sources_json": json.dumps(cfg["sources"]),
            "rules_json": json.dumps(
                {
                    "rules": cfg["rules"],
                    "fuzzy_rules": [],
                    "exclusions": [],
                    "overrides": [],
                    "use_default_exclusions": False,
                    "lenses": cfg["lenses"],
                }
            ),
            "mappings_json": json.dumps([]),
        }
        parsed = parse_config_snapshot_row(snap_row)
        assert len(parsed["lenses"]) == 3
        assert parsed["lenses"][0]["lens_id"] == "strict_person"
        assert parsed["lenses"][0]["include_types"] == ["EMAIL"]
    finally:
        adapter.close()


def test_lens_max_group_size_enforcement(tmp_path) -> None:
    db_path = str(tmp_path / "lens_mgs.duckdb")
    adapter = DuckDBAdapter(db_path)
    try:
        SchemaManager(adapter).initialize(reset=True)
        adapter.execute("CREATE SCHEMA IF NOT EXISTS src")
        adapter.execute("""
            CREATE OR REPLACE TABLE src.users (
                user_id VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                updated_at TIMESTAMP
            )
        """)
        # 3 users sharing the same phone; u1 and u2 also share an email
        adapter.execute("""
            INSERT INTO src.users VALUES
                ('u1', 'shared@example.com', '555-0001', TIMESTAMP '2026-01-01 00:00:00'),
                ('u2', 'shared@example.com', '555-0001', TIMESTAMP '2026-01-01 00:00:00'),
                ('u3', 'other@example.com',  '555-0001', TIMESTAMP '2026-01-01 00:00:00')
        """)
        cfg = {
            "sources": [
                {
                    "id": "users",
                    "table": "src.users",
                    "entity_key": "user_id",
                    "watermark_column": "updated_at",
                    "identifiers": [
                        {"type": "EMAIL", "expr": "email"},
                        {"type": "PHONE", "expr": "phone"},
                    ],
                }
            ],
            "rules": [
                {"id": "EMAIL", "match_key": "EMAIL", "canonicalize": "LOWERCASE", "priority": 1},
                {"id": "PHONE", "match_key": "PHONE", "canonicalize": "NONE", "priority": 2},
            ],
            "lenses": [
                {"id": "uncapped", "include_types": ["EMAIL", "PHONE"]},
                {"id": "capped_2", "include_types": ["EMAIL", "PHONE"], "max_group_size": 2},
            ],
        }
        apply_config_to_warehouse(adapter, cfg)
        res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
        assert res.status == "SUCCESS"

        uncapped = _membership_map(adapter, "uncapped")
        capped = _membership_map(adapter, "capped_2")

        assert len(set(uncapped.values())) == 1
        assert len(set(capped.values())) == 3
    finally:
        adapter.close()


def test_multi_lens_single_pass_full_and_source_filter(tmp_path) -> None:
    db_path = str(tmp_path / "lens_full.duckdb")
    adapter = DuckDBAdapter(db_path)
    try:
        SchemaManager(adapter).initialize(reset=True)
        _seed_two_source_dataset(adapter)
        apply_config_to_warehouse(adapter, _base_config_with_lenses())

        res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
        assert res.status == "SUCCESS"

        # strict_person: only EMAIL edges -> c1+p1 merge, c2, c3, p2 stay separate
        strict = _membership_map(adapter, "strict_person")
        assert strict["crm_customers:c1"] == strict["pos_orders:p1"]
        assert strict["crm_customers:c1"] != strict["crm_customers:c2"]
        assert strict["crm_customers:c3"] != strict["pos_orders:p2"]

        # household: EMAIL + PHONE -> c1, c2, p1 merge; c3 and p2 merge
        hh = _membership_map(adapter, "household")
        assert hh["crm_customers:c1"] == hh["crm_customers:c2"] == hh["pos_orders:p1"]
        assert hh["crm_customers:c3"] == hh["pos_orders:p2"]
        assert hh["crm_customers:c1"] != hh["crm_customers:c3"]

        # crm_only: source_filter ["crm*"] -> pos_orders excluded, c1 & c2 merge via phone, c3 separate
        crm = _membership_map(adapter, "crm_only")
        assert set(crm.keys()) == {"crm_customers:c1", "crm_customers:c2", "crm_customers:c3"}
        assert crm["crm_customers:c1"] == crm["crm_customers:c2"]
        assert crm["crm_customers:c1"] != crm["crm_customers:c3"]

        # Cluster summary table populated per lens
        cluster_counts = {
            r["lens_id"]: r["cnt"]
            for r in adapter.query(
                "SELECT lens_id, COUNT(*) AS cnt FROM idr_out.identity_lens_clusters_current GROUP BY lens_id"
            )
        }
        assert cluster_counts["strict_person"] == 4
        assert cluster_counts["household"] == 2
        assert cluster_counts["crm_only"] == 2
    finally:
        adapter.close()


def test_multi_lens_incr_stabilization(tmp_path) -> None:
    db_path = str(tmp_path / "lens_incr.duckdb")
    adapter = DuckDBAdapter(db_path)
    try:
        SchemaManager(adapter).initialize(reset=True)
        _seed_two_source_dataset(adapter)
        apply_config_to_warehouse(adapter, _base_config_with_lenses())

        runner = IDRRunner(adapter)
        res1 = runner.run(RunConfig(run_mode="FULL"))
        assert res1.status == "SUCCESS"

        hh_before = _membership_map(adapter, "household")
        strict_before = _membership_map(adapter, "strict_person")

        # Incremental update: add new pos_orders row p3 sharing email with c2
        adapter.execute("""
            INSERT INTO src.pos_orders VALUES
                ('p3', 'bob@example.com', '555-8888', TIMESTAMP '2026-01-02 00:00:00')
        """)
        res2 = runner.run(RunConfig(run_mode="INCR"))
        assert res2.status == "SUCCESS"

        hh_after = _membership_map(adapter, "household")
        strict_after = _membership_map(adapter, "strict_person")

        # Existing resolved_ids remain stable across INCR runs
        assert hh_after["crm_customers:c1"] == hh_before["crm_customers:c1"]
        assert hh_after["crm_customers:c3"] == hh_before["crm_customers:c3"]
        assert hh_after["pos_orders:p3"] == hh_after["crm_customers:c2"]

        assert strict_after["crm_customers:c2"] == strict_before["crm_customers:c2"]
        assert strict_after["pos_orders:p3"] == strict_after["crm_customers:c2"]
        assert strict_after["crm_customers:c1"] != strict_after["crm_customers:c2"]
    finally:
        adapter.close()


def test_compute_lens_diff_and_cli(tmp_path, capsys) -> None:
    db_path = str(tmp_path / "lens_cli.duckdb")
    adapter = DuckDBAdapter(db_path)
    try:
        SchemaManager(adapter).initialize(reset=True)
        _seed_two_source_dataset(adapter)
        apply_config_to_warehouse(adapter, _base_config_with_lenses())
    finally:
        adapter.close()

    assert cli_main(["run", "--platform", "duckdb", "--db", db_path, "--mode", "FULL", "--lens", "strict_person"]) == 0
    assert cli_main(["run", "--platform", "duckdb", "--db", db_path, "--mode", "FULL"]) == 0

    adapter = DuckDBAdapter(db_path)
    try:
        diff = compute_lens_diff(adapter, "strict_person", "household")
        assert diff["lens_a"] == "strict_person"
        assert diff["lens_b"] == "household"
        assert diff["lens_a_clusters"] == 4
        assert diff["lens_b_clusters"] == 2
        assert diff["entities_compared"] == 5
        assert len(diff["merged_in_b_only"]) >= 1
        assert diff["merged_in_a_only"] == []
    finally:
        adapter.close()

    capsys.readouterr()
    assert cli_main(["lenses", "list", "--platform", "duckdb", "--db", db_path]) == 0
    out_list = capsys.readouterr().out
    assert "strict_person" in out_list
    assert "household" in out_list

    assert cli_main(["lenses", "diff", "strict_person", "household", "--platform", "duckdb", "--db", db_path]) == 0
    out_diff = capsys.readouterr().out
    assert "strict_person" in out_diff
    assert "household" in out_diff


def test_multi_lens_api_endpoints(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    import idr_api.dependencies as deps
    from idr_api.main import app

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    db_path = str(tmp_path / "lens_api.duckdb")
    adapter = DuckDBAdapter(db_path)
    try:
        SchemaManager(adapter).initialize(reset=True)
        _seed_two_source_dataset(adapter)
        apply_config_to_warehouse(adapter, _base_config_with_lenses())
    finally:
        adapter.close()

    client = TestClient(app, raise_server_exceptions=False)
    conn_resp = client.post(
        "/api/setup/connect",
        json={"platform": "duckdb", "params": {"path": db_path}},
    )
    assert conn_resp.status_code == 200

    # Config endpoint returns lenses
    cfg_resp = client.get("/api/setup/config")
    assert cfg_resp.status_code == 200
    cfg_data = cfg_resp.json()
    assert len(cfg_data.get("lenses") or []) == 3

    # Run pipeline for a single lens via API
    run_resp = client.post(
        "/api/setup/run",
        json={"mode": "FULL", "strict": False, "max_iterations": 15, "dry_run": False, "lens": "strict_person"},
    )
    assert run_resp.status_code == 200

    # Run pipeline for all lenses via API
    run_all_resp = client.post(
        "/api/setup/run",
        json={"mode": "FULL", "strict": False, "max_iterations": 15, "dry_run": False},
    )
    assert run_all_resp.status_code == 200

    # List lenses
    lenses_resp = client.get("/api/lenses")
    assert lenses_resp.status_code == 200
    lenses = {lens_item["id"]: lens_item for lens_item in lenses_resp.json()["lenses"]}
    assert lenses["strict_person"]["cluster_count"] == 4
    assert lenses["household"]["cluster_count"] == 2

    # List clusters for a lens
    clusters_resp = client.get("/api/lenses/household/clusters")
    assert clusters_resp.status_code == 200
    clusters = clusters_resp.json()
    assert len(clusters) == 2

    # Cluster detail for a lens
    rid = clusters[0]["resolved_id"]
    detail_resp = client.get(f"/api/lenses/household/clusters/{rid}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["resolved_id"] == rid
    assert len(detail["entities"]) == clusters[0]["cluster_size"]

    # Diff two lenses
    diff_resp = client.get("/api/lenses/diff?lens_a=strict_person&lens_b=household")
    assert diff_resp.status_code == 200
    diff = diff_resp.json()
    assert diff["lens_a_clusters"] == 4
    assert diff["lens_b_clusters"] == 2
    assert len(diff["merged_in_b_only"]) >= 1
    client.post("/api/disconnect")
