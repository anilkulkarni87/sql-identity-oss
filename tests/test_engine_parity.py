"""
Engine Parity & Sustainability Tests for SQL Identity Resolution.

Verifies:
1. `config_to_dbt_seeds` accurately compiles YAML / live metadata into the 6 standard `dbt_idr` seed CSVs.
2. `idr_core` (Python SQL runner) and `dbt_idr` (dbt SQL package) produce identical connected-component
   clusters (`resolved_id` -> member `entity_key` partitions) on the integration test dataset.
3. CLI ergonomics (`idr dbt export`, `idr ui --help`, auto-platform detection).
4. MCP `export_dbt_seeds` tool functionality.
"""

import csv
import os
from argparse import Namespace
from pathlib import Path

import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.cli import _resolve_platform, main as cli_main
from idr_core.config import config_to_dbt_seeds, config_to_sql, sql_to_config
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_IT_SEEDS = REPO_ROOT / "dbt_idr" / "integration_tests" / "seeds"
DBT_IT_DB = REPO_ROOT / "dbt_idr" / "integration_tests" / "test_idr.duckdb"


def _build_integration_test_config() -> dict:
    """Return the IDR configuration equivalent to `dbt_idr/integration_tests/seeds/`."""
    return {
        "sources": [
            {
                "id": "crm",
                "table": "main.sample_crm_customers",
                "entity_key": "customer_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "EMAIL", "expr": "email"},
                    {"type": "PHONE", "expr": "phone"},
                ],
                "attributes": [
                    {"name": "email", "expr": "email"},
                    {"name": "phone", "expr": "phone"},
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                ],
            },
            {
                "id": "orders",
                "table": "main.sample_orders",
                "entity_key": "order_id",
                "watermark_column": "order_date",
                "identifiers": [
                    {"type": "EMAIL", "expr": "customer_email"},
                    {"type": "PHONE", "expr": "contact_phone"},
                ],
                "attributes": [
                    {"name": "email", "expr": "customer_email"},
                    {"name": "phone", "expr": "contact_phone"},
                ],
            },
            {
                "id": "pos",
                "table": "main.sample_pos_transactions",
                "entity_key": "transaction_id",
                "watermark_column": "transaction_date",
                "identifiers": [
                    {"type": "LOYALTY", "expr": "loyalty_card_id"},
                    {"type": "PHONE", "expr": "contact_phone"},
                ],
                "attributes": [
                    {"name": "phone", "expr": "contact_phone"},
                ],
            },
        ],
        "rules": [
            {
                "id": "email_exact",
                "type": "EXACT",
                "match_keys": ["EMAIL"],
                "priority": 1,
                "canonicalize": "LOWERCASE",
                "max_group_size": 100,
            },
            {
                "id": "phone_exact",
                "type": "EXACT",
                "match_keys": ["PHONE"],
                "priority": 2,
                "canonicalize": "NONE",
                "max_group_size": 100,
            },
            {
                "id": "loyalty_exact",
                "type": "EXACT",
                "match_keys": ["LOYALTY"],
                "priority": 3,
                "canonicalize": "NONE",
                "max_group_size": 10,
            },
        ],
        "exclusions": [
            {"type": "EMAIL", "value": "test@%", "match": "LIKE", "reason": "Test emails"},
            {"type": "PHONE", "value": "0000000000", "match": "EXACT", "reason": "Invalid phone"},
        ],
        "survivorship": [
            {"attribute": "email", "strategy": "RECENCY", "recency_field": "record_updated_at"},
            {"attribute": "phone", "strategy": "RECENCY", "recency_field": "record_updated_at"},
            {
                "attribute": "first_name",
                "strategy": "RECENCY",
                "recency_field": "record_updated_at",
            },
            {"attribute": "last_name", "strategy": "RECENCY", "recency_field": "record_updated_at"},
        ],
    }


def test_config_to_dbt_seeds_roundtrip(tmp_path):
    """Verify config_to_dbt_seeds writes all 7 expected dbt_idr seed CSVs with valid headers."""
    cfg = _build_integration_test_config()
    cfg["overrides"] = [
        {
            "type": "FORCE_LINK",
            "entity_key_a": "crm:10",
            "entity_key_b": "orders:200",
            "reason": "VIP manual merge",
        }
    ]
    out_dir = tmp_path / "seeds"
    written = config_to_dbt_seeds(cfg, str(out_dir))

    expected_files = {
        "idr_sources.csv",
        "idr_identifier_mappings.csv",
        "idr_attribute_mappings.csv",
        "idr_rules.csv",
        "idr_exclusions.csv",
        "idr_survivorship_rules.csv",
        "idr_overrides.csv",
    }
    assert set(written.keys()) == expected_files

    # Verify overrides CSV rows
    with open(written["idr_overrides.csv"], encoding="utf-8") as f:
        ov_rows = list(csv.DictReader(f))
    assert len(ov_rows) == 1
    assert ov_rows[0]["override_type"] == "FORCE_LINK"
    assert ov_rows[0]["entity_key_a"] == "crm:10"
    assert ov_rows[0]["entity_key_b"] == "orders:200"
    assert ov_rows[0]["is_active"] == "true"

    # Verify sources CSV rows
    with open(written["idr_sources.csv"], encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 3
    assert [r["source_id"] for r in rows] == ["crm", "orders", "pos"]
    assert rows[0]["table_name"] == "sample_crm_customers"
    assert rows[0]["schema"] == "main"

    # Verify rules CSV rows
    with open(written["idr_rules.csv"], encoding="utf-8") as f:
        rule_rows = list(csv.DictReader(f))
    assert len(rule_rows) == 3
    assert {r["identifier_type"] for r in rule_rows} == {"EMAIL", "PHONE", "LOYALTY"}


def test_idr_core_vs_dbt_idr_cluster_parity(tmp_path):
    """
    Verify that `idr_core` (Python SQL engine) and `dbt_idr` (dbt Jinja SQL engine)
    produce identical identity clusters on the integration test dataset.
    """
    db_file = tmp_path / "parity.duckdb"
    conn = duckdb.connect(str(db_file))

    # Load the 3 sample CSVs from dbt_idr/integration_tests/seeds/ as varchar to match CSV raw strings
    for table_name in ("sample_crm_customers", "sample_orders", "sample_pos_transactions"):
        csv_path = DBT_IT_SEEDS / f"{table_name}.csv"
        conn.execute(
            f"CREATE TABLE main.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true)"
        )
    conn.close()

    adapter = DuckDBAdapter(str(db_file))
    try:
        SchemaManager(adapter).initialize(reset=True)
        cfg = _build_integration_test_config()
        for stmt in config_to_sql(cfg, dialect="duckdb"):
            adapter.execute(stmt)

        runner = IDRRunner(adapter)
        res = runner.run(RunConfig(run_mode="FULL", max_iters=30))
        assert "SUCCESS" in res.status

        # Fetch clusters from idr_core
        core_rows = adapter.query(
            "SELECT resolved_id, entity_key FROM idr_out.identity_resolved_membership_current"
        )
        core_clusters = {}
        for r in core_rows:
            core_clusters.setdefault(r["resolved_id"], set()).add(r["entity_key"])

        core_partitions = {frozenset(members) for members in core_clusters.values()}

        # Verify expected multi-entity merges across CRM, Orders, and POS:
        # Alice: crm:1, crm:4, crm:6, orders:101, orders:106, pos:1001, pos:1004, pos:1010
        alice_cluster = frozenset(
            {
                "crm:1",
                "crm:4",
                "crm:6",
                "orders:101",
                "orders:106",
                "pos:1001",
                "pos:1004",
                "pos:1010",
            }
        )
        assert alice_cluster in core_partitions

        # Bob: crm:2, crm:9, orders:102, pos:1002, pos:1008
        bob_cluster = frozenset({"crm:2", "crm:9", "orders:102", "pos:1002", "pos:1008"})
        assert bob_cluster in core_partitions

        # If dbt_idr integration test DB exists on disk, verify 100% cluster isomorphism
        if DBT_IT_DB.exists():
            dbt_conn = duckdb.connect(str(DBT_IT_DB), read_only=True)
            try:
                dbt_rows = dbt_conn.execute(
                    "SELECT resolved_id, entity_key FROM main_idr_out.identity_membership"
                ).fetchall()
                dbt_clusters = {}
                for resolved_id, entity_key in dbt_rows:
                    dbt_clusters.setdefault(resolved_id, set()).add(entity_key)
                dbt_partitions = {frozenset(m) for m in dbt_clusters.values()}
                assert core_partitions == dbt_partitions
            finally:
                dbt_conn.close()
    finally:
        adapter.close()


def test_cli_dbt_export_and_platform_autodetect(tmp_path, monkeypatch):
    """Verify `idr dbt export` CLI command and automatic `--platform` resolution."""
    import yaml

    cfg = _build_integration_test_config()
    cfg_file = tmp_path / "test_config.yaml"
    cfg_file.write_text(yaml.dump(cfg), encoding="utf-8")

    out_dir = tmp_path / "exported_seeds"
    rc = cli_main(["dbt", "export", "--file", str(cfg_file), "--output-dir", str(out_dir)])
    assert rc == 0
    assert (out_dir / "idr_sources.csv").is_file()
    assert (out_dir / "idr_rules.csv").is_file()

    # Verify auto-platform resolution from --db
    args_db = Namespace(platform=None, db="my.duckdb", project=None)
    assert _resolve_platform(args_db) == "duckdb"

    # Verify auto-platform resolution from --project
    args_bq = Namespace(platform=None, db=None, project="my-gcp-proj")
    assert _resolve_platform(args_bq) == "bigquery"

    # Verify auto-platform resolution from IDR_PLATFORM env var
    monkeypatch.setenv("IDR_PLATFORM", "snowflake")
    args_env = Namespace(platform=None, db=None, project=None)
    assert _resolve_platform(args_env) == "snowflake"


def test_mcp_export_dbt_seeds(tmp_path):
    """Verify the FastMCP `export_dbt_seeds` tool writes seed CSVs from YAML string."""
    import yaml

    from idr_mcp.server import export_dbt_seeds

    cfg = _build_integration_test_config()
    out_dir = tmp_path / "mcp_seeds"
    res = export_dbt_seeds(output_dir=str(out_dir), config_yaml=yaml.dump(cfg))
    assert res["status"] == "SUCCESS"
    assert res["sources_count"] == 3
    assert res["rules_count"] == 3
    assert os.path.isfile(res["files_written"]["idr_rules.csv"])

