"""
Tests verifying DDL synchronization across all 4 dialects, dbt macro safety,
and exclusions round-trip in sql_to_config.
"""

from pathlib import Path

import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql, sql_to_config
from idr_core.schema_defs import SYSTEM_TABLES
from idr_core.schema_manager import SchemaManager

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_schema_manager_generate_ddl_covers_all_system_tables(dialect):
    ddl = SchemaManager.generate_ddl(dialect)
    for table_def in SYSTEM_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table_def.fqn}" in ddl
    if dialect == "bigquery":
        assert "FLOAT64" in ddl
        assert "NOT ENFORCED" in ddl
    elif dialect == "databricks":
        assert "DOUBLE" in ddl
        assert "NOT ENFORCED" in ddl


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_static_ddl_files_include_core_tables(dialect):
    ddl_path = REPO_ROOT / "sql" / "ddl" / f"{dialect}.sql"
    content = ddl_path.read_text()
    required_tables = [
        "idr_meta.source_table",
        "idr_meta.rule",
        "idr_meta.fuzzy_rule",
        "idr_meta.identifier_exclusion",
        "idr_out.identity_edges_current",
        "idr_out.identity_resolved_membership_current",
        "idr_out.identity_clusters_current",
        "idr_out.identity_identifiers_current",
        "idr_out.skipped_identifier_groups",
        "idr_out.config_snapshot",
    ]
    for tbl in required_tables:
        assert tbl in content, f"{tbl} missing from sql/ddl/{dialect}.sql"
    assert "sample_entity_keys" in content
    assert "max_allowed" in content


def test_dbt_label_propagation_macro_avoids_unsafe_rename():
    macro_path = REPO_ROOT / "dbt_idr" / "macros" / "label_propagation.sql"
    content = macro_path.read_text().lower()
    assert "rename to" not in content
    assert "create or replace table" in content


def test_exclusions_roundtrip_in_sql_to_config():
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    sm = SchemaManager(adapter)
    sm.initialize(reset=True)

    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.crm",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "first_name", "expr": "first_name"}],
            }
        ],
        "rules": [{"id": "r1", "type": "EXACT", "match_keys": ["email"], "priority": 1}],
        "exclusions": [
            {
                "type": "email",
                "value": "test@test.com",
                "match": "EXACT",
                "reason": "Placeholder email",
            },
            {
                "type": "email",
                "value": "%@noreply.com",
                "match": "LIKE",
                "reason": "No-reply domain",
            },
        ],
    }

    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    reconstructed = sql_to_config(adapter)
    assert "exclusions" in reconstructed
    assert len(reconstructed["exclusions"]) == 2
    values = {e["value"]: e for e in reconstructed["exclusions"]}
    assert values["test@test.com"]["match"] == "EXACT"
    assert values["%@noreply.com"]["match"] == "LIKE"
    adapter.close()

