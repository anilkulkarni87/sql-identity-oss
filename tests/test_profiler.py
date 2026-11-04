"""
Tests for idr_core.profiler (IDRDataProfiler).
"""

import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import validate_config
from idr_core.profiler import IDRDataProfiler


@pytest.fixture
def profiler_duckdb():
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)

    conn.execute("CREATE SCHEMA IF NOT EXISTS ecommerce_test")
    conn.execute("""
        CREATE TABLE ecommerce_test.customers (
            customer_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            contact_email VARCHAR,
            mailing_address VARCHAR,
            email_verified BOOLEAN,
            mobile_no VARCHAR,
            created_at TIMESTAMP
        )
    """)

    # Populate with realistic data including dummy supernode values
    conn.execute("""
        INSERT INTO ecommerce_test.customers VALUES
            ('c1', 'Alice', 'Smith', 'alice@corp.com', '123 Main St', TRUE, '555-0101', '2026-01-01 10:00:00'),
            ('c2', 'Bob', 'Jones', 'bob@corp.com', '456 Oak Ave', TRUE, '555-0102', '2026-01-02 11:00:00'),
            ('c3', 'Charlie', 'Brown', 'charlie@corp.com', '789 Pine Rd', FALSE, '555-0103', '2026-01-03 12:00:00'),
            ('c4', 'Dummy1', 'Shared', 'shared_support@example.com', 'PO Box 1', TRUE, '555-9999', '2026-01-04 13:00:00'),
            ('c5', 'Dummy2', 'Shared', 'shared_support@example.com', 'PO Box 2', TRUE, '555-9999', '2026-01-05 14:00:00'),
            ('c6', 'Dummy3', 'Shared', 'shared_support@example.com', 'PO Box 3', TRUE, '555-9999', '2026-01-06 15:00:00')
    """)

    return adapter


def test_profiler_inspects_values_and_rejects_false_positives(profiler_duckdb):
    """Verify that profiler inspects actual values, not just naive column names."""
    profiler = IDRDataProfiler(profiler_duckdb)
    result = profiler.profile_and_generate_config(["ecommerce_test.customers"], sample_size=100)

    assert result["status"] == "SUCCESS"
    assert result["tables_profiled"] == 1
    assert result["is_valid_idr_config"] is True

    cfg = result["suggested_config"]
    source = cfg["sources"][0]

    # 1. Entity Key must be customer_id (100% unique, zero nulls)
    assert source["entity_key"] == "customer_id"

    # 2. Watermark must be created_at (timestamp)
    assert source["watermark_column"] == "created_at"

    # 3. contact_email should be recognized as email
    id_map = {i["type"]: i["expr"] for i in source["identifiers"]}
    assert id_map.get("email") == "contact_email"

    # 4. mobile_no should be recognized as phone
    assert id_map.get("phone") == "mobile_no"

    # 5. mailing_address and email_verified MUST NOT be treated as identifiers!
    assert "mailing_address" not in id_map.values()
    assert "email_verified" not in id_map.values()

    # 6. Attributes should include first_name and last_name
    attrs = {a["name"] for a in source["attributes"]}
    assert "first_name" in attrs
    assert "last_name" in attrs

    # 7. Supernode detection: shared_support@example.com and 555-9999 should be auto-flagged in exclusions
    excl_vals = {e["value"] for e in cfg["exclusions"]}
    assert "shared_support@example.com" in excl_vals or "555-9999" in excl_vals

    # 8. Rules must be generated and valid
    rule_keys = [r["match_keys"] for r in cfg["rules"]]
    assert ["email"] in rule_keys
    assert ["phone"] in rule_keys

    # 9. IDR config validator must pass
    assert validate_config(cfg) is True


def test_discover_and_suggest_rules_output_file(profiler_duckdb, tmp_path):
    """Verify discover_and_suggest_rules saves YAML to output_file."""
    from idr_core.connection_manager import ConnectionManager
    from idr_mcp import server as mcp_server

    ConnectionManager.instance().set_adapter(profiler_duckdb, {"platform": "duckdb"})
    out_yaml = tmp_path / "suggested_idr_config.yaml"

    res = mcp_server.discover_and_suggest_rules(
        table_fqn="ecommerce_test.customers",
        output_file=str(out_yaml),
    )

    assert res.get("status") == "SUCCESS"
    assert out_yaml.exists()
    assert out_yaml.stat().st_size > 50
    assert "sources:" in out_yaml.read_text()
    assert res.get("saved_to_file") == str(out_yaml.resolve())


def test_profiler_rejects_date_as_phone_and_excludes_non_identity_tables(profiler_duckdb):
    """Verify that temporal columns (e.g. end_date) are never classified as phone, and non-customer tables are excluded."""
    # Create an experiments table with dates that could falsely match phone digits if dashes were removed
    profiler_duckdb.execute("""
        CREATE TABLE ecommerce_test.experiments (
            experiment_id VARCHAR,
            name VARCHAR,
            status VARCHAR,
            start_date VARCHAR,
            end_date VARCHAR
        )
    """)
    profiler_duckdb.execute("""
        INSERT INTO ecommerce_test.experiments VALUES
            ('exp_1', 'Checkout Flow A/B', 'ACTIVE', '2026-01-01', '2026-03-31'),
            ('exp_2', 'Search Relevance Test', 'COMPLETED', '2025-11-01', '2026-02-28')
    """)

    profiler = IDRDataProfiler(profiler_duckdb)
    result = profiler.profile_and_generate_config(
        ["ecommerce_test.customers", "ecommerce_test.experiments"],
        sample_size=100
    )

    assert result["status"] == "SUCCESS"
    cfg = result["suggested_config"]
    source_tables = [s["table"] for s in cfg["sources"]]

    # customers has valid identifiers, so it MUST be included
    assert "ecommerce_test.customers" in source_tables

    # experiments has NO identifiers (end_date must NOT be tagged as phone), so it MUST be excluded from sources
    assert "ecommerce_test.experiments" not in source_tables

    # Check diagnostic reason
    diag = result["profiling_diagnostics"]["ecommerce_test.experiments"]
    assert diag["included"] is False
    assert "0 customer identifiers" in diag["reason"]


