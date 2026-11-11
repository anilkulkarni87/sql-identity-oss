"""
Unit tests for `idr bootstrap` (AutoBootstrapper) and `idr doctor` (IDRDoctor).
"""

import os
import tempfile
import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.bootstrap import AutoBootstrapper
from idr_core.doctor import CheckStatus, IDRDoctor
from idr_core.cli import main


@pytest.fixture
def test_db_path(tmp_path):
    """Create a temporary DuckDB database with mock retail tables."""
    db_file = str(tmp_path / "retail_test.duckdb")
    conn = duckdb.connect(db_file)

    # 1. Create a mock customers table
    conn.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR,
            email_address VARCHAR,
            phone_num VARCHAR,
            created_at TIMESTAMP,
            first_name VARCHAR,
            city VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO customers VALUES
        ('c1', 'alice@example.com', '+15551234567', '2026-01-01 10:00:00', 'Alice', 'New York'),
        ('c2', 'bob@example.com', '+15559876543', '2026-01-02 11:00:00', 'Bob', 'Los Angeles'),
        ('c3', 'carol@example.com', '+15550001111', '2026-01-03 12:00:00', 'Carol', 'Chicago')
    """)

    # 2. Create a mock orders table
    conn.execute("""
        CREATE TABLE orders (
            order_id VARCHAR,
            customer_id VARCHAR,
            order_email VARCHAR,
            order_date TIMESTAMP,
            total_amount DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        ('o101', 'c1', 'alice@example.com', '2026-02-01 14:00:00', 120.50),
        ('o102', 'c2', 'bob@example.com', '2026-02-02 15:30:00', 85.00),
        ('o103', 'c1', 'alice@example.com', '2026-02-03 16:15:00', 210.00)
    """)

    conn.close()
    return db_file


def test_doctor_uninitialized_database(test_db_path):
    """IDRDoctor correctly flags uninitialized warehouse schemas."""
    adapter = DuckDBAdapter(test_db_path)
    doctor = IDRDoctor(adapter)
    report = doctor.run_diagnostics()

    assert report.platform == "duckdb"
    assert report.overall_status == CheckStatus.FAIL
    assert any("Core IDR System Tables" in c.name for c in report.checks)
    adapter.close()


def test_doctor_initialized_database(test_db_path):
    """IDRDoctor passes after 'idr init' is run."""
    # Run init via CLI
    ret = main(["init", "--platform", "duckdb", "--db", test_db_path])
    assert ret == 0

    adapter = DuckDBAdapter(test_db_path)
    doctor = IDRDoctor(adapter)
    report = doctor.run_diagnostics()

    # Schemas & tables should now PASS
    schema_check = next(c for c in report.checks if "Core IDR System Tables" in c.name)
    assert schema_check.status == CheckStatus.PASS

    # Staging permissions probe should PASS
    perm_check = next(c for c in report.checks if "DDL & DML Staging Probe" in c.name)
    assert perm_check.status == CheckStatus.PASS

    adapter.close()


def test_autobootstrapper_generates_valid_yaml(test_db_path, tmp_path):
    """AutoBootstrapper correctly detects entity keys, watermarks, emails, phones, and writes YAML."""
    out_yaml = str(tmp_path / "auto_config.yaml")
    adapter = DuckDBAdapter(test_db_path)
    bootstrapper = AutoBootstrapper(adapter)

    res = bootstrapper.bootstrap(
        candidate_tables=["customers", "orders"],
        output_path=out_yaml,
        apply_to_warehouse=False,
    )

    assert res["status"] == "SUCCESS"
    assert res["sources_configured"] == 2
    assert os.path.exists(out_yaml)

    # Check generated configuration content
    cfg = res["suggested_config"]
    sources = {s["table"]: s for s in cfg["sources"]}

    # Verify customers table mapping
    assert "customers" in sources
    assert sources["customers"]["entity_key"] == "customer_id"
    assert sources["customers"]["watermark_column"] == "created_at"
    customer_id_types = {i["type"] for i in sources["customers"]["identifiers"]}
    assert "email" in customer_id_types or "phone" in customer_id_types

    # Verify orders table mapping
    assert "orders" in sources
    assert sources["orders"]["entity_key"] == "order_id"
    assert sources["orders"]["watermark_column"] == "order_date"
    order_id_types = {i["type"] for i in sources["orders"]["identifiers"]}
    assert "email" in order_id_types or "customer_id" in order_id_types

    # Rules generated
    assert len(cfg["rules"]) >= 1

    adapter.close()


def test_cli_bootstrap_and_doctor_integration(test_db_path, tmp_path):
    """End-to-end test of CLI `idr bootstrap` and `idr doctor` commands."""
    config_file = str(tmp_path / "cli_generated.yaml")

    # 1. Initialize DB
    assert main(["init", "--platform", "duckdb", "--db", test_db_path]) == 0

    # 2. Run CLI bootstrap with --apply
    ret = main([
        "bootstrap",
        "--platform", "duckdb",
        "--db", test_db_path,
        "--tables", "customers,orders",
        "--output", config_file,
        "--apply",
    ])
    assert ret == 0
    assert os.path.exists(config_file)

    # 3. Run CLI doctor
    doctor_ret = main(["doctor", "--platform", "duckdb", "--db", test_db_path])
    assert doctor_ret == 0
