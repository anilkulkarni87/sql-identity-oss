"""
Shared pytest fixtures and markers for IDR tests.

Prefer using `duckdb_adapter` or `seeded_duckdb_adapter` in new domain test modules
instead of duplicating schema initialization and sample source table DDL.
"""

from typing import Generator

import pytest


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line("markers", "duckdb: marks tests for DuckDB")
    config.addinivalue_line("markers", "snowflake: marks tests for Snowflake")
    config.addinivalue_line("markers", "bigquery: marks tests for BigQuery")
    config.addinivalue_line("markers", "databricks: marks tests for Databricks")
    config.addinivalue_line("markers", "slow: marks tests as slow")
    config.addinivalue_line("markers", "unit: fast isolated unit tests without full pipeline runs")
    config.addinivalue_line("markers", "integration: warehouse/API/MCP integration tests")
    config.addinivalue_line("markers", "e2e: full multi-stage pipeline end-to-end tests")
    config.addinivalue_line("markers", "ai: Zero-PII AI config, triage, and Copilot tests")
    config.addinivalue_line("markers", "hygiene: repository structure, ratchet, and contract guards")


@pytest.fixture
def duckdb_adapter() -> Generator:
    """Fresh in-memory DuckDBAdapter with `idr_meta`, `idr_work`, and `idr_out` initialized."""
    duckdb = pytest.importorskip("duckdb")
    from idr_core.adapters.duckdb import DuckDBAdapter
    from idr_core.schema_manager import SchemaManager

    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)
    try:
        yield adapter
    finally:
        adapter.close()


@pytest.fixture
def seeded_duckdb_adapter(duckdb_adapter):
    """Initialized DuckDBAdapter pre-seeded with `raw_crm` + `raw_orders` and EMAIL/PHONE rules."""
    from idr_core.config import apply_config_to_warehouse

    duckdb_adapter.execute(
        """
        CREATE TABLE raw_crm (
            customer_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    duckdb_adapter.execute(
        """
        INSERT INTO raw_crm (customer_id, email, phone, first_name, last_name, updated_at)
        VALUES
            ('C1', 'alice@acme.io', '5550101111', 'Alice', 'Chen', TIMESTAMP '2026-01-01 10:00:00'),
            ('C2', 'alice@acme.io', '5550102222', 'Alicia', 'Chen', TIMESTAMP '2026-01-02 12:00:00'),
            ('C3', 'bob@acme.io', '5550103333', 'Bob', 'Patel', TIMESTAMP '2026-01-03 09:00:00')
        """
    )
    duckdb_adapter.execute(
        """
        CREATE TABLE raw_orders (
            order_id VARCHAR,
            buyer_email VARCHAR,
            order_total DOUBLE,
            ordered_at TIMESTAMP
        )
        """
    )
    duckdb_adapter.execute(
        """
        INSERT INTO raw_orders (order_id, buyer_email, order_total, ordered_at)
        VALUES
            ('O100', 'alice@acme.io', 149.50, TIMESTAMP '2026-01-04 15:30:00'),
            ('O101', 'bob@acme.io', 79.00, TIMESTAMP '2026-01-05 11:15:00')
        """
    )

    config = {
        "sources": [
            {
                "id": "crm",
                "table": "raw_crm",
                "entity_key": "customer_id",
                "entity_type": "PRIMARY",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "EMAIL", "expr": "email"},
                    {"type": "PHONE", "expr": "phone"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                    {"name": "updated_at", "expr": "updated_at"},
                ],
            },
            {
                "id": "orders",
                "table": "raw_orders",
                "entity_key": "order_id",
                "entity_type": "EVENT",
                "watermark_column": "ordered_at",
                "identifiers": [
                    {"type": "EMAIL", "expr": "buyer_email"},
                ],
                "attributes": [],
            },
        ],
        "rules": [
            {"id": "r_email", "type": "EMAIL", "canonicalize": "EMAIL_STANDARD", "max_group_size": 1000},
            {"id": "r_phone", "type": "PHONE", "canonicalize": "PHONE_DIGITS", "max_group_size": 500},
        ],
        "survivorship": [
            {"attribute": "first_name", "strategy": "RECENCY", "recency_field": "updated_at"},
            {"attribute": "last_name", "strategy": "RECENCY", "recency_field": "updated_at"},
        ],
    }
    apply_config_to_warehouse(duckdb_adapter, config)
    return duckdb_adapter
