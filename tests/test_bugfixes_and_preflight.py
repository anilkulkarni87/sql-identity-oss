"""
Regression tests for:
1. Empty / short normalized identifiers never forming edges (even with PII hashing)
2. Pruning removed/inactive source_table rows from membership on FULL runs
3. Preflight warnings for one-sided and unruled identifier_mappings
4. Expanded DEFAULT_EXCLUSIONS (GDPR redaction / anonymized placeholders)
"""

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.canonicalizers import build_canonicalize_expr
from idr_core.config import DEFAULT_EXCLUSIONS, config_to_sql
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def adapter():
    db = DuckDBAdapter(":memory:")
    SchemaManager(db).initialize()
    yield db
    db.close()


def test_canonicalizers_return_null_for_empty_or_short_values(adapter):
    """PHONE_DIGITS / PHONE_US_10 (<7 digits) and ALPHANUMERIC_UPPER (empty) must return NULL."""
    conn = adapter.conn
    for raw in ("'REDACTED'", "'---'", "'N/A'", "'12345'", "'123-456'"):
        p_digits = build_canonicalize_expr(raw, "PHONE_DIGITS", "duckdb")
        p_us10 = build_canonicalize_expr(raw, "PHONE_US_10", "duckdb")
        assert conn.execute(f"SELECT {p_digits}").fetchone()[0] is None, f"PHONE_DIGITS failed for {raw}"
        assert conn.execute(f"SELECT {p_us10}").fetchone()[0] is None, f"PHONE_US_10 failed for {raw}"

    for raw in ("'---'", "'!!!'", "'   '"):
        alpha = build_canonicalize_expr(raw, "ALPHANUMERIC_UPPER", "duckdb")
        assert conn.execute(f"SELECT {alpha}").fetchone()[0] is None, f"ALPHANUMERIC_UPPER failed for {raw}"

    # Valid 7+ digit phones still normalize cleanly
    assert conn.execute(f"SELECT {build_canonicalize_expr(chr(39) + '555-1234' + chr(39), 'PHONE_DIGITS', 'duckdb')}").fetchone()[0] == "5551234"
    assert conn.execute(f"SELECT {build_canonicalize_expr(chr(39) + '+1 (555) 234-5678' + chr(39), 'PHONE_US_10', 'duckdb')}").fetchone()[0] == "5552345678"


def test_empty_normalized_identifiers_never_merge_customers(adapter):
    """GDPR-erased / redacted phones and custom-canonicalized empty strings must never form edges."""
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("""
        CREATE TABLE raw.customers (
            customer_id VARCHAR,
            phone VARCHAR,
            loyalty_code VARCHAR
        )
    """)
    adapter.execute("""
        INSERT INTO raw.customers VALUES
            ('c1', 'REDACTED', '---'),
            ('c2', 'REDACTED', '!!!'),
            ('c3', '---', '   '),
            ('c4', '123', 'ABC-123'),
            ('c5', '+1 (555) 234-5678', 'ABC123'),
            ('c6', '555-234-5678', 'XYZ-999')
    """)

    cfg = {
        "sources": [
            {
                "id": "customers",
                "table": "raw.customers",
                "entity_key": "customer_id",
                "identifiers": [
                    {"type": "PHONE", "expr": "phone"},
                    {"type": "LOYALTY", "expr": "loyalty_code"},
                ],
            }
        ],
        "rules": [
            {"id": "r_phone", "match_keys": ["PHONE"], "canonicalize": "PHONE_US_10", "is_hashed": True},
            {"id": "r_loyalty", "match_keys": ["LOYALTY"], "canonicalize": "ALPHANUMERIC_UPPER"},
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    result = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"

    # c1, c2, c3 must remain 3 distinct singleton clusters (no false merge on '' or SHA256(''))
    memberships = {
        row["entity_key"]: row["resolved_id"]
        for row in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    assert len({memberships["customers:c1"], memberships["customers:c2"], memberships["customers:c3"]}) == 3
    # Valid matches still work: c4-c5 via loyalty ABC123, c5-c6 via phone 5552345678 -> all 3 in one cluster
    assert memberships["customers:c4"] == memberships["customers:c5"] == memberships["customers:c6"]
    # No empty normalized values persisted in identifiers_current or edges_current
    empty_ids = adapter.query_one(
        "SELECT COUNT(*) FROM idr_out.identity_identifiers_current WHERE identifier_value_norm IS NULL OR LENGTH(TRIM(identifier_value_norm)) = 0"
    )
    assert empty_ids == 0


def test_full_run_prunes_removed_source_table_memberships(adapter):
    """Removing or deactivating a source_table and running FULL must prune ghost memberships."""
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.src_a (id VARCHAR, email VARCHAR)")
    adapter.execute("CREATE TABLE raw.src_b (id VARCHAR, email VARCHAR)")
    adapter.execute("INSERT INTO raw.src_a VALUES ('a1', 'alice@corp.io'), ('a2', 'bob@corp.io')")
    adapter.execute("INSERT INTO raw.src_b VALUES ('b1', 'alice@corp.io'), ('b2', 'carol@corp.io')")

    cfg_two = {
        "sources": [
            {"id": "src_a", "table": "raw.src_a", "entity_key": "id", "identifiers": [{"type": "EMAIL", "expr": "email"}]},
            {"id": "src_b", "table": "raw.src_b", "entity_key": "id", "identifiers": [{"type": "EMAIL", "expr": "email"}]},
        ],
        "rules": [{"id": "r_email", "match_keys": ["EMAIL"], "canonicalize": "EMAIL_STANDARD"}],
    }
    for stmt in config_to_sql(cfg_two, dialect="duckdb"):
        adapter.execute(stmt)

    res1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res1.status == "SUCCESS"
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current") == 4

    # Remove src_b from idr_meta.source_table and re-run FULL
    adapter.execute("DELETE FROM idr_meta.identifier_mapping WHERE table_id = 'src_b'")
    adapter.execute("DELETE FROM idr_meta.source_table WHERE table_id = 'src_b'")

    res2 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res2.status == "SUCCESS"

    remaining_keys = [
        r["entity_key"]
        for r in adapter.query("SELECT entity_key FROM idr_out.identity_resolved_membership_current ORDER BY entity_key")
    ]
    assert remaining_keys == ["src_a:a1", "src_a:a2"]
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current") == 0
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") == 2


def test_preflight_warns_on_one_sided_and_unruled_mappings(adapter):
    """Preflight warns when an identifier is mapped on only 1 table or has no active rule."""
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.orders (id VARCHAR, email VARCHAR, loyalty_id VARCHAR)")
    adapter.execute("CREATE TABLE raw.shipments (id VARCHAR, email VARCHAR, order_id VARCHAR)")
    adapter.execute("INSERT INTO raw.orders VALUES ('o1', 'a@corp.io', 'L1')")
    adapter.execute("INSERT INTO raw.shipments VALUES ('s1', 'a@corp.io', 'O1')")

    cfg = {
        "sources": [
            {
                "id": "orders",
                "table": "raw.orders",
                "entity_key": "id",
                "identifiers": [
                    {"type": "EMAIL", "expr": "email"},
                    {"type": "LOYALTY_ID", "expr": "loyalty_id"},
                ],
            },
            {
                "id": "shipments",
                "table": "raw.shipments",
                "entity_key": "id",
                "identifiers": [
                    {"type": "EMAIL", "expr": "email"},
                    {"type": "ORDER_ID", "expr": "order_id"},
                ],
            },
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["EMAIL"], "canonicalize": "EMAIL_STANDARD"},
            {"id": "r_order", "match_keys": ["ORDER_ID"], "canonicalize": "EXACT"},
            # Deliberately no rule for LOYALTY_ID
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    result = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"
    joined = "\n".join(result.warnings)
    assert "ORDER_ID" in joined and "only mapped on 1 active source table" in joined
    assert "LOYALTY_ID" in joined and "no active rule" in joined
    assert "EMAIL" not in joined


def test_default_exclusions_block_gdpr_erased_and_redacted_placeholders(adapter):
    """Expanded DEFAULT_EXCLUSIONS blocks GDPR-erased emails and redacted placeholders."""
    patterns = {e["identifier_value_pattern"] for e in DEFAULT_EXCLUSIONS}
    assert "erased_%@%" in patterns
    assert "%@anonymized.%" in patterns
    assert "redacted" in patterns

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.users (id VARCHAR, email VARCHAR)")
    adapter.execute("""
        INSERT INTO raw.users VALUES
            ('u1', 'erased_user@anonymized.local'),
            ('u2', 'erased_user@anonymized.local'),
            ('u3', 'real@corp.io'),
            ('u4', 'real@corp.io')
    """)

    cfg = {
        "use_default_exclusions": True,
        "sources": [
            {
                "id": "users",
                "table": "raw.users",
                "entity_key": "id",
                "identifiers": [{"type": "EMAIL", "expr": "email"}],
            }
        ],
        "rules": [{"id": "r_email", "match_keys": ["EMAIL"], "canonicalize": "EMAIL_STANDARD"}],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    result = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"

    memberships = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    assert memberships["users:u1"] != memberships["users:u2"]
    assert memberships["users:u3"] == memberships["users:u4"]


def test_profiler_and_rule_generator_disambiguate_homonym_attributes(adapter):
    """Profiler flags generic and 0%-overlap categorical homonyms; RuleGenerator namespaces them while preserving shared attributes."""
    from idr_core.config_schema import AttributeMappingSpec, IdentifierMappingSpec, SourceSpec
    from idr_core.profiler import IDRDataProfiler
    from idr_core.rule_generator import AIRuleGenerator

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("""
        CREATE TABLE raw.orders (
            order_id VARCHAR, email VARCHAR, status VARCHAR, disposition VARCHAR, city VARCHAR
        )
    """)
    adapter.execute("""
        CREATE TABLE raw.shipments (
            shipment_id VARCHAR, email VARCHAR, status VARCHAR, disposition VARCHAR, city VARCHAR
        )
    """)
    adapter.execute("""
        INSERT INTO raw.orders VALUES
            ('o1', 'a@corp.io', 'PAID', 'APPROVED', 'Seattle'),
            ('o2', 'b@corp.io', 'CANCELLED', 'REFUNDED', 'Austin')
    """)
    adapter.execute("""
        INSERT INTO raw.shipments VALUES
            ('s1', 'a@corp.io', 'IN_TRANSIT', 'CARRIER_DELAY', 'Seattle'),
            ('s2', 'b@corp.io', 'DELIVERED', 'SIGNED', 'Austin')
    """)

    profile = IDRDataProfiler(adapter).profile_tables_safe(["raw.orders", "raw.shipments"])
    homonyms_by_col = {h["column"]: h["reason"] for h in profile.attribute_homonyms}
    assert homonyms_by_col.get("status") == "generic_homonym_name"
    assert homonyms_by_col.get("disposition") == "zero_value_overlap"
    assert "city" not in homonyms_by_col

    sources = [
        SourceSpec(
            id="raw_orders",
            table="raw.orders",
            entity_key="order_id",
            identifiers=[IdentifierMappingSpec(type="email", expr="email")],
            attributes=[
                AttributeMappingSpec(name="status", expr="status"),
                AttributeMappingSpec(name="disposition", expr="disposition"),
                AttributeMappingSpec(name="city", expr="city"),
            ],
        ),
        SourceSpec(
            id="raw_shipments",
            table="raw.shipments",
            entity_key="shipment_id",
            identifiers=[IdentifierMappingSpec(type="email", expr="email")],
            attributes=[
                AttributeMappingSpec(name="status", expr="status"),
                AttributeMappingSpec(name="disposition", expr="disposition"),
                AttributeMappingSpec(name="city", expr="city"),
            ],
        ),
    ]
    explanations = []
    AIRuleGenerator._disambiguate_homonym_attributes(sources, profile, explanations)

    orders_attrs = {a.expr: a.name for a in sources[0].attributes}
    shipments_attrs = {a.expr: a.name for a in sources[1].attributes}
    assert orders_attrs == {"status": "orders_status", "disposition": "orders_disposition", "city": "city"}
    assert shipments_attrs == {"status": "shipments_status", "disposition": "shipments_disposition", "city": "city"}
    assert len(explanations) == 2

