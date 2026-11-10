"""
Tests for the 5 Core Enterprise Identity Resolution Features:
1. Built-In Domain Canonicalizers + Custom SQL Expressions + Python Registry
2. Curated Default Toxic Identifier Exclusion Pack + Manual SQL Table Preservation
3. Manual Stewardship Overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE)
4. Persistent resolved_id Across Runs (Largest cluster wins) + resolved_id_crosswalk
5. GDPR / CCPA Right-to-be-Forgotten Cascading Graph Purge + SHA-256 Audit Log
"""

import duckdb
import pytest

from idr_core import (
    DEFAULT_EXCLUSIONS,
    IDRRunner,
    PrivacyManager,
    RunConfig,
    list_canonicalizers,
    register_canonicalizer,
    validate_config,
)
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.canonicalizers import build_canonicalize_expr
from idr_core.config import config_to_sql, sql_to_config
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def adapter():
    conn = duckdb.connect(":memory:")
    ad = DuckDBAdapter(conn)
    SchemaManager(ad).initialize(reset=True)
    yield ad
    ad.close()


# =====================================================================
# 1. Domain Canonicalizers + Custom SQL + Python Registry
# =====================================================================


def test_email_standard_vs_email_strip_plus(adapter):
    """
    EMAIL_STANDARD preserves +tag (only trims & lowercases),
    whereas EMAIL_STRIP_PLUS strips +tag before @.
    """
    conn = adapter.conn
    e_std = build_canonicalize_expr("'  Alice.Smith+Promo@Example.COM  '", "EMAIL_STANDARD", "duckdb")
    e_plus = build_canonicalize_expr("'  Alice.Smith+Promo@Example.COM  '", "EMAIL_STRIP_PLUS", "duckdb")

    val_std = conn.execute(f"SELECT {e_std}").fetchone()[0]
    val_plus = conn.execute(f"SELECT {e_plus}").fetchone()[0]

    assert val_std == "alice.smith+promo@example.com"
    assert val_plus == "alice.smith@example.com"


def test_phone_and_alphanumeric_canonicalizers_across_dialects(adapter):
    """Verify PHONE_DIGITS, PHONE_US_10, and ALPHANUMERIC_UPPER on DuckDB and dialect SQL generation."""
    conn = adapter.conn
    p_digits = build_canonicalize_expr("'+1 (555) 234-5678'", "PHONE_DIGITS", "duckdb")
    p_us10 = build_canonicalize_expr("'+1 (555) 234-5678'", "PHONE_US_10", "duckdb")
    p_us10_short = build_canonicalize_expr("'(555) 234-5678'", "PHONE_US_10", "duckdb")
    alpha = build_canonicalize_expr("' ab-12_34! '", "ALPHANUMERIC_UPPER", "duckdb")

    assert conn.execute(f"SELECT {p_digits}").fetchone()[0] == "15552345678"
    assert conn.execute(f"SELECT {p_us10}").fetchone()[0] == "5552345678"
    assert conn.execute(f"SELECT {p_us10_short}").fetchone()[0] == "5552345678"
    assert conn.execute(f"SELECT {alpha}").fetchone()[0] == "AB1234"

    # Verify dialect-specific regex flags (3 args on Snowflake/BigQuery/Databricks, 4 args with 'g' on DuckDB)
    sf_expr = build_canonicalize_expr("phone_col", "PHONE_DIGITS", "snowflake")
    assert "'g'" not in sf_expr
    ddb_expr = build_canonicalize_expr("phone_col", "PHONE_DIGITS", "duckdb")
    assert "'g'" in ddb_expr


def test_custom_sql_and_python_registered_canonicalizers_end_to_end(adapter):
    """
    Verify that users can:
    1) Register a custom Python canonicalizer via register_canonicalizer()
    2) Pass an inline warehouse SQL expression using {val} or a dialect dictionary in YAML
    3) Run the full IDR pipeline with these canonicalizers.
    """
    register_canonicalizer(
        "STRIP_LEADING_ZEROS",
        lambda col, dialect: f"LTRIM(TRIM({col}), '0')",
    )
    assert "STRIP_LEADING_ZEROS" in list_canonicalizers()

    adapter.execute("""
        CREATE TABLE raw_customers (
            id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            loyalty_code VARCHAR,
            acct_num VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO raw_customers VALUES
        ('1', 'alice+tag@example.com', '+1 (555) 111-2222', '0009876', '42', CURRENT_TIMESTAMP),
        ('2', 'ALICE@example.com', '555-111-2222', '9876', '000042', CURRENT_TIMESTAMP)
    """)

    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw_customers",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                    {"type": "loyalty", "expr": "loyalty_code"},
                    {"type": "account", "expr": "acct_num"},
                ],
            }
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STRIP_PLUS"},
            {"id": "r_phone", "match_keys": ["phone"], "canonicalize": "PHONE_US_10"},
            {"id": "r_loyalty", "match_keys": ["loyalty"], "canonicalize": "STRIP_LEADING_ZEROS"},
            {
                "id": "r_acct",
                "match_keys": ["account"],
                "canonicalize": {
                    "duckdb": "LPAD(LTRIM(TRIM({val}), '0'), 6, '0')",
                    "default": "TRIM({val})",
                },
            },
        ],
    }
    validate_config(cfg)
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res.status.startswith("SUCCESS")

    clusters = adapter.query("SELECT resolved_id, cluster_size FROM idr_out.identity_clusters_current")
    assert len(clusters) == 1
    assert clusters[0]["cluster_size"] == 2

    # Check all 4 canonicalized identifier types created edges between crm:1 and crm:2
    edge_types = {
        r["identifier_type"]
        for r in adapter.query("SELECT DISTINCT identifier_type FROM idr_out.identity_edges_current")
    }
    assert edge_types == {"email", "phone", "loyalty", "account"}


# =====================================================================
# 2. Default Toxic Exclusions + Manual SQL Table Preservation
# =====================================================================


def test_default_exclusions_and_manual_table_preservation(adapter):
    """
    Verify:
    1) seed_default_exclusions seeds the curated pack (including ANY wildcard).
    2) Rows manually INSERTed into idr_meta.identifier_exclusion via SQL are preserved
       when config_to_sql runs.
    3) Both default exclusions and manually inserted exclusions prevent toxic linking.
    """
    seeded = SchemaManager(adapter).seed_default_exclusions()
    assert seeded == len(DEFAULT_EXCLUSIONS)

    # User manually inserts a custom toxic identifier directly into the warehouse table
    adapter.execute("""
        INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason)
        VALUES ('email', 'store_pos_register@retailer.com', 'EXACT', 'Manual SQL insert by data steward')
    """)

    adapter.execute("""
        CREATE TABLE pos_orders (
            order_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO pos_orders VALUES
        ('101', 'noreply@company.com', '0000000000', CURRENT_TIMESTAMP),
        ('102', 'noreply@company.com', '0000000000', CURRENT_TIMESTAMP),
        ('103', 'store_pos_register@retailer.com', '9999999999', CURRENT_TIMESTAMP),
        ('104', 'store_pos_register@retailer.com', 'N/A', CURRENT_TIMESTAMP),
        ('105', 'N/A', 'N/A', CURRENT_TIMESTAMP)
    """)

    cfg = {
        "sources": [
            {
                "id": "pos",
                "table": "pos_orders",
                "entity_key": "order_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
            }
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"},
            {"id": "r_phone", "match_keys": ["phone"], "canonicalize": "EXACT"},
        ],
        "exclusions": [
            {"type": "phone", "value": "1234567890", "match_type": "EXACT", "reason": "YAML exclusion"}
        ],
    }

    # Applying YAML config must NOT wipe the manual SQL insert or seeded defaults!
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    manual_still_present = adapter.query_one("""
        SELECT COUNT(*) FROM idr_meta.identifier_exclusion
        WHERE identifier_value_pattern = 'store_pos_register@retailer.com'
    """)
    assert manual_still_present == 1

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res.status.startswith("SUCCESS")
    assert res.edges_created == 0

    # All 5 orders remain unlinked singletons because every shared identifier was toxic!
    clusters = adapter.query("SELECT * FROM idr_out.identity_clusters_current")
    assert len(clusters) == 5


# =====================================================================
# 3. Manual Stewardship Overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE)
# =====================================================================


def test_manual_stewardship_overrides(adapter):
    """
    Verify FORCE_LINK, DO_NOT_LINK, and ISOLATE overrides work together:
    - crm:1 and crm:2 share an email, but DO_NOT_LINK keeps them in separate clusters.
    - crm:3 and crm:4 share NO identifiers, but FORCE_LINK merges them into 1 cluster.
    - crm:5 shares an email with crm:3, but ISOLATE forces crm:5 to stay a singleton.
    """
    adapter.execute("""
        CREATE TABLE override_users (
            id VARCHAR,
            email VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO override_users VALUES
        ('1', 'shared_family@example.com', CURRENT_TIMESTAMP),
        ('2', 'shared_family@example.com', CURRENT_TIMESTAMP),
        ('3', 'person_a@example.com', CURRENT_TIMESTAMP),
        ('4', 'person_b_different@example.com', CURRENT_TIMESTAMP),
        ('5', 'person_a@example.com', CURRENT_TIMESTAMP)
    """)

    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "override_users",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
            }
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"}
        ],
        "overrides": [
            {
                "override_type": "DO_NOT_LINK",
                "entity_key_a": "crm:1",
                "entity_key_b": "crm:2",
                "reason": "Twins sharing family email",
            },
            {
                "override_type": "FORCE_LINK",
                "entity_key_a": "crm:3",
                "entity_key_b": "crm:4",
                "reason": "Confirmed account merge ticket #104",
            },
        ],
    }

    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    # Also test direct SQL INSERT into idr_meta.identity_override for ISOLATE
    adapter.execute("""
        INSERT INTO idr_meta.identity_override
        (override_type, entity_key_a, entity_key_b, reason, is_active)
        VALUES ('ISOLATE', 'crm:5', '__ISOLATE__', 'Quarantined test record', TRUE)
    """)

    # Verify sql_to_config round-trips overrides
    exported = sql_to_config(adapter)
    assert len(exported["overrides"]) == 3

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res.status.startswith("SUCCESS")

    memberships = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }

    # crm:1 and crm:2 must NOT be in the same cluster
    assert memberships["crm:1"] != memberships["crm:2"]
    # crm:3 and crm:4 MUST be in the same cluster
    assert memberships["crm:3"] == memberships["crm:4"]
    # crm:5 MUST be isolated from crm:3
    assert memberships["crm:5"] != memberships["crm:3"]
    assert memberships["crm:5"] == "crm:5"


# =====================================================================
# 4. Persistent resolved_id Across Runs + Crosswalk
# =====================================================================


def test_persistent_resolved_id_and_largest_cluster_wins_crosswalk(adapter):
    """
    Verify:
    1) Run 1 creates:
       - Cluster 'orders:500' (size 2: orders:500, orders:501)
       - Cluster 'web:800' (size 3: web:800, web:801, web:802)
    2) Run 2 adds 'aaa:001' (lexicographically smaller than 'orders:500'!) sharing email with 'orders:500'.
       The cluster's resolved_id MUST remain 'orders:500' (NOT flip to 'aaa:001').
    3) Run 3 adds a bridge linking Cluster 'orders:500' (now size 3) with Cluster 'web:800' (size 4 after new node).
       Because 'web:800' is the LARGER existing cluster (4 vs 3), 'web:800' wins (even though 'orders:500' < 'web:800'!),
       and 'orders:500' -> 'web:800' is logged in idr_out.resolved_id_crosswalk with change_type='MERGED'.
    """
    adapter.execute("""
        CREATE TABLE unified_events (
            source_prefix VARCHAR,
            id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            updated_at TIMESTAMP
        )
    """)

    # Run 1 data:
    # Cluster 1: orders:500, orders:501 (size 2) -> resolved_id = 'orders:500'
    # Cluster 2: web:800, web:801, web:802, web:803 (size 4) -> resolved_id = 'web:800'
    adapter.execute("""
        INSERT INTO unified_events VALUES
        ('orders', 'orders:500', 'c1@example.com', '111', CURRENT_TIMESTAMP),
        ('orders', 'orders:501', 'c1@example.com', '112', CURRENT_TIMESTAMP),
        ('web', 'web:800', 'c2@example.com', '221', CURRENT_TIMESTAMP),
        ('web', 'web:801', 'c2@example.com', '222', CURRENT_TIMESTAMP),
        ('web', 'web:802', 'c2@example.com', '223', CURRENT_TIMESTAMP),
        ('web', 'web:803', 'c2@example.com', '224', CURRENT_TIMESTAMP)
    """)

    cfg = {
        "sources": [
            {
                "id": "src",
                "table": "unified_events",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
            }
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"},
            {"id": "r_phone", "match_keys": ["phone"], "canonicalize": "EXACT"},
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    # --- RUN 1 ---
    IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    m1 = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    assert m1["src:orders:500"] == "src:orders:500"
    assert m1["src:web:800"] == "src:web:800"

    # --- RUN 2: Add 'aaa:001' (lexicographically smaller than 'orders:500') joining c1@example.com ---
    adapter.execute("""
        INSERT INTO unified_events VALUES
        ('aaa', 'aaa:001', 'c1@example.com', '119', CURRENT_TIMESTAMP)
    """)
    IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    m2 = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    # Existing cluster resolved_id 'src:orders:500' MUST be preserved!
    assert m2["src:aaa:001"] == "src:orders:500"
    assert m2["src:orders:500"] == "src:orders:500"
    assert m2["src:orders:501"] == "src:orders:500"

    # --- RUN 3: Bridge 'src:orders:500' (size 3) and 'src:web:800' (size 4) via phone '221' ---
    adapter.execute("""
        INSERT INTO unified_events VALUES
        ('bridge', 'bridge:999', 'c1@example.com', '221', CURRENT_TIMESTAMP)
    """)
    IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    m3 = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    # Largest cluster ('src:web:800' had 4 entities vs 'src:orders:500' had 3 entities) wins!
    assert m3["src:web:800"] == "src:web:800"
    assert m3["src:orders:500"] == "src:web:800"
    assert m3["src:aaa:001"] == "src:web:800"

    # Verify crosswalk logged the retirement of 'src:orders:500' -> 'src:web:800'
    cw = adapter.query("""
        SELECT previous_resolved_id, current_resolved_id, change_type, entities_moved
        FROM idr_out.resolved_id_crosswalk
        WHERE previous_resolved_id = 'src:orders:500'
    """)
    assert len(cw) == 1
    assert cw[0]["current_resolved_id"] == "src:web:800"
    assert cw[0]["change_type"] == "MERGED"
    assert cw[0]["entities_moved"] == 3


# =====================================================================
# 5. GDPR / CCPA Right-to-be-Forgotten Cascading Graph Purge
# =====================================================================


def test_gdpr_privacy_forget_cascading_purge_and_audit_log(adapter):
    """
    Verify PrivacyManager.forget():
    1) Discovers all cross-source entities in Alice's cluster from a single email.
    2) Dry run previews counts without deleting rows.
    3) Live purge deletes Alice's entire cluster across idr_out.* and idr_work.*,
       leaves Bob's cluster intact, and writes a SHA-256 receipt with zero cleartext PII.
    """
    adapter.execute("""
        CREATE TABLE crm_users (id VARCHAR, email VARCHAR, phone VARCHAR, updated_at TIMESTAMP);
        CREATE TABLE web_orders (order_id VARCHAR, phone VARCHAR, updated_at TIMESTAMP);
        INSERT INTO crm_users VALUES
            ('101', 'alice@example.com', '5550101', CURRENT_TIMESTAMP),
            ('202', 'bob@example.com', '5550202', CURRENT_TIMESTAMP);
        INSERT INTO web_orders VALUES
            ('901', '5550101', CURRENT_TIMESTAMP),
            ('902', '5550202', CURRENT_TIMESTAMP);
    """)

    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "crm_users",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
            },
            {
                "id": "orders",
                "table": "web_orders",
                "entity_key": "order_id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "phone", "expr": "phone"}],
            },
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"},
            {"id": "r_phone", "match_keys": ["phone"], "canonicalize": "EXACT"},
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    IDRRunner(adapter).run(RunConfig(run_mode="FULL"))

    pm = PrivacyManager(adapter)

    # 1. Dry-run preview by email (discovers both crm:101 and orders:901 via transitive phone link!)
    preview = pm.forget(identifier_value="alice@example.com", dry_run=True)
    assert preview["status"] == "DRY_RUN"
    assert sorted(preview["entity_keys"]) == ["crm:101", "orders:901"]
    assert preview["entities_purged"] == 2
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current") == 4

    # 2. Execute live RTBF purge
    receipt = pm.forget(identifier_value="alice@example.com", dry_run=False, reason="GDPR_ART_17")
    assert receipt["status"] == "COMPLETED"
    assert sorted(receipt["entity_keys"]) == ["crm:101", "orders:901"]
    assert receipt["entities_purged"] == 2

    # Alice's entities and cluster are completely gone from output and work tables
    remaining_members = [
        r["entity_key"]
        for r in adapter.query("SELECT entity_key FROM idr_out.identity_resolved_membership_current")
    ]
    assert sorted(remaining_members) == ["crm:202", "orders:902"]

    remaining_ids = [
        r["entity_key"]
        for r in adapter.query("SELECT entity_key FROM idr_out.identity_identifiers_current")
    ]
    assert "crm:101" not in remaining_ids
    assert "orders:901" not in remaining_ids

    # Audit log has SHA-256 receipt and NO cleartext 'alice@example.com'
    audit_rows = adapter.query("SELECT * FROM idr_meta.privacy_audit_log")
    assert len(audit_rows) == 1
    assert audit_rows[0]["status"] == "COMPLETED"
    assert audit_rows[0]["entities_purged"] == 2
    assert "alice" not in str(audit_rows[0]).lower()


def test_cluster_split_crosswalk(adapter):
    """
    Verify that when an existing cluster is split by a DO_NOT_LINK override in a subsequent run,
    one component retains the original resolved_id and the split component is logged in
    idr_out.resolved_id_crosswalk with change_type='SPLIT'.
    """
    adapter.execute("""
        CREATE TABLE split_users (id VARCHAR, email VARCHAR, updated_at TIMESTAMP);
        INSERT INTO split_users VALUES
            ('10', 'family@example.com', CURRENT_TIMESTAMP),
            ('20', 'family@example.com', CURRENT_TIMESTAMP);
    """)
    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "split_users",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
            }
        ],
        "rules": [{"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"}],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    # Run 1: crm:10 and crm:20 merge into resolved_id = 'crm:10'
    IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    m1 = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    assert m1["crm:10"] == "crm:10"
    assert m1["crm:20"] == "crm:10"

    # Run 2: Steward adds DO_NOT_LINK between crm:10 and crm:20
    adapter.execute("""
        INSERT INTO idr_meta.identity_override (override_type, entity_key_a, entity_key_b, reason, is_active)
        VALUES ('DO_NOT_LINK', 'crm:10', 'crm:20', 'Not the same person', TRUE)
    """)
    IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    m2 = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    assert m2["crm:10"] == "crm:10"
    assert m2["crm:20"] == "crm:20"

    cw = adapter.query("""
        SELECT previous_resolved_id, current_resolved_id, change_type, entities_moved
        FROM idr_out.resolved_id_crosswalk
        WHERE previous_resolved_id = 'crm:10' AND current_resolved_id = 'crm:20'
    """)
    assert len(cw) == 1
    assert cw[0]["change_type"] == "SPLIT"
    assert cw[0]["entities_moved"] == 1


def test_cli_init_and_privacy_forget(tmp_path):
    """Verify `idr init` seeds default exclusions by default, and `idr privacy forget` purges clusters."""
    from idr_core.cli import main as cli_main

    db_file = str(tmp_path / "cli_enterprise.duckdb")

    # 1. `idr init` seeds default exclusions
    rc = cli_main(["init", "--db", db_file])
    assert rc == 0

    ad = DuckDBAdapter(db_file)
    cnt = ad.query_one("SELECT COUNT(*) FROM idr_meta.identifier_exclusion")
    assert cnt == len(DEFAULT_EXCLUSIONS)

    # Seed a cluster and run IDR (use non-RFC2606 domain since default exclusions block %@example.com)
    ad.execute("""
        CREATE TABLE raw_u (id VARCHAR, email VARCHAR, updated_at TIMESTAMP);
        INSERT INTO raw_u VALUES ('1', 'forgetme@acmecorp.io', CURRENT_TIMESTAMP);
    """)
    cfg = {
        "sources": [
            {
                "id": "u",
                "table": "raw_u",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
            }
        ],
        "rules": [{"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"}],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        ad.execute(stmt)
    IDRRunner(ad).run(RunConfig(run_mode="FULL"))
    ad.close()

    # 2. `idr privacy forget --identifier forgetme@acmecorp.io`
    rc_forget = cli_main(["privacy", "forget", "--db", db_file, "--identifier", "forgetme@acmecorp.io"])
    assert rc_forget == 0

    ad2 = DuckDBAdapter(db_file)
    assert ad2.query_one("SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current") == 0
    assert ad2.query_one("SELECT COUNT(*) FROM idr_meta.privacy_audit_log") == 1
    ad2.close()


def test_mcp_forget_and_override_tools(adapter):
    """Verify MCP tools manage_identity_override and forget_customer_identity."""
    from idr_core.connection_manager import ConnectionManager
    from idr_mcp.server import forget_customer_identity, manage_identity_override

    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})

    # 1. Add, list, and remove override via MCP tool
    add_res = manage_identity_override(
        action="add",
        override_type="FORCE_LINK",
        entity_key_a="crm:1",
        entity_key_b="crm:2",
        reason="Agent confirmed merge",
    )
    assert add_res["status"] == "SUCCESS"

    list_res = manage_identity_override(action="list")
    assert len(list_res["overrides"]) == 1
    assert list_res["overrides"][0]["override_type"] == "FORCE_LINK"

    rem_res = manage_identity_override(
        action="remove",
        override_type="FORCE_LINK",
        entity_key_a="crm:1",
        entity_key_b="crm:2",
    )
    assert rem_res["status"] == "SUCCESS"
    assert len(manage_identity_override(action="list")["overrides"]) == 0

    # 2. Test forget_customer_identity dry_run on non-existent subject
    f_res = forget_customer_identity(identifier_value="nobody@example.com", dry_run=True)
    assert f_res["status"] == "DRY_RUN"
    assert f_res["entities_purged"] == 0


def test_ui_api_endpoints_for_enterprise_features(tmp_path, monkeypatch):
    """
    Verify the FastAPI UI endpoints (/api/setup/config/save, /api/overrides,
    /api/clusters/{id} crosswalk_history, and /api/privacy/forget) work end-to-end.
    """
    monkeypatch.setenv("IDR_ALLOW_INSECURE_DEV_AUTH", "true")
    from fastapi.testclient import TestClient

    from idr_api.main import app

    db_file = str(tmp_path / "ui_api_enterprise.duckdb")
    client = TestClient(app)

    # 1. Connect UI to DuckDB
    resp = client.post("/api/setup/connect", json={"platform": "duckdb", "params": {"path": db_file}})
    assert resp.status_code == 200

    # Seed source table
    ad = DuckDBAdapter(db_file)
    ad.execute("""
        CREATE TABLE ui_users (id VARCHAR, email VARCHAR, updated_at TIMESTAMP);
        INSERT INTO ui_users VALUES
            ('10', 'Alice.Smith+Promo@acmecorp.io', CURRENT_TIMESTAMP),
            ('20', 'alice.smith@acmecorp.io', CURRENT_TIMESTAMP);
    """)
    ad.close()

    # 2. Save config via UI with EMAIL_STRIP_PLUS, use_default_exclusions, and overrides
    save_resp = client.post(
        "/api/setup/config/save",
        json={
            "config": {
                "sources": [
                    {
                        "id": "crm",
                        "table": "ui_users",
                        "entity_key": "id",
                        "watermark_column": "updated_at",
                        "identifiers": [{"type": "email", "expr": "email"}],
                    }
                ],
                "rules": [
                    {
                        "id": "r_email",
                        "type": "EXACT",
                        "match_keys": ["email"],
                        "canonicalize": "EMAIL_STRIP_PLUS",
                        "priority": 1,
                    }
                ],
                "use_default_exclusions": True,
                "overrides": [],
            }
        },
    )
    assert save_resp.status_code == 200

    # 3. Trigger FULL run from UI
    run_resp = client.post("/api/setup/run", json={"mode": "FULL", "strict": False, "max_iterations": 30, "dry_run": False})
    assert run_resp.status_code == 200
    assert run_resp.json()["clusters_impacted"] == 1

    # 4. Add a DO_NOT_LINK override via /api/overrides and re-run to trigger a SPLIT crosswalk
    ov_resp = client.post(
        "/api/overrides",
        json={
            "action": "add",
            "override_type": "DO_NOT_LINK",
            "entity_key_a": "crm:10",
            "entity_key_b": "crm:20",
            "reason": "UI Explorer Unlink Pair",
        },
    )
    assert ov_resp.status_code == 200

    run2_resp = client.post("/api/setup/run", json={"mode": "FULL", "strict": False, "max_iterations": 30, "dry_run": False})
    assert run2_resp.status_code == 200

    # 5. Fetch cluster detail for crm:20 and verify crosswalk_history is returned to UI
    cluster_resp = client.get("/api/clusters/crm:20")
    assert cluster_resp.status_code == 200
    c_data = cluster_resp.json()
    assert len(c_data.get("crosswalk_history") or []) == 1
    assert c_data["crosswalk_history"][0]["previous_resolved_id"] == "crm:10"
    assert c_data["crosswalk_history"][0]["change_type"] == "SPLIT"

    # 6. Execute GDPR RTBF forget from UI via /api/privacy/forget
    rtbf_resp = client.post(
        "/api/privacy/forget",
        json={"resolved_id": "crm:20", "dry_run": False, "reason": "UI_GDPR_FORGET"},
    )
    assert rtbf_resp.status_code == 200
    assert rtbf_resp.json()["status"] == "COMPLETED"
    assert rtbf_resp.json()["entities_purged"] == 1


def test_incremental_no_changes_and_watermark_preservation(adapter):
    """
    Verify that:
    1. When a multi-table config has both watermarked tables and no-watermark tables,
       an INCR run with 0 new records returns SUCCESS_NO_CHANGES with 0 entities processed.
    2. When 1 new record arrives in one table, candidate expansion (watermark_value = NULL)
       does NOT wipe out last_watermark_value for the other watermarked tables!
    3. A subsequent INCR run with 0 new records again returns SUCCESS_NO_CHANGES (0 entities).
    4. IDRRunner progress_callback emits live stage events.
    """
    adapter.execute("""
        CREATE TABLE orders_tbl (order_id VARCHAR, email VARCHAR, order_ts TIMESTAMP);
        INSERT INTO orders_tbl VALUES
            ('o1', 'alice@acmecorp.io', TIMESTAMP '2026-01-01 10:00:00'),
            ('o2', 'bob@acmecorp.io', TIMESTAMP '2026-01-01 11:00:00');

        CREATE TABLE addresses_tbl (addr_id VARCHAR, email VARCHAR, city VARCHAR);
        INSERT INTO addresses_tbl VALUES
            ('a1', 'alice@acmecorp.io', 'Seattle'),
            ('a2', 'bob@acmecorp.io', 'Austin');
    """)

    cfg = {
        "sources": [
            {
                "id": "orders",
                "table": "orders_tbl",
                "entity_key": "order_id",
                "watermark_column": "order_ts",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [],
            },
            {
                "id": "addresses",
                "table": "addresses_tbl",
                "entity_key": "addr_id",
                "watermark_column": None,  # No watermark column!
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "city", "expr": "city"}],
            },
        ],
        "rules": [{"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"}],
        "survivorship": [{"attribute": "city", "strategy": "RECENCY", "recency_field": None}],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    events = []
    # Run 1: FULL run processes all 4 entities (2 orders + 2 addresses)
    r1 = IDRRunner(adapter, progress_callback=lambda e: events.append(e)).run(RunConfig(run_mode="FULL"))
    assert r1.status == "SUCCESS"
    assert r1.entities_processed == 4
    assert any(e["event"] == "stage_start" and e["stage"] == "entity_extraction" for e in events)
    assert any(e["event"] == "run_complete" for e in events)

    # Run 2: INCR run immediately on same dataset with 0 new records -> MUST short-circuit with 0 entities!
    r2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r2.status == "SUCCESS_NO_CHANGES"
    assert r2.entities_processed == 0

    # Run 3: Insert 1 new address row into the no-watermark table (links to alice@acmecorp.io in orders)
    adapter.execute("INSERT INTO addresses_tbl VALUES ('a3', 'alice@acmecorp.io', 'Portland')")
    r3 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r3.status == "SUCCESS"

    # Verify orders watermark in idr_meta.run_state was NOT wiped out to NULL by candidate expansion!
    orders_wm = adapter.query_one(
        "SELECT last_watermark_value FROM idr_meta.run_state WHERE table_id = 'orders'"
    )
    assert orders_wm is not None

    # Run 4: Subsequent INCR run with 0 new records -> MUST again return SUCCESS_NO_CHANGES (0 entities)!
    r4 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r4.status == "SUCCESS_NO_CHANGES"
    assert r4.entities_processed == 0

    # Run 5: In-place UPDATE to an existing row (addr_id = 'a2') in the no-watermark table!
    # Change bob's address email to alice@acmecorp.io -> INCR mode must detect the update via state diff
    # and merge addresses:a2 into alice's cluster!
    adapter.execute("UPDATE addresses_tbl SET email = 'alice@acmecorp.io' WHERE addr_id = 'a2'")
    r5 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r5.status == "SUCCESS"
    a2_cluster = adapter.query_one(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'addresses:a2'"
    )
    a1_cluster = adapter.query_one(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'addresses:a1'"
    )
    assert a2_cluster == a1_cluster



