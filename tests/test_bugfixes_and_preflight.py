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


def test_phase1_pk_as_identifier_and_domain_agnostic_join_discovery(adapter):
    """
    Verify Phase 1 intelligence:
    1. PK-to-FK promotion: orders.order_id stays entity_key AND is promoted to identifier_type='order_id'
       when shipments.order_id overlaps with it.
    2. Domain-agnostic join key discovery: healthcare mrn <-> patient_mrn is discovered via zero-PII
       hash overlap without hardcoded domain regexes, while descriptive 'city' is not promoted.
    """
    from idr_core.profiler import IDRDataProfiler

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.orders (order_id VARCHAR, total_amt DOUBLE)")
    adapter.execute("CREATE TABLE raw.shipments (shipment_id VARCHAR, order_id VARCHAR, carrier VARCHAR)")
    adapter.execute("""
        INSERT INTO raw.orders VALUES
            ('ORD-1001', 50.0), ('ORD-1002', 75.0), ('ORD-1003', 120.0), ('ORD-1004', 90.0)
    """)
    adapter.execute("""
        INSERT INTO raw.shipments VALUES
            ('SHP-1', 'ORD-1001', 'UPS'), ('SHP-2', 'ORD-1002', 'UPS'),
            ('SHP-3', 'ORD-1003', 'FEDEX'), ('SHP-4', 'ORD-1004', 'FEDEX')
    """)

    # Healthcare tables with domain-specific unlabeled keys (mrn <-> patient_mrn)
    adapter.execute("CREATE TABLE raw.patients (pat_row_id VARCHAR, mrn VARCHAR, city VARCHAR)")
    adapter.execute("CREATE TABLE raw.lab_results (lab_row_id VARCHAR, patient_mrn VARCHAR, city VARCHAR)")
    adapter.execute("""
        INSERT INTO raw.patients VALUES
            ('P1', 'MRN-88001', 'Seattle'), ('P2', 'MRN-88002', 'Seattle'),
            ('P3', 'MRN-88003', 'Austin'), ('P4', 'MRN-88004', 'Austin')
    """)
    adapter.execute("""
        INSERT INTO raw.lab_results VALUES
            ('L1', 'MRN-88001', 'Seattle'), ('L2', 'MRN-88002', 'Seattle'),
            ('L3', 'MRN-88003', 'Austin'), ('L4', 'MRN-88004', 'Austin')
    """)

    prof = IDRDataProfiler(adapter).profile_tables_safe(
        ["raw.orders", "raw.shipments", "raw.patients", "raw.lab_results"]
    )
    tmap = prof.table_map()

    # 1. PK-to-FK promotion on orders.order_id <-> shipments.order_id
    orders_pk = {c.name: c for c in tmap["raw.orders"].columns}["order_id"]
    ship_fk = {c.name: c for c in tmap["raw.shipments"].columns}["order_id"]
    assert orders_pk.hint_role == "entity_key"
    assert orders_pk.hint_identifier_type == "order_id"
    assert ship_fk.hint_identifier_type == "order_id"

    # 2. Domain-agnostic join discovery on patients.mrn <-> lab_results.patient_mrn
    pat_cols = {c.name: c for c in tmap["raw.patients"].columns}
    lab_cols = {c.name: c for c in tmap["raw.lab_results"].columns}
    assert pat_cols["mrn"].hint_identifier_type == "mrn"
    assert lab_cols["patient_mrn"].hint_identifier_type == "mrn"
    assert pat_cols["city"].hint_identifier_type is None
    assert lab_cols["city"].hint_identifier_type is None


def test_phase1_table_role_classification_and_event_attr_filtering(adapter):
    """
    Verify 4-way table role classification (MASTER_ENTITY, SUB_ENTITY, EVENT_FACT, REFERENCE_LOOKUP)
    and that EVENT_FACT tables do not auto-dump transactional columns into golden_profile attributes.
    """
    from idr_core.config_schema import IdentifierMappingSpec, PassATableDecision
    from idr_core.profiler import IDRDataProfiler
    from idr_core.rule_generator import AIRuleGenerator

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.customers (customer_id VARCHAR, email VARCHAR, first_name VARCHAR)")
    adapter.execute("CREATE TABLE raw.addresses (address_id VARCHAR, customer_id VARCHAR, city VARCHAR)")
    adapter.execute("CREATE TABLE raw.clickstream (event_id VARCHAR, customer_id VARCHAR, page_url VARCHAR, event_ts VARCHAR)")
    adapter.execute("CREATE TABLE raw.products (sku VARCHAR, product_name VARCHAR, price DOUBLE)")

    adapter.execute("""
        INSERT INTO raw.customers VALUES
            ('C1', 'a@corp.io', 'Alice'), ('C2', 'b@corp.io', 'Bob'), ('C3', 'c@corp.io', 'Carol')
    """)
    adapter.execute("""
        INSERT INTO raw.addresses VALUES
            ('A1', 'C1', 'Seattle'), ('A2', 'C1', 'Bellevue'), ('A3', 'C2', 'Austin'), ('A4', 'C3', 'Denver')
    """)
    adapter.execute("""
        INSERT INTO raw.clickstream VALUES
            ('E1', 'C1', '/home', '2026-01-01T10:00:00'),
            ('E2', 'C1', '/cart', '2026-01-01T10:05:00'),
            ('E3', 'C1', '/checkout', '2026-01-01T10:06:00'),
            ('E4', 'C2', '/home', '2026-01-02T11:00:00')
    """)
    adapter.execute("INSERT INTO raw.products VALUES ('SKU1', 'Shoes', 99.0), ('SKU2', 'Hat', 25.0)")

    prof = IDRDataProfiler(adapter).profile_tables_safe(
        ["raw.customers", "raw.addresses", "raw.clickstream", "raw.products"]
    )
    roles = {t.table_fqn: t.table_role for t in prof.tables}
    assert roles["raw.customers"] == "MASTER_ENTITY"
    assert roles["raw.addresses"] == "SUB_ENTITY"
    assert roles["raw.clickstream"] == "EVENT_FACT"
    assert roles["raw.products"] == "REFERENCE_LOOKUP"

    # Materialize sources via AIRuleGenerator: clickstream (EVENT_FACT) should not auto-populate page_url
    gen = AIRuleGenerator(adapter)
    decisions = [
        PassATableDecision(
            table="raw.customers",
            source_id="customers",
            include_in_idr=True,
            entity_key="customer_id",
            identifiers=[IdentifierMappingSpec(type="email", expr="email")],
            attributes=[],
            reasoning="Master customer table.",
        ),
        PassATableDecision(
            table="raw.clickstream",
            source_id="clickstream",
            include_in_idr=True,
            entity_key="event_id",
            watermark_column="event_ts",
            identifiers=[IdentifierMappingSpec(type="customer_id", expr="customer_id")],
            attributes=[],
            reasoning="Clickstream telemetry.",
        ),
    ]
    sources, _ = gen._materialize_sources(decisions, prof)
    src_by_id = {s.id: s for s in sources}
    # customers.customer_id was auto-merged back in as an identifier via PK->FK overlap!
    assert {i.type for i in src_by_id["customers"].identifiers} >= {"email", "customer_id"}
    assert [a.name for a in src_by_id["customers"].attributes] == ["first_name"]
    assert src_by_id["clickstream"].attributes == []


def test_phase2_pushdown_stats_heavy_hitters_and_progress_events(adapter):
    """
    Verify Phase 2 intelligence:
    1. Heavy-hitter SQL pushdown detects placeholder emails ('test@test.com', 'erased_user@anonymized.local')
       as auto-discovered exclusions AND flags statistical supernode outliers without exposing raw PII.
    2. Full-table pushdown stats run cleanly when row_count > sample_size.
    3. AIRuleGenerator emits structured progress events via on_progress.
    """
    from idr_core.profiler import IDRDataProfiler
    from idr_core.rule_generator import AIRuleGenerator

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.accounts (acct_id VARCHAR, email VARCHAR, phone VARCHAR)")

    rows = []
    # 35 rows sharing a placeholder email + 30 rows sharing a kiosk email (statistical outlier)
    for i in range(35):
        rows.append(f"('P{i}', 'test@test.com', '555-000-{1000+i}')")
    for i in range(30):
        rows.append(f"('K{i}', 'kiosk@store.internal', '555-111-{1000+i}')")
    for i in range(60):
        rows.append(f"('U{i}', 'user{i}@realmail.org', '555-222-{1000+i}')")
    adapter.execute(f"INSERT INTO raw.accounts VALUES {', '.join(rows)}")

    progress_events = []
    prof = IDRDataProfiler(adapter).profile_tables_safe(
        ["raw.accounts"],
        sample_size=50,  # Force full-table SQL pushdown (125 > 50)
        on_progress=lambda evt: progress_events.append(evt),
    )

    # Full-table pushdown updated true max_frequency_count (>= 35)
    email_col = {c.name: c for c in prof.table_map()["raw.accounts"].columns}["email"]
    assert email_col.max_frequency_count >= 35

    # Placeholder 'test@test.com' auto-discovered as exclusion
    excl_patterns = {e["identifier_value_pattern"] for e in prof.discovered_exclusions}
    assert "test@test.com" in excl_patterns

    # Heavy-hitter risk reported with counts only (zero raw PII in risk dict)
    assert len(prof.heavy_hitter_risks) >= 1
    risk = prof.heavy_hitter_risks[0]
    assert risk["max_frequency"] >= 35
    assert "kiosk@store.internal" not in str(risk)

    # Progress callback emitted discovery event
    assert any(e["stage"] == "discovery" for e in progress_events)

    # AIRuleGenerator.generate emits profiling + discovery events even in deterministic fallback
    gen_events = []
    plan = AIRuleGenerator(adapter).generate(
        ["raw.accounts"],
        sample_size=50,
        on_progress=lambda evt: gen_events.append(evt),
    )
    assert any(e["stage"] == "profiling" for e in gen_events)
    assert plan.profile_summary.get("discovered_exclusions_count", 0) >= 1


def test_phase3_dry_run_critique_telemetry_and_patches(adapter):
    """
    Verify Phase 3 closed-loop Dry Run critique:
    - Collects zero-PII cluster size histogram, per-source singleton rates, and zero-edge rules.
    - Proposes actionable one-click patches (remove unused rule, exclude high-singleton source,
      raise permissive fuzzy threshold).
    """
    from idr_core.ai_assistant import critique_dry_run_with_ai

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.customers (id VARCHAR, email VARCHAR)")
    adapter.execute("CREATE TABLE raw.clickstream (id VARCHAR, email VARCHAR, order_id VARCHAR)")

    cust_rows = [f"('c{i}', 'u{i}@corp.io')" for i in range(10)]
    click_rows = [f"('e{i}', 'u{i}@corp.io', 'o{i}')" for i in range(5)] + [
        f"('anon{i}', NULL, 'ao{i}')" for i in range(30)
    ]
    adapter.execute(f"INSERT INTO raw.customers VALUES {', '.join(cust_rows)}")
    adapter.execute(f"INSERT INTO raw.clickstream VALUES {', '.join(click_rows)}")

    cfg = {
        "use_default_exclusions": False,
        "sources": [
            {
                "id": "customers",
                "table": "raw.customers",
                "entity_key": "id",
                "identifiers": [{"type": "email", "expr": "email"}],
            },
            {
                "id": "clickstream",
                "table": "raw.clickstream",
                "entity_key": "id",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "order_id", "expr": "order_id"},
                ],
            },
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"},
            {"id": "r_order", "match_keys": ["order_id"], "canonicalize": "EXACT"},
        ],
        "fuzzy_rules": [
            {"id": "f_name", "name": "Name Fuzzy", "blocking_key": "x", "score_expr": "1.0", "threshold": 0.80}
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    dry_res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", dry_run=True))
    assert dry_res.status == "DRY_RUN_COMPLETE"

    critique = critique_dry_run_with_ai(
        adapter=adapter,
        draft_config=cfg,
        dry_run_summary={},
        warnings=dry_res.warnings,
    )
    assert critique["pii_masked"] is True
    assert critique["health"] in ("needs_tuning", "over_merged_risk")
    assert critique["telemetry"]["total_entities"] == 45
    assert any(z["identifier_type"] == "order_id" for z in critique["telemetry"]["zero_edge_rules"])

    actions = {f["patch"]["action"] for f in critique["findings"] if f.get("patch")}
    assert "remove_rule" in actions
    assert "remove_source" in actions
    assert "raise_fuzzy_threshold" in actions




