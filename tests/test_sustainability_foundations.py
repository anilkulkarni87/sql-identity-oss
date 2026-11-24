"""Tests for the 4 core sustainability foundations:
1. Declarative SchemaManager.ensure_schema() upgrades + opt-in generate_evidence
2. Structural role (PRIMARY/EVENT) decoupled from semantic entity_kind (zero cross-kind false merges)
3. Issuer-scoped identifiers (issuer / issuer_expr) with NULL/sentinel safety & pre-hash scoping
4. Domain-neutral MCP tools (resolve_entity_360, forget_entity_identity) + legacy customer aliases
"""

import duckdb

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import apply_config_to_warehouse, config_to_sql, sql_to_config, validate_config
from idr_core.connection_manager import ConnectionManager
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager
from idr_mcp import server as mcp_server


def test_declarative_schema_upgrade_and_opt_in_edge_evidence(tmp_path):
    """Verify legacy schemas auto-upgrade via SchemaManager.ensure_schema() and edge_evidence is opt-in."""
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)

    # Simulate an older v0.5 deployment missing entity_kind, issuer_expr, and cluster corroboration cols
    conn.execute("CREATE SCHEMA idr_meta")
    conn.execute("CREATE SCHEMA idr_work")
    conn.execute("CREATE SCHEMA idr_out")
    conn.execute("""
        CREATE TABLE idr_meta.source_table (
            table_id VARCHAR PRIMARY KEY,
            table_fqn VARCHAR,
            entity_type VARCHAR,
            entity_key_expr VARCHAR,
            watermark_column VARCHAR,
            is_active BOOLEAN
        )
    """)
    conn.execute("""
        CREATE TABLE idr_meta.identifier_mapping (
            table_id VARCHAR,
            identifier_type VARCHAR,
            identifier_value_expr VARCHAR,
            is_hashed BOOLEAN,
            PRIMARY KEY (table_id, identifier_type)
        )
    """)
    conn.execute("""
        CREATE TABLE idr_out.identity_clusters_current (
            resolved_id VARCHAR PRIMARY KEY,
            cluster_size INTEGER,
            run_id VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    conn.execute("CREATE TABLE raw_users (id VARCHAR, email VARCHAR, updated_at TIMESTAMP)")
    conn.execute("""
        INSERT INTO raw_users VALUES
        ('u1', 'a@acme.io', '2026-01-01 00:00:00'),
        ('u2', 'a@acme.io', '2026-01-02 00:00:00')
    """)

    applied = SchemaManager(adapter).ensure_schema()
    assert any("idr_meta.source_table.entity_kind" in item for item in applied)
    assert any("idr_meta.identifier_mapping.issuer_expr" in item for item in applied)
    assert any("idr_out.identity_clusters_current.subject_fanout" in item for item in applied)

    cfg = {
        "sources": [
            {
                "id": "users",
                "table": "raw_users",
                "entity_type": "PRIMARY",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "EMAIL", "expr": "email"}],
                "attributes": [],
            }
        ],
        "rules": [{"id": "r_email", "type": "EXACT", "match_keys": ["EMAIL"], "priority": 1}],
    }
    apply_config_to_warehouse(adapter, cfg)

    # 1. Default run: edge_evidence table exists, yet 0 rows written because generate_evidence defaults to False
    res1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))
    assert res1.status == "SUCCESS"
    assert adapter.table_exists("idr_out.edge_evidence")
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.edge_evidence") == 0

    # 2. Enabling generate_evidence in config persists via config_extension and does NOT invalidate INCR baseline
    cfg["generate_evidence"] = True
    apply_config_to_warehouse(adapter, cfg)
    roundtrip = sql_to_config(adapter)
    assert roundtrip.get("generate_evidence") is True

    conn.execute("INSERT INTO raw_users VALUES ('u3', 'a@acme.io', '2026-01-03 00:00:00')")
    res2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert res2.status == "SUCCESS"
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.edge_evidence") > 0
    adapter.close()


def test_entity_kind_isolation_prevents_cross_domain_merges():
    """Shared switchboard phone/name merges person<->person and org<->org, never person<->org."""
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)

    conn.execute("CREATE SCHEMA raw")
    conn.execute("""
        CREATE TABLE raw.contacts (id VARCHAR, phone VARCHAR, name VARCHAR)
    """)
    conn.execute("""
        INSERT INTO raw.contacts VALUES
        ('p1', '5551002000', 'Acme Corp'),
        ('p2', '5551002000', 'Acme Corp')
    """)
    conn.execute("""
        CREATE TABLE raw.organizations (org_id VARCHAR, main_phone VARCHAR, legal_name VARCHAR)
    """)
    conn.execute("""
        INSERT INTO raw.organizations VALUES
        ('o1', '5551002000', 'Acme Corp'),
        ('o2', '5551002000', 'Acme Corp')
    """)

    cfg = {
        "sources": [
            {
                "id": "contacts",
                "table": "raw.contacts",
                "entity_type": "PRIMARY",
                "entity_kind": "person",
                "entity_key": "id",
                "identifiers": [{"type": "PHONE", "expr": "phone"}],
                "attributes": [{"name": "name", "expr": "name"}],
            },
            {
                "id": "orgs",
                "table": "raw.organizations",
                "entity_type": "PRIMARY",
                "entity_kind": "organization",
                "entity_key": "org_id",
                "identifiers": [{"type": "PHONE", "expr": "main_phone"}],
                "attributes": [{"name": "name", "expr": "legal_name"}],
            },
        ],
        "rules": [
            {"id": "exact_phone", "type": "EXACT", "match_keys": ["PHONE"], "canonicalize": "PHONE_DIGITS", "priority": 1}
        ],
        "fuzzy_rules": [
            {
                "id": "fuzzy_name",
                "name": "Exact Name Block",
                "blocking_key": "LOWER(name)",
                "score_expr": "CASE WHEN LOWER(<a>.name) = LOWER(<b>.name) THEN 1.0 ELSE 0.0 END",
                "threshold": 0.9,
                "priority": 10,
            }
        ],
    }
    validate_config(cfg)
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    saved = sql_to_config(adapter)
    orgs_src = next(s for s in saved["sources"] if s["id"] == "orgs")
    assert orgs_src["entity_kind"] == "organization"
    assert orgs_src["entity_type"] == "PRIMARY"

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=False))
    assert res.status == "SUCCESS"

    rows = adapter.query(
        "SELECT entity_key, resolved_id, super_cluster_id FROM idr_out.identity_resolved_membership_current"
    )
    by_key = {r["entity_key"]: r for r in rows}

    # Contacts merge with each other; organizations merge with each other; NEVER across kinds
    assert by_key["contacts:p1"]["resolved_id"] == by_key["contacts:p2"]["resolved_id"]
    assert by_key["orgs:o1"]["resolved_id"] == by_key["orgs:o2"]["resolved_id"]
    assert by_key["contacts:p1"]["resolved_id"] != by_key["orgs:o1"]["resolved_id"]
    assert by_key["contacts:p1"]["super_cluster_id"] != by_key["orgs:o1"]["super_cluster_id"]

    # Changing entity_kind invalidates matching_config_hash and requires FULL rebuild before INCR
    cfg["sources"][1]["entity_kind"] = "vendor"
    apply_config_to_warehouse(adapter, cfg)
    incr_res = IDRRunner(adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert incr_res.status == "FAILED"
    assert incr_res.error_code == "FULL_REBUILD_REQUIRED"

    adapter.close()


def test_issuer_scoped_identifiers_and_null_issuer_guard():
    """Verify MRN/SKU issuer_expr scopes normalized values and drops NULL/unknown issuers safely."""
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)

    conn.execute("CREATE SCHEMA ehr")
    conn.execute("""
        CREATE TABLE ehr.patients (
            row_id VARCHAR,
            facility_id VARCHAR,
            mrn VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO ehr.patients VALUES
        ('r1', 'hosp_east', '10042'),
        ('r2', 'HOSP_EAST ', '10042'),
        ('r3', 'hosp_west', '10042'),
        ('r4', NULL, '10042'),
        ('r5', 'unknown', '10042')
    """)

    cfg = {
        "sources": [
            {
                "id": "ehr",
                "table": "ehr.patients",
                "entity_type": "PRIMARY",
                "entity_kind": "patient",
                "entity_key": "row_id",
                "identifiers": [{"type": "MRN", "expr": "mrn", "issuer": "facility_id"}],
                "attributes": [],
            }
        ],
        "rules": [
            {"id": "exact_mrn", "type": "EXACT", "match_keys": ["MRN"], "canonicalize": "ALPHANUMERIC_UPPER", "priority": 1}
        ],
    }
    validate_config(cfg)
    apply_config_to_warehouse(adapter, cfg)

    roundtrip = sql_to_config(adapter)
    assert roundtrip["sources"][0]["identifiers"][0]["issuer"] == "facility_id"

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))
    assert res.status == "SUCCESS"

    members = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    }
    # r1 and r2 share HOSP_EAST|10042 -> merged!
    assert members["ehr:r1"] == members["ehr:r2"]
    # r3 is HOSP_WEST|10042 -> distinct cluster!
    assert members["ehr:r3"] != members["ehr:r1"]
    # r4 (NULL issuer) and r5 ('unknown' issuer) must NOT merge with each other or with r1/r3!
    assert members["ehr:r4"] != members["ehr:r5"]
    assert members["ehr:r4"] not in (members["ehr:r1"], members["ehr:r3"])

    norm_vals = {
        r["identifier_value_norm"]
        for r in adapter.query("SELECT identifier_value_norm FROM idr_out.identity_identifiers_current")
    }
    assert norm_vals == {"HOSP_EAST|10042", "HOSP_WEST|10042"}
    adapter.close()


def test_mcp_domain_neutral_tools_and_legacy_aliases(monkeypatch):
    """Verify resolve_entity_360 and forget_entity_identity match legacy customer aliases."""
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)

    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE TABLE raw.accounts (id VARCHAR, domain VARCHAR, org_name VARCHAR)")
    conn.execute("""
        INSERT INTO raw.accounts VALUES
        ('a1', 'acme.io', 'Acme Inc'),
        ('a2', 'acme.io', 'Acme Incorporated')
    """)

    cfg = {
        "sources": [
            {
                "id": "acct",
                "table": "raw.accounts",
                "entity_type": "PRIMARY",
                "entity_kind": "organization",
                "entity_key": "id",
                "identifiers": [{"type": "DOMAIN", "expr": "domain"}],
                "attributes": [{"name": "org_name", "expr": "org_name"}],
            }
        ],
        "rules": [{"id": "exact_domain", "type": "EXACT", "match_keys": ["DOMAIN"], "priority": 1}],
        "survivorship": [{"attribute": "org_name", "strategy": "FREQUENCY"}],
    }
    apply_config_to_warehouse(adapter, cfg)
    IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))

    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    monkeypatch.setenv("IDR_PII_ACCESS", "full")

    ent_360 = mcp_server.resolve_entity_360("acme.io")
    cust_360 = mcp_server.resolve_customer_360("acme.io")
    assert ent_360["found"] is True
    assert ent_360["cluster_size"] == 2
    assert ent_360["resolved_id"] == cust_360["resolved_id"]

    preview = mcp_server.forget_entity_identity(identifier_value="acme.io", dry_run=True)
    assert preview["status"] == "DRY_RUN"
    assert preview["entities_purged"] == 2
    token = preview["confirmation_token"]

    purged = mcp_server.forget_entity_identity(
        identifier_value="acme.io",
        dry_run=False,
        confirmation_token=token,
    )
    assert purged["status"] == "COMPLETED"
    assert purged["entities_purged"] == 2
    adapter.close()
