"""
Domain tests for multi-entity-kind graph isolation (`entity_kind`) and
issuer-scoped local identifiers (`issuer` / `issuer_expr`).
"""

from __future__ import annotations

import pytest

from idr_core.config import apply_config_to_warehouse, config_to_sql, sql_to_config, validate_config
from idr_core.runner import IDRRunner, RunConfig


@pytest.mark.e2e
def test_entity_kind_isolation_prevents_cross_domain_merges(duckdb_adapter):
    """Shared switchboard phone/name merges person<->person and org<->org, never person<->org."""
    duckdb_adapter.execute("CREATE SCHEMA raw")
    duckdb_adapter.execute("""
        CREATE TABLE raw.contacts (id VARCHAR, phone VARCHAR, name VARCHAR)
    """)
    duckdb_adapter.execute("""
        INSERT INTO raw.contacts VALUES
        ('p1', '5551002000', 'Acme Corp'),
        ('p2', '5551002000', 'Acme Corp')
    """)
    duckdb_adapter.execute("""
        CREATE TABLE raw.organizations (org_id VARCHAR, main_phone VARCHAR, legal_name VARCHAR)
    """)
    duckdb_adapter.execute("""
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
            {
                "id": "exact_phone",
                "type": "EXACT",
                "match_keys": ["PHONE"],
                "canonicalize": "PHONE_DIGITS",
                "priority": 1,
            }
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
        duckdb_adapter.execute(stmt)

    saved = sql_to_config(duckdb_adapter)
    orgs_src = next(s for s in saved["sources"] if s["id"] == "orgs")
    assert orgs_src["entity_kind"] == "organization"
    assert orgs_src["entity_type"] == "PRIMARY"

    res = IDRRunner(duckdb_adapter).run(RunConfig(run_mode="FULL", strict=False))
    assert res.status == "SUCCESS"

    rows = duckdb_adapter.query(
        "SELECT entity_key, resolved_id, super_cluster_id "
        "FROM idr_out.identity_resolved_membership_current"
    )
    by_key = {r["entity_key"]: r for r in rows}

    assert by_key["contacts:p1"]["resolved_id"] == by_key["contacts:p2"]["resolved_id"]
    assert by_key["orgs:o1"]["resolved_id"] == by_key["orgs:o2"]["resolved_id"]
    assert by_key["contacts:p1"]["resolved_id"] != by_key["orgs:o1"]["resolved_id"]
    assert by_key["contacts:p1"]["super_cluster_id"] != by_key["orgs:o1"]["super_cluster_id"]

    cfg["sources"][1]["entity_kind"] = "vendor"
    apply_config_to_warehouse(duckdb_adapter, cfg)
    incr_res = IDRRunner(duckdb_adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert incr_res.status == "FAILED"
    assert incr_res.error_code == "FULL_REBUILD_REQUIRED"


@pytest.mark.e2e
def test_issuer_scoped_identifiers_and_null_issuer_guard(duckdb_adapter):
    """Verify MRN/SKU issuer_expr scopes normalized values and drops NULL/unknown issuers safely."""
    duckdb_adapter.execute("CREATE SCHEMA ehr")
    duckdb_adapter.execute("""
        CREATE TABLE ehr.patients (
            row_id VARCHAR,
            facility_id VARCHAR,
            mrn VARCHAR
        )
    """)
    duckdb_adapter.execute("""
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
            {
                "id": "exact_mrn",
                "type": "EXACT",
                "match_keys": ["MRN"],
                "canonicalize": "ALPHANUMERIC_UPPER",
                "priority": 1,
            }
        ],
    }
    validate_config(cfg)
    apply_config_to_warehouse(duckdb_adapter, cfg)

    roundtrip = sql_to_config(duckdb_adapter)
    assert roundtrip["sources"][0]["identifiers"][0]["issuer"] == "facility_id"

    res = IDRRunner(duckdb_adapter).run(RunConfig(run_mode="FULL", strict=True))
    assert res.status == "SUCCESS"

    members = {
        r["entity_key"]: r["resolved_id"]
        for r in duckdb_adapter.query(
            "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
        )
    }
    assert members["ehr:r1"] == members["ehr:r2"]
    assert members["ehr:r3"] != members["ehr:r1"]
    assert members["ehr:r4"] != members["ehr:r5"]
    assert members["ehr:r4"] not in (members["ehr:r1"], members["ehr:r3"])

    norm_vals = {
        r["identifier_value_norm"]
        for r in duckdb_adapter.query(
            "SELECT identifier_value_norm FROM idr_out.identity_identifiers_current"
        )
    }
    assert norm_vals == {"HOSP_EAST|10042", "HOSP_WEST|10042"}
