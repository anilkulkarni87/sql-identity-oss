"""Dimension-aware state outputs with no domain-specific table or status assumptions."""

import copy

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import apply_config_to_warehouse, sql_to_config, validate_config
from idr_core.runner import IDRRunner, RunConfig


@pytest.fixture
def state_data():
    a = DuckDBAdapter(":memory:")
    a.execute("CREATE SCHEMA raw")
    a.execute(
        "CREATE TABLE raw.devices AS SELECT * FROM (VALUES ('r1','device1','north','componentA','working',TIMESTAMP '2026-01-01'),('r2','device1','north','componentB','paused',TIMESTAMP '2026-01-02'),('r3','device1','north','componentA','serviced',TIMESTAMP '2026-01-03')) t(record_id,device_id,site,component,state,event_time)"
    )
    cfg = {
        "sources": [
            {
                "id": "devices",
                "table": "raw.devices",
                "entity_key": "record_id",
                "watermark_column": "event_time",
                "identifiers": [{"type": "device", "expr": "device_id"}],
                "attributes": [{"name": "latest_site", "expr": "site"}],
            }
        ],
        "rules": [{"id": "device", "match_keys": ["device"], "canonicalize": "EXACT"}],
        "survivorship": [{"attribute": "latest_site", "strategy": "RECENCY"}],
        "grouped_outputs": [
            {
                "id": "component_state",
                "dimensions": ["location", "part"],
                "fields": ["condition"],
                "sources": [
                    {
                        "source_id": "devices",
                        "dimensions": {"location": "site", "part": "component"},
                        "fields": {"condition": "state"},
                        "recency_field": "event_time",
                    }
                ],
            }
        ],
    }
    apply_config_to_warehouse(a, cfg)
    yield a, cfg
    a.close()


def run(a, mode="FULL"):
    return IDRRunner(a).run(RunConfig(strict=True, run_mode=mode))


def test_grouped_state_preserves_independent_dimensions_and_provenance(state_data):
    a, cfg = state_data
    assert sql_to_config(a)["grouped_outputs"] == cfg["grouped_outputs"]
    result = run(a)
    assert result.status == "SUCCESS"
    assert result.grouped_output_counts == {"component_state": 2}
    rows = a.query(
        "SELECT location,part,condition,_idr_source_key,_idr_conflict FROM idr_out.grouped_component_state ORDER BY part"
    )
    assert rows == [
        {
            "location": "north",
            "part": "componentA",
            "condition": "serviced",
            "_idr_source_key": "r3",
            "_idr_conflict": False,
        },
        {
            "location": "north",
            "part": "componentB",
            "condition": "paused",
            "_idr_source_key": "r2",
            "_idr_conflict": False,
        },
    ]


def test_conflicting_states_fail_and_restore_profiles_and_memberships(state_data):
    a, _ = state_data
    assert run(a).status == "SUCCESS"
    before = {
        t: a.query(f"SELECT * FROM idr_out.{t} ORDER BY ALL")
        for t in [
            "golden_profiles",
            "identity_resolved_membership_current",
            "grouped_component_state",
            "resolved_id_crosswalk",
        ]
    }
    a.execute(
        "INSERT INTO raw.devices VALUES('r4','device1','north','componentA','different',TIMESTAMP '2026-01-03')"
    )
    result = run(a)
    assert result.status == "FAILED" and "conflicting equally authoritative" in result.error
    assert all(
        a.query(f"SELECT * FROM idr_out.{t} ORDER BY ALL") == rows for t, rows in before.items()
    )


def test_mark_conflicts_does_not_publish_arbitrary_state(state_data):
    a, cfg = state_data
    cfg["grouped_outputs"][0]["conflict_policy"] = "mark"
    apply_config_to_warehouse(a, cfg)
    a.execute(
        "INSERT INTO raw.devices VALUES('r4','device1','north','componentA','different',TIMESTAMP '2026-01-03')"
    )
    assert run(a).status == "SUCCESS"
    row = a.query(
        "SELECT condition,_idr_source_key,_idr_conflict FROM idr_out.grouped_component_state WHERE part='componentA'"
    )[0]
    assert row == {"condition": None, "_idr_source_key": None, "_idr_conflict": True}


def test_incremental_grouped_state_matches_full_rebuild(state_data):
    a, _ = state_data
    assert run(a).status == "SUCCESS"
    a.execute(
        "INSERT INTO raw.devices VALUES('r4','device1','north','componentB','replaced',TIMESTAMP '2026-02-01')"
    )
    assert run(a, "INCR").status == "SUCCESS"
    before = a.query(
        "SELECT resolved_id,location,part,condition,_idr_source_key FROM idr_out.grouped_component_state ORDER BY ALL"
    )
    assert run(a).status == "SUCCESS"
    assert (
        a.query(
            "SELECT resolved_id,location,part,condition,_idr_source_key FROM idr_out.grouped_component_state ORDER BY ALL"
        )
        == before
    )
    assert len(before) == 2


def test_grouped_output_rejects_unknown_sources_and_reserved_fields(state_data):
    _, cfg = state_data
    changed = copy.deepcopy(cfg)
    changed["grouped_outputs"][0]["sources"][0]["source_id"] = "unknown"
    with pytest.raises(ValueError, match="unknown source"):
        validate_config(changed)
    changed = copy.deepcopy(cfg)
    changed["grouped_outputs"][0]["fields"] = ["run_id"]
    with pytest.raises(ValueError, match="internal provenance"):
        validate_config(changed)


def test_grouped_state_can_be_previewed_without_publishing(state_data):
    from idr_core.ai_review import AIReviewService
    from idr_core.profile_preview import ProfilePreviewOptions, preview_profiles

    a, cfg = state_data
    service = AIReviewService(a)
    p = service.preview(cfg, {"strict": True})
    assert not a.table_exists("idr_out.grouped_component_state")
    result = preview_profiles(
        service,
        cfg,
        p["run_id"],
        p["draft_hash"],
        "default",
        ProfilePreviewOptions(
            mode="targeted", grouped_outputs=["component_state"], allow_unknown_scan_cost=True
        ),
    )
    assert result["grouped_outputs"] == {"component_state": {"rows": 2, "conflicts": 0}}
    assert not a.table_exists("idr_out.grouped_component_state")


def test_incremental_conflict_restores_affected_groups_and_profiles(state_data):
    a, _ = state_data
    assert run(a).status == "SUCCESS"
    before = {
        t: a.query(f"SELECT * FROM idr_out.{t} ORDER BY ALL")
        for t in [
            "golden_profiles",
            "identity_resolved_membership_current",
            "grouped_component_state",
        ]
    }
    a.execute(
        "INSERT INTO raw.devices VALUES('r4','device1','north','componentA','first',TIMESTAMP '2026-02-01'),('r5','device1','north','componentA','second',TIMESTAMP '2026-02-01')"
    )
    assert run(a, "INCR").status == "FAILED"
    assert all(
        a.query(f"SELECT * FROM idr_out.{t} ORDER BY ALL") == rows for t, rows in before.items()
    )


def test_missing_dimension_requires_explicit_policy(state_data):
    a, cfg = state_data
    a.execute("UPDATE raw.devices SET site=NULL WHERE record_id='r1'")
    assert run(a).status == "FAILED"
    cfg["grouped_outputs"][0]["null_dimensions"] = "keep"
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == "SUCCESS"
    assert (
        a.query_one("SELECT COUNT(*) FROM idr_out.grouped_component_state WHERE location IS NULL")
        == 1
    )
