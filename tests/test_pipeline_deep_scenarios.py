"""
Deep E2E scenario tests for SQL Identity Resolution pipeline:
1. Dry-run mode (NEW, MOVED, UNCHANGED diff calculation & non-mutation of current tables)
2. Strict mode (deterministic-only bypass of fuzzy rules vs full fuzzy super-clustering)
3. 4-hop transitive graph resolution across 3 source tables and 4 identifier types
4. Identifier exclusions (EXACT and LIKE wildcard patterns)
5. Preflight concurrency lock & automatic stale RUNNING recovery (>4h)
"""

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def duck_env(tmp_path):
    db_path = str(tmp_path / "deep_scenarios.duckdb")
    adapter = DuckDBAdapter(db_path)
    SchemaManager(adapter).initialize(reset=True)
    yield adapter
    adapter.close()


def test_dry_run_mode_computes_diffs_without_mutating_production_tables(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_crm (
            id VARCHAR, email VARCHAR, phone VARCHAR, updated_at TIMESTAMP
        );
        INSERT INTO raw_crm VALUES
            ('c1', 'alice@example.com', '111-1111', '2026-01-01 00:00:00'),
            ('c2', 'bob@example.com', '222-2222', '2026-01-01 00:00:00');

        INSERT INTO idr_meta.source_table VALUES
            ('crm', 'raw_crm', 'PERSON', 'id', 'updated_at', 0, TRUE);
        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE),
            ('r_phone', 'phone', 'NONE', 1000, 2, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES
            ('crm', 'email', 'email', FALSE),
            ('crm', 'phone', 'phone', FALSE);
    """)

    runner = IDRRunner(adapter)
    baseline = runner.run(RunConfig(run_mode="FULL", dry_run=False))
    assert baseline.status == "SUCCESS"

    before_members = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert len(before_members) == 2

    # Add c3 (bridges c1 and c2 via phone='222-2222' and email='alice@example.com') and c4 (brand new)
    adapter.execute("""
        INSERT INTO raw_crm VALUES
            ('c3', 'alice@example.com', '222-2222', '2026-01-02 00:00:00'),
            ('c4', 'newuser@example.com', '999-9999', '2026-01-02 00:00:00');
    """)

    dry_res = runner.run(RunConfig(run_mode="FULL", dry_run=True))
    assert dry_res.status == "DRY_RUN_COMPLETE"

    # 1. Production membership table MUST NOT be mutated by dry run
    after_members = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert after_members == before_members

    # 2. Verify dry_run_results and dry_run_summary
    diffs = adapter.query(
        f"SELECT entity_key, change_type, current_resolved_id, new_resolved_id FROM idr_out.dry_run_results WHERE run_id = '{dry_res.run_id}'"
    )
    diff_map = {r["entity_key"]: r["change_type"] for r in diffs}
    assert diff_map["crm:c1"] == "UNCHANGED"
    assert diff_map["crm:c2"] == "MOVED"  # Merged into crm:c1's cluster!
    assert diff_map["crm:c3"] == "NEW"
    assert diff_map["crm:c4"] == "NEW"

    summary = adapter.query(
        f"SELECT * FROM idr_out.dry_run_summary WHERE run_id = '{dry_res.run_id}'"
    )
    assert len(summary) == 1
    assert summary[0]["total_entities"] == 4
    assert summary[0]["new_entities"] == 2
    assert summary[0]["moved_entities"] == 1
    assert summary[0]["unchanged_entities"] == 1


def test_strict_mode_bypasses_fuzzy_matching(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_users (
            id VARCHAR, email VARCHAR, last_name VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
        );
        INSERT INTO raw_users VALUES
            ('u1', 'steven@a.com', 'miller', 'Steven', '2026-01-01 00:00:00'),
            ('u2', 'stephen@b.com', 'miller', 'Stephen', '2026-01-01 00:00:00');

        INSERT INTO idr_meta.source_table VALUES
            ('users', 'raw_users', 'PERSON', 'id', 'updated_at', 0, TRUE);
        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES
            ('users', 'email', 'email', FALSE);
        INSERT INTO idr_meta.entity_attribute_mapping VALUES
            ('users', 'last_name', 'last_name'),
            ('users', 'first_name', 'first_name');
        INSERT INTO idr_meta.fuzzy_rule VALUES
            ('f_name', 'Name Sim', 'last_name', 'jaro_winkler_similarity(<a>.first_name, <b>.first_name)', 0.80, 1, TRUE);
    """)

    runner = IDRRunner(adapter)

    # 1. Run with strict=True -> fuzzy rule must be skipped
    res_strict = runner.run(RunConfig(run_mode="FULL", strict=True))
    assert res_strict.status == "SUCCESS"
    rows_strict = adapter.query(
        "SELECT entity_key, resolved_id, super_cluster_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert rows_strict[0]["super_cluster_id"] != rows_strict[1]["super_cluster_id"]

    # 2. Run with strict=False -> fuzzy rule merges u1 and u2 into a shared super_cluster_id
    res_fuzzy = runner.run(RunConfig(run_mode="FULL", strict=False))
    assert res_fuzzy.status == "SUCCESS"
    rows_fuzzy = adapter.query(
        "SELECT entity_key, resolved_id, super_cluster_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert rows_fuzzy[0]["super_cluster_id"] == rows_fuzzy[1]["super_cluster_id"]


def test_four_hop_transitive_chain_across_three_sources(duck_env):
    """
    Chain:
      crm:n1 --(email)--> web:n2 --(phone)--> pos:n3 --(loyalty)--> crm:n4 --(ssn)--> web:n5
    All 5 entities must converge to the lexicographically smallest entity_key ('crm:n1').
    """
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE src_crm (id VARCHAR, email VARCHAR, loyalty VARCHAR, ssn VARCHAR);
        CREATE TABLE src_web (id VARCHAR, email VARCHAR, phone VARCHAR, ssn VARCHAR);
        CREATE TABLE src_pos (id VARCHAR, phone VARCHAR, loyalty VARCHAR);

        INSERT INTO src_crm VALUES
            ('n1', 'chain@example.com', NULL, NULL),
            ('n4', NULL, 'LYL-777', 'SSN-999');
        INSERT INTO src_web VALUES
            ('n2', 'chain@example.com', '555-1234', NULL),
            ('n5', NULL, NULL, 'SSN-999');
        INSERT INTO src_pos VALUES
            ('n3', '555-1234', 'LYL-777');

        INSERT INTO idr_meta.source_table VALUES
            ('crm', 'src_crm', 'PERSON', 'id', NULL, 0, TRUE),
            ('web', 'src_web', 'PERSON', 'id', NULL, 0, TRUE),
            ('pos', 'src_pos', 'PERSON', 'id', NULL, 0, TRUE);

        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE),
            ('r_phone', 'phone', 'NONE', 1000, 2, TRUE),
            ('r_loyalty', 'loyalty', 'UPPERCASE', 1000, 3, TRUE),
            ('r_ssn', 'ssn', 'UPPERCASE', 1000, 4, TRUE);

        INSERT INTO idr_meta.identifier_mapping VALUES
            ('crm', 'email', 'email', FALSE),
            ('crm', 'loyalty', 'loyalty', FALSE),
            ('crm', 'ssn', 'ssn', FALSE),
            ('web', 'email', 'email', FALSE),
            ('web', 'phone', 'phone', FALSE),
            ('web', 'ssn', 'ssn', FALSE),
            ('pos', 'phone', 'phone', FALSE),
            ('pos', 'loyalty', 'loyalty', FALSE);
    """)

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", max_iters=20))
    assert res.status == "SUCCESS"
    assert res.entities_processed == 5

    members = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
    )
    assert len(members) == 5
    resolved_ids = {m["resolved_id"] for m in members}
    assert resolved_ids == {"crm:n1"}


def test_identifier_exclusions_exact_and_like_patterns(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_leads (id VARCHAR, email VARCHAR, phone VARCHAR);
        INSERT INTO raw_leads VALUES
            ('l1', 'test@test.com', '000-000-0000'),
            ('l2', 'test@test.com', '000-999-9999'),
            ('l3', 'bot1@noreply.org', '555-1111'),
            ('l4', 'bot2@noreply.org', '555-1111');

        INSERT INTO idr_meta.source_table VALUES
            ('leads', 'raw_leads', 'PERSON', 'id', NULL, 0, TRUE);
        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE),
            ('r_phone', 'phone', 'NONE', 1000, 2, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES
            ('leads', 'email', 'email', FALSE),
            ('leads', 'phone', 'phone', FALSE);

        INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
            ('email', 'test@test.com', 'EXACT', 'Test email'),
            ('email', '%@noreply.org', 'LIKE', 'No-reply domain'),
            ('phone', '000-%', 'LIKE', 'Invalid prefix');
    """)

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res.status == "SUCCESS"

    # l1 and l2 must NOT link because test@test.com and 000-% are excluded.
    # l3 and l4 MUST link via valid phone 555-1111.
    members = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query(
            "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
        )
    }
    assert members["leads:l1"] != members["leads:l2"]
    assert members["leads:l3"] == members["leads:l4"]


def test_preflight_blocks_concurrent_run_and_recovers_stale_run(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_c (id VARCHAR, email VARCHAR);
        INSERT INTO raw_c VALUES ('1', 'a@b.com');
        INSERT INTO idr_meta.source_table VALUES ('c', 'raw_c', 'PERSON', 'id', NULL, 0, TRUE);
        INSERT INTO idr_meta.rule VALUES ('r_email', 'email', 'LOWERCASE', 100, 1, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES ('c', 'email', 'email', FALSE);
    """)

    # 1. Insert active RUNNING run (started now)
    adapter.execute("""
        INSERT INTO idr_out.run_history (run_id, run_mode, status, started_at)
        VALUES ('run_active_lock', 'FULL', 'RUNNING', CURRENT_TIMESTAMP)
    """)

    runner = IDRRunner(adapter)
    blocked = runner.run(RunConfig(run_mode="FULL"))
    assert blocked.status == "FAILED"
    assert "Concurrent run detected: run_active_lock" in (blocked.error or "")

    # 2. Age run_active_lock to 5 hours ago -> Preflight marks it INTERRUPTED and succeeds
    adapter.execute("""
        UPDATE idr_out.run_history
        SET started_at = CURRENT_TIMESTAMP - INTERVAL '5 hours'
        WHERE run_id = 'run_active_lock'
    """)

    recovered = runner.run(RunConfig(run_mode="FULL"))
    assert recovered.status == "SUCCESS"

    old_status = adapter.query_one(
        "SELECT status FROM idr_out.run_history WHERE run_id = 'run_active_lock'"
    )
    assert old_status == "INTERRUPTED"
