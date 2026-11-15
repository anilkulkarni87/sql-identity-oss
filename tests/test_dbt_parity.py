"""Cross-engine parity test: verifies idr_core and dbt_idr produce identical clusters and golden profiles on DuckDB."""

import csv
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_IDR_DIR = REPO_ROOT / "dbt_idr"


def _load_csv_into_duckdb(conn: duckdb.DuckDBPyConnection, table_name: str, csv_path: Path) -> None:
    conn.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=true)"
    )


def _read_csv_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_idr_core_and_dbt_idr_parity(tmp_path):
    venv_dbt = Path(sys.executable).parent / "dbt"
    dbt_bin = str(venv_dbt) if venv_dbt.exists() else shutil.which("dbt")
    if not dbt_bin:
        pytest.skip("dbt CLI not installed")

    try:
        import dbt.adapters.duckdb  # noqa: F401
    except Exception:
        pytest.skip("dbt-duckdb adapter not installed")

    # Copy dbt_idr into tmp_path without dbt_packages/target/logs/test_idr.duckdb
    # (dbt_packages/dbt_idr is a symlink to dbt_idr, which would recurse infinitely)
    work_dbt_idr = tmp_path / "dbt_idr"
    shutil.copytree(
        DBT_IDR_DIR,
        work_dbt_idr,
        ignore=shutil.ignore_patterns("dbt_packages", "target", "logs", "*.duckdb", "*.duckdb.wal"),
    )
    integ_dir = work_dbt_idr / "integration_tests"
    dbt_pkgs_dir = integ_dir / "dbt_packages"
    dbt_pkgs_dir.mkdir(exist_ok=True)
    (dbt_pkgs_dir / "dbt_idr").symlink_to(work_dbt_idr)
    src_dbt_utils = DBT_IDR_DIR / "integration_tests" / "dbt_packages" / "dbt_utils"
    if src_dbt_utils.exists():
        shutil.copytree(src_dbt_utils, dbt_pkgs_dir / "dbt_utils")

    # Run dbt seed + dbt run in isolated tmp_path
    for cmd in ([dbt_bin, "seed", "--profiles-dir", "."], [dbt_bin, "run", "--profiles-dir", "."]):
        proc = subprocess.run(
            cmd,
            cwd=str(integ_dir),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert proc.returncode == 0, f"{' '.join(cmd)} failed:\n{proc.stdout}\n{proc.stderr}"

    dbt_db_path = integ_dir / "test_idr.duckdb"
    assert dbt_db_path.exists()

    # Build matching idr_core DuckDB database from the exact same seed CSVs & config
    seeds_dir = integ_dir / "seeds"
    core_db_path = tmp_path / "core_idr.duckdb"
    core_adapter = DuckDBAdapter(str(core_db_path))
    try:
        _load_csv_into_duckdb(core_adapter.conn, "main.sample_crm_customers", seeds_dir / "sample_crm_customers.csv")
        _load_csv_into_duckdb(core_adapter.conn, "main.sample_orders", seeds_dir / "sample_orders.csv")
        _load_csv_into_duckdb(core_adapter.conn, "main.sample_pos_transactions", seeds_dir / "sample_pos_transactions.csv")

        SchemaManager(core_adapter).initialize(reset=True)

        sources_rows = _read_csv_rows(seeds_dir / "idr_sources.csv")
        id_map_rows = _read_csv_rows(seeds_dir / "idr_identifier_mappings.csv")
        attr_map_rows = _read_csv_rows(seeds_dir / "idr_attribute_mappings.csv")
        rule_rows = _read_csv_rows(seeds_dir / "idr_rules.csv")
        surv_rows = _read_csv_rows(seeds_dir / "idr_survivorship_rules.csv")
        excl_rows = _read_csv_rows(seeds_dir / "idr_exclusions.csv")

        config_dict = {
            "sources": [
                {
                    "id": s["source_id"],
                    "table": f"{s['schema']}.{s['table_name']}",
                    "entity_key": s["entity_key_column"],
                    "watermark_column": s["watermark_column"],
                    "identifiers": [
                        {"type": m["identifier_type"], "expr": m["column_name"]}
                        for m in id_map_rows
                        if m["source_id"] == s["source_id"]
                    ],
                    "attributes": [
                        {"name": a["attribute_name"], "expr": a["column_expr"]}
                        for a in attr_map_rows
                        if a["source_id"] == s["source_id"]
                    ],
                }
                for s in sources_rows
            ],
            "rules": [
                {
                    "id": r["rule_id"],
                    "type": "EXACT",
                    "match_keys": [r["identifier_type"]],
                    "priority": int(r["priority"]),
                    "canonicalize": r["canonicalize"],
                    "max_group_size": int(r["max_group_size"]),
                }
                for r in rule_rows
            ],
            "exclusions": [
                {
                    "identifier_type": e["identifier_type"],
                    "pattern": e["identifier_value_pattern"],
                    "match_type": e["match_type"],
                    "reason": e["reason"],
                }
                for e in excl_rows
            ],
            "survivorship": [
                {
                    "attribute": sv["attribute_name"],
                    "strategy": "RECENCY" if sv["strategy"] == "MOST_RECENT" else sv["strategy"],
                }
                for sv in surv_rows
            ],
        }

        sql_stmts = config_to_sql(config_dict, dialect="duckdb")
        for stmt in sql_stmts:
            core_adapter.execute(stmt)
        result = IDRRunner(core_adapter).run(RunConfig(run_mode="FULL"))
        assert result.status == "SUCCESS"

        # Query idr_core outputs
        core_members = core_adapter.query(
            "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
        )
        core_profiles = core_adapter.query(
            "SELECT resolved_id, email, phone, first_name, last_name FROM idr_out.golden_profiles"
        )
    finally:
        core_adapter.close()

    # Query dbt_idr outputs
    dbt_conn = duckdb.connect(str(dbt_db_path), read_only=True)
    try:
        dbt_members_rows = dbt_conn.execute(
            "SELECT entity_key, resolved_id FROM main_idr_out.identity_membership"
        ).fetchall()
        dbt_profiles_rows = dbt_conn.execute(
            "SELECT resolved_id, email_primary, phone_primary, first_name_primary, last_name_primary FROM main_idr_out.golden_profiles"
        ).fetchall()
    finally:
        dbt_conn.close()

    # 1. Identical entity_key universe
    core_by_entity = {r["entity_key"]: r["resolved_id"] for r in core_members}
    dbt_by_entity = {row[0]: row[1] for row in dbt_members_rows}
    assert set(core_by_entity.keys()) == set(dbt_by_entity.keys())

    # 2. Identical cluster partitions (entity_key sets per cluster)
    core_clusters = {}
    for ek, rid in core_by_entity.items():
        core_clusters.setdefault(rid, set()).add(ek)

    dbt_clusters = {}
    for ek, rid in dbt_by_entity.items():
        dbt_clusters.setdefault(rid, set()).add(ek)

    core_partitions = {frozenset(v) for v in core_clusters.values()}
    dbt_partitions = {frozenset(v) for v in dbt_clusters.values()}
    assert core_partitions == dbt_partitions

    # 3. Identical golden profile attributes per cluster partition
    def _norm_row(vals):
        return tuple(str(v) if v is not None else None for v in vals)

    core_prof_by_part = {
        frozenset(core_clusters[r["resolved_id"]]): _norm_row(
            (r["email"], r["phone"], r["first_name"], r["last_name"])
        )
        for r in core_profiles
    }
    dbt_prof_by_part = {
        frozenset(dbt_clusters[row[0]]): _norm_row((row[1], row[2], row[3], row[4]))
        for row in dbt_profiles_rows
    }
    assert core_prof_by_part == dbt_prof_by_part
