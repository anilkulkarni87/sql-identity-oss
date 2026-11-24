"""
Repository structure, file-size ratchet, and test-suite organization guards.

Why this exists:
1. Prevents new source files from growing into 2,000-line monoliths (`MAX_NEW_SOURCE_LINES = 800`)
   and ratchets existing large files so feature additions extract focused modules/components.
2. Freezes legacy sprint/grab-bag test files (`FROZEN_LEGACY_TEST_FILES`) so new tests are placed
   into cohesive, domain-named test modules (never `test_tasks_*`, `test_phase_*`, `test_pr_*`).
3. Guards the public re-export surface of `idr_core.config` so decomposing `config.py` into a
   package preserves 100% backward compatibility across all callers.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Standard line budget for any newly created source module/component.
MAX_NEW_SOURCE_LINES = 800

# Standard line budget for domain-aligned test modules (excluding frozen legacy files below).
MAX_DOMAIN_TEST_LINES = 1050

# Ratcheted line ceilings for existing source files currently > MAX_NEW_SOURCE_LINES.
# Lower these ceilings when extracting submodules; do NOT raise them to cram in new features.
LEGACY_SOURCE_CEILINGS: Dict[str, int] = {
    "idr_core/config.py": 2200,
    "idr_core/cli.py": 1840,
    "idr_core/rule_generator.py": 1825,
    "idr_core/stages/graph.py": 1690,
    "idr_core/ai_assistant.py": 1615,
    "idr_core/profiler.py": 1585,
    "idr_core/ai_review.py": 1080,
    "idr_core/runner.py": 1060,
    "idr_core/stages/output.py": 1015,
    "idr_core/stages/extraction.py": 965,
    "idr_api/routers/setup.py": 855,
    "idr_mcp/server.py": 1700,
    "idr_ui/src/pages/Explorer.tsx": 2040,
    "idr_ui/src/pages/Setup/StepReview.tsx": 1350,
    "idr_ui/src/pages/Setup/StepMap.tsx": 1295,
    "idr_ui/src/pages/Setup/StepRules.tsx": 1265,
    "idr_ui/src/api/client.ts": 845,
}

# Frozen chronological / grab-bag test suites: (max_test_functions, max_lines).
# Do NOT append new `def test_*` functions here — add tests to the matching domain suite instead
# (e.g., `test_config.py`, `test_stages_and_profiles.py`, `test_api_and_cli.py`, `test_mcp_server.py`).
FROZEN_LEGACY_TEST_FILES: Dict[str, Tuple[int, int]] = {
    "tests/test_enterprise_features.py": (26, 1630),
    "tests/test_production_readiness.py": (19, 1075),
    "tests/test_bugfixes_and_preflight.py": (13, 700),
    "tests/test_tasks_1_2_3.py": (9, 350),
    "tests/test_batched_save_and_gdpr_suppression.py": (4, 190),
    "tests/test_sustainability_foundations.py": (4, 330),
}

# Banned sprint / PR / grab-bag naming patterns for any newly created test file.
_BANNED_TEST_NAME_RE = re.compile(
    r"^test_(tasks?|phase\d*|sprint\d*|pr\d*|issue\d*|bugfix(es)?|fix(es)?|misc|temp|tmp|new|wip)(_|$)",
    re.IGNORECASE,
)

_TEST_FUNC_RE = re.compile(r"^\s*(?:async\s+)?def\s+test_", re.MULTILINE)


def _line_count(path: Path) -> int:
    with path.open("rb") as fh:
        return sum(1 for _ in fh)


@pytest.mark.hygiene
@pytest.mark.unit
def test_source_file_size_ratchet():
    """Enforce <= 800 lines on new source files and ratcheted ceilings on legacy monoliths."""
    violations = []
    source_roots = ("idr_core", "idr_api", "idr_mcp", "idr_ui/src")

    for root_rel in source_roots:
        root = REPO_ROOT / root_rel
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix not in {".py", ".ts", ".tsx"}:
                continue
            if "__pycache__" in path.parts or "static" in path.parts or "dist" in path.parts:
                continue

            rel = path.relative_to(REPO_ROOT).as_posix()
            lines = _line_count(path)
            ceiling = LEGACY_SOURCE_CEILINGS.get(rel, MAX_NEW_SOURCE_LINES)
            if lines > ceiling:
                kind = "ratcheted legacy ceiling" if rel in LEGACY_SOURCE_CEILINGS else "new-module ceiling"
                violations.append(
                    f"{rel}: {lines} lines exceeds {kind} of {ceiling} lines "
                    f"(extract a focused submodule/component instead of growing this file)"
                )

    assert not violations, "Source file size ratchet violated:\n  - " + "\n  - ".join(violations)


@pytest.mark.hygiene
@pytest.mark.unit
def test_frozen_grab_bag_test_suites_do_not_grow():
    """Prevent appending new tests to legacy milestone/grab-bag test files."""
    violations = []

    for rel, (max_funcs, max_lines) in FROZEN_LEGACY_TEST_FILES.items():
        path = REPO_ROOT / rel
        if not path.exists():
            # Dissolving/removing a legacy grab-bag file in a cleanup PR is encouraged.
            continue
        text = path.read_text(encoding="utf-8")
        func_count = len(_TEST_FUNC_RE.findall(text))
        lines = len(text.splitlines())
        if func_count > max_funcs or lines > max_lines:
            violations.append(
                f"{rel}: has {func_count} tests / {lines} lines "
                f"(frozen at <= {max_funcs} tests / <= {max_lines} lines). "
                f"Place new tests in the appropriate domain test file instead."
            )

    assert not violations, "Frozen legacy test suite grew:\n  - " + "\n  - ".join(violations)


@pytest.mark.hygiene
@pytest.mark.unit
def test_domain_aligned_test_file_naming_and_size():
    """Require domain-aligned names (`test_<subsystem>.py`) and bounded sizes on test suites."""
    violations = []
    tests_dir = REPO_ROOT / "tests"

    for path in sorted(tests_dir.rglob("test_*.py")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in FROZEN_LEGACY_TEST_FILES:
            continue

        if _BANNED_TEST_NAME_RE.match(path.stem):
            violations.append(
                f"{rel}: sprint/milestone/bugfix test filename is not allowed; "
                f"name test files after the architectural domain (e.g., `test_privacy.py`, `test_lenses.py`)."
            )

        lines = _line_count(path)
        if lines > MAX_DOMAIN_TEST_LINES:
            violations.append(
                f"{rel}: {lines} lines exceeds domain test suite limit of {MAX_DOMAIN_TEST_LINES} lines; "
                f"split into focused domain test modules."
            )

    assert not violations, "Test suite hygiene violated:\n  - " + "\n  - ".join(violations)


@pytest.mark.hygiene
@pytest.mark.unit
def test_idr_core_config_public_api_contract():
    """Guard `idr_core.config` exports so future package decomposition stays 100% compatible."""
    import idr_core.config as cfg

    expected_exports = [
        "DEFAULT_EXCLUSIONS",
        "apply_config_to_warehouse",
        "build_where_clause",
        "compute_dry_run_summary",
        "compute_lens_diff",
        "config_to_dbt_seeds",
        "config_to_sql",
        "get_attr_expr",
        "get_config_snapshots",
        "load_config",
        "parse_config_snapshot_row",
        "resolve_entity_key_expr",
        "restore_meta_tables",
        "snapshot_meta_tables",
        "sql_to_config",
        "validate_config",
        "validate_float",
        "validate_fqn",
        "validate_identifier",
        "validate_integer",
        "validate_sql_expr",
    ]
    missing = [name for name in expected_exports if not hasattr(cfg, name)]
    assert not missing, f"idr_core.config is missing public exports: {missing}"


@pytest.mark.hygiene
def test_shared_seeded_duckdb_fixture_smoke(seeded_duckdb_adapter):
    """Verify the shared `seeded_duckdb_adapter` fixture in `conftest.py` runs end-to-end cleanly."""
    from idr_core.runner import IDRRunner, RunConfig

    result = IDRRunner(seeded_duckdb_adapter).run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"
    # C1 + C2 + O100 share alice@acme.io (1 cluster of 3); C3 + O101 share bob@acme.io (1 cluster of 2)
    rows = seeded_duckdb_adapter.query(
        "SELECT cluster_size, subject_count, child_count "
        "FROM idr_out.identity_clusters_current ORDER BY cluster_size DESC"
    )
    assert len(rows) == 2
    assert (rows[0]["cluster_size"], rows[0]["subject_count"], rows[0]["child_count"]) == (3, 2, 1)
    assert (rows[1]["cluster_size"], rows[1]["subject_count"], rows[1]["child_count"]) == (2, 1, 1)
