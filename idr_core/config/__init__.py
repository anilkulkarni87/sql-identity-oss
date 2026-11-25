"""
Configuration management package for IDR (`idr_core.config`).

Decomposed into cohesive, bounded submodules (`<= 800` lines each) while preserving
100% backward compatibility on `from idr_core.config import ...`:
- `validators`: SQL identifier/FQN/expression validators, `DEFAULT_EXCLUSIONS`, watermark/attr helpers
- `sql_helpers`: Dialect-specific UPSERT (`ON CONFLICT` / `MERGE INTO`) and literal formatting
- `loader`: YAML loading (`load_config`) and top-level schema validation (`validate_config`)
- `roundtrip_sql`: Co-located `config_to_sql` and `sql_to_config`
- `dbt_export`: `config_to_dbt_seeds` CSV export for `dbt_idr`
- `warehouse_sync`: `apply_config_to_warehouse`, `config_snapshot` history, dry-run meta backup/restore
- `graph_diff`: `compute_dry_run_summary` and `compute_lens_diff`
"""

from __future__ import annotations

from .dbt_export import config_to_dbt_seeds
from .graph_diff import compute_dry_run_summary, compute_lens_diff
from .loader import HAS_YAML, load_config, validate_config
from .roundtrip_sql import config_to_sql, sql_to_config
from .validators import (
    _DANGEROUS_SQL_RE,
    _SAFE_FQN_RE,
    _SAFE_IDENTIFIER_RE,
    DEFAULT_EXCLUSIONS,
    build_where_clause,
    get_attr_expr,
    resolve_entity_key_expr,
    validate_float,
    validate_fqn,
    validate_identifier,
    validate_integer,
    validate_sql_expr,
)
from .warehouse_sync import (
    META_CONFIG_TABLES,
    apply_config_to_warehouse,
    get_config_snapshots,
    parse_config_snapshot_row,
    restore_meta_tables,
    snapshot_meta_tables,
)

__all__ = [
    "DEFAULT_EXCLUSIONS",
    "HAS_YAML",
    "META_CONFIG_TABLES",
    "_DANGEROUS_SQL_RE",
    "_SAFE_FQN_RE",
    "_SAFE_IDENTIFIER_RE",
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
