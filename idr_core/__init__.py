"""
SQL Identity Resolution - Core Library

Multi-platform deterministic identity resolution for data warehouses.
Supports: DuckDB, BigQuery, Snowflake (Snowpark), Databricks

Usage:
    from idr_core import IDRRunner, load_config
    from idr_core.adapters.duckdb import DuckDBAdapter

    adapter = DuckDBAdapter("my_db.duckdb")
    runner = IDRRunner(adapter)
    result = runner.run()
"""

__version__ = "0.6.1"

from .canonicalizers import list_canonicalizers, register_canonicalizer
from .config import DEFAULT_EXCLUSIONS, load_config, validate_config
from .privacy import PrivacyManager
from .runner import IDRRunner, RunConfig, RunResult

__all__ = [
    "IDRRunner",
    "RunConfig",
    "RunResult",
    "load_config",
    "validate_config",
    "register_canonicalizer",
    "list_canonicalizers",
    "DEFAULT_EXCLUSIONS",
    "PrivacyManager",
    "__version__",
]
