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

__version__ = "0.5.1"

from .config import load_config, validate_config
from .runner import IDRRunner, RunConfig, RunResult

__all__ = [
    "IDRRunner",
    "RunConfig",
    "RunResult",
    "load_config",
    "validate_config",
    "__version__",
]
