"""Platform adapters for IDR with community plugin discovery."""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Any, Dict, List, Optional, Type

from .base import DIALECT_CONFIG, IDRAdapter, get_dialect_config

logger = logging.getLogger(__name__)

_ADAPTER_REGISTRY: Dict[str, Type[IDRAdapter]] = {}
_ADAPTER_PLUGINS_LOADED = False


def register_adapter(name: str, adapter_cls: Type[IDRAdapter]) -> None:
    """Register a platform adapter class under `name` (e.g. 'clickhouse', 'postgres')."""
    if not name or not isinstance(name, str):
        raise ValueError("Adapter name must be a non-empty string")
    _ADAPTER_REGISTRY[name.strip().lower()] = adapter_cls


def _register_builtins() -> None:
    if _ADAPTER_REGISTRY:
        return
    from .bigquery import BigQueryAdapter
    from .databricks import DatabricksAdapter
    from .duckdb import DuckDBAdapter
    from .snowflake import SnowflakeAdapter

    _ADAPTER_REGISTRY.update(
        {
            "duckdb": DuckDBAdapter,
            "snowflake": SnowflakeAdapter,
            "bigquery": BigQueryAdapter,
            "databricks": DatabricksAdapter,
        }
    )


def load_adapter_plugins(force: bool = False) -> int:
    """Load third-party warehouse adapters registered via Python entry_points."""
    global _ADAPTER_PLUGINS_LOADED
    _register_builtins()
    if _ADAPTER_PLUGINS_LOADED and not force:
        return 0
    _ADAPTER_PLUGINS_LOADED = True
    loaded = 0
    try:
        eps = entry_points(group="idr.adapters")
    except Exception as exc:
        logger.warning("Failed to inspect idr.adapters entry points: %s", exc)
        return 0

    for ep in eps:
        try:
            cls = ep.load()
            register_adapter(ep.name, cls)
            loaded += 1
        except Exception as exc:
            logger.warning("Failed to load adapter plugin '%s': %s", ep.name, exc)
    return loaded


def list_adapters() -> List[str]:
    """Return sorted list of registered adapter names (built-in + plugins)."""
    load_adapter_plugins()
    return sorted(_ADAPTER_REGISTRY.keys())


def get_adapter_class(name: str) -> Optional[Type[Any]]:
    """Look up an adapter class by platform name."""
    load_adapter_plugins()
    return _ADAPTER_REGISTRY.get(str(name or "").strip().lower())


__all__ = [
    "IDRAdapter",
    "DIALECT_CONFIG",
    "get_dialect_config",
    "register_adapter",
    "load_adapter_plugins",
    "list_adapters",
    "get_adapter_class",
]
