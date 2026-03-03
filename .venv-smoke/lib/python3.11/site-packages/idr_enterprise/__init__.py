"""
Enterprise add-ons for SQL Identity Resolution.

This package is intentionally optional and loaded dynamically via environment
configuration. OSS builds do not require it.
"""

from idr_enterprise.session_store import (
    EnterpriseInMemoryConnectionSessionStore,
    RedisConnectionSessionStore,
)

__all__ = [
    "EnterpriseInMemoryConnectionSessionStore",
    "RedisConnectionSessionStore",
]
