"""
Enterprise session store backends.

This module is optional and intended for enterprise deployments that need
pluggable connection-session lifecycle behavior.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

from idr_api.session_store import InMemoryConnectionSessionStore
from idr_core.adapters.base import IDRAdapter

LOGGER = logging.getLogger(__name__)


class EnterpriseInMemoryConnectionSessionStore(InMemoryConnectionSessionStore):
    """
    Enterprise default store.

    Behavior matches OSS in-memory semantics; this class exists to provide a
    stable enterprise extension point for deployments.
    """

    def __init__(self, ttl_seconds: int = 3600):
        super().__init__(ttl_seconds=ttl_seconds)


class RedisConnectionSessionStore(InMemoryConnectionSessionStore):
    """
    In-memory adapter registry with Redis-backed lease tracking.

    Note: live adapter objects still reside in-process. Redis is used for
    session lease metadata so enterprise deployments can coordinate session
    lifecycle and visibility across replicas.
    """

    def __init__(
        self,
        ttl_seconds: int = 3600,
        redis_url: Optional[str] = None,
        namespace: str = "idr:session_store",
    ):
        super().__init__(ttl_seconds=ttl_seconds)
        self._redis_url = (redis_url or os.getenv("IDR_REDIS_URL", "")).strip()
        if not self._redis_url:
            raise RuntimeError(
                "IDR_REDIS_URL is required for RedisConnectionSessionStore "
                "(example: redis://redis:6379/0)"
            )

        configured_namespace = os.getenv("IDR_REDIS_NAMESPACE", "").strip()
        self._namespace = configured_namespace or namespace

        try:
            from redis import Redis
        except Exception as exc:
            raise RuntimeError(
                "Redis session store requires the 'redis' package. "
                "Install sql-identity-resolution[enterprise]."
            ) from exc

        self._redis = Redis.from_url(self._redis_url, decode_responses=True)
        try:
            self._redis.ping()
        except Exception as exc:
            raise RuntimeError(f"Unable to connect to Redis at {self._redis_url}: {exc}") from exc

    def _lease_key(self, user_key: str) -> str:
        return f"{self._namespace}:user:{user_key}"

    def _write_lease(self, user_key: str) -> None:
        try:
            ttl = max(0, int(self._ttl_seconds))
            value = str(int(time.time()))
            if ttl > 0:
                self._redis.set(self._lease_key(user_key), value, ex=ttl)
            else:
                self._redis.set(self._lease_key(user_key), value)
        except Exception as exc:
            LOGGER.warning("Failed to update Redis lease for user %s: %s", user_key, exc)

    def _delete_lease(self, user_key: str) -> None:
        try:
            self._redis.delete(self._lease_key(user_key))
        except Exception as exc:
            LOGGER.warning("Failed to delete Redis lease for user %s: %s", user_key, exc)

    def set_adapter(self, user_key: str, adapter: IDRAdapter, config: Dict[str, Any]) -> None:
        super().set_adapter(user_key, adapter, config)
        self._write_lease(user_key)

    def get_adapter(self, user_key: str) -> Optional[IDRAdapter]:
        adapter = super().get_adapter(user_key)
        if adapter:
            self._write_lease(user_key)
        return adapter

    def disconnect_user(self, user_key: str) -> None:
        super().disconnect_user(user_key)
        self._delete_lease(user_key)

    def disconnect_all(self) -> None:
        with self._lock:
            user_keys = list(self._states.keys())
        super().disconnect_all()
        for user_key in user_keys:
            self._delete_lease(user_key)
