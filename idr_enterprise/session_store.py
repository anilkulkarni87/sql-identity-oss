"""
Enterprise session store backends.

This module is optional and intended for enterprise deployments that need
pluggable connection-session lifecycle behavior.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from typing import Any, Callable, Dict, Optional

from idr_api.session_store import InMemoryConnectionSessionStore
from idr_core.adapters.base import IDRAdapter

LOGGER = logging.getLogger(__name__)

# Sensitive keys that must never be stored in plaintext inside Redis.
REDIS_SECRET_KEYS = frozenset({
    "password",
    "access_token",
    "token",
    "client_secret",
    "private_key",
    "private_key_passphrase",
    "private_key_file_pwd",
    "credentials_json",
    "credentials",
})
_ENC_MARKER = "__idr_enc__"


def _default_adapter_factory(config: Dict[str, Any]) -> Optional[IDRAdapter]:
    """Reconstruct an IDRAdapter from persisted session config across workers."""
    platform = (config.get("platform") or "").lower()
    if not platform:
        return None

    if platform == "duckdb":
        from idr_core.adapters.duckdb import DuckDBAdapter

        db_path = config.get("path") or config.get("db_path") or config.get("database") or ":memory:"
        if db_path == ":memory:":
            return None
        return DuckDBAdapter(db_path)

    if platform == "snowflake":
        import snowflake.connector

        from idr_core.adapters.snowflake import SnowflakeAdapter
        from idr_core.connection_manager import build_snowflake_connect_kwargs

        conn = snowflake.connector.connect(**build_snowflake_connect_kwargs(config))
        return SnowflakeAdapter(conn)

    if platform == "bigquery":
        from google.cloud import bigquery
        from google.oauth2 import service_account

        from idr_core.adapters.bigquery import BigQueryAdapter

        project = config.get("project") or config.get("project_id")
        credentials_json = config.get("credentials_json") or config.get("credentials")
        if credentials_json:
            info = (
                json.loads(credentials_json)
                if isinstance(credentials_json, str)
                else credentials_json
            )
            creds = service_account.Credentials.from_service_account_info(info)
            client = bigquery.Client(project=project, credentials=creds)
        else:
            client = bigquery.Client(project=project)
        return BigQueryAdapter(client, project=project, location=config.get("location", "US"))

    if platform == "databricks":
        from databricks import sql

        from idr_core.adapters.databricks import DatabricksAdapter
        from idr_core.connection_manager import build_databricks_connect_kwargs

        dbx_kwargs, catalog = build_databricks_connect_kwargs(config)
        conn = sql.connect(**dbx_kwargs)
        return DatabricksAdapter(conn, catalog=catalog)

    return None


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
    In-memory adapter registry with Redis-backed session config & lease tracking.

    Live adapter objects reside in-process for connection pooling, while Redis
    stores the session lease and connection configuration so any worker replica
    in a multi-worker/HA deployment can transparently reconstruct the adapter.
    """

    def __init__(
        self,
        ttl_seconds: int = 3600,
        redis_url: Optional[str] = None,
        namespace: str = "idr:session_store",
        redis_client: Optional[Any] = None,
        adapter_factory: Optional[Callable[[Dict[str, Any]], Optional[IDRAdapter]]] = None,
    ):
        super().__init__(ttl_seconds=ttl_seconds)
        self._adapter_factory = adapter_factory or _default_adapter_factory
        configured_namespace = os.getenv("IDR_REDIS_NAMESPACE", "").strip()
        self._namespace = configured_namespace or namespace

        if redis_client is not None:
            self._redis = redis_client
            self._redis_url = redis_url or "mock://redis"
            return

        self._redis_url = (redis_url or os.getenv("IDR_REDIS_URL", "")).strip()
        if not self._redis_url:
            raise RuntimeError(
                "IDR_REDIS_URL is required for RedisConnectionSessionStore "
                "(example: redis://redis:6379/0)"
            )

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

    @staticmethod
    def _get_fernet() -> Optional[Any]:
        secret = (os.environ.get("IDR_SESSION_SECRET_KEY") or "").strip()
        if not secret:
            return None
        try:
            from cryptography.fernet import Fernet
        except Exception as exc:
            raise RuntimeError(
                "IDR_SESSION_SECRET_KEY is configured, but the 'cryptography' package is not installed."
            ) from exc
        derived_key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
        return Fernet(derived_key)

    def _sanitize_config_for_redis(self, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Strip plaintext secrets by default, or encrypt them if IDR_SESSION_SECRET_KEY is set."""
        sanitized: Dict[str, Any] = dict(config or {})
        fernet = self._get_fernet()
        for key in list(sanitized.keys()):
            if key.lower() not in REDIS_SECRET_KEYS:
                continue
            val = sanitized[key]
            if val is None or val == "":
                sanitized.pop(key, None)
                continue
            if fernet is not None:
                raw_bytes = json.dumps(val).encode("utf-8")
                sanitized[key] = {_ENC_MARKER: fernet.encrypt(raw_bytes).decode("ascii")}
            else:
                sanitized.pop(key, None)
        return sanitized

    def _restore_config_from_redis(self, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt encrypted secret fields when possible, or drop unrecoverable secrets safely."""
        restored: Dict[str, Any] = dict(raw_config or {})
        fernet = self._get_fernet()
        for key in list(restored.keys()):
            val = restored[key]
            if isinstance(val, dict) and _ENC_MARKER in val:
                if fernet is None:
                    LOGGER.warning(
                        "Encrypted session secret '%s' found in Redis without IDR_SESSION_SECRET_KEY; "
                        "falling back to environment credentials.",
                        key,
                    )
                    restored.pop(key, None)
                    continue
                try:
                    decrypted = fernet.decrypt(str(val[_ENC_MARKER]).encode("ascii"))
                    restored[key] = json.loads(decrypted.decode("utf-8"))
                except Exception as exc:
                    LOGGER.warning(
                        "Failed to decrypt session secret '%s' from Redis (%s); "
                        "falling back to environment credentials.",
                        key,
                        exc,
                    )
                    restored.pop(key, None)
            elif key.lower() in REDIS_SECRET_KEYS:
                # Defensive scrub: reject/ignore any legacy plaintext secret stored in Redis.
                restored.pop(key, None)
        return restored

    def _write_lease(self, user_key: str, config: Optional[Dict[str, Any]] = None) -> None:
        try:
            ttl = max(0, int(self._ttl_seconds))
            if config is None:
                config = super().get_config(user_key)
            safe_config = self._sanitize_config_for_redis(config)
            payload = json.dumps({"updated_at": int(time.time()), "config": safe_config})
            if ttl > 0:
                self._redis.set(self._lease_key(user_key), payload, ex=ttl)
            else:
                self._redis.set(self._lease_key(user_key), payload)
        except Exception as exc:
            LOGGER.warning("Failed to update Redis lease for user %s: %s", user_key, exc)

    def _read_config_from_redis(self, user_key: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self._redis.get(self._lease_key(user_key))
            if not raw:
                return None
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and isinstance(parsed.get("config"), dict):
                return self._restore_config_from_redis(parsed["config"])
        except Exception as exc:
            LOGGER.warning("Failed to read Redis session for user %s: %s", user_key, exc)
        return None

    def _delete_lease(self, user_key: str) -> None:
        try:
            self._redis.delete(self._lease_key(user_key))
        except Exception as exc:
            LOGGER.warning("Failed to delete Redis lease for user %s: %s", user_key, exc)

    def set_adapter(self, user_key: str, adapter: IDRAdapter, config: Dict[str, Any]) -> None:
        super().set_adapter(user_key, adapter, config)
        self._write_lease(user_key, config=config)

    def get_adapter(self, user_key: str) -> Optional[IDRAdapter]:
        adapter = super().get_adapter(user_key)
        if adapter:
            self._write_lease(user_key)
            return adapter

        # Multi-worker recovery: reconstruct adapter from Redis session config
        config = self._read_config_from_redis(user_key)
        if config:
            try:
                reconstructed = self._adapter_factory(config)
                if reconstructed is not None:
                    super().set_adapter(user_key, reconstructed, config)
                    self._write_lease(user_key, config=config)
                    return reconstructed
            except Exception as exc:
                LOGGER.warning("Failed to reconstruct adapter for user %s: %s", user_key, exc)
        return None

    def get_config(self, user_key: str) -> Dict[str, Any]:
        cfg = super().get_config(user_key)
        if cfg:
            return cfg
        redis_cfg = self._read_config_from_redis(user_key)
        return dict(redis_cfg) if redis_cfg else {}

    def disconnect_user(self, user_key: str) -> None:
        super().disconnect_user(user_key)
        self._delete_lease(user_key)

    def disconnect_all(self) -> None:
        with self._lock:
            user_keys = list(self._states.keys())
        super().disconnect_all()
        for user_key in user_keys:
            self._delete_lease(user_key)
