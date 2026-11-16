import base64
import json
import os
import threading
import urllib.parse
import urllib.request
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from idr_core.adapters.base import IDRAdapter


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(key, default)
    return val if val else default


def build_snowflake_connect_kwargs(config: Dict[str, Any]) -> Dict[str, Any]:
    """Build snowflake.connector.connect kwargs supporting Password, OAuth, and RSA Key-Pair JWT."""
    raw_account = (config.get("account") or _env("SNOWFLAKE_ACCOUNT") or "").strip()
    for prefix in ("https://", "http://"):
        if raw_account.lower().startswith(prefix):
            raw_account = raw_account[len(prefix) :]
    raw_account = raw_account.split("/")[0].strip()
    if raw_account.lower().endswith(".snowflakecomputing.com"):
        raw_account = raw_account[: -len(".snowflakecomputing.com")]

    kwargs: Dict[str, Any] = {
        "account": raw_account,
        "user": (config.get("user") or _env("SNOWFLAKE_USER") or "").strip() or None,
        "warehouse": (config.get("warehouse") or _env("SNOWFLAKE_WAREHOUSE") or "").strip() or None,
        "database": (
            config.get("sf_database") or config.get("database") or _env("SNOWFLAKE_DATABASE") or ""
        ).strip()
        or None,
        "schema": (
            config.get("schema_name") or config.get("schema") or _env("SNOWFLAKE_SCHEMA") or "PUBLIC"
        ).strip(),
        "login_timeout": 20,
    }

    role = (config.get("role") or _env("SNOWFLAKE_ROLE") or "").strip()
    if role:
        kwargs["role"] = role

    authenticator = (config.get("authenticator") or _env("SNOWFLAKE_AUTHENTICATOR") or "").strip()
    if authenticator:
        kwargs["authenticator"] = authenticator

    oauth_token = (config.get("token") or _env("SNOWFLAKE_OAUTH_TOKEN") or "").strip()
    if oauth_token:
        kwargs["token"] = oauth_token

    # RSA Key-Pair JWT authentication (preferred over single-factor password)
    key_path = (
        config.get("private_key_file")
        or config.get("private_key_path")
        or _env("SNOWFLAKE_PRIVATE_KEY_PATH")
        or _env("SNOWFLAKE_PRIVATE_KEY_FILE")
        or ""
    ).strip()
    key_pem = (config.get("private_key") or _env("SNOWFLAKE_PRIVATE_KEY") or "").strip()
    passphrase = (
        config.get("private_key_passphrase")
        or config.get("private_key_file_pwd")
        or _env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    )

    if key_path:
        kwargs["private_key_file"] = os.path.expanduser(key_path)
        if passphrase:
            kwargs["private_key_file_pwd"] = passphrase
        if not authenticator:
            kwargs["authenticator"] = "SNOWFLAKE_JWT"
    elif key_pem and "-----BEGIN" in key_pem:
        from cryptography.hazmat.primitives import serialization

        pwd_bytes = passphrase.encode("utf-8") if isinstance(passphrase, str) and passphrase else None
        try:
            p_key = serialization.load_pem_private_key(key_pem.encode("utf-8"), password=pwd_bytes)
        except TypeError:
            p_key = serialization.load_pem_private_key(key_pem.encode("utf-8"), password=None)
        kwargs["private_key"] = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        if not authenticator:
            kwargs["authenticator"] = "SNOWFLAKE_JWT"
    else:
        password = config.get("password") or _env("SNOWFLAKE_PASSWORD")
        if password:
            kwargs["password"] = password

    return {k: v for k, v in kwargs.items() if v is not None}


def _fetch_databricks_oauth_token(host: str, client_id: str, client_secret: str) -> str:
    """Exchange Databricks Service Principal client_id/secret for an OAuth M2M bearer token."""
    token_url = f"https://{host}/oidc/v1/token"
    body = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": "all-apis"}
    ).encode("utf-8")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        token_url,
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15.0) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    tok = payload.get("access_token")
    if not tok:
        raise RuntimeError(f"Databricks OAuth token response missing access_token: {payload}")
    return str(tok)


def build_databricks_connect_kwargs(config: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
    """Build databricks.sql.connect kwargs supporting PAT and OAuth M2M Service Principal."""
    raw_host = (
        config.get("server_hostname")
        or config.get("host")
        or _env("DATABRICKS_HOST")
        or _env("DATABRICKS_SERVER_HOSTNAME")
        or _env("DATABRICKS_URL")
        or _env("DATABRICKS_WORKSPACE_URL")
        or ""
    )
    clean_host = (
        raw_host.strip("'\" \t").replace("https://", "").replace("http://", "").split("/")[0]
    )
    http_path = (
        config.get("http_path")
        or _env("DATABRICKS_HTTP_PATH")
        or _env("DATABRICKS_SQL_HTTP_PATH")
        or ""
    ).strip("'\" \t")
    token = (
        config.get("access_token")
        or config.get("token")
        or _env("DATABRICKS_TOKEN")
        or _env("DATABRICKS_ACCESS_TOKEN")
        or _env("DATABRICKS_PAT")
        or ""
    ).strip("'\" \t")
    client_id = (config.get("client_id") or _env("DATABRICKS_CLIENT_ID") or "").strip("'\" \t")
    client_secret = (
        config.get("client_secret") or _env("DATABRICKS_CLIENT_SECRET") or ""
    ).strip("'\" \t")
    catalog = (config.get("catalog") or _env("DATABRICKS_CATALOG") or "main").strip("'\" \t") or None

    connect_kwargs: Dict[str, Any] = {
        "server_hostname": clean_host,
        "http_path": http_path,
    }
    if token:
        connect_kwargs["access_token"] = token
    elif client_id and client_secret:
        try:
            from databricks.sdk.core import Config, oauth_service_principal

            sdk_cfg = Config(
                host=f"https://{clean_host}",
                client_id=client_id,
                client_secret=client_secret,
            )
            connect_kwargs["credentials_provider"] = lambda: oauth_service_principal(sdk_cfg)
        except ImportError:
            connect_kwargs["access_token"] = _fetch_databricks_oauth_token(
                clean_host, client_id, client_secret
            )

    return connect_kwargs, catalog


def is_reconnectable_config(config: Optional[Dict[str, Any]]) -> bool:
    """Return True if config identifies a persistent warehouse/DB that can safely reconnect."""
    if not config:
        return False
    platform = str(config.get("platform") or "").strip().lower()
    if not platform:
        return False
    if platform == "duckdb":
        db = str(
            config.get("database")
            or config.get("path")
            or config.get("db_path")
            or _env("IDR_DATABASE")
            or _env("IDR_DB_PATH")
            or ""
        ).strip("'\" ")
        return bool(db and db != ":memory:")
    return True


def create_adapter(config: Dict[str, Any]) -> IDRAdapter:
    """Canonical factory constructing a platform IDRAdapter from config + env fallbacks."""
    platform = str(config.get("platform") or _env("IDR_PLATFORM") or "").strip().lower()
    schema_mapping = config.get("schema_mapping")

    if platform == "duckdb":
        import duckdb

        from idr_core.adapters.duckdb import DuckDBAdapter

        db_path = (
            config.get("database")
            or config.get("path")
            or config.get("db_path")
            or _env("IDR_DATABASE")
            or _env("IDR_DB_PATH")
            or ":memory:"
        )
        db_path = str(db_path).strip("'\"")
        conn = duckdb.connect(db_path, read_only=False)
        return DuckDBAdapter(conn, schema_mapping=schema_mapping)

    if platform == "bigquery":
        from google.cloud import bigquery

        from idr_core.adapters.bigquery import BigQueryAdapter

        project = config.get("project_id") or config.get("project") or _env("IDR_PROJECT")
        location = config.get("location") or _env("IDR_LOCATION") or "US"
        client = bigquery.Client(project=project)
        return BigQueryAdapter(client, project, location=location, dataset_mapping=schema_mapping)

    if platform == "snowflake":
        import snowflake.connector

        from idr_core.adapters.snowflake import SnowflakeAdapter

        sf_kwargs = build_snowflake_connect_kwargs(config)
        conn = snowflake.connector.connect(**sf_kwargs)
        return SnowflakeAdapter(conn, schema_mapping=schema_mapping)

    if platform == "databricks":
        from databricks import sql

        from idr_core.adapters.databricks import DatabricksAdapter

        dbx_kwargs, catalog = build_databricks_connect_kwargs(config)
        conn = sql.connect(**dbx_kwargs)
        return DatabricksAdapter(conn, catalog=catalog, schema_mapping=schema_mapping)

    raise ValueError(f"Unsupported or unspecified platform: '{platform}'")


class ConnectionState(str, Enum):
    """Explicit lifecycle states for database connectivity."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED_UNINITIALIZED = "CONNECTED_UNINITIALIZED"
    CONNECTED_READY = "CONNECTED_READY"
    STALE = "STALE"


class ConnectionManager:
    """
    Thread-safe singleton manager for database connections.
    Handles connection lifecycle, validation, and auto-reconnection.
    """

    _instance = None
    _init_lock = threading.Lock()
    _adapter: Optional[IDRAdapter] = None
    _connection_params: Dict[str, Any] = {}
    _state: ConnectionState = ConnectionState.DISCONNECTED
    _lock: threading.RLock = threading.RLock()

    def __new__(cls):
        with cls._init_lock:
            if cls._instance is None:
                inst = super(ConnectionManager, cls).__new__(cls)
                inst._lock = threading.RLock()
                cls._instance = inst
        return cls._instance

    def get_adapter(self, auto_reconnect: bool = True) -> Optional[IDRAdapter]:
        """Get the active adapter instance, attempting reconnect if params are stored."""
        with self._lock:
            if (
                self._adapter is None
                and auto_reconnect
                and is_reconnectable_config(self._connection_params)
            ):
                return self.reconnect()
            return self._adapter

    def set_adapter(
        self,
        adapter: IDRAdapter,
        params: Dict[str, Any],
        *,
        close_previous: bool = True,
    ):
        """Set a new active adapter and store params for reconnection."""
        with self._lock:
            if close_previous and self._adapter and self._adapter is not adapter:
                try:
                    self._adapter.close()
                except Exception as e:
                    print(f"Warning: Failed to close old adapter: {e}")

            self._adapter = adapter
            self._connection_params = dict(params or {})
            self._state = ConnectionState.CONNECTED_READY

    def reconnect(self) -> Optional[IDRAdapter]:
        """Recreate the adapter using stored connection parameters."""
        with self._lock:
            if not is_reconnectable_config(self._connection_params):
                return None
            if self._adapter is not None:
                try:
                    self._adapter.close()
                except Exception:
                    pass
                self._adapter = None
            new_adapter = create_adapter(self._connection_params)
            self._adapter = new_adapter
            self._state = ConnectionState.CONNECTED_READY
            return new_adapter

    def is_connected(self) -> bool:
        with self._lock:
            return self._adapter is not None

    def get_state(self, auto_reconnect: bool = False) -> ConnectionState:
        """Inspect and return the current connection lifecycle state."""
        with self._lock:
            if self._adapter is None:
                return ConnectionState.DISCONNECTED

            try:
                self._adapter.query_one("SELECT 1")
            except Exception:
                if auto_reconnect and is_reconnectable_config(self._connection_params):
                    try:
                        if self.reconnect() is None:
                            return ConnectionState.STALE
                    except Exception:
                        return ConnectionState.STALE
                else:
                    return ConnectionState.STALE

            try:
                if hasattr(self._adapter, "table_exists"):
                    if self._adapter.table_exists("idr_meta.source_table"):
                        return ConnectionState.CONNECTED_READY
                    return ConnectionState.CONNECTED_UNINITIALIZED
            except Exception:
                pass

            return ConnectionState.CONNECTED_READY

    def disconnect(self):
        with self._lock:
            if self._adapter:
                try:
                    self._adapter.close()
                except Exception:
                    pass
                self._adapter = None
                self._connection_params = {}
                self._state = ConnectionState.DISCONNECTED

    # Helper to get singleton instance easily
    @classmethod
    def instance(cls):
        return cls()
