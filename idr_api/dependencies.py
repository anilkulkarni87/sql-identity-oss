import os
import time
from asyncio import Lock
from typing import Any, Dict, Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from idr_core.adapters.base import IDRAdapter
from idr_core.runner import IDRRunner
from idr_api.session_store import ConnectionSessionStore, load_connection_session_store

# --- Configuration ---
# Allow overriding these via env vars
OIDC_ISSUER = os.getenv("IDR_AUTH_ISSUER", "")
OIDC_AUDIENCE = os.getenv("IDR_AUTH_AUDIENCE", "account")
OIDC_JWKS_URL = os.getenv("IDR_AUTH_JWKS_URL", "")
OIDC_JWKS_TTL_SECONDS = int(os.getenv("IDR_AUTH_JWKS_TTL_SECONDS", "300"))
OIDC_JWKS_HTTP_TIMEOUT_SECONDS = float(os.getenv("IDR_AUTH_JWKS_HTTP_TIMEOUT_SECONDS", "5"))
ALLOW_INSECURE_DEV_AUTH = (
    os.getenv("IDR_ALLOW_INSECURE_DEV_AUTH", "false").strip().lower() in {"1", "true", "yes", "on"}
)
CONNECTION_IDLE_TTL_SECONDS = int(os.getenv("IDR_CONNECTION_IDLE_TTL_SECONDS", "3600"))
DEFAULT_SCOPE = "__default__"


class IDRManager:
    """Singleton to hold the active adapter."""

    _instance = None

    def __init__(self):
        self._store: ConnectionSessionStore = load_connection_session_store(
            ttl_seconds=CONNECTION_IDLE_TTL_SECONDS
        )

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = IDRManager()
        return cls._instance

    def set_adapter_for_user(self, user_key: str, adapter: IDRAdapter, config: Dict[str, Any]):
        self._store.set_adapter(user_key, adapter, config)

    def get_adapter_for_user(self, user_key: str) -> Optional[IDRAdapter]:
        return self._store.get_adapter(user_key)

    def get_config_for_user(self, user_key: str) -> Dict[str, Any]:
        return self._store.get_config(user_key)

    def disconnect_user(self, user_key: str) -> None:
        self._store.disconnect_user(user_key)

    def get_any_adapter(self) -> Optional[IDRAdapter]:
        return self._store.get_any_adapter()

    def connection_count(self) -> int:
        return self._store.connection_count()

    # Compatibility wrappers for existing callers/tests.
    def set_adapter(self, adapter: Optional[IDRAdapter], config: Dict[str, Any]):
        if adapter is None:
            self.disconnect()
            return
        self.set_adapter_for_user(DEFAULT_SCOPE, adapter, config)

    def get_adapter(self) -> Optional[IDRAdapter]:
        return self.get_adapter_for_user(DEFAULT_SCOPE)

    def is_connected(self, user_key: Optional[str] = None) -> bool:
        if user_key is None:
            return self.connection_count() > 0
        return self.get_adapter_for_user(user_key) is not None

    def disconnect(self) -> None:
        self._store.disconnect_all()

    def get_runner(self) -> IDRRunner:
        adapter = self.get_any_adapter()
        if not adapter:
            raise HTTPException(status_code=503, detail="Database not connected")
        return IDRRunner(adapter)


def get_manager() -> IDRManager:
    return IDRManager.get_instance()


def get_user_key(user: Dict[str, Any]) -> str:
    return str(user.get("sub") or user.get("email") or DEFAULT_SCOPE)


# --- Auth Logic ---

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

# Cache keys to avoid fetching on every request
_jwks_cache = {
    "keys_by_kid": {},
    "fetched_at": 0.0,
}
_jwks_lock = Lock()


def _jwks_endpoint() -> str:
    return OIDC_JWKS_URL if OIDC_JWKS_URL else f"{OIDC_ISSUER}/protocol/openid-connect/certs"


def _jwks_cache_is_valid(now: Optional[float] = None) -> bool:
    now = now if now is not None else time.time()
    keys_by_kid = _jwks_cache.get("keys_by_kid") or {}
    fetched_at = float(_jwks_cache.get("fetched_at") or 0.0)
    ttl = max(1, int(OIDC_JWKS_TTL_SECONDS))
    return bool(keys_by_kid) and (now - fetched_at) < ttl


def _reset_jwks_cache_for_tests() -> None:
    """Test helper to reset JWKS cache state."""
    _jwks_cache["keys_by_kid"] = {}
    _jwks_cache["fetched_at"] = 0.0


async def _fetch_jwks(force: bool = False) -> Dict[str, Dict[str, Any]]:
    """Fetch JWKS and cache by kid."""
    now = time.time()
    if not force and _jwks_cache_is_valid(now):
        return _jwks_cache["keys_by_kid"]

    async with _jwks_lock:
        now = time.time()
        if not force and _jwks_cache_is_valid(now):
            return _jwks_cache["keys_by_kid"]

        jwks_url = _jwks_endpoint()
        try:
            async with httpx.AsyncClient(timeout=OIDC_JWKS_HTTP_TIMEOUT_SECONDS) as client:
                resp = await client.get(jwks_url)
                resp.raise_for_status()
                payload = resp.json()
        except Exception as e:
            raise RuntimeError(f"Unable to fetch OIDC JWKS from {jwks_url}: {e}") from e

        keys = payload.get("keys") if isinstance(payload, dict) else None
        if not keys or not isinstance(keys, list):
            raise RuntimeError("OIDC JWKS response is missing a valid 'keys' list")

        keys_by_kid = {}
        for key in keys:
            if isinstance(key, dict) and key.get("kid"):
                keys_by_kid[key["kid"]] = key

        if not keys_by_kid:
            raise RuntimeError("OIDC JWKS did not include any keys with 'kid'")

        _jwks_cache["keys_by_kid"] = keys_by_kid
        _jwks_cache["fetched_at"] = now
        return keys_by_kid


async def _get_jwk_for_token(token: str) -> Dict[str, Any]:
    """Resolve the correct JWK by token header kid."""
    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token header: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing 'kid' header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    keys_by_kid = await _fetch_jwks(force=False)
    key = keys_by_kid.get(kid)
    if key:
        return key

    # One forced refresh in case keys rotated.
    keys_by_kid = await _fetch_jwks(force=True)
    key = keys_by_kid.get(kid)
    if key:
        return key

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=f"Unknown signing key id: {kid}",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_current_user(token: str = Depends(oauth2_scheme)):
    """
    Validate the JWT token and return the user.
    If IDR_AUTH_ISSUER is not set, auth fails closed unless
    IDR_ALLOW_INSECURE_DEV_AUTH=true is explicitly set.
    """
    # 1. Unconfigured Auth
    if not OIDC_ISSUER:
        if not ALLOW_INSECURE_DEV_AUTH:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "Authentication is not configured. Set IDR_AUTH_ISSUER "
                    "or explicitly allow insecure dev mode with "
                    "IDR_ALLOW_INSECURE_DEV_AUTH=true."
                ),
            )
        return {"sub": "dev-user", "email": "dev@local", "name": "Developer", "roles": ["admin"]}

    # 2. Production Mode (Verify Token)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        key = await _get_jwk_for_token(token)

        # Decode & Validate
        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=OIDC_AUDIENCE,
            issuer=OIDC_ISSUER,
            options={"verify_at_hash": False},  # Relax check for access tokens
        )

        if not payload.get("sub"):
            raise JWTError("Missing subject claim")

        return payload

    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        ) from e
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_adapter(current_user: Dict[str, Any] = Depends(get_current_user)) -> IDRAdapter:
    user_key = get_user_key(current_user)
    adapter = get_manager().get_adapter_for_user(user_key)
    if not adapter:
        raise HTTPException(status_code=400, detail="Database not connected")
    return adapter
