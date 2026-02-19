"""
Auth hardening tests for idr_api.dependencies.
"""

import asyncio
import os
import sys

import pytest
from fastapi import HTTPException
from jose import JWTError

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import idr_api.dependencies as deps


def test_get_jwk_for_token_retries_on_kid_miss(monkeypatch):
    async def fake_fetch_jwks(force=False):
        if force:
            return {"new-kid": {"kid": "new-kid"}}
        return {"old-kid": {"kid": "old-kid"}}

    monkeypatch.setattr(deps.jwt, "get_unverified_header", lambda _t: {"kid": "new-kid"})
    monkeypatch.setattr(deps, "_fetch_jwks", fake_fetch_jwks)

    key = asyncio.run(deps._get_jwk_for_token("token"))
    assert key["kid"] == "new-kid"


def test_get_jwk_for_token_unknown_kid_raises_401(monkeypatch):
    async def fake_fetch_jwks(force=False):
        return {"known-kid": {"kid": "known-kid"}}

    monkeypatch.setattr(deps.jwt, "get_unverified_header", lambda _t: {"kid": "missing-kid"})
    monkeypatch.setattr(deps, "_fetch_jwks", fake_fetch_jwks)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps._get_jwk_for_token("token"))

    assert exc.value.status_code == 401
    assert "Unknown signing key id" in exc.value.detail


def test_get_jwk_for_token_missing_kid_raises_401(monkeypatch):
    monkeypatch.setattr(deps.jwt, "get_unverified_header", lambda _t: {})

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps._get_jwk_for_token("token"))

    assert exc.value.status_code == 401
    assert "missing 'kid'" in exc.value.detail


def test_get_current_user_jwt_decode_error_maps_to_401(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setattr(deps, "OIDC_AUDIENCE", "account")

    async def fake_get_jwk_for_token(_t):
        return {"kid": "k1"}

    monkeypatch.setattr(deps, "_get_jwk_for_token", fake_get_jwk_for_token)

    def fake_decode(*_args, **_kwargs):
        raise JWTError("bad signature")

    monkeypatch.setattr(deps.jwt, "decode", fake_decode)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps.get_current_user(token="token"))

    assert exc.value.status_code == 401
    assert "Could not validate credentials" in exc.value.detail


def test_get_current_user_jwks_fetch_error_maps_to_503(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "https://issuer.example.com")

    async def fake_get_jwk_for_token(_t):
        raise RuntimeError("jwks unavailable")

    monkeypatch.setattr(deps, "_get_jwk_for_token", fake_get_jwk_for_token)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps.get_current_user(token="token"))

    assert exc.value.status_code == 503
    assert "jwks unavailable" in exc.value.detail


def test_get_current_user_dev_mode_returns_default_user(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    user = asyncio.run(deps.get_current_user(token=None))
    assert user["sub"] == "dev-user"


def test_get_current_user_dev_mode_fails_closed_without_override(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", False)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps.get_current_user(token=None))

    assert exc.value.status_code == 503
    assert "Authentication is not configured" in exc.value.detail
