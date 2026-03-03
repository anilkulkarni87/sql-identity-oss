"""
Service account + scoped API token store.

Stores service accounts and token hashes in SQLite. Tokens are returned once
on creation and validated via constant-time hash comparisons.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _default_db_path() -> str:
    return os.getenv("IDR_SERVICE_AUTH_DB_PATH", "/tmp/idr_service_auth.sqlite3")


class ServiceTokenStore:
    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path or _default_db_path()
        self._lock = RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS service_accounts (
                    service_account_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS service_account_tokens (
                    token_id TEXT PRIMARY KEY,
                    service_account_id TEXT NOT NULL,
                    token_name TEXT,
                    token_hash TEXT NOT NULL,
                    permissions_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    last_used_at TEXT,
                    revoked_at TEXT,
                    FOREIGN KEY(service_account_id) REFERENCES service_accounts(service_account_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_service_tokens_account "
                "ON service_account_tokens(service_account_id, created_at DESC)"
            )

    @staticmethod
    def _token_hash(secret_value: str) -> str:
        return hashlib.sha256(secret_value.encode("utf-8")).hexdigest()

    @staticmethod
    def _token_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        payload = dict(row)
        payload["permissions"] = json.loads(payload.pop("permissions_json"))
        return payload

    def create_service_account(self, name: str, description: Optional[str] = None) -> Dict[str, Any]:
        account_id = f"sa_{uuid.uuid4().hex[:12]}"
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO service_accounts (
                    service_account_id, name, description, active, created_at, updated_at
                )
                VALUES (?, ?, ?, 1, ?, ?)
                """,
                (account_id, name.strip(), description, now_iso, now_iso),
            )
            row = conn.execute(
                "SELECT * FROM service_accounts WHERE service_account_id = ?",
                (account_id,),
            ).fetchone()
            return dict(row)

    def list_service_accounts(self) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    sa.*,
                    (
                      SELECT COUNT(*)
                      FROM service_account_tokens sat
                      WHERE sat.service_account_id = sa.service_account_id
                        AND sat.revoked_at IS NULL
                    ) AS active_token_count
                FROM service_accounts sa
                ORDER BY sa.created_at DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def create_token(
        self,
        service_account_id: str,
        permissions: List[str],
        token_name: Optional[str] = None,
        expires_in_hours: Optional[int] = None,
    ) -> Dict[str, Any]:
        token_id = f"sat{uuid.uuid4().hex[:12]}"
        secret = secrets.token_urlsafe(32)
        full_token = f"idr_sa_{token_id}_{secret}"
        token_hash = self._token_hash(secret)
        now_iso = _utc_now_iso()
        expires_at = None
        if expires_in_hours is not None:
            expires_at = (datetime.utcnow() + timedelta(hours=max(1, int(expires_in_hours)))).isoformat() + "Z"

        normalized_permissions = sorted({str(p).strip() for p in permissions if str(p).strip()})
        if not normalized_permissions:
            raise ValueError("permissions must include at least one permission")

        with self._lock, self._connect() as conn:
            account = conn.execute(
                "SELECT active FROM service_accounts WHERE service_account_id = ?",
                (service_account_id,),
            ).fetchone()
            if not account:
                raise ValueError("service account not found")
            if int(account["active"]) != 1:
                raise ValueError("service account is inactive")

            conn.execute(
                """
                INSERT INTO service_account_tokens (
                    token_id, service_account_id, token_name, token_hash, permissions_json,
                    created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token_id,
                    service_account_id,
                    token_name,
                    token_hash,
                    json.dumps(normalized_permissions),
                    now_iso,
                    expires_at,
                ),
            )

        return {
            "token_id": token_id,
            "service_account_id": service_account_id,
            "token_name": token_name,
            "permissions": normalized_permissions,
            "created_at": now_iso,
            "expires_at": expires_at,
            "token": full_token,
        }

    def list_tokens(self, service_account_id: str) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT token_id, service_account_id, token_name, permissions_json, created_at,
                       expires_at, last_used_at, revoked_at
                FROM service_account_tokens
                WHERE service_account_id = ?
                ORDER BY created_at DESC
                """,
                (service_account_id,),
            ).fetchall()
            return [self._token_row_to_dict(row) for row in rows]

    def revoke_token(self, token_id: str) -> Optional[Dict[str, Any]]:
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT token_id, service_account_id, token_name, permissions_json, created_at,
                       expires_at, last_used_at, revoked_at
                FROM service_account_tokens
                WHERE token_id = ?
                """,
                (token_id,),
            ).fetchone()
            if not row:
                return None
            if row["revoked_at"] is None:
                conn.execute(
                    "UPDATE service_account_tokens SET revoked_at = ? WHERE token_id = ?",
                    (now_iso, token_id),
                )
                row = conn.execute(
                    """
                    SELECT token_id, service_account_id, token_name, permissions_json, created_at,
                           expires_at, last_used_at, revoked_at
                    FROM service_account_tokens
                    WHERE token_id = ?
                    """,
                    (token_id,),
                ).fetchone()
            return self._token_row_to_dict(row)

    @staticmethod
    def _parse_token(token: str) -> Optional[Dict[str, str]]:
        # Format: idr_sa_<token_id>_<secret>
        parts = token.split("_", 3)
        if len(parts) != 4:
            return None
        if parts[0] != "idr" or parts[1] != "sa":
            return None
        token_id = parts[2].strip()
        secret = parts[3].strip()
        if not token_id or not secret:
            return None
        return {"token_id": token_id, "secret": secret}

    def authenticate_token(self, token: str) -> Optional[Dict[str, Any]]:
        parsed = self._parse_token(token)
        if not parsed:
            return None

        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    sat.token_id,
                    sat.service_account_id,
                    sat.token_hash,
                    sat.permissions_json,
                    sat.expires_at,
                    sat.revoked_at,
                    sa.name,
                    sa.active
                FROM service_account_tokens sat
                JOIN service_accounts sa
                  ON sa.service_account_id = sat.service_account_id
                WHERE sat.token_id = ?
                """,
                (parsed["token_id"],),
            ).fetchone()
            if not row:
                return None
            if int(row["active"]) != 1:
                return None
            if row["revoked_at"] is not None:
                return None
            if row["expires_at"] and row["expires_at"] < _utc_now_iso():
                return None

            candidate_hash = self._token_hash(parsed["secret"])
            if not hmac.compare_digest(candidate_hash, row["token_hash"]):
                return None

            conn.execute(
                "UPDATE service_account_tokens SET last_used_at = ? WHERE token_id = ?",
                (_utc_now_iso(), row["token_id"]),
            )

            permissions = json.loads(row["permissions_json"])
            return {
                "sub": f"service-account:{row['service_account_id']}",
                "service_account_id": row["service_account_id"],
                "service_account_name": row["name"],
                "token_id": row["token_id"],
                "roles": ["service_account"],
                "permissions": permissions,
                "scope": " ".join(permissions),
                "auth_type": "service_token",
            }

    def reset_for_tests(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM service_account_tokens")
            conn.execute("DELETE FROM service_accounts")


_SERVICE_TOKEN_STORE = ServiceTokenStore(db_path=_default_db_path())


def get_service_token_store() -> ServiceTokenStore:
    return _SERVICE_TOKEN_STORE
