"""
Append-only audit event store.

Used for admin/config/run control-plane actions. Events are immutable by
schema policy (SQLite triggers prevent UPDATE/DELETE).
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _default_db_path() -> str:
    return os.getenv("IDR_AUDIT_DB_PATH", "/tmp/idr_audit.sqlite3")


class AuditEventStore:
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
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts TEXT NOT NULL,
                    actor_sub TEXT NOT NULL,
                    actor_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    resource_id TEXT,
                    outcome TEXT NOT NULL,
                    details_json TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_events_ts "
                "ON audit_events(event_ts DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_events_action "
                "ON audit_events(action, event_ts DESC)"
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_update
                BEFORE UPDATE ON audit_events
                BEGIN
                    SELECT RAISE(ABORT, 'audit_events are immutable');
                END
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_delete
                BEFORE DELETE ON audit_events
                BEGIN
                    SELECT RAISE(ABORT, 'audit_events are immutable');
                END
                """
            )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        payload = dict(row)
        payload["details"] = (
            json.loads(payload.pop("details_json")) if payload.get("details_json") else None
        )
        return payload

    def append_event(
        self,
        actor_sub: str,
        actor_type: str,
        action: str,
        resource_type: str,
        resource_id: Optional[str],
        outcome: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_events (
                    event_ts, actor_sub, actor_type, action, resource_type, resource_id, outcome, details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _utc_now_iso(),
                    actor_sub,
                    actor_type,
                    action,
                    resource_type,
                    resource_id,
                    outcome,
                    json.dumps(details) if details is not None else None,
                ),
            )
            row = conn.execute(
                "SELECT * FROM audit_events WHERE event_id = last_insert_rowid()"
            ).fetchone()
            return self._row_to_dict(row)

    def list_events(
        self,
        limit: int = 100,
        action: Optional[str] = None,
        actor_sub: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            clauses: List[str] = []
            params: List[Any] = []
            if action:
                clauses.append("action = ?")
                params.append(action)
            if actor_sub:
                clauses.append("actor_sub = ?")
                params.append(actor_sub)

            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            params.append(max(1, int(limit)))

            rows = conn.execute(
                f"""
                SELECT * FROM audit_events
                {where}
                ORDER BY event_id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def reset_for_tests(self) -> None:
        with self._lock, self._connect() as conn:
            # Disable trigger only for deterministic tests.
            conn.execute("DROP TRIGGER IF EXISTS trg_audit_events_no_update")
            conn.execute("DROP TRIGGER IF EXISTS trg_audit_events_no_delete")
            conn.execute("DELETE FROM audit_events")
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_update
                BEFORE UPDATE ON audit_events
                BEGIN
                    SELECT RAISE(ABORT, 'audit_events are immutable');
                END
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_delete
                BEFORE DELETE ON audit_events
                BEGIN
                    SELECT RAISE(ABORT, 'audit_events are immutable');
                END
                """
            )

    def assert_immutable(self) -> bool:
        """Best-effort check used in tests to verify UPDATE/DELETE are blocked."""
        with self._lock, self._connect() as conn:
            try:
                conn.execute("UPDATE audit_events SET outcome = outcome WHERE 1=0")
            except sqlite3.DatabaseError:
                return True
            return False


_AUDIT_STORE = AuditEventStore(db_path=_default_db_path())


def get_audit_store() -> AuditEventStore:
    return _AUDIT_STORE


def emit_audit_event(
    current_user: Dict[str, Any],
    action: str,
    resource_type: str,
    resource_id: Optional[str],
    outcome: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    actor_sub = str(current_user.get("sub") or current_user.get("email") or "unknown")
    actor_type = str(current_user.get("auth_type") or "user")
    return get_audit_store().append_event(
        actor_sub=actor_sub,
        actor_type=actor_type,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        outcome=outcome,
        details=details,
    )
