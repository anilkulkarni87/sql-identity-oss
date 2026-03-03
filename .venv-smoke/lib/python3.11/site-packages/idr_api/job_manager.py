"""
Durable run job queue for async pipeline submission.

Backed by SQLite so submitted jobs survive API process restarts and can be
retried with simple policy controls.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request

from idr_core.secrets import get_secret

ACTIVE_STATUSES = {"QUEUED", "RETRYING", "RUNNING", "CANCEL_REQUESTED"}
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _default_db_path() -> str:
    return os.getenv("IDR_RUN_JOB_DB_PATH", "/tmp/idr_run_jobs.sqlite3")


class SQLiteRunJobManager:
    def __init__(
        self,
        db_path: Optional[str] = None,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 1.0,
    ):
        self._db_path = db_path or _default_db_path()
        self._max_attempts = max(1, int(max_attempts))
        self._retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self._webhook_url = os.getenv("IDR_RUN_JOB_WEBHOOK_URL", "").strip()
        self._webhook_timeout_seconds = float(
            os.getenv("IDR_RUN_JOB_WEBHOOK_TIMEOUT_SECONDS", "3.0")
        )
        self._lock = RLock()
        self._init_db()
        self._recover_inflight_jobs()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_jobs (
                    job_id TEXT PRIMARY KEY,
                    user_key TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    result_json TEXT,
                    error TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_jobs_user_submitted "
                "ON run_jobs(user_key, submitted_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_jobs_status_next_attempt "
                "ON run_jobs(status, next_attempt_at)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_job_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    user_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_job_events_job_event "
                "ON run_job_events(job_id, event_id DESC)"
            )

    def _recover_inflight_jobs(self) -> None:
        events: List[Dict[str, Any]] = []
        with self._lock, self._connect() as conn:
            now_iso = _utc_now_iso()
            now_ts = time.time()
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT job_id, user_key, status, cancel_requested, attempt_count, max_attempts, error
                FROM run_jobs
                WHERE status IN ('RUNNING', 'CANCEL_REQUESTED')
                """
            ).fetchall()

            for row in rows:
                if int(row["cancel_requested"]) == 1:
                    conn.execute(
                        """
                        UPDATE run_jobs
                        SET status = 'CANCELLED',
                            finished_at = ?,
                            error = COALESCE(error, 'Cancelled during restart recovery')
                        WHERE job_id = ?
                        """,
                        (now_iso, row["job_id"]),
                    )
                    events.append(
                        self._insert_event(
                            conn,
                            job_id=row["job_id"],
                            user_key=row["user_key"],
                            event_type="JOB_RECOVERED_CANCELLED",
                            status="CANCELLED",
                            payload={"reason": "cancel_requested_during_restart"},
                        )
                    )
                elif int(row["attempt_count"]) >= int(row["max_attempts"]):
                    conn.execute(
                        """
                        UPDATE run_jobs
                        SET status = 'FAILED',
                            finished_at = ?,
                            error = COALESCE(
                                error,
                                'Run interrupted and max attempts exhausted during restart recovery'
                            )
                        WHERE job_id = ?
                        """,
                        (now_iso, row["job_id"]),
                    )
                    events.append(
                        self._insert_event(
                            conn,
                            job_id=row["job_id"],
                            user_key=row["user_key"],
                            event_type="JOB_RECOVERED_FAILED",
                            status="FAILED",
                            payload={
                                "reason": "max_attempts_exhausted_on_restart",
                                "attempt_count": int(row["attempt_count"]),
                                "max_attempts": int(row["max_attempts"]),
                            },
                        )
                    )
                else:
                    conn.execute(
                        """
                        UPDATE run_jobs
                        SET status = 'RETRYING',
                            error = COALESCE(error, 'Recovered after API restart'),
                            next_attempt_at = ?
                        WHERE job_id = ?
                        """,
                        (now_ts, row["job_id"]),
                    )
                    events.append(
                        self._insert_event(
                            conn,
                            job_id=row["job_id"],
                            user_key=row["user_key"],
                            event_type="JOB_RECOVERED_RETRYING",
                            status="RETRYING",
                            payload={
                                "reason": "inflight_job_recovered",
                                "attempt_count": int(row["attempt_count"]),
                                "max_attempts": int(row["max_attempts"]),
                            },
                        )
                    )

            conn.execute("COMMIT")
        self._publish_events(events)

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        payload = dict(row)
        payload["request"] = json.loads(payload.pop("request_json"))
        payload["result"] = (
            json.loads(payload.pop("result_json")) if payload.get("result_json") else None
        )
        payload.pop("result_json", None)
        payload["cancel_requested"] = bool(payload.get("cancel_requested"))
        return payload

    @staticmethod
    def _event_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        payload = dict(row)
        payload["payload"] = (
            json.loads(payload.pop("payload_json")) if payload.get("payload_json") else None
        )
        payload.pop("payload_json", None)
        return payload

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        user_key: str,
        event_type: str,
        status: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        created_at = _utc_now_iso()
        conn.execute(
            """
            INSERT INTO run_job_events (
                job_id, user_key, event_type, status, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                user_key,
                event_type,
                status,
                json.dumps(payload) if payload is not None else None,
                created_at,
            ),
        )
        row = conn.execute(
            "SELECT * FROM run_job_events WHERE event_id = last_insert_rowid()"
        ).fetchone()
        return self._event_row_to_dict(row)

    def _post_webhook(self, event: Dict[str, Any]) -> None:
        if not self._webhook_url:
            return

        body = json.dumps(event).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        webhook_bearer_token = get_secret("IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN", default="")
        if webhook_bearer_token:
            headers["Authorization"] = f"Bearer {webhook_bearer_token}"

        req = urllib_request.Request(self._webhook_url, data=body, headers=headers, method="POST")
        with urllib_request.urlopen(req, timeout=self._webhook_timeout_seconds):
            return

    def _publish_events(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        for event in events:
            try:
                self._post_webhook(event)
            except Exception:
                # Webhook delivery is best effort; job transitions remain durable.
                pass

    def create_job(self, user_key: str, request: Dict[str, Any]) -> Dict[str, Any]:
        events: List[Dict[str, Any]] = []
        with self._lock, self._connect() as conn:
            now_iso = _utc_now_iso()
            job_id = f"job_{uuid.uuid4().hex[:12]}"
            conn.execute(
                """
                INSERT INTO run_jobs (
                    job_id, user_key, request_json, status, submitted_at,
                    max_attempts, next_attempt_at
                )
                VALUES (?, ?, ?, 'QUEUED', ?, ?, ?)
                """,
                (
                    job_id,
                    user_key,
                    json.dumps(request),
                    now_iso,
                    self._max_attempts,
                    time.time(),
                ),
            )
            events.append(
                self._insert_event(
                    conn,
                    job_id=job_id,
                    user_key=user_key,
                    event_type="JOB_SUBMITTED",
                    status="QUEUED",
                    payload={"request": request},
                )
            )
            row = conn.execute("SELECT * FROM run_jobs WHERE job_id = ?", (job_id,)).fetchone()
            result = self._row_to_dict(row)
        self._publish_events(events)
        return result

    def get_job(self, job_id: str, user_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if user_key is None:
                row = conn.execute("SELECT * FROM run_jobs WHERE job_id = ?", (job_id,)).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM run_jobs WHERE job_id = ? AND user_key = ?",
                    (job_id, user_key),
                ).fetchone()
            return self._row_to_dict(row) if row else None

    def list_jobs(self, user_key: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM run_jobs
                WHERE user_key = ?
                ORDER BY submitted_at DESC
                LIMIT ?
                """,
                (user_key, max(1, int(limit))),
            ).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def has_active_job_for_user(self, user_key: str) -> bool:
        with self._lock, self._connect() as conn:
            placeholders = ",".join(["?"] * len(ACTIVE_STATUSES))
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM run_jobs
                WHERE user_key = ? AND status IN ({placeholders})
                """,
                (user_key, *ACTIVE_STATUSES),
            ).fetchone()
            return bool(row and row["cnt"] > 0)

    def claim_next_job(self) -> Optional[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM run_jobs
                WHERE status IN ('QUEUED', 'RETRYING')
                  AND cancel_requested = 0
                  AND next_attempt_at <= ?
                ORDER BY submitted_at ASC
                LIMIT 1
                """,
                (time.time(),),
            ).fetchone()

            if not row:
                conn.execute("COMMIT")
                return None

            now_iso = _utc_now_iso()
            conn.execute(
                """
                UPDATE run_jobs
                SET status = 'RUNNING',
                    started_at = COALESCE(started_at, ?),
                    attempt_count = attempt_count + 1,
                    error = NULL
                WHERE job_id = ?
                """,
                (now_iso, row["job_id"]),
            )
            updated = conn.execute(
                "SELECT * FROM run_jobs WHERE job_id = ?",
                (row["job_id"],),
            ).fetchone()
            events.append(
                self._insert_event(
                    conn,
                    job_id=updated["job_id"],
                    user_key=updated["user_key"],
                    event_type="JOB_STARTED",
                    status="RUNNING",
                    payload={"attempt_count": updated["attempt_count"]},
                )
            )
            conn.execute("COMMIT")
            claimed = self._row_to_dict(updated)
        self._publish_events(events)
        return claimed

    def mark_succeeded(self, job_id: str, result: Dict[str, Any]) -> None:
        events: List[Dict[str, Any]] = []
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT cancel_requested, user_key FROM run_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if not row:
                return
            status = "CANCELLED" if row["cancel_requested"] else "SUCCEEDED"
            now_iso = _utc_now_iso()
            conn.execute(
                """
                UPDATE run_jobs
                SET status = ?, result_json = ?, finished_at = ?, error = NULL
                WHERE job_id = ?
                """,
                (status, json.dumps(result), now_iso, job_id),
            )
            events.append(
                self._insert_event(
                    conn,
                    job_id=job_id,
                    user_key=row["user_key"],
                    event_type="JOB_CANCELLED" if status == "CANCELLED" else "JOB_SUCCEEDED",
                    status=status,
                    payload={"result": result},
                )
            )
        self._publish_events(events)

    def mark_failed_or_retry(self, job_id: str, error: str) -> None:
        events: List[Dict[str, Any]] = []
        publish_now = False
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT attempt_count, max_attempts, cancel_requested, user_key
                FROM run_jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
            if not row:
                return

            now_iso = _utc_now_iso()
            if row["cancel_requested"]:
                conn.execute(
                    """
                    UPDATE run_jobs
                    SET status = 'CANCELLED', error = ?, finished_at = ?
                    WHERE job_id = ?
                    """,
                    (error, now_iso, job_id),
                )
                events.append(
                    self._insert_event(
                        conn,
                        job_id=job_id,
                        user_key=row["user_key"],
                        event_type="JOB_CANCELLED",
                        status="CANCELLED",
                        payload={"error": error},
                    )
                )
                publish_now = True

            elif row["attempt_count"] < row["max_attempts"]:
                backoff = self._retry_backoff_seconds * max(1, int(row["attempt_count"]))
                conn.execute(
                    """
                    UPDATE run_jobs
                    SET status = 'RETRYING',
                        error = ?,
                        next_attempt_at = ?
                    WHERE job_id = ?
                    """,
                    (error, time.time() + backoff, job_id),
                )
                events.append(
                    self._insert_event(
                        conn,
                        job_id=job_id,
                        user_key=row["user_key"],
                        event_type="JOB_RETRY_SCHEDULED",
                        status="RETRYING",
                        payload={"error": error, "next_retry_in_seconds": backoff},
                    )
                )
                publish_now = True

            else:
                conn.execute(
                    """
                    UPDATE run_jobs
                    SET status = 'FAILED',
                        error = ?,
                        finished_at = ?
                    WHERE job_id = ?
                    """,
                    (error, now_iso, job_id),
                )
                events.append(
                    self._insert_event(
                        conn,
                        job_id=job_id,
                        user_key=row["user_key"],
                        event_type="JOB_FAILED",
                        status="FAILED",
                        payload={"error": error},
                    )
                )
                publish_now = True
        if publish_now:
            self._publish_events(events)

    def cancel_job(self, job_id: str, user_key: str) -> Optional[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM run_jobs WHERE job_id = ? AND user_key = ?",
                (job_id, user_key),
            ).fetchone()
            if not row:
                conn.execute("COMMIT")
                return None

            status = row["status"]
            if status in TERMINAL_STATUSES:
                conn.execute("COMMIT")
                return self._row_to_dict(row)

            now_iso = _utc_now_iso()
            if status in {"QUEUED", "RETRYING"}:
                conn.execute(
                    """
                    UPDATE run_jobs
                    SET cancel_requested = 1,
                        status = 'CANCELLED',
                        error = 'Cancelled by user',
                        finished_at = ?
                    WHERE job_id = ?
                    """,
                    (now_iso, job_id),
                )
                events.append(
                    self._insert_event(
                        conn,
                        job_id=job_id,
                        user_key=user_key,
                        event_type="JOB_CANCELLED",
                        status="CANCELLED",
                        payload={"reason": "Cancelled by user before execution"},
                    )
                )
            else:
                conn.execute(
                    """
                    UPDATE run_jobs
                    SET cancel_requested = 1,
                        status = 'CANCEL_REQUESTED'
                    WHERE job_id = ?
                    """,
                    (job_id,),
                )
                events.append(
                    self._insert_event(
                        conn,
                        job_id=job_id,
                        user_key=user_key,
                        event_type="JOB_CANCEL_REQUESTED",
                        status="CANCEL_REQUESTED",
                        payload={"reason": "Cancellation requested while job is running"},
                    )
                )

            updated = conn.execute("SELECT * FROM run_jobs WHERE job_id = ?", (job_id,)).fetchone()
            conn.execute("COMMIT")
            cancelled = self._row_to_dict(updated)
        self._publish_events(events)
        return cancelled

    def list_job_events(
        self, job_id: str, user_key: str, limit: int = 100, after_event_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            params: List[Any] = [job_id, user_key]
            predicate = ""
            if after_event_id is not None:
                predicate = " AND event_id > ?"
                params.append(int(after_event_id))

            params.append(max(1, int(limit)))
            rows = conn.execute(
                f"""
                SELECT * FROM run_job_events
                WHERE job_id = ?
                  AND user_key = ?
                  {predicate}
                ORDER BY event_id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            return [self._event_row_to_dict(row) for row in rows]

    def reset_for_tests(self) -> None:
        """Clear all jobs. Intended for unit/integration tests only."""
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM run_jobs")
            conn.execute("DELETE FROM run_job_events")


_RUN_JOB_MANAGER = SQLiteRunJobManager(
    db_path=_default_db_path(),
    max_attempts=int(os.getenv("IDR_RUN_JOB_MAX_ATTEMPTS", "3")),
    retry_backoff_seconds=float(os.getenv("IDR_RUN_JOB_RETRY_BACKOFF_SECONDS", "1.0")),
)


def get_run_job_manager() -> SQLiteRunJobManager:
    return _RUN_JOB_MANAGER
