"""
Lightweight non-blocking webhook emitter for autonomous IDR operations.

Emits JSON events to ``IDR_WEBHOOK_URL`` (with optional HMAC-SHA256 signature
via ``IDR_WEBHOOK_SECRET`` in ``X-IDR-Signature``) when pipeline runs finish,
skipped identifier groups spike, or GDPR erasure completes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def build_webhook_signature(body_bytes: bytes, secret: str) -> str:
    """Compute ``sha256=<hex>`` HMAC signature for a webhook payload."""
    digest = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def emit_webhook_event(
    event_type: str,
    payload: Dict[str, Any],
    webhook_url: Optional[str] = None,
    secret: Optional[str] = None,
    timeout: float = 3.0,
) -> bool:
    """
    Emit a structured JSON webhook event if ``IDR_WEBHOOK_URL`` (or ``webhook_url``) is set.

    Best-effort: network/HTTP failures are logged as warnings and never raise,
    so webhook delivery issues never break warehouse pipelines or RTBF purges.
    """
    url = (webhook_url if webhook_url is not None else os.environ.get("IDR_WEBHOOK_URL", "")).strip()
    if not url:
        return False

    signing_secret = (
        secret if secret is not None else os.environ.get("IDR_WEBHOOK_SECRET", "")
    ).strip()

    envelope = {
        "event": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": payload,
    }
    body_bytes = json.dumps(envelope, default=str).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "sql-identity-resolution/webhook",
        "X-IDR-Event": event_type,
    }
    if signing_secret:
        headers["X-IDR-Signature"] = build_webhook_signature(body_bytes, signing_secret)

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        logger.warning("Failed to deliver IDR webhook '%s' to %s: %s", event_type, url, exc)
        return False

