"""
Audit router — immutable audit event retrieval.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query

from ..audit import get_audit_store
from ..dependencies import require_permissions

router = APIRouter(prefix="/api/audit", tags=["audit"])
audit_store = get_audit_store()


@router.get("/events", dependencies=[Depends(require_permissions("audit.read"))])
async def list_audit_events(
    limit: int = Query(100, ge=1, le=1000),
    action: Optional[str] = Query(None),
    actor_sub: Optional[str] = Query(None),
):
    return {
        "events": audit_store.list_events(limit=limit, action=action, actor_sub=actor_sub),
    }
