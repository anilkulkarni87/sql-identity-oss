"""
Auth management router — service accounts and scoped API tokens.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..audit import emit_audit_event
from ..dependencies import (
    extract_user_roles,
    get_current_user,
    get_effective_permissions,
    require_permissions,
)
from ..service_auth import get_service_token_store

router = APIRouter(prefix="/api/auth", tags=["auth"])
service_tokens = get_service_token_store()


class ServiceAccountCreateRequest(BaseModel):
    name: str = Field(min_length=2, max_length=128)
    description: Optional[str] = None


class ServiceTokenCreateRequest(BaseModel):
    permissions: List[str] = Field(default_factory=list, min_length=1)
    token_name: Optional[str] = Field(default=None, max_length=128)
    expires_in_hours: Optional[int] = Field(default=None, ge=1, le=24 * 365)


@router.get("/whoami")
async def whoami(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Return the current authenticated principal."""
    scope = current_user.get("scope")
    if isinstance(scope, list):
        scope = " ".join(str(item).strip() for item in scope if str(item).strip()) or None
    elif not isinstance(scope, str):
        scope = None

    return {
        "sub": current_user.get("sub"),
        "auth_type": current_user.get("auth_type", "oidc_or_dev"),
        "roles": sorted(extract_user_roles(current_user)),
        "permissions": sorted(get_effective_permissions(current_user)),
        "scope": scope,
    }


@router.get("/service-accounts", dependencies=[Depends(require_permissions("auth.read"))])
async def list_service_accounts():
    return {"service_accounts": service_tokens.list_service_accounts()}


@router.post("/service-accounts", dependencies=[Depends(require_permissions("auth.manage"))])
async def create_service_account(
    request: ServiceAccountCreateRequest, current_user: Dict[str, Any] = Depends(get_current_user)
):
    try:
        account = service_tokens.create_service_account(
            name=request.name,
            description=request.description,
        )
        emit_audit_event(
            current_user=current_user,
            action="auth.service_account.create",
            resource_type="service_account",
            resource_id=account["service_account_id"],
            outcome="success",
            details={"name": account["name"]},
        )
        return {"service_account": account}
    except Exception as e:
        emit_audit_event(
            current_user=current_user,
            action="auth.service_account.create",
            resource_type="service_account",
            resource_id=None,
            outcome="error",
            details={"name": request.name, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/service-accounts/{service_account_id}/tokens",
    dependencies=[Depends(require_permissions("auth.read"))],
)
async def list_service_account_tokens(service_account_id: str):
    return {"tokens": service_tokens.list_tokens(service_account_id=service_account_id)}


@router.post(
    "/service-accounts/{service_account_id}/tokens",
    dependencies=[Depends(require_permissions("auth.manage"))],
)
async def create_service_account_token(
    service_account_id: str,
    request: ServiceTokenCreateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        token = service_tokens.create_token(
            service_account_id=service_account_id,
            permissions=request.permissions,
            token_name=request.token_name,
            expires_in_hours=request.expires_in_hours,
        )
        emit_audit_event(
            current_user=current_user,
            action="auth.service_token.create",
            resource_type="service_token",
            resource_id=token["token_id"],
            outcome="success",
            details={
                "service_account_id": service_account_id,
                "permissions": token["permissions"],
                "token_name": token.get("token_name"),
            },
        )
        return {"token": token}
    except Exception as e:
        emit_audit_event(
            current_user=current_user,
            action="auth.service_token.create",
            resource_type="service_token",
            resource_id=None,
            outcome="error",
            details={"service_account_id": service_account_id, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/tokens/{token_id}/revoke", dependencies=[Depends(require_permissions("auth.manage"))])
async def revoke_service_account_token(
    token_id: str, current_user: Dict[str, Any] = Depends(get_current_user)
):
    token = service_tokens.revoke_token(token_id=token_id)
    if not token:
        emit_audit_event(
            current_user=current_user,
            action="auth.service_token.revoke",
            resource_type="service_token",
            resource_id=token_id,
            outcome="error",
            details={"reason": "not_found"},
        )
        raise HTTPException(status_code=404, detail="Token not found")
    emit_audit_event(
        current_user=current_user,
        action="auth.service_token.revoke",
        resource_type="service_token",
        resource_id=token_id,
        outcome="success",
        details={"service_account_id": token.get("service_account_id")},
    )
    return {"token": token}
