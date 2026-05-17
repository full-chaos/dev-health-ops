from __future__ import annotations

import logging
import uuid as uuid_mod
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.api.middleware.rate_limit import AUTH_VALIDATE_LIMIT, limiter
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.refresh_tokens import revoke_token
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.errors import error_detail

from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import User

from .common import _parse_uuid
from .dependencies import get_current_user, get_current_user_optional

logger = logging.getLogger(__name__)

router = APIRouter()


class TokenValidateRequest(BaseModel):
    token: str


class TokenValidateResponse(BaseModel):
    valid: bool
    user_id: str | None = None
    email: str | None = None
    org_id: str | None = None
    role: str | None = None
    expires_at: str | None = None


class LogoutRequest(BaseModel):
    refresh_token: str


class MeResponse(BaseModel):
    id: str
    email: str
    username: str | None = None
    full_name: str | None = None
    org_id: str
    role: str
    is_superuser: bool = False
    permissions: list[str] = []


@router.get("/me", response_model=MeResponse)
async def get_me(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> MeResponse:
    from dev_health_ops.api.services.permissions import get_user_permissions

    permissions = list(get_user_permissions(user))

    return MeResponse(
        id=user.user_id,
        email=user.email,
        username=user.username,
        full_name=user.full_name,
        org_id=user.org_id,
        role=user.role,
        is_superuser=user.is_superuser,
        permissions=permissions,
    )


@router.post("/validate", response_model=TokenValidateResponse)
@limiter.limit(AUTH_VALIDATE_LIMIT)
async def validate_token(
    payload: TokenValidateRequest,
    request: Request,
) -> TokenValidateResponse:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(payload.token)

    if not user:
        return TokenValidateResponse(valid=False)

    try:
        user_uuid = uuid_mod.UUID(user.user_id)
    except ValueError:
        return TokenValidateResponse(valid=False)

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User.id, User.is_active).where(User.id == user_uuid)
        )
        db_user = result.one_or_none()

    if not db_user or not db_user.is_active:
        return TokenValidateResponse(valid=False)

    return TokenValidateResponse(
        valid=True,
        user_id=user.user_id,
        email=user.email,
        org_id=user.org_id,
        role=user.role,
    )


@router.post("/logout")
async def logout(
    payload: LogoutRequest,
    request: Request,
    user: Annotated[AuthenticatedUser | None, Depends(get_current_user_optional)],
) -> dict:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    auth_service = get_auth_service()
    refresh_payload = auth_service.validate_token(
        payload.refresh_token, token_type="refresh"
    )
    if refresh_payload and refresh_payload.get("jti"):
        async with get_postgres_session() as db:
            await revoke_token(db, str(refresh_payload["jti"]))

    if user and user.org_id:
        user_uuid = _parse_uuid(user.user_id)
        org_uuid = _parse_uuid(user.org_id)
        if user_uuid is not None and org_uuid is not None:
            async with get_postgres_session() as db:
                emit_audit_log(
                    db,
                    org_id=org_uuid,
                    action=AuditAction.LOGOUT,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=user.user_id,
                    user_id=user_uuid,
                    description="User logged out",
                    request=request,
                )
                await db.commit()

    return {"message": "Logout successful"}
