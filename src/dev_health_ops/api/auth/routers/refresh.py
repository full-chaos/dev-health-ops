from __future__ import annotations

import logging
import uuid as uuid_mod

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.api.middleware.rate_limit import AUTH_REFRESH_LIMIT, limiter
from dev_health_ops.api.services.refresh_tokens import (
    find_by_hash,
    revoke_family,
    rotate_token,
)
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, Organization, User

from .common import (
    UserInfo,
    _expiry_to_utc,
    _extract_unverified_org_and_subject,
    _parse_uuid,
    _require_uuid,
)

logger = logging.getLogger(__name__)

router = APIRouter()


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class TokenRefreshResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: UserInfo | None = None


@router.post("/refresh", response_model=TokenRefreshResponse)
@limiter.limit(AUTH_REFRESH_LIMIT)
async def refresh_token(
    payload: TokenRefreshRequest,
    request: Request,
) -> TokenRefreshResponse:
    from dev_health_ops.api.auth.router import get_auth_service, get_postgres_session

    auth_service = get_auth_service()

    refresh_payload = auth_service.validate_token(
        payload.refresh_token, token_type="refresh"
    )
    if not refresh_payload:
        invalid_org_id, subject = _extract_unverified_org_and_subject(
            payload.refresh_token
        )
        if invalid_org_id is not None:
            async with get_postgres_session() as db:
                org_result = await db.execute(
                    select(Organization.id).where(Organization.id == invalid_org_id)
                )
                if org_result.scalar_one_or_none() is not None:
                    emit_audit_log(
                        db,
                        org_id=invalid_org_id,
                        action=AuditAction.LOGIN_FAILED,
                        resource_type=AuditResourceType.SESSION,
                        resource_id=subject or "unknown",
                        user_id=_parse_uuid(subject),
                        description="Refresh token validation failed",
                        request=request,
                        status="failure",
                        error_message="Invalid or expired refresh token",
                    )
                    await db.commit()
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid or expired refresh token"),
        )

    user_id = str(refresh_payload["sub"])
    refresh_org_id = str(refresh_payload.get("org_id", ""))
    token_jti = refresh_payload.get("jti")
    if not token_jti:
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid refresh token"),
        )

    async with get_postgres_session() as db:
        token_record = await find_by_hash(db, str(token_jti))
        if token_record is None:
            raise HTTPException(
                status_code=401,
                detail=error_detail("Invalid or expired refresh token"),
            )

        if token_record.revoked_at is not None:
            await revoke_family(db, str(token_record.family_id))
            raise HTTPException(
                status_code=401,
                detail=error_detail("Refresh token reuse detected"),
            )

        user_result = await db.execute(
            select(User).where(User.id == uuid_mod.UUID(user_id))
        )
        user = user_result.scalar_one_or_none()
        if not user:
            parsed_org_id = _parse_uuid(refresh_org_id)
            if parsed_org_id is not None:
                emit_audit_log(
                    db,
                    org_id=parsed_org_id,
                    action=AuditAction.LOGIN_FAILED,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=user_id,
                    description="Token refresh failed: user not found",
                    request=request,
                    status="failure",
                    error_message="User not found",
                )
                await db.commit()
            raise HTTPException(
                status_code=401,
                detail=error_detail("User not found"),
            )

        role = "member"
        if refresh_org_id:
            membership_result = await db.execute(
                select(Membership).where(
                    Membership.user_id == user.id,
                    Membership.org_id == uuid_mod.UUID(refresh_org_id),
                )
            )
            membership = membership_result.scalar_one_or_none()
            if membership:
                role = str(membership.role)

        new_refresh_token = auth_service.create_refresh_token(
            user_id=user_id,
            org_id=refresh_org_id,
            family_id=str(token_record.family_id),
        )
        new_refresh_payload = auth_service.validate_token(
            new_refresh_token, token_type="refresh"
        )
        if not new_refresh_payload or not new_refresh_payload.get("jti"):
            raise HTTPException(
                status_code=401,
                detail=error_detail("Unable to rotate refresh token"),
            )

        new_expires_at = _expiry_to_utc(new_refresh_payload.get("exp"))
        if new_expires_at is None:
            raise HTTPException(
                status_code=401,
                detail=error_detail("Unable to rotate refresh token"),
            )

        rotated = await rotate_token(
            db=db,
            old_token_hash=str(token_jti),
            new_token_hash=str(new_refresh_payload["jti"]),
            new_expires_at=new_expires_at,
        )
        if rotated is None:
            raise HTTPException(
                status_code=401,
                detail=error_detail("Invalid refresh token"),
            )

        parsed_org_id = _parse_uuid(refresh_org_id)
        if parsed_org_id is not None:
            refreshed_user_id = _require_uuid(user.id, "user.id")
            emit_audit_log(
                db,
                org_id=parsed_org_id,
                action=AuditAction.LOGIN,
                resource_type=AuditResourceType.SESSION,
                resource_id=user_id,
                user_id=refreshed_user_id,
                description="Access token refreshed",
                request=request,
            )
            await db.commit()

        new_access_token = auth_service.create_access_token(
            user_id=user_id,
            email=str(user.email),
            org_id=refresh_org_id,
            role=role,
            is_superuser=bool(user.is_superuser),
            username=str(user.username) if user.username is not None else None,
            full_name=str(user.full_name) if user.full_name is not None else None,
        )

    return TokenRefreshResponse(
        access_token=new_access_token,
        refresh_token=new_refresh_token,
        token_type="bearer",
        expires_in=3600,
        user=UserInfo(
            id=user_id,
            email=str(user.email),
            org_id=refresh_org_id,
            role=role,
            is_superuser=bool(user.is_superuser),
        ),
    )
