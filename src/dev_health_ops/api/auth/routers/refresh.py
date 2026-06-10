from __future__ import annotations

import logging
import uuid as uuid_mod
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from dev_health_ops.api.middleware.rate_limit import AUTH_REFRESH_LIMIT, limiter
from dev_health_ops.api.services.refresh_tokens import (
    find_by_hash,
    find_by_hash_for_update,
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

# Concurrent-rotation grace window.  When a token is presented and found
# already revoked, but it was rotated less than this many seconds ago AND a
# successor is recorded, we treat the presentation as an idempotent replay of
# the racing concurrent refresh rather than as malicious reuse.  The window is
# intentionally small: large enough to cover typical round-trip latency between
# two browser tabs hitting the endpoint simultaneously, but short enough that a
# genuinely stolen-and-replayed old token (outside this window) is still caught
# and the family revoked.
ROTATION_GRACE_WINDOW_SECONDS: int = 30


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
        # Acquire a row-level write lock before reading the token state.
        # On Postgres this serializes concurrent rotations of the same token:
        # the second request blocks here until the first commits, then reads
        # the committed state (revoked + successor_jti populated).
        # On SQLite (tests) with_for_update() is a no-op; correctness still
        # holds because the grace-window check below handles that path.
        token_record = await find_by_hash_for_update(db, str(token_jti))
        if token_record is None:
            raise HTTPException(
                status_code=401,
                detail=error_detail("Invalid or expired refresh token"),
            )

        if token_record.revoked_at is not None:
            # ── Grace-window / idempotency check ─────────────────────────────
            # A token that was rotated very recently may be presented a second
            # time by a concurrent request (e.g. two browser tabs hitting the
            # refresh endpoint simultaneously).  In that case the token was
            # legitimately rotated by the first request; the second presentation
            # is NOT malicious reuse.  If the successor is recorded and still
            # valid, return the *same* successor JWT instead of revoking the
            # family.
            if (
                token_record.successor_jti is not None
                and token_record.revoked_at is not None
            ):
                # Normalise to UTC-aware before arithmetic; SQLite returns naive
                # datetimes even when the column is DateTime(timezone=True).
                revoked_at = token_record.revoked_at
                if revoked_at.tzinfo is None:
                    revoked_at = revoked_at.replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - revoked_at).total_seconds()
                if elapsed <= ROTATION_GRACE_WINDOW_SECONDS:
                    successor_record = await find_by_hash(
                        db, token_record.successor_jti
                    )
                    if (
                        successor_record is not None
                        and successor_record.revoked_at is None
                    ):
                        # Load user for access-token generation.
                        user_result = await db.execute(
                            select(User).where(User.id == uuid_mod.UUID(user_id))
                        )
                        user = user_result.scalar_one_or_none()
                        if user is not None:
                            role = "member"
                            if refresh_org_id:
                                membership_result = await db.execute(
                                    select(Membership).where(
                                        Membership.user_id == user.id,
                                        Membership.org_id
                                        == uuid_mod.UUID(refresh_org_id),
                                    )
                                )
                                membership = membership_result.scalar_one_or_none()
                                if membership:
                                    role = str(membership.role)

                            # Re-issue the *same* successor JWT (same JTI that
                            # was already committed to the DB by the first
                            # request).  This keeps exactly one valid token in
                            # circulation for this rotation slot.
                            reissued_refresh_token = (
                                auth_service.create_refresh_token_with_jti(
                                    jti=token_record.successor_jti,
                                    user_id=user_id,
                                    org_id=refresh_org_id,
                                    family_id=str(token_record.family_id),
                                    expires_at=successor_record.expires_at,
                                )
                            )
                            new_access_token = auth_service.create_access_token(
                                user_id=user_id,
                                email=str(user.email),
                                org_id=refresh_org_id,
                                role=role,
                                is_superuser=bool(user.is_superuser),
                                username=(
                                    str(user.username)
                                    if user.username is not None
                                    else None
                                ),
                                full_name=(
                                    str(user.full_name)
                                    if user.full_name is not None
                                    else None
                                ),
                            )
                            logger.debug(
                                "Concurrent-rotation grace window: replayed successor "
                                "for family %s (elapsed %.2fs)",
                                token_record.family_id,
                                elapsed,
                            )
                            return TokenRefreshResponse(
                                access_token=new_access_token,
                                refresh_token=reissued_refresh_token,
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

            # ── Genuine reuse (outside grace window or no successor) ──────────
            await revoke_family(db, str(token_record.family_id))
            await db.commit()
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

        # Pass the already-locked record so rotate_token reuses the FOR UPDATE
        # reference instead of issuing an unlocked re-fetch.
        rotated = await rotate_token(
            db=db,
            old_token_hash=str(token_jti),
            new_token_hash=str(new_refresh_payload["jti"]),
            new_expires_at=new_expires_at,
            existing_record=token_record,
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
