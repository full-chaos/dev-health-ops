from __future__ import annotations

import logging
import uuid as uuid_mod
from datetime import datetime, timezone

import bcrypt
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, EmailStr
from sqlalchemy import func, select

from dev_health_ops.api.middleware.rate_limit import (
    AUTH_LOGIN_IP_LIMIT,
    AUTH_LOGIN_LIMIT,
    get_auth_key,
    limiter,
)
from dev_health_ops.api.services.login_attempts import (
    check_lockout,
    clear_attempts,
    get_lockout_remaining_seconds,
    record_failed_attempt,
)
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, User

from .common import (
    LoginResponse,
    UserInfo,
    _expiry_to_utc,
    _optional_uuid,
    _parse_uuid,
    _require_uuid,
    _resolve_login_audit_org_id,
)

logger = logging.getLogger(__name__)

# Intentional fixed bcrypt hash for missing-user timing mitigation; not a credential.
# nosemgrep: generic.secrets.security.detected-bcrypt-hash.detected-bcrypt-hash
DUMMY_PASSWORD_HASH = "$2b$12$C6UzMDM.H6dfI/f/IKcEeO4x1n4Q4M4WQ0uGAaHo9dZYkTfLZNV6G"

router = APIRouter()


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    org_id: str | None = None


class EmailVerificationRequiredResponse(BaseModel):
    status: str
    email: str
    message: str


@router.post(
    "/login",
    response_model=LoginResponse | EmailVerificationRequiredResponse,
)
@limiter.limit(AUTH_LOGIN_IP_LIMIT)
@limiter.limit(AUTH_LOGIN_LIMIT, key_func=get_auth_key)
async def login(
    payload: LoginRequest,
    request: Request,
) -> LoginResponse | EmailVerificationRequiredResponse:
    from dev_health_ops.api.auth.router import (
        create_refresh_token_record,
        get_auth_service,
        get_postgres_session,
    )

    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        stmt = select(User).where(func.lower(User.email) == email_normalized)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        if await check_lockout(db, email_normalized):
            retry_after_seconds = await get_lockout_remaining_seconds(
                db, email_normalized
            )
            if retry_after_seconds <= 0:
                retry_after_seconds = 1

            failure_org_id = await _resolve_login_audit_org_id(db, user, payload.org_id)
            if failure_org_id is not None:
                failure_user_id = _optional_uuid(
                    user.id if user is not None else None, "user.id"
                )
                emit_audit_log(
                    db,
                    org_id=failure_org_id,
                    action=AuditAction.LOGIN_FAILED,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=email_normalized,
                    user_id=failure_user_id,
                    description="Login failed: account locked",
                    changes={"email": email_normalized},
                    request=request,
                    status="failure",
                    error_message="Account temporarily locked due to failed login attempts",
                )

            raise HTTPException(
                status_code=429,
                detail=error_detail(
                    "Too many failed login attempts. Please try again later.",
                    retry_after_seconds=retry_after_seconds,
                ),
            )

        primary_org_id: uuid_mod.UUID | None = None
        if user is not None:
            primary_org_result = await db.execute(
                select(Membership.org_id).where(Membership.user_id == user.id).limit(1)
            )
            primary_org_id = _optional_uuid(
                primary_org_result.scalar_one_or_none(), "primary_org_id"
            )

        hash_for_timing_check = DUMMY_PASSWORD_HASH
        if (
            user is not None
            and user.is_active is True
            and user.password_hash is not None
        ):
            hash_for_timing_check = str(user.password_hash)

        try:
            password_matches = bcrypt.checkpw(
                payload.password.encode("utf-8"),
                hash_for_timing_check.encode("utf-8"),
            )
        except ValueError:
            password_matches = False

        failure_description = ""
        failure_error_message = ""
        failure_resource_id = email_normalized
        failure_user_id = None

        if user is None:
            failure_description = "Login failed: user not found"
            failure_error_message = "Invalid credentials"
        elif user.is_active is not True:
            failure_description = "Login failed: account is disabled"
            failure_error_message = "Account is disabled"
            resolved_user_id = _require_uuid(user.id, "user.id")
            failure_resource_id = str(resolved_user_id)
            failure_user_id = resolved_user_id
        elif user.password_hash is None:
            failure_description = "Login failed: password login unavailable"
            failure_error_message = "Password login not available for this account"
            resolved_user_id = _require_uuid(user.id, "user.id")
            failure_resource_id = str(resolved_user_id)
            failure_user_id = resolved_user_id
        elif not password_matches:
            failure_description = "Login failed: invalid credentials"
            failure_error_message = "Invalid credentials"
            resolved_user_id = _require_uuid(user.id, "user.id")
            failure_resource_id = str(resolved_user_id)
            failure_user_id = resolved_user_id

        if failure_description:
            await record_failed_attempt(db, email_normalized)
            failure_org_id = await _resolve_login_audit_org_id(db, user, payload.org_id)
            if failure_org_id is not None:
                emit_audit_log(
                    db,
                    org_id=failure_org_id,
                    action=AuditAction.LOGIN_FAILED,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=failure_resource_id,
                    user_id=failure_user_id,
                    description=failure_description,
                    changes={"email": email_normalized},
                    request=request,
                    status="failure",
                    error_message=failure_error_message,
                )
            if user is None:
                logger.warning(
                    "Login attempt for non-existent user: %s",
                    sanitize_for_log(payload.email),
                )
            elif failure_error_message == "Invalid credentials":
                # Email is sanitized before logging; no password or token is logged.
                # nosemgrep: python.lang.security.audit.logging.logger-credential-leak.python-logger-credential-disclosure
                logger.warning(
                    "Invalid password for user: %s", sanitize_for_log(payload.email)
                )
            raise HTTPException(
                status_code=401,
                detail=error_detail("Invalid credentials"),
            )

        assert user is not None
        await clear_attempts(db, email_normalized)

        auth_provider = str(getattr(user, "auth_provider", "local")).lower()
        if auth_provider == "local" and not bool(getattr(user, "is_verified", False)):
            blocked_org_id = primary_org_id or await _resolve_login_audit_org_id(
                db,
                user,
                payload.org_id,
            )
            if blocked_org_id is not None:
                user_id = _require_uuid(user.id, "user.id")
                emit_audit_log(
                    db,
                    org_id=blocked_org_id,
                    action=AuditAction.LOGIN_FAILED,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=str(user_id),
                    user_id=user_id,
                    description="Login blocked: email not verified",
                    changes={"email": email_normalized},
                    request=request,
                    status="failure",
                    error_message="Email not verified",
                )
            await db.commit()
            return EmailVerificationRequiredResponse(
                status="email_verification_required",
                email=str(user.email),
                message="Please verify your email address before logging in",
            )

        membership_stmt = select(Membership).where(Membership.user_id == user.id)

        if payload.org_id:
            membership_stmt = membership_stmt.where(Membership.org_id == payload.org_id)

        membership_result = await db.execute(membership_stmt)
        membership = membership_result.scalar_one_or_none()

        needs_onboarding = membership is None and not bool(user.is_superuser)

        if payload.org_id and not membership:
            any_membership_result = await db.execute(
                select(Membership.id).where(Membership.user_id == user.id)
            )
            if any_membership_result.first() is not None:
                raise HTTPException(
                    status_code=401,
                    detail=error_detail(
                        "User is not a member of the selected organization"
                    ),
                )

        setattr(user, "last_login_at", datetime.now(timezone.utc))

        membership_org_id = (
            _require_uuid(membership.org_id, "membership.org_id")
            if membership is not None
            else None
        )
        success_org_id = _parse_uuid(payload.org_id) or (
            membership_org_id if membership is not None else primary_org_id
        )
        if success_org_id is not None:
            user_id = _require_uuid(user.id, "user.id")
            emit_audit_log(
                db,
                org_id=success_org_id,
                action=AuditAction.LOGIN,
                resource_type=AuditResourceType.SESSION,
                resource_id=str(user_id),
                user_id=user_id,
                description="User logged in",
                request=request,
            )

        await db.commit()

        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id) if membership else "",
            role=str(membership.role) if membership else "member",
            is_superuser=bool(user.is_superuser),
            username=str(user.username) if user.username is not None else None,
            full_name=str(user.full_name) if user.full_name is not None else None,
        )

        refresh_payload = auth_service.validate_token(
            token_pair.refresh_token, token_type="refresh"
        )
        if refresh_payload and membership and refresh_payload.get("jti"):
            expires_at = _expiry_to_utc(refresh_payload.get("exp"))
            if expires_at is not None:
                await create_refresh_token_record(
                    db=db,
                    user_id=str(user.id),
                    org_id=str(membership.org_id),
                    token_hash=str(refresh_payload["jti"]),
                    family_id=str(refresh_payload.get("family_id") or uuid_mod.uuid4()),
                    expires_at=expires_at,
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent"),
                )

        return LoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            needs_onboarding=needs_onboarding,
            user=UserInfo(
                id=str(user.id),
                email=str(user.email),
                username=str(user.username) if user.username is not None else None,
                full_name=str(user.full_name) if user.full_name is not None else None,
                org_id=str(membership.org_id) if membership else None,
                role=str(membership.role) if membership else "member",
                is_superuser=bool(user.is_superuser),
            ),
        )
