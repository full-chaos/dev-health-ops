from __future__ import annotations

import importlib
import logging
import re
import uuid as uuid_mod
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel, EmailStr
from sqlalchemy import func, select

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    get_auth_service,
    extract_token_from_header,
)
from dev_health_ops.api.services.refresh_tokens import (
    create_refresh_token as create_refresh_token_record,
    find_by_hash,
    revoke_family,
    revoke_token,
    rotate_token,
)
from dev_health_ops.api.services.invites import (
    accept_invite as accept_org_invite,
    validate_invite as validate_org_invite,
)
from dev_health_ops.api.services.login_attempts import (
    check_lockout,
    clear_attempts,
    get_lockout_remaining_seconds,
    record_failed_attempt,
)
from dev_health_ops.api.middleware.rate_limit import (
    AUTH_LOGIN_IP_LIMIT,
    AUTH_LOGIN_LIMIT,
    AUTH_REFRESH_LIMIT,
    AUTH_REGISTER_LIMIT,
    AUTH_VALIDATE_LIMIT,
    get_auth_key,
    limiter,
)
from dev_health_ops.api.utils.password_policy import validate_password
from dev_health_ops.api.utils.audit import emit_audit_log
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.users import Membership, Organization, User

logger = logging.getLogger(__name__)
DUMMY_PASSWORD_HASH = "$2b$12$C6UzMDM.H6dfI/f/IKcEeO4x1n4Q4M4WQ0uGAaHo9dZYkTfLZNV6G"


router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


# --- Request/Response Models ---


class RegisterRequest(BaseModel):
    """Register a new user with email and password."""

    email: EmailStr
    password: str
    full_name: str | None = None
    org_name: str | None = None  # Optional: name for new org, defaults to "My Org"


class RegisterResponse(BaseModel):
    """Registration response."""

    message: str
    user_id: str
    org_id: str


class VerifyEmailResponse(BaseModel):
    message: str
    verified: bool | None = None


class ResendVerificationRequest(BaseModel):
    email: EmailStr


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class LoginRequest(BaseModel):
    """Login with email and password."""

    email: EmailStr
    password: str
    org_id: str | None = None  # Optional: select org if user has multiple


class LoginResponse(BaseModel):
    """Login response with tokens and user info."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    needs_onboarding: bool = False
    user: "UserInfo"


class EmailVerificationRequiredResponse(BaseModel):
    status: str
    email: str
    message: str


class UserInfo(BaseModel):
    """Basic user information."""

    id: str
    email: str
    username: str | None = None
    full_name: str | None = None
    org_id: str | None = None
    role: str
    is_superuser: bool = False


class OnboardRequest(BaseModel):
    action: str
    org_name: str | None = None
    invite_code: str | None = None


class OnboardResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    org_id: str
    org_name: str
    role: str


class AcceptInviteRequest(BaseModel):
    token: str


class AcceptInviteResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    org_id: str
    org_name: str
    role: str


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class TokenRefreshResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: "UserInfo | None" = None


class TokenValidateRequest(BaseModel):
    token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class TokenValidateResponse(BaseModel):
    valid: bool
    user_id: str | None = None
    email: str | None = None
    org_id: str | None = None
    role: str | None = None
    expires_at: str | None = None


class MeResponse(BaseModel):
    """Current user information."""

    id: str
    email: str
    username: str | None = None
    full_name: str | None = None
    org_id: str
    role: str
    is_superuser: bool = False
    permissions: list[str] = []


# --- Dependencies ---


async def get_current_user(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser:
    """FastAPI dependency: validates JWT then verifies user exists + is_active in DB.

    Raises HTTPException 401 if not authenticated.
    """
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = extract_token_from_header(authorization)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Invalid authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(token)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify user still exists and is active in the database
    try:
        user_uuid = uuid_mod.UUID(user.user_id)
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail="Invalid user identity",
            headers={"WWW-Authenticate": "Bearer"},
        )

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User.id, User.is_active).where(User.id == user_uuid)
        )
        db_user = result.one_or_none()

    if not db_user:
        logger.warning("JWT valid but user not found in DB: %s", user.user_id)
        raise HTTPException(
            status_code=401,
            detail="User no longer exists",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not db_user.is_active:
        logger.warning("JWT valid but user is deactivated: %s", user.user_id)
        raise HTTPException(
            status_code=401,
            detail="Account is disabled",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def get_current_user_optional(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser | None:
    """Optionally get authenticated user. Returns None if not authenticated.

    When a token IS present, verifies user exists + is_active in DB.
    """
    if not authorization:
        return None

    token = extract_token_from_header(authorization)
    if not token:
        return None

    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(token)
    if not user:
        return None

    try:
        user_uuid = uuid_mod.UUID(user.user_id)
    except ValueError:
        return None

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User.id, User.is_active).where(User.id == user_uuid)
        )
        db_user = result.one_or_none()

    if not db_user or not db_user.is_active:
        return None

    return user


# --- Endpoints ---


@router.post("/register", response_model=RegisterResponse, status_code=201)
@limiter.limit(AUTH_REGISTER_LIMIT)
async def register(payload: RegisterRequest, request: Request) -> RegisterResponse:
    import bcrypt
    from datetime import datetime, timezone

    email_verification_service = importlib.import_module(
        "dev_health_ops.api.services.email_verification"
    )
    create_verification_token = getattr(
        email_verification_service,
        "create_email_verification_token",
    )
    send_verification = getattr(email_verification_service, "send_verification_email")

    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        password_violations = validate_password(payload.password)
        if password_violations:
            raise HTTPException(
                status_code=422,
                detail={"violations": password_violations},
            )

        stmt = select(User).where(func.lower(User.email) == email_normalized)
        result = await db.execute(stmt)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            existing_org_result = await db.execute(
                select(Membership.org_id)
                .where(Membership.user_id == existing_user.id)
                .limit(1)
            )
            existing_org_id = existing_org_result.scalar_one_or_none()
            if existing_org_id is not None:
                emit_audit_log(
                    db,
                    org_id=existing_org_id,
                    action=AuditAction.CREATE,
                    resource_type=AuditResourceType.USER,
                    resource_id=str(existing_user.id),
                    user_id=existing_user.id,
                    description="User registration failed: email already registered",
                    changes={"email": email_normalized},
                    request=request,
                    status="failure",
                    error_message="Email already registered",
                )
            raise HTTPException(status_code=400, detail="Email already registered")

        password_hash = bcrypt.hashpw(
            payload.password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        user = User(
            email=email_normalized,
            password_hash=password_hash,
            full_name=payload.full_name,
            auth_provider="local",
            is_active=True,
            is_verified=False,
        )
        db.add(user)
        await db.flush()

        org_name = payload.org_name or "My Organization"
        org_slug = org_name.lower().replace(" ", "-")[:50]
        org_slug = f"{org_slug}-{str(user.id)[:8]}"

        org = Organization(
            slug=org_slug,
            name=org_name,
            tier="community",
            is_active=True,
        )
        db.add(org)
        await db.flush()

        membership = Membership(
            user_id=user.id,
            org_id=org.id,
            role="owner",
            joined_at=datetime.now(timezone.utc),
        )
        db.add(membership)

        emit_audit_log(
            db,
            org_id=org.id,
            action=AuditAction.CREATE,
            resource_type=AuditResourceType.USER,
            resource_id=str(user.id),
            user_id=user.id,
            description="User registered",
            changes={"email": email_normalized, "organization_id": str(org.id)},
            request=request,
        )

        verification_token = await create_verification_token(db, user.id)

        await db.commit()

        try:
            await send_verification(
                to_email=str(user.email),
                full_name=str(user.full_name) if user.full_name is not None else None,
                token=verification_token,
            )
        except Exception:
            logger.exception(
                "Failed to send verification email for %s",
                sanitize_for_log(payload.email),
            )

        logger.info("User registered: %s", sanitize_for_log(payload.email))

        return RegisterResponse(
            message="Registration successful",
            user_id=str(user.id),
            org_id=str(org.id),
        )


@router.get("/verify", response_model=VerifyEmailResponse)
@limiter.limit("10/hour", key_func=get_auth_key)
async def verify_email(
    token: Annotated[str, Query(min_length=1)],
    request: Request,
) -> VerifyEmailResponse:
    email_verification_service = importlib.import_module(
        "dev_health_ops.api.services.email_verification"
    )
    verify_token = getattr(email_verification_service, "verify_email_token")

    async with get_postgres_session() as db:
        user = await verify_token(db, token)
        if user is None:
            raise HTTPException(
                status_code=400,
                detail="Invalid or expired verification token",
            )
        await db.commit()

    return VerifyEmailResponse(
        message="Email verified successfully",
        verified=True,
    )


@router.post("/resend-verification", response_model=VerifyEmailResponse)
@limiter.limit("3/hour", key_func=get_auth_key)
async def resend_verification_email(
    payload: ResendVerificationRequest,
    request: Request,
) -> VerifyEmailResponse:
    email_verification_service = importlib.import_module(
        "dev_health_ops.api.services.email_verification"
    )
    create_verification_token = getattr(
        email_verification_service,
        "create_email_verification_token",
    )
    send_verification = getattr(email_verification_service, "send_verification_email")

    generic_response = VerifyEmailResponse(
        message="If an account exists with that email, a verification link has been sent"
    )
    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        result = await db.execute(
            select(User).where(func.lower(User.email) == email_normalized)
        )
        user = result.scalar_one_or_none()
        if user is None or bool(getattr(user, "is_verified", False)):
            return generic_response

        verification_token = await create_verification_token(db, user.id)
        await db.commit()

        try:
            await send_verification(
                to_email=str(user.email),
                full_name=str(user.full_name) if user.full_name is not None else None,
                token=verification_token,
            )
        except Exception:
            logger.exception(
                "Failed to resend verification email for %s",
                sanitize_for_log(payload.email),
            )
        return generic_response


@router.post("/forgot-password", response_model=VerifyEmailResponse)
@limiter.limit("3/hour", key_func=get_auth_key)
async def forgot_password(
    payload: ForgotPasswordRequest,
    request: Request,
) -> VerifyEmailResponse:
    password_reset_service = importlib.import_module(
        "dev_health_ops.api.services.password_reset"
    )
    create_reset_token = getattr(password_reset_service, "create_password_reset_token")
    send_reset_email = getattr(password_reset_service, "send_password_reset_email")

    generic_response = VerifyEmailResponse(
        message="If the account exists, a password reset email has been sent"
    )
    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        result = await db.execute(
            select(User).where(func.lower(User.email) == email_normalized)
        )
        user = result.scalar_one_or_none()
        if user is None:
            return generic_response

        reset_token = await create_reset_token(db, user.id)
        await db.commit()

        try:
            await send_reset_email(
                to_email=str(user.email),
                full_name=str(user.full_name) if user.full_name is not None else None,
                token=reset_token,
            )
        except Exception:
            logger.exception(
                "Failed to send password reset email for %s",
                sanitize_for_log(payload.email),
            )
        return generic_response


@router.post("/reset-password", response_model=VerifyEmailResponse)
async def reset_password(payload: ResetPasswordRequest) -> VerifyEmailResponse:
    password_reset_service = importlib.import_module(
        "dev_health_ops.api.services.password_reset"
    )
    reset_with_token = getattr(password_reset_service, "reset_password_with_token")

    async with get_postgres_session() as db:
        user = await reset_with_token(db, payload.token, payload.new_password)
        if user is None:
            raise HTTPException(status_code=400, detail="Invalid or expired token")
        await db.commit()

    return VerifyEmailResponse(message="Password reset successful")


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
    """Authenticate user and return tokens.

    For local auth, validates email/password.
    For OAuth users, use the OAuth flow instead.
    """
    import bcrypt
    from datetime import datetime, timezone

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
                emit_audit_log(
                    db,
                    org_id=failure_org_id,
                    action=AuditAction.LOGIN_FAILED,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=email_normalized,
                    user_id=user.id if user is not None else None,
                    description="Login failed: account locked",
                    changes={"email": email_normalized},
                    request=request,
                    status="failure",
                    error_message="Account temporarily locked due to failed login attempts",
                )

            raise HTTPException(
                status_code=429,
                detail={
                    "message": "Too many failed login attempts. Please try again later.",
                    "retry_after_seconds": retry_after_seconds,
                },
            )

        primary_org_id: uuid_mod.UUID | None = None
        if user is not None:
            primary_org_result = await db.execute(
                select(Membership.org_id).where(Membership.user_id == user.id).limit(1)
            )
            primary_org_id = primary_org_result.scalar_one_or_none()

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
            failure_resource_id = str(user.id)
            failure_user_id = user.id
        elif user.password_hash is None:
            failure_description = "Login failed: password login unavailable"
            failure_error_message = "Password login not available for this account"
            failure_resource_id = str(user.id)
            failure_user_id = user.id
        elif not password_matches:
            failure_description = "Login failed: invalid credentials"
            failure_error_message = "Invalid credentials"
            failure_resource_id = str(user.id)
            failure_user_id = user.id

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
                logger.warning(
                    "Invalid password for user: %s", sanitize_for_log(payload.email)
                )
            raise HTTPException(status_code=401, detail="Invalid credentials")

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
                emit_audit_log(
                    db,
                    org_id=blocked_org_id,
                    action=AuditAction.LOGIN_FAILED,
                    resource_type=AuditResourceType.SESSION,
                    resource_id=str(user.id),
                    user_id=user.id,
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

        # Get user's membership/org
        membership_stmt = select(Membership).where(Membership.user_id == user.id)

        if payload.org_id:
            # User selected a specific org
            membership_stmt = membership_stmt.where(Membership.org_id == payload.org_id)

        membership_result = await db.execute(membership_stmt)
        membership = membership_result.scalar_one_or_none()

        needs_onboarding = membership is None

        if payload.org_id and not membership:
            any_membership_result = await db.execute(
                select(Membership.id).where(Membership.user_id == user.id)
            )
            if any_membership_result.first() is not None:
                raise HTTPException(
                    status_code=401,
                    detail="User is not a member of the selected organization",
                )

        # Update last login
        setattr(user, "last_login_at", datetime.now(timezone.utc))

        success_org_id = _parse_uuid(payload.org_id) or (
            membership.org_id if membership else primary_org_id
        )
        if success_org_id is not None:
            emit_audit_log(
                db,
                org_id=success_org_id,
                action=AuditAction.LOGIN,
                resource_type=AuditResourceType.SESSION,
                resource_id=str(user.id),
                user_id=user.id,
                description="User logged in",
                request=request,
            )

        await db.commit()

        # Create tokens
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


def _slugify_org_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug[:50] or "my-organization"


def _parse_uuid(value: str | None) -> uuid_mod.UUID | None:
    if not value:
        return None
    try:
        return uuid_mod.UUID(value)
    except ValueError:
        return None


def _expiry_to_utc(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return None


async def _resolve_login_audit_org_id(
    db,
    user: User | None,
    payload_org_id: str | None,
) -> uuid_mod.UUID | None:
    parsed_org_id = _parse_uuid(payload_org_id)
    if parsed_org_id is not None:
        org_result = await db.execute(
            select(Organization.id).where(Organization.id == parsed_org_id)
        )
        if org_result.scalar_one_or_none() is not None:
            return parsed_org_id

    if user is None:
        return None

    membership_result = await db.execute(
        select(Membership.org_id).where(Membership.user_id == user.id).limit(1)
    )
    return membership_result.scalar_one_or_none()


async def _issue_membership_tokens(
    db,
    request: Request,
    db_user: User,
    membership: Membership,
):
    auth_service = get_auth_service()
    token_pair = auth_service.create_token_pair(
        user_id=str(db_user.id),
        email=str(db_user.email),
        org_id=str(membership.org_id),
        role=str(membership.role),
        is_superuser=bool(db_user.is_superuser),
        username=str(db_user.username) if db_user.username is not None else None,
        full_name=str(db_user.full_name) if db_user.full_name is not None else None,
    )

    refresh_payload = auth_service.validate_token(
        token_pair.refresh_token, token_type="refresh"
    )
    if refresh_payload and refresh_payload.get("jti"):
        expires_at = _expiry_to_utc(refresh_payload.get("exp"))
        if expires_at is not None:
            await create_refresh_token_record(
                db=db,
                user_id=str(db_user.id),
                org_id=str(membership.org_id),
                token_hash=str(refresh_payload["jti"]),
                family_id=str(refresh_payload.get("family_id") or uuid_mod.uuid4()),
                expires_at=expires_at,
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
            )

    return token_pair


def _extract_unverified_org_and_subject(
    token: str,
) -> tuple[uuid_mod.UUID | None, str | None]:
    try:
        from jose import jwt

        claims = jwt.get_unverified_claims(token)
    except Exception:
        return None, None

    return _parse_uuid(claims.get("org_id")), claims.get("sub")


@router.post("/accept-invite", response_model=AcceptInviteResponse)
async def accept_invite(
    payload: AcceptInviteRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    request: Request,
) -> AcceptInviteResponse:
    async with get_postgres_session() as db:
        try:
            user_uuid = uuid_mod.UUID(user.user_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=401, detail="Invalid user identity"
            ) from exc

        user_result = await db.execute(select(User).where(User.id == user_uuid))
        db_user = user_result.scalar_one_or_none()
        if db_user is None:
            raise HTTPException(status_code=401, detail="User not found")

        invite = await validate_org_invite(db, payload.token)
        if invite is None:
            raise HTTPException(status_code=400, detail="Invalid or expired invite")

        org_result = await db.execute(
            select(Organization).where(Organization.id == invite.org_id)
        )
        org = org_result.scalar_one_or_none()
        if org is None:
            raise HTTPException(status_code=404, detail="Organization not found")

        try:
            membership = await accept_org_invite(db, invite, db_user.id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        emit_audit_log(
            db,
            org_id=invite.org_id,
            action=AuditAction.MEMBER_JOINED,
            resource_type=AuditResourceType.MEMBERSHIP,
            resource_id=str(membership.id),
            user_id=db_user.id,
            description="Invite accepted",
            changes={
                "invite_id": str(invite.id),
                "user_id": str(db_user.id),
                "role": membership.role,
            },
            request=request,
        )

        await db.commit()
        token_pair = await _issue_membership_tokens(db, request, db_user, membership)

        return AcceptInviteResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            org_id=str(org.id),
            org_name=str(org.name),
            role=str(membership.role),
        )


@router.post("/onboard", response_model=OnboardResponse)
async def onboard(
    payload: OnboardRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    request: Request,
) -> OnboardResponse:
    from datetime import datetime, timezone

    async with get_postgres_session() as db:
        try:
            user_uuid = uuid_mod.UUID(user.user_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=401, detail="Invalid user identity"
            ) from exc

        user_result = await db.execute(select(User).where(User.id == user_uuid))
        db_user = user_result.scalar_one_or_none()
        if not db_user:
            raise HTTPException(status_code=401, detail="User not found")

        membership_result = await db.execute(
            select(Membership.id).where(Membership.user_id == db_user.id)
        )
        if membership_result.first() is not None:
            raise HTTPException(status_code=400, detail="Already onboarded")

        if payload.action == "join_org":
            if not payload.invite_code:
                raise HTTPException(status_code=400, detail="invite_code is required")

            invite = await validate_org_invite(db, payload.invite_code)
            if invite is None:
                raise HTTPException(status_code=400, detail="Invalid or expired invite")

            org_result = await db.execute(
                select(Organization).where(Organization.id == invite.org_id)
            )
            org = org_result.scalar_one_or_none()
            if org is None:
                raise HTTPException(status_code=404, detail="Organization not found")

            try:
                membership = await accept_org_invite(db, invite, db_user.id)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

            emit_audit_log(
                db,
                org_id=org.id,
                action=AuditAction.MEMBER_JOINED,
                resource_type=AuditResourceType.MEMBERSHIP,
                resource_id=str(membership.id),
                user_id=db_user.id,
                description="Invite accepted during onboarding",
                changes={
                    "invite_id": str(invite.id),
                    "user_id": str(db_user.id),
                    "role": membership.role,
                },
                request=request,
            )

            await db.commit()
            token_pair = await _issue_membership_tokens(
                db, request, db_user, membership
            )

            return OnboardResponse(
                access_token=token_pair.access_token,
                refresh_token=token_pair.refresh_token,
                token_type=token_pair.token_type,
                expires_in=token_pair.expires_in,
                org_id=str(org.id),
                org_name=str(org.name),
                role=str(membership.role),
            )

        if payload.action != "create_org":
            raise HTTPException(
                status_code=400,
                detail="Invalid action. Use 'create_org' or 'join_org'",
            )

        org_name = payload.org_name or "My Organization"
        org_slug = f"{_slugify_org_name(org_name)}-{str(db_user.id)[:8]}"

        org = Organization(
            slug=org_slug,
            name=org_name,
            tier="community",
            is_active=True,
        )
        db.add(org)
        await db.flush()

        membership = Membership(
            user_id=db_user.id,
            org_id=org.id,
            role="owner",
            joined_at=datetime.now(timezone.utc),
        )
        db.add(membership)

        emit_audit_log(
            db,
            org_id=org.id,
            action=AuditAction.CREATE,
            resource_type=AuditResourceType.ORGANIZATION,
            resource_id=str(org.id),
            user_id=db_user.id,
            description="Organization created during onboarding",
            changes={"organization_name": org_name},
            request=request,
        )

        await db.commit()

        token_pair = await _issue_membership_tokens(db, request, db_user, membership)

        return OnboardResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            org_id=str(org.id),
            org_name=str(org.name),
            role="owner",
        )


@router.get("/me", response_model=MeResponse)
async def get_me(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> MeResponse:
    """Get current authenticated user info and permissions."""
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


@router.post("/refresh", response_model=TokenRefreshResponse)
@limiter.limit(AUTH_REFRESH_LIMIT)
async def refresh_token(
    payload: TokenRefreshRequest,
    request: Request,
) -> TokenRefreshResponse:
    auth_service = get_auth_service()

    refresh_payload = auth_service.validate_token(
        payload.refresh_token, token_type="refresh"
    )
    if not refresh_payload:
        org_id, subject = _extract_unverified_org_and_subject(payload.refresh_token)
        if org_id is not None:
            async with get_postgres_session() as db:
                org_result = await db.execute(
                    select(Organization.id).where(Organization.id == org_id)
                )
                if org_result.scalar_one_or_none() is not None:
                    emit_audit_log(
                        db,
                        org_id=org_id,
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
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    user_id = str(refresh_payload["sub"])
    org_id = str(refresh_payload.get("org_id", ""))
    token_jti = refresh_payload.get("jti")
    if not token_jti:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    async with get_postgres_session() as db:
        token_record = await find_by_hash(db, str(token_jti))
        if token_record is None:
            raise HTTPException(
                status_code=401, detail="Invalid or expired refresh token"
            )

        if token_record.revoked_at is not None:
            await revoke_family(db, str(token_record.family_id))
            raise HTTPException(status_code=401, detail="Refresh token reuse detected")

        user_result = await db.execute(
            select(User).where(User.id == uuid_mod.UUID(user_id))
        )
        user = user_result.scalar_one_or_none()
        if not user:
            parsed_org_id = _parse_uuid(org_id)
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
            raise HTTPException(status_code=401, detail="User not found")

        role = "member"
        if org_id:
            membership_result = await db.execute(
                select(Membership).where(
                    Membership.user_id == user.id,
                    Membership.org_id == uuid_mod.UUID(org_id),
                )
            )
            membership = membership_result.scalar_one_or_none()
            if membership:
                role = str(membership.role)

        new_refresh_token = auth_service.create_refresh_token(
            user_id=user_id,
            org_id=org_id,
            family_id=str(token_record.family_id),
        )
        new_refresh_payload = auth_service.validate_token(
            new_refresh_token, token_type="refresh"
        )
        if not new_refresh_payload or not new_refresh_payload.get("jti"):
            raise HTTPException(
                status_code=401, detail="Unable to rotate refresh token"
            )

        new_expires_at = _expiry_to_utc(new_refresh_payload.get("exp"))
        if new_expires_at is None:
            raise HTTPException(
                status_code=401, detail="Unable to rotate refresh token"
            )

        rotated = await rotate_token(
            db=db,
            old_token_hash=str(token_jti),
            new_token_hash=str(new_refresh_payload["jti"]),
            new_expires_at=new_expires_at,
        )
        if rotated is None:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        parsed_org_id = _parse_uuid(org_id)
        if parsed_org_id is not None:
            emit_audit_log(
                db,
                org_id=parsed_org_id,
                action=AuditAction.LOGIN,
                resource_type=AuditResourceType.SESSION,
                resource_id=user_id,
                user_id=user.id,
                description="Access token refreshed",
                request=request,
            )
            await db.commit()

        new_access_token = auth_service.create_access_token(
            user_id=user_id,
            email=str(user.email),
            org_id=org_id,
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
            org_id=org_id,
            role=role,
            is_superuser=bool(user.is_superuser),
        ),
    )


@router.post("/validate", response_model=TokenValidateResponse)
@limiter.limit(AUTH_VALIDATE_LIMIT)
async def validate_token(
    payload: TokenValidateRequest,
    request: Request,
) -> TokenValidateResponse:
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


# --- SSO Endpoints (conditionally loaded from enterprise module) ---
try:
    from .sso import sso_router

    router.include_router(sso_router)
except ImportError as exc:
    # SSO module is optional (e.g., only available in enterprise deployments);
    # if it's not installed, we skip registering SSO routes.
    logger.info(
        "SSO router not loaded because optional 'sso' module is missing: %s", exc
    )
