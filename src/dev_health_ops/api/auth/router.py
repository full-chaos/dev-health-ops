from __future__ import annotations

import logging
import os
import re
import uuid as uuid_mod
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, EmailStr
from sqlalchemy import select

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    get_auth_service,
    extract_token_from_header,
)
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.users import Membership, Organization, User

logger = logging.getLogger(__name__)


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


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class TokenRefreshResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: "UserInfo | None" = None


class TokenValidateRequest(BaseModel):
    token: str


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
async def register(payload: RegisterRequest) -> RegisterResponse:
    import bcrypt
    from datetime import datetime, timezone

    async with get_postgres_session() as db:
        stmt = select(User).where(User.email == payload.email)
        result = await db.execute(stmt)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            raise HTTPException(status_code=400, detail="Email already registered")

        password_hash = bcrypt.hashpw(
            payload.password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        user = User(
            email=payload.email,
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

        from dev_health_ops.models.users import Organization, Membership

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
        await db.commit()

        logger.info("User registered: %s", sanitize_for_log(payload.email))

        return RegisterResponse(
            message="Registration successful",
            user_id=str(user.id),
            org_id=str(org.id),
        )


@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest) -> LoginResponse:
    """Authenticate user and return tokens.

    For local auth, validates email/password.
    For OAuth users, use the OAuth flow instead.
    """
    import bcrypt
    from datetime import datetime, timezone

    async with get_postgres_session() as db:
        # Find user by email
        stmt = select(User).where(User.email == payload.email)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            logger.warning(
                "Login attempt for non-existent user: %s",
                sanitize_for_log(payload.email),
            )
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if user.is_active is not True:
            raise HTTPException(status_code=401, detail="Account is disabled")

        # Verify password
        password_hash = (
            str(user.password_hash) if user.password_hash is not None else ""
        )
        if not password_hash:
            raise HTTPException(
                status_code=401,
                detail="Password login not available for this account",
            )

        if not bcrypt.checkpw(
            payload.password.encode("utf-8"), password_hash.encode("utf-8")
        ):
            logger.warning(
                "Invalid password for user: %s", sanitize_for_log(payload.email)
            )
            raise HTTPException(status_code=401, detail="Invalid credentials")

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


@router.post("/onboard", response_model=OnboardResponse)
async def onboard(
    payload: OnboardRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
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
            raise HTTPException(
                status_code=501,
                detail="Invite-based joining not yet implemented",
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
        await db.commit()

        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(db_user.id),
            email=str(db_user.email),
            org_id=str(org.id),
            role="owner",
            is_superuser=bool(db_user.is_superuser),
            username=str(db_user.username) if db_user.username is not None else None,
            full_name=str(db_user.full_name) if db_user.full_name is not None else None,
        )

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
async def refresh_token(payload: TokenRefreshRequest) -> TokenRefreshResponse:
    auth_service = get_auth_service()

    refresh_payload = auth_service.validate_token(
        payload.refresh_token, token_type="refresh"
    )
    if not refresh_payload:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    user_id = refresh_payload["sub"]
    org_id = refresh_payload.get("org_id", "")

    async with get_postgres_session() as db:
        user_result = await db.execute(
            select(User).where(User.id == uuid_mod.UUID(user_id))
        )
        user = user_result.scalar_one_or_none()
        if not user:
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

    new_access_token = auth_service.create_access_token(
        user_id=user_id,
        email=str(user.email),
        org_id=org_id,
        role=role,
        is_superuser=bool(user.is_superuser),
        username=str(user.username) if user.username else None,
        full_name=str(user.full_name) if user.full_name else None,
    )

    return TokenRefreshResponse(
        access_token=new_access_token,
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
async def validate_token(payload: TokenValidateRequest) -> TokenValidateResponse:
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
async def logout() -> dict:
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
