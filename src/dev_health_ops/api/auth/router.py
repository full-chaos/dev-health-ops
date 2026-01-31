from __future__ import annotations

import logging
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
from dev_health_ops.api.auth.schemas import (
    OIDCAuthRequest,
    OIDCAuthResponse,
    SAMLAuthRequest,
    SAMLAuthResponse,
    SAMLMetadataResponse,
    SSOProviderCreate,
    SSOProviderListResponse,
    SSOProviderResponse,
    SSOProviderUpdate,
)
from dev_health_ops.api.services.sso import SSOService
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.users import User, Membership

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


# --- Request/Response Models ---


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
    user: "UserInfo"


class UserInfo(BaseModel):
    """Basic user information."""

    id: str
    email: str
    username: str | None = None
    full_name: str | None = None
    org_id: str
    role: str
    is_superuser: bool = False


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class TokenRefreshResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


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
    """FastAPI dependency to get authenticated user from JWT.

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

    return user


async def get_current_user_optional(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser | None:
    """FastAPI dependency to optionally get authenticated user.

    Returns None if not authenticated (does not raise).
    """
    if not authorization:
        return None

    token = extract_token_from_header(authorization)
    if not token:
        return None

    auth_service = get_auth_service()
    return auth_service.get_authenticated_user(token)


# --- Endpoints ---


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

        if not user.is_active:
            raise HTTPException(status_code=401, detail="Account is disabled")

        # Verify password
        if not user.password_hash:
            raise HTTPException(
                status_code=401,
                detail="Password login not available for this account",
            )

        if not bcrypt.checkpw(
            payload.password.encode("utf-8"), user.password_hash.encode("utf-8")
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

        if not membership:
            raise HTTPException(
                status_code=401,
                detail="User is not a member of any organization",
            )

        # Update last login
        user.last_login_at = datetime.now(timezone.utc)
        await db.commit()

        # Create tokens
        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=user.email,
            org_id=str(membership.org_id),
            role=membership.role,
            is_superuser=user.is_superuser,
            username=user.username,
            full_name=user.full_name,
        )

        return LoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user=UserInfo(
                id=str(user.id),
                email=user.email,
                username=user.username,
                full_name=user.full_name,
                org_id=str(membership.org_id),
                role=membership.role,
                is_superuser=user.is_superuser,
            ),
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

    new_access_token = auth_service.create_access_token(
        user_id=refresh_payload["sub"],
        email=refresh_payload.get("email", ""),
        org_id=refresh_payload.get("org_id", ""),
        role=refresh_payload.get("role", "member"),
        is_superuser=refresh_payload.get("is_superuser", False),
    )

    return TokenRefreshResponse(
        access_token=new_access_token,
        token_type="bearer",
        expires_in=3600,
    )


@router.post("/validate", response_model=TokenValidateResponse)
async def validate_token(payload: TokenValidateRequest) -> TokenValidateResponse:
    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(payload.token)

    if not user:
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


# --- SSO Provider Management Endpoints ---


def _provider_to_response(provider) -> SSOProviderResponse:
    config = dict(provider.config) if provider.config else {}
    if "client_secret" in config:
        config["client_secret"] = "********"
    if "certificate" in config:
        config["certificate"] = (
            f"{config['certificate'][:50]}..."
            if len(config.get("certificate", "")) > 50
            else config.get("certificate", "")
        )

    return SSOProviderResponse(
        id=str(provider.id),
        org_id=str(provider.org_id),
        name=str(provider.name),
        protocol=str(provider.protocol),
        status=str(provider.status),
        is_default=bool(provider.is_default),
        allow_idp_initiated=bool(provider.allow_idp_initiated),
        auto_provision_users=bool(provider.auto_provision_users),
        default_role=str(provider.default_role),
        config=config,
        allowed_domains=list(provider.allowed_domains)
        if provider.allowed_domains
        else [],
        last_metadata_sync_at=provider.last_metadata_sync_at,
        last_login_at=provider.last_login_at,
        last_error=str(provider.last_error) if provider.last_error else None,
        last_error_at=provider.last_error_at,
        created_at=provider.created_at,
        updated_at=provider.updated_at,
    )


@router.get("/sso/providers", response_model=SSOProviderListResponse)
async def list_sso_providers(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    protocol: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> SSOProviderListResponse:
    import uuid as uuid_mod

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        providers, total = await sso_service.list_providers(
            org_id=uuid_mod.UUID(user.org_id),
            protocol=protocol,
            status=status,
            limit=limit,
            offset=offset,
        )

        return SSOProviderListResponse(
            items=[_provider_to_response(p) for p in providers],
            total=total,
            limit=limit,
            offset=offset,
        )


@router.post("/sso/providers", response_model=SSOProviderResponse, status_code=201)
async def create_sso_provider(
    payload: SSOProviderCreate,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    if payload.protocol == "saml" and not payload.saml_config:
        raise HTTPException(
            status_code=400, detail="SAML config required for SAML providers"
        )
    if payload.protocol == "oidc" and not payload.oidc_config:
        raise HTTPException(
            status_code=400, detail="OIDC config required for OIDC providers"
        )

    config: dict = {}
    encrypted_secrets: dict = {}

    if payload.saml_config:
        config = payload.saml_config.model_dump()
    elif payload.oidc_config:
        oidc_data = payload.oidc_config.model_dump()
        encrypted_secrets["client_secret"] = oidc_data.pop("client_secret", None)
        config = oidc_data

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.create_provider(
            org_id=uuid_mod.UUID(user.org_id),
            name=payload.name,
            protocol=payload.protocol,
            config=config,
            encrypted_secrets=encrypted_secrets if encrypted_secrets else None,
            is_default=payload.is_default,
            allow_idp_initiated=payload.allow_idp_initiated,
            auto_provision_users=payload.auto_provision_users,
            default_role=payload.default_role,
            allowed_domains=payload.allowed_domains,
        )
        await db.commit()

        return _provider_to_response(provider)


@router.get("/sso/providers/{provider_id}", response_model=SSOProviderResponse)
async def get_sso_provider(
    provider_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.get_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
        )

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        return _provider_to_response(provider)


@router.patch("/sso/providers/{provider_id}", response_model=SSOProviderResponse)
async def update_sso_provider(
    provider_id: str,
    payload: SSOProviderUpdate,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.get_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
        )

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        config = None
        encrypted_secrets = None

        if payload.saml_config:
            config = payload.saml_config.model_dump()
        elif payload.oidc_config:
            oidc_data = payload.oidc_config.model_dump()
            encrypted_secrets = {"client_secret": oidc_data.pop("client_secret", None)}
            config = oidc_data

        provider = await sso_service.update_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
            name=payload.name,
            config=config,
            encrypted_secrets=encrypted_secrets,
            is_default=payload.is_default,
            allow_idp_initiated=payload.allow_idp_initiated,
            auto_provision_users=payload.auto_provision_users,
            default_role=payload.default_role,
            allowed_domains=payload.allowed_domains,
        )
        await db.commit()

        return _provider_to_response(provider)


@router.delete("/sso/providers/{provider_id}", status_code=204)
async def delete_sso_provider(
    provider_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> None:
    import uuid as uuid_mod

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        deleted = await sso_service.delete_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
        )

        if not deleted:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        await db.commit()


@router.post(
    "/sso/providers/{provider_id}/activate", response_model=SSOProviderResponse
)
async def activate_sso_provider(
    provider_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.activate_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
        )

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        await db.commit()
        return _provider_to_response(provider)


@router.post(
    "/sso/providers/{provider_id}/deactivate", response_model=SSOProviderResponse
)
async def deactivate_sso_provider(
    provider_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.deactivate_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
        )

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        await db.commit()
        return _provider_to_response(provider)


# --- SAML Flow Endpoints ---


@router.get("/saml/{provider_id}/metadata", response_model=SAMLMetadataResponse)
async def get_saml_metadata(provider_id: str) -> SAMLMetadataResponse:
    import uuid as uuid_mod
    import os

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)

        stmt = select(User).limit(1)
        await db.execute(stmt)

        from dev_health_ops.models.sso import SSOProvider

        provider_stmt = select(SSOProvider).where(
            SSOProvider.id == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        if not provider.is_saml:
            raise HTTPException(status_code=400, detail="Provider is not SAML")

        metadata_xml = sso_service.generate_saml_sp_metadata(provider)
        config = provider.get_saml_config()

        return SAMLMetadataResponse(
            metadata_xml=metadata_xml,
            entity_id=config.get("sp_entity_id")
            or f"{base_url}/saml/{provider_id}/metadata",
            acs_url=config.get("sp_acs_url") or f"{base_url}/saml/{provider_id}/acs",
        )


@router.post("/saml/{provider_id}/initiate", response_model=SAMLAuthResponse)
async def initiate_saml_auth(
    provider_id: str,
    payload: SAMLAuthRequest,
) -> SAMLAuthResponse:
    import uuid as uuid_mod
    import os

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)

        from dev_health_ops.models.sso import SSOProvider

        provider_stmt = select(SSOProvider).where(
            SSOProvider.id == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        if not provider.is_saml:
            raise HTTPException(status_code=400, detail="Provider is not SAML")

        if provider.status != "active":
            raise HTTPException(status_code=400, detail="SSO provider is not active")

        redirect_url = sso_service.generate_saml_auth_request_url(
            provider, relay_state=payload.relay_state
        )

        return SAMLAuthResponse(redirect_url=redirect_url)


# --- OIDC Flow Endpoints ---


@router.post("/oidc/{provider_id}/authorize", response_model=OIDCAuthResponse)
async def initiate_oidc_auth(
    provider_id: str,
    payload: OIDCAuthRequest,
) -> OIDCAuthResponse:
    import uuid as uuid_mod
    import os

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)

        from dev_health_ops.models.sso import SSOProvider

        provider_stmt = select(SSOProvider).where(
            SSOProvider.id == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        if not provider.is_oidc:
            raise HTTPException(status_code=400, detail="Provider is not OIDC")

        if provider.status != "active":
            raise HTTPException(status_code=400, detail="SSO provider is not active")

        auth_request = sso_service.generate_oidc_authorization_request(
            provider,
            redirect_uri=payload.redirect_uri,
            use_pkce=payload.use_pkce,
        )

        return OIDCAuthResponse(
            authorization_url=auth_request.authorization_url,
            state=auth_request.state,
        )
