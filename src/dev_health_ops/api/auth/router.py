from __future__ import annotations

import logging
import os
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
from dev_health_ops.api.auth.schemas import (
    OAuthAuthRequest,
    OAuthAuthResponse,
    OAuthCallbackRequest,
    OAuthProviderCreate,
    OAuthProviderUpdate,
    OIDCAuthRequest,
    OIDCAuthResponse,
    OIDCCallbackRequest,
    SAMLAuthRequest,
    SAMLAuthResponse,
    SAMLCallbackRequest,
    SAMLMetadataResponse,
    SSOLoginResponse,
    SSOProviderCreate,
    SSOProviderListResponse,
    SSOProviderResponse,
    SSOProviderUpdate,
)
from dev_health_ops.api.services.audit import AuditService
from dev_health_ops.api.services.sso import SSOProcessingError, SSOService
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing import require_feature
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.sso import SSOProvider
from dev_health_ops.models.users import User, Membership

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
            tier="free",
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
@require_feature("sso", required_tier="enterprise")
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
@require_feature("sso", required_tier="enterprise")
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
@require_feature("sso", required_tier="enterprise")
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
@require_feature("sso", required_tier="enterprise")
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
@require_feature("sso", required_tier="enterprise")
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
@require_feature("sso", required_tier="enterprise")
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
@require_feature("sso", required_tier="enterprise")
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


@router.post("/saml/{provider_id}/acs", response_model=SSOLoginResponse)
@require_feature("sso", required_tier="enterprise")
async def saml_acs_callback(
    provider_id: str,
    payload: SAMLCallbackRequest,
) -> SSOLoginResponse:
    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)
        audit_service = AuditService(db)

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

        try:
            saml_info = await sso_service.process_saml_response(
                provider=provider,
                saml_response=payload.saml_response,
                relay_state=payload.relay_state,
            )
        except SSOProcessingError as exc:
            logger.error("SAML callback failed: %s", sanitize_for_log(str(exc)))
            await sso_service.record_error(
                org_id=provider.org_id,
                provider_id=provider.id,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider.org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider.id),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "saml"},
            )
            await db.commit()
            raise HTTPException(
                status_code=400, detail="SAML authentication failed"
            )

        email = saml_info.get("email")
        if not email:
            raise HTTPException(
                status_code=400,
                detail="Email address is missing from SAML response",
            )
        
        if provider.allowed_domains:
            if "@" not in email:
                raise HTTPException(
                    status_code=400,
                    detail="Email address from SAML response is malformed",
                )
            email_domain = email.rsplit("@", 1)[-1]
            if email_domain not in provider.allowed_domains:
                raise HTTPException(
                    status_code=403,
                    detail=f"Email domain '{email_domain}' is not allowed for this provider",
                )

        try:
            user, membership, _ = await sso_service.provision_or_get_user(
                org_id=provider.org_id,
                email=email,
                name=saml_info.get("full_name"),
                provider_id=provider.id,
                external_id=saml_info.get("external_id"),
            )
        except SSOProcessingError as exc:
            logger.error(
                "SAML user provisioning failed: %s",
                sanitize_for_log(str(exc)),
            )
            await sso_service.record_error(
                org_id=provider.org_id,
                provider_id=provider.id,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider.org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider.id),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "saml", "stage": "provisioning"},
            )
            await db.commit()
            raise HTTPException(
                status_code=400,
                detail="SAML user provisioning failed",
            )

        await sso_service.record_login(org_id=provider.org_id, provider_id=provider.id)
        await audit_service.log(
            org_id=provider.org_id,
            action=AuditAction.SSO_LOGIN,
            resource_type=AuditResourceType.SESSION,
            resource_id=str(user.id),
            user_id=user.id,
            extra_metadata={"provider_id": str(provider.id), "protocol": "saml"},
        )

        await db.commit()

        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id),
            role=str(membership.role),
            is_superuser=bool(user.is_superuser),
            username=str(user.username) if user.username else None,
            full_name=str(user.full_name) if user.full_name else None,
        )

        return SSOLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id),
            role=str(membership.role),
        )


# --- OIDC Flow Endpoints ---


@router.post("/oidc/{provider_id}/authorize", response_model=OIDCAuthResponse)
@require_feature("sso", required_tier="enterprise")
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


@router.post("/oidc/{provider_id}/callback", response_model=SSOLoginResponse)
@require_feature("sso", required_tier="enterprise")
async def oidc_callback(
    provider_id: str,
    payload: OIDCCallbackRequest,
) -> SSOLoginResponse:
    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)
        audit_service = AuditService(db)

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

        try:
            oidc_info = await sso_service.process_oidc_callback(
                provider=provider,
                code=payload.code,
                state=payload.state,
                code_verifier=payload.code_verifier,
            )
        except SSOProcessingError as exc:
            logger.error("OIDC callback failed: %s", sanitize_for_log(str(exc)))
            await sso_service.record_error(
                org_id=provider.org_id,
                provider_id=provider.id,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider.org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider.id),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "oidc"},
            )
            await db.commit()
            raise HTTPException(
                status_code=400, detail="OIDC authentication failed"
            )

        email = oidc_info.get("email")
        if not email:
            raise HTTPException(
                status_code=400,
                detail="Email address is missing from OIDC claims",
            )
        
        if provider.allowed_domains:
            if "@" not in email:
                raise HTTPException(
                    status_code=400,
                    detail="Email address from OIDC claims is malformed",
                )
            email_domain = email.rsplit("@", 1)[-1]
            if email_domain not in provider.allowed_domains:
                raise HTTPException(
                    status_code=403,
                    detail=f"Email domain '{email_domain}' is not allowed for this provider",
                )

        try:
            user, membership, _ = await sso_service.provision_or_get_user(
                org_id=provider.org_id,
                email=email,
                name=oidc_info.get("full_name"),
                provider_id=provider.id,
                external_id=oidc_info.get("external_id"),
            )
        except SSOProcessingError as exc:
            logger.error(
                "OIDC user provisioning failed: %s",
                sanitize_for_log(str(exc)),
            )
            await sso_service.record_error(
                org_id=provider.org_id,
                provider_id=provider.id,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider.org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider.id),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "oidc", "stage": "provisioning"},
            )
            await db.commit()
            raise HTTPException(
                status_code=400,
                detail="OIDC user provisioning failed",
            )

        await sso_service.record_login(org_id=provider.org_id, provider_id=provider.id)
        await audit_service.log(
            org_id=provider.org_id,
            action=AuditAction.SSO_LOGIN,
            resource_type=AuditResourceType.SESSION,
            resource_id=str(user.id),
            user_id=user.id,
            extra_metadata={"provider_id": str(provider.id), "protocol": "oidc"},
        )

        await db.commit()

        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id),
            role=str(membership.role),
            is_superuser=bool(user.is_superuser),
            username=str(user.username) if user.username else None,
            full_name=str(user.full_name) if user.full_name else None,
        )

        return SSOLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id),
            role=str(membership.role),
        )


# --- OAuth Flow Endpoints ---


@router.post("/oauth/providers", response_model=SSOProviderResponse, status_code=201)
@require_feature("sso", required_tier="enterprise")
async def create_oauth_provider(
    payload: OAuthProviderCreate,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod
    import os

    from dev_health_ops.api.services.oauth import (
        get_default_scopes,
        validate_oauth_config,
    )

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    provider_type_map = {
        "github": "oauth_github",
        "gitlab": "oauth_gitlab",
        "google": "oauth_google",
    }
    protocol = provider_type_map.get(payload.provider_type)
    if not protocol:
        raise HTTPException(status_code=400, detail="Invalid OAuth provider type")

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")
    scopes = payload.oauth_config.scopes or get_default_scopes(payload.provider_type)

    # Validate OAuth config before persisting
    validation_result = validate_oauth_config(
        provider_type=payload.provider_type,
        client_id=payload.oauth_config.client_id,
        client_secret=payload.oauth_config.client_secret,
        scopes=scopes,
        base_url=payload.oauth_config.base_url,
    )
    if not validation_result.valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid OAuth configuration: {'; '.join(validation_result.errors)}",
        )

    config = {
        "client_id": payload.oauth_config.client_id,
        "redirect_uri": None,  # Will be set after provider.id is available
        "scopes": scopes,
        "base_url": payload.oauth_config.base_url,
    }
    encrypted_secrets = {"client_secret": payload.oauth_config.client_secret}

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.create_provider(
            org_id=uuid_mod.UUID(user.org_id),
            name=payload.name,
            protocol=protocol,
            config=config,
            encrypted_secrets=encrypted_secrets,
            is_default=payload.is_default,
            allow_idp_initiated=False,
            auto_provision_users=payload.auto_provision_users,
            default_role=payload.default_role,
            allowed_domains=payload.allowed_domains,
        )

        # Update config with correct redirect_uri now that we have provider.id
        config["redirect_uri"] = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"
        provider = await sso_service.update_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=provider.id,
            config=config,
        )

        await db.commit()

        return _provider_to_response(provider)


@router.patch("/oauth/providers/{provider_id}", response_model=SSOProviderResponse)
@require_feature("sso", required_tier="enterprise")
async def update_oauth_provider(
    provider_id: str,
    payload: OAuthProviderUpdate,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import uuid as uuid_mod

    from dev_health_ops.api.services.oauth import validate_oauth_config

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    async with get_postgres_session() as db:
        sso_service = SSOService(db)
        provider = await sso_service.get_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
        )

        if not provider:
            raise HTTPException(status_code=404, detail="OAuth provider not found")

        if not provider.is_oauth:
            raise HTTPException(status_code=400, detail="Provider is not OAuth")

        config = None
        encrypted_secrets = None

        if payload.oauth_config:
            config = dict(provider.config) if provider.config else {}
            config["client_id"] = payload.oauth_config.client_id
            if payload.oauth_config.scopes:
                config["scopes"] = payload.oauth_config.scopes
            if payload.oauth_config.base_url:
                config["base_url"] = payload.oauth_config.base_url
            encrypted_secrets = {"client_secret": payload.oauth_config.client_secret}

            # Validate the updated OAuth config
            validation_result = validate_oauth_config(
                provider_type=provider.oauth_provider_type,
                client_id=payload.oauth_config.client_id,
                client_secret=payload.oauth_config.client_secret,
                scopes=config.get("scopes"),
                base_url=config.get("base_url"),
            )
            if not validation_result.valid:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid OAuth configuration: {'; '.join(validation_result.errors)}",
                )

        provider = await sso_service.update_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=uuid_mod.UUID(provider_id),
            name=payload.name,
            config=config,
            encrypted_secrets=encrypted_secrets,
            is_default=payload.is_default,
            auto_provision_users=payload.auto_provision_users,
            default_role=payload.default_role,
            allowed_domains=payload.allowed_domains,
        )
        await db.commit()

        return _provider_to_response(provider)


@router.post("/oauth/{provider_id}/authorize", response_model=OAuthAuthResponse)
@require_feature("sso", required_tier="enterprise")
async def initiate_oauth_auth(
    provider_id: str,
    payload: OAuthAuthRequest,
) -> OAuthAuthResponse:
    import uuid as uuid_mod
    import os

    from dev_health_ops.api.services.oauth import (
        create_oauth_provider as create_oauth_provider_instance,
        OAuthConfig,
    )
    from dev_health_ops.models.sso import SSOProvider

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        provider_stmt = select(SSOProvider).where(
            SSOProvider.id == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="OAuth provider not found")

        if not provider.is_oauth:
            raise HTTPException(status_code=400, detail="Provider is not OAuth")

        if provider.status != "active":
            raise HTTPException(status_code=400, detail="OAuth provider is not active")

        oauth_config = provider.get_oauth_config()
        secrets = provider.encrypted_secrets or {}

        # Use stored redirect_uri if available, otherwise generate from provider.id
        redirect_uri = oauth_config.get("redirect_uri")
        if not redirect_uri:
            redirect_uri = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"

        config = OAuthConfig(
            client_id=oauth_config["client_id"],
            client_secret=secrets.get("client_secret", ""),
            redirect_uri=redirect_uri,
            scopes=oauth_config.get("scopes", []),
        )

        oauth_provider_instance = create_oauth_provider_instance(
            provider_type=provider.oauth_provider_type,
            config=config,
            base_url=oauth_config.get("base_url"),
        )

        auth_request = oauth_provider_instance.generate_authorization_request()

        return OAuthAuthResponse(
            authorization_url=auth_request.authorization_url,
            state=auth_request.state,
        )


@router.post("/oauth/{provider_id}/callback", response_model=SSOLoginResponse)
@require_feature("sso", required_tier="enterprise")
async def oauth_callback(
    provider_id: str,
    payload: OAuthCallbackRequest,
) -> SSOLoginResponse:
    import uuid as uuid_mod
    import os
    from datetime import datetime, timezone

    from dev_health_ops.api.services.oauth import (
        create_oauth_provider as create_oauth_provider_instance,
        OAuthConfig,
        OAuthProviderError,
    )
    from dev_health_ops.models.sso import SSOProvider
    from dev_health_ops.models.users import AuthProvider

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        provider_stmt = select(SSOProvider).where(
            SSOProvider.id == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="OAuth provider not found")

        if not provider.is_oauth:
            raise HTTPException(status_code=400, detail="Provider is not OAuth")

        if provider.status != "active":
            raise HTTPException(status_code=400, detail="OAuth provider is not active")

        oauth_config = provider.get_oauth_config()
        secrets = provider.encrypted_secrets or {}

        redirect_uri = oauth_config.get("redirect_uri")
        if not redirect_uri:
            redirect_uri = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"

        config = OAuthConfig(
            client_id=oauth_config["client_id"],
            client_secret=secrets.get("client_secret", ""),
            redirect_uri=redirect_uri,
            scopes=oauth_config.get("scopes", []),
        )

        oauth_provider_instance = create_oauth_provider_instance(
            provider_type=provider.oauth_provider_type,
            config=config,
            base_url=oauth_config.get("base_url"),
        )

        try:
            token_response = await oauth_provider_instance.exchange_code_for_token(
                code=payload.code,
                state=payload.state,
            )
            user_info = await oauth_provider_instance.fetch_user_info(
                access_token=token_response.access_token
            )
        except OAuthProviderError as e:
            logger.error("OAuth callback failed: %s", str(e))
            sso_service = SSOService(db)
            await sso_service.record_error(
                org_id=provider.org_id,
                provider_id=provider.id,
                error=str(e),
            )
            await db.commit()
            raise HTTPException(
                status_code=400, detail=f"OAuth authentication failed: {e}"
            )

        if provider.allowed_domains:
            email_domain = (
                user_info.email.split("@")[-1] if "@" in user_info.email else ""
            )
            if email_domain not in provider.allowed_domains:
                raise HTTPException(
                    status_code=403,
                    detail=f"Email domain '{email_domain}' is not allowed for this provider",
                )

        auth_provider_map = {
            "github": AuthProvider.GITHUB.value,
            "gitlab": AuthProvider.GITLAB.value,
            "google": AuthProvider.GOOGLE.value,
        }
        auth_provider_value = auth_provider_map.get(
            provider.oauth_provider_type, "oauth"
        )

        user_stmt = select(User).where(User.email == user_info.email)
        user_result = await db.execute(user_stmt)
        user = user_result.scalar_one_or_none()

        if not user:
            if not provider.auto_provision_users:
                raise HTTPException(
                    status_code=403,
                    detail="User not found and auto-provisioning is disabled",
                )

            user = User(
                email=user_info.email,
                username=user_info.username,
                full_name=user_info.full_name,
                avatar_url=user_info.avatar_url,
                auth_provider=auth_provider_value,
                auth_provider_id=user_info.provider_user_id,
                is_active=True,
                is_verified=True,
            )
            db.add(user)
            await db.flush()

            membership = Membership(
                user_id=user.id,
                org_id=provider.org_id,
                role=provider.default_role,
                joined_at=datetime.now(timezone.utc),
            )
            db.add(membership)
            await db.flush()

            logger.info(
                "Auto-provisioned user %s via OAuth provider %s",
                sanitize_for_log(user_info.email),
                provider.name,
            )
        else:
            if user.auth_provider != auth_provider_value:
                user.auth_provider = auth_provider_value
                user.auth_provider_id = user_info.provider_user_id

            if user_info.avatar_url and not user.avatar_url:
                user.avatar_url = user_info.avatar_url
            if user_info.full_name and not user.full_name:
                user.full_name = user_info.full_name

        user.last_login_at = datetime.now(timezone.utc)

        membership_stmt = select(Membership).where(
            Membership.user_id == user.id,
            Membership.org_id == provider.org_id,
        )
        membership_result = await db.execute(membership_stmt)
        membership = membership_result.scalar_one_or_none()

        if not membership:
            if not provider.auto_provision_users:
                raise HTTPException(
                    status_code=403,
                    detail="User is not a member of this organization",
                )

            membership = Membership(
                user_id=user.id,
                org_id=provider.org_id,
                role=provider.default_role,
                joined_at=datetime.now(timezone.utc),
            )
            db.add(membership)
            await db.flush()

        sso_service = SSOService(db)
        await sso_service.record_login(
            org_id=provider.org_id,
            provider_id=provider.id,
        )

        await db.commit()

        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id),
            role=str(membership.role),
            is_superuser=bool(user.is_superuser),
            username=str(user.username) if user.username else None,
            full_name=str(user.full_name) if user.full_name else None,
        )

        return SSOLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id),
            role=str(membership.role),
        )


@router.get("/oauth/{provider_type}/authorize", response_model=OAuthAuthResponse)
@require_feature("sso", required_tier="enterprise")
async def initiate_oauth_by_type(
    provider_type: str,
    org_id: str,
    redirect_uri: str | None = None,
) -> OAuthAuthResponse:
    import uuid as uuid_mod
    import os

    from dev_health_ops.api.services.oauth import (
        create_oauth_provider as create_oauth_provider_instance,
        OAuthConfig,
    )
    from dev_health_ops.models.sso import SSOProvider, SSOProtocol

    if provider_type not in ("github", "gitlab", "google"):
        raise HTTPException(status_code=400, detail="Invalid OAuth provider type")

    protocol_map = {
        "github": SSOProtocol.OAUTH_GITHUB.value,
        "gitlab": SSOProtocol.OAUTH_GITLAB.value,
        "google": SSOProtocol.OAUTH_GOOGLE.value,
    }
    protocol = protocol_map[provider_type]

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        provider_stmt = select(SSOProvider).where(
            SSOProvider.org_id == uuid_mod.UUID(org_id),
            SSOProvider.protocol == protocol,
            SSOProvider.status == "active",
            SSOProvider.is_default.is_(True),  # noqa: E712
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            provider_stmt = select(SSOProvider).where(
                SSOProvider.org_id == uuid_mod.UUID(org_id),
                SSOProvider.protocol == protocol,
                SSOProvider.status == "active",
            )
            result = await db.execute(provider_stmt)
            provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(
                status_code=404,
                detail=f"No active {provider_type} OAuth provider found for this organization",
            )

        oauth_config = provider.get_oauth_config()
        secrets = provider.encrypted_secrets or {}

        # Use stored redirect_uri if available, otherwise generate from provider.id
        final_redirect_uri = oauth_config.get("redirect_uri")
        if not final_redirect_uri:
            final_redirect_uri = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"

        config = OAuthConfig(
            client_id=oauth_config["client_id"],
            client_secret=secrets.get("client_secret", ""),
            redirect_uri=final_redirect_uri,
            scopes=oauth_config.get("scopes", []),
        )

        oauth_provider_instance = create_oauth_provider_instance(
            provider_type=provider_type,
            config=config,
            base_url=oauth_config.get("base_url"),
        )

        auth_request = oauth_provider_instance.generate_authorization_request()

        return OAuthAuthResponse(
            authorization_url=auth_request.authorization_url,
            state=auth_request.state,
        )
