"""SSO enterprise auth endpoints — SAML, OIDC, and OAuth.

Extracted from the main auth router. All endpoints are gated behind
the ``sso`` enterprise feature via ``@require_feature``.
"""

from __future__ import annotations

import logging
import os
import uuid as uuid_mod
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from dev_health_ops.api.auth.router import get_current_user
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
from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    get_auth_service,
)
from dev_health_ops.api.services.oauth import OAuthProviderType
from dev_health_ops.api.services.settings import decrypt_value
from dev_health_ops.api.services.sso import SSOProcessingError, SSOService
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing import require_feature
from dev_health_ops.models.audit import AuditAction, AuditResourceType
from dev_health_ops.models.sso import SSOProvider
from dev_health_ops.models.users import Membership, User

logger = logging.getLogger(__name__)

sso_router = APIRouter(tags=["sso"])


def _require_uuid(value: object, field_name: str) -> uuid_mod.UUID:
    if isinstance(value, uuid_mod.UUID):
        return value
    raise TypeError(f"{field_name} must be a UUID")


def _require_str(value: object, field_name: str) -> str:
    if isinstance(value, str):
        return value
    raise TypeError(f"{field_name} must be a string")


def _optional_str(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_str(value, field_name)


def _require_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"{field_name} must be a bool")


def _require_datetime(value: object, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    raise TypeError(f"{field_name} must be a datetime")


def _optional_datetime(value: object, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _require_datetime(value, field_name)


def _string_dict(value: object, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"{field_name} must be a dict")
    if not all(isinstance(key, str) for key in value):
        raise TypeError(f"{field_name} keys must be strings")
    return dict(value)


def _string_list(value: object, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"{field_name} must be a list")
    items: list[str] = []
    for item in value:
        if isinstance(item, str):
            items.append(item)
            continue
        if item is not None:
            raise TypeError(f"{field_name} items must be strings")
    return items


def _require_oauth_provider_type(value: object) -> OAuthProviderType:
    if isinstance(value, OAuthProviderType):
        return value
    if isinstance(value, str):
        return OAuthProviderType(value)
    raise TypeError("OAuth provider type must be a string")


def _provider_protocol(provider: SSOProvider) -> str:
    return _require_str(provider.protocol, "provider.protocol")


def _provider_status(provider: SSOProvider) -> str:
    return _require_str(provider.status, "provider.status")


def _is_saml_provider(provider: SSOProvider) -> bool:
    return _provider_protocol(provider) == "saml"


def _is_oidc_provider(provider: SSOProvider) -> bool:
    return _provider_protocol(provider) == "oidc"


def _is_oauth_provider(provider: SSOProvider) -> bool:
    return _provider_protocol(provider).startswith("oauth_")


def _provider_oauth_type(provider: SSOProvider) -> OAuthProviderType:
    protocol = _provider_protocol(provider)
    protocol_map = {
        "oauth_github": OAuthProviderType.GITHUB,
        "oauth_gitlab": OAuthProviderType.GITLAB,
        "oauth_google": OAuthProviderType.GOOGLE,
    }
    try:
        return protocol_map[protocol]
    except KeyError as exc:
        raise TypeError("Provider is not OAuth") from exc


def _present_str(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    text = _require_str(value, field_name)
    return text if text else None


def _decrypt_secret(
    encrypted_secrets: dict[str, Any] | None, key: str, default: str = ""
) -> str:
    """Decrypt a value from the encrypted_secrets dict, with fallback for legacy plaintext."""
    raw = (encrypted_secrets or {}).get(key, default)
    if not raw:
        return default
    if not isinstance(raw, str):
        return default
    try:
        return decrypt_value(raw)
    except Exception:
        return raw  # fallback for pre-encryption values


# --- SSO Provider Management Endpoints ---


def _provider_to_response(provider: SSOProvider) -> SSOProviderResponse:
    config = _string_dict(provider.config, "provider.config")
    if "client_secret" in config:
        config["client_secret"] = "********"
    if "certificate" in config:
        certificate = config.get("certificate")
        if isinstance(certificate, str):
            config["certificate"] = (
                f"{certificate[:50]}..." if len(certificate) > 50 else certificate
            )

    allowed_domains = _string_list(provider.allowed_domains, "provider.allowed_domains")

    return SSOProviderResponse(
        id=str(provider.id),
        org_id=str(provider.org_id),
        name=_require_str(provider.name, "provider.name"),
        protocol=_require_str(provider.protocol, "provider.protocol"),
        status=_require_str(provider.status, "provider.status"),
        is_default=bool(provider.is_default),
        allow_idp_initiated=bool(provider.allow_idp_initiated),
        auto_provision_users=bool(provider.auto_provision_users),
        default_role=_require_str(provider.default_role, "provider.default_role"),
        config=config,
        allowed_domains=allowed_domains,
        last_metadata_sync_at=_optional_datetime(
            provider.last_metadata_sync_at, "provider.last_metadata_sync_at"
        ),
        last_login_at=_optional_datetime(
            provider.last_login_at, "provider.last_login_at"
        ),
        last_error=_optional_str(provider.last_error, "provider.last_error"),
        last_error_at=_optional_datetime(
            provider.last_error_at, "provider.last_error_at"
        ),
        created_at=_require_datetime(provider.created_at, "provider.created_at"),
        updated_at=_require_datetime(provider.updated_at, "provider.updated_at"),
    )


@sso_router.get("/sso/providers", response_model=SSOProviderListResponse)
@require_feature("sso_saml", required_tier="enterprise")
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


@sso_router.post("/sso/providers", response_model=SSOProviderResponse, status_code=201)
@require_feature("sso_saml", required_tier="enterprise")
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


@sso_router.get("/sso/providers/{provider_id}", response_model=SSOProviderResponse)
@require_feature("sso_saml", required_tier="enterprise")
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


@sso_router.patch("/sso/providers/{provider_id}", response_model=SSOProviderResponse)
@require_feature("sso_saml", required_tier="enterprise")
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
        if provider is None:
            raise HTTPException(status_code=404, detail="SSO provider not found")
        await db.commit()

        return _provider_to_response(provider)


@sso_router.delete("/sso/providers/{provider_id}", status_code=204)
@require_feature("sso_saml", required_tier="enterprise")
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


@sso_router.post(
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


@sso_router.post(
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


@sso_router.get("/saml/{provider_id}/metadata", response_model=SAMLMetadataResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def get_saml_metadata(provider_id: str) -> SAMLMetadataResponse:
    import os
    import uuid as uuid_mod

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)

        stmt = select(User).limit(1)
        await db.execute(stmt)

        from dev_health_ops.models.sso import SSOProvider

        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        if not _is_saml_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not SAML")

        metadata_xml = sso_service.generate_saml_sp_metadata(provider)
        config = provider.get_saml_config()

        return SAMLMetadataResponse(
            metadata_xml=metadata_xml,
            entity_id=config.get("sp_entity_id")
            or f"{base_url}/saml/{provider_id}/metadata",
            acs_url=config.get("sp_acs_url") or f"{base_url}/saml/{provider_id}/acs",
        )


@sso_router.post("/saml/{provider_id}/initiate", response_model=SAMLAuthResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def initiate_saml_auth(
    provider_id: str,
    payload: SAMLAuthRequest,
) -> SAMLAuthResponse:
    import os
    import uuid as uuid_mod

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)

        from dev_health_ops.models.sso import SSOProvider

        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        if not _is_saml_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not SAML")

        if _provider_status(provider) != "active":
            raise HTTPException(status_code=400, detail="SSO provider is not active")

        redirect_url = sso_service.generate_saml_auth_request_url(
            provider, relay_state=payload.relay_state
        )

        return SAMLAuthResponse(redirect_url=redirect_url)


@sso_router.post("/saml/{provider_id}/acs", response_model=SSOLoginResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def saml_acs_callback(
    provider_id: str,
    payload: SAMLCallbackRequest,
) -> SSOLoginResponse:
    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)
        audit_service = AuditService(db)

        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")
        if not _is_saml_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not SAML")
        if _provider_status(provider) != "active":
            raise HTTPException(status_code=400, detail="SSO provider is not active")

        provider_org_id = _require_uuid(provider.org_id, "provider.org_id")
        provider_uuid = _require_uuid(provider.id, "provider.id")

        try:
            saml_info = await sso_service.process_saml_response(
                provider=provider,
                saml_response=payload.saml_response,
                relay_state=payload.relay_state,
            )
        except SSOProcessingError as exc:
            logger.error("SAML callback failed: %s", sanitize_for_log(str(exc)))
            await sso_service.record_error(
                org_id=provider_org_id,
                provider_id=provider_uuid,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider_org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider_uuid),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "saml"},
            )
            await db.commit()
            raise HTTPException(status_code=400, detail="SAML authentication failed")

        email = saml_info.get("email")
        if not email:
            raise HTTPException(
                status_code=400,
                detail="Email address is missing from SAML response",
            )

        allowed_domains = _string_list(
            provider.allowed_domains, "provider.allowed_domains"
        )
        if allowed_domains:
            if "@" not in email:
                raise HTTPException(
                    status_code=400,
                    detail="Email address from SAML response is malformed",
                )
            email_domain = email.rsplit("@", 1)[-1]
            if email_domain not in allowed_domains:
                raise HTTPException(
                    status_code=403,
                    detail=f"Email domain '{email_domain}' is not allowed for this provider",
                )

        try:
            user, membership, _ = await sso_service.provision_or_get_user(
                org_id=provider_org_id,
                email=email,
                name=saml_info.get("full_name"),
                provider_id=provider_uuid,
                external_id=saml_info.get("external_id"),
            )
        except SSOProcessingError as exc:
            logger.error(
                "SAML user provisioning failed: %s",
                sanitize_for_log(str(exc)),
            )
            await sso_service.record_error(
                org_id=provider_org_id,
                provider_id=provider_uuid,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider_org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider_uuid),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "saml", "stage": "provisioning"},
            )
            await db.commit()
            raise HTTPException(
                status_code=400,
                detail="SAML user provisioning failed",
            )

        await sso_service.record_login(
            org_id=provider_org_id, provider_id=provider_uuid
        )
        user_id = _require_uuid(user.id, "user.id")
        await audit_service.log(
            org_id=provider_org_id,
            action=AuditAction.SSO_LOGIN,
            resource_type=AuditResourceType.SESSION,
            resource_id=str(user_id),
            user_id=user_id,
            extra_metadata={"provider_id": str(provider_uuid), "protocol": "saml"},
        )

        await db.commit()

        auth_service = get_auth_service()
        org_id = str(membership.org_id) if membership else ""
        role = str(membership.role) if membership else "member"
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=org_id,
            role=role,
            is_superuser=bool(user.is_superuser),
            username=_present_str(user.username, "user.username"),
            full_name=_present_str(user.full_name, "user.full_name"),
        )

        return SSOLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user_id=str(user.id),
            email=str(user.email),
            org_id=org_id,
            role=role,
        )


# --- OIDC Flow Endpoints ---


@sso_router.post("/oidc/{provider_id}/authorize", response_model=OIDCAuthResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def initiate_oidc_auth(
    provider_id: str,
    payload: OIDCAuthRequest,
) -> OIDCAuthResponse:
    import os
    import uuid as uuid_mod

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)

        from dev_health_ops.models.sso import SSOProvider

        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")

        if not _is_oidc_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not OIDC")

        if _provider_status(provider) != "active":
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


@sso_router.post("/oidc/{provider_id}/callback", response_model=SSOLoginResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def oidc_callback(
    provider_id: str,
    payload: OIDCCallbackRequest,
) -> SSOLoginResponse:
    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        sso_service = SSOService(db, base_url=base_url)
        audit_service = AuditService(db)

        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="SSO provider not found")
        if not _is_oidc_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not OIDC")
        if _provider_status(provider) != "active":
            raise HTTPException(status_code=400, detail="SSO provider is not active")

        provider_org_id = _require_uuid(provider.org_id, "provider.org_id")
        provider_uuid = _require_uuid(provider.id, "provider.id")

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
                org_id=provider_org_id,
                provider_id=provider_uuid,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider_org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider_uuid),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "oidc"},
            )
            await db.commit()
            raise HTTPException(status_code=400, detail="OIDC authentication failed")

        email = oidc_info.get("email")
        if not email:
            raise HTTPException(
                status_code=400,
                detail="Email address is missing from OIDC claims",
            )

        allowed_domains = _string_list(
            provider.allowed_domains, "provider.allowed_domains"
        )
        if allowed_domains:
            if "@" not in email:
                raise HTTPException(
                    status_code=400,
                    detail="Email address from OIDC claims is malformed",
                )
            email_domain = email.rsplit("@", 1)[-1]
            if email_domain not in allowed_domains:
                raise HTTPException(
                    status_code=403,
                    detail=f"Email domain '{email_domain}' is not allowed for this provider",
                )

        try:
            user, membership, _ = await sso_service.provision_or_get_user(
                org_id=provider_org_id,
                email=email,
                name=oidc_info.get("full_name"),
                provider_id=provider_uuid,
                external_id=oidc_info.get("external_id"),
            )
        except SSOProcessingError as exc:
            logger.error(
                "OIDC user provisioning failed: %s",
                sanitize_for_log(str(exc)),
            )
            await sso_service.record_error(
                org_id=provider_org_id,
                provider_id=provider_uuid,
                error=str(exc),
            )
            await audit_service.log(
                org_id=provider_org_id,
                action=AuditAction.SSO_LOGIN,
                resource_type=AuditResourceType.SSO_PROVIDER,
                resource_id=str(provider_uuid),
                status="failure",
                error_message=str(exc),
                extra_metadata={"protocol": "oidc", "stage": "provisioning"},
            )
            await db.commit()
            raise HTTPException(
                status_code=400,
                detail="OIDC user provisioning failed",
            )

        await sso_service.record_login(
            org_id=provider_org_id, provider_id=provider_uuid
        )
        user_id = _require_uuid(user.id, "user.id")
        await audit_service.log(
            org_id=provider_org_id,
            action=AuditAction.SSO_LOGIN,
            resource_type=AuditResourceType.SESSION,
            resource_id=str(user_id),
            user_id=user_id,
            extra_metadata={"provider_id": str(provider_uuid), "protocol": "oidc"},
        )

        await db.commit()

        auth_service = get_auth_service()
        org_id = str(membership.org_id) if membership else ""
        role = str(membership.role) if membership else "member"
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=org_id,
            role=role,
            is_superuser=bool(user.is_superuser),
            username=_present_str(user.username, "user.username"),
            full_name=_present_str(user.full_name, "user.full_name"),
        )

        return SSOLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user_id=str(user.id),
            email=str(user.email),
            org_id=org_id,
            role=role,
        )


# --- OAuth Flow Endpoints ---


@sso_router.post(
    "/oauth/providers", response_model=SSOProviderResponse, status_code=201
)
@require_feature("sso_saml", required_tier="enterprise")
async def create_oauth_provider(
    payload: OAuthProviderCreate,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> SSOProviderResponse:
    import os
    import uuid as uuid_mod

    from dev_health_ops.api.services.oauth import (
        OAuthProviderType,
        get_default_scopes,
        validate_oauth_config,
    )

    if user.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    oauth_provider_type = OAuthProviderType(payload.provider_type)
    provider_type_map = {
        OAuthProviderType.GITHUB: "oauth_github",
        OAuthProviderType.GITLAB: "oauth_gitlab",
        OAuthProviderType.GOOGLE: "oauth_google",
    }
    protocol = provider_type_map[oauth_provider_type]

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")
    scopes = payload.oauth_config.scopes or get_default_scopes(oauth_provider_type)

    # Validate OAuth config before persisting
    validation_result = validate_oauth_config(
        provider_type=oauth_provider_type,
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
        provider_uuid = _require_uuid(provider.id, "provider.id")
        config["redirect_uri"] = (
            f"{base_url}/api/v1/auth/oauth/{provider_uuid}/callback"
        )
        updated_provider = await sso_service.update_provider(
            org_id=uuid_mod.UUID(user.org_id),
            provider_id=provider_uuid,
            config=config,
        )
        if updated_provider is None:
            raise HTTPException(status_code=404, detail="OAuth provider not found")

        await db.commit()

        return _provider_to_response(updated_provider)


@sso_router.patch("/oauth/providers/{provider_id}", response_model=SSOProviderResponse)
@require_feature("sso_saml", required_tier="enterprise")
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

        if not _is_oauth_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not OAuth")

        config = None
        encrypted_secrets = None

        if payload.oauth_config:
            config = _string_dict(provider.config, "provider.config")
            config["client_id"] = payload.oauth_config.client_id
            if payload.oauth_config.scopes:
                config["scopes"] = payload.oauth_config.scopes
            if payload.oauth_config.base_url:
                config["base_url"] = payload.oauth_config.base_url
            encrypted_secrets = {"client_secret": payload.oauth_config.client_secret}

            # Validate the updated OAuth config
            oauth_provider_type = _provider_oauth_type(provider)
            validation_result = validate_oauth_config(
                provider_type=oauth_provider_type,
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

        updated_provider = await sso_service.update_provider(
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
        if updated_provider is None:
            raise HTTPException(status_code=404, detail="OAuth provider not found")
        await db.commit()

        return _provider_to_response(updated_provider)


@sso_router.post("/oauth/{provider_id}/authorize", response_model=OAuthAuthResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def initiate_oauth_auth(
    provider_id: str,
    payload: OAuthAuthRequest,
) -> OAuthAuthResponse:
    import os
    import uuid as uuid_mod

    from dev_health_ops.api.services.oauth import (
        OAuthConfig,
    )
    from dev_health_ops.api.services.oauth import (
        create_oauth_provider as create_oauth_provider_instance,
    )
    from dev_health_ops.models.sso import SSOProvider

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="OAuth provider not found")

        if not _is_oauth_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not OAuth")

        if _provider_status(provider) != "active":
            raise HTTPException(status_code=400, detail="OAuth provider is not active")

        oauth_config = provider.get_oauth_config()
        oauth_provider_type = _provider_oauth_type(provider)

        # Use stored redirect_uri if available, otherwise generate from provider.id
        redirect_uri = oauth_config.get("redirect_uri")
        if not redirect_uri:
            redirect_uri = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"

        config = OAuthConfig(
            client_id=_require_str(oauth_config["client_id"], "oauth_config.client_id"),
            client_secret=_decrypt_secret(
                _string_dict(provider.encrypted_secrets, "provider.encrypted_secrets"),
                "client_secret",
            ),
            redirect_uri=redirect_uri,
            scopes=_string_list(oauth_config.get("scopes", []), "oauth_config.scopes"),
        )

        oauth_provider_instance = create_oauth_provider_instance(
            provider_type=oauth_provider_type,
            config=config,
            base_url=_optional_str(
                oauth_config.get("base_url"), "oauth_config.base_url"
            ),
        )

        auth_request = oauth_provider_instance.generate_authorization_request()

        return OAuthAuthResponse(
            authorization_url=auth_request.authorization_url,
            state=auth_request.state,
        )


@sso_router.post("/oauth/{provider_id}/callback", response_model=SSOLoginResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def oauth_callback(
    provider_id: str,
    payload: OAuthCallbackRequest,
) -> SSOLoginResponse:
    import os
    import uuid as uuid_mod
    from datetime import datetime, timezone

    from dev_health_ops.api.services.oauth import (
        OAuthConfig,
        OAuthProviderError,
    )
    from dev_health_ops.api.services.oauth import (
        create_oauth_provider as create_oauth_provider_instance,
    )
    from dev_health_ops.models.sso import SSOProvider
    from dev_health_ops.models.users import AuthProvider

    base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

    async with get_postgres_session() as db:
        provider_stmt = select(SSOProvider).where(
            getattr(SSOProvider, "id") == uuid_mod.UUID(provider_id)
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(status_code=404, detail="OAuth provider not found")

        if not _is_oauth_provider(provider):
            raise HTTPException(status_code=400, detail="Provider is not OAuth")

        if _provider_status(provider) != "active":
            raise HTTPException(status_code=400, detail="OAuth provider is not active")

        oauth_config = provider.get_oauth_config()
        oauth_provider_type = _provider_oauth_type(provider)
        provider_org_id = _require_uuid(provider.org_id, "provider.org_id")
        provider_uuid = _require_uuid(provider.id, "provider.id")

        redirect_uri = oauth_config.get("redirect_uri")
        if not redirect_uri:
            redirect_uri = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"

        config = OAuthConfig(
            client_id=_require_str(oauth_config["client_id"], "oauth_config.client_id"),
            client_secret=_decrypt_secret(
                _string_dict(provider.encrypted_secrets, "provider.encrypted_secrets"),
                "client_secret",
            ),
            redirect_uri=redirect_uri,
            scopes=_string_list(oauth_config.get("scopes", []), "oauth_config.scopes"),
        )

        oauth_provider_instance = create_oauth_provider_instance(
            provider_type=oauth_provider_type,
            config=config,
            base_url=_optional_str(
                oauth_config.get("base_url"), "oauth_config.base_url"
            ),
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
                org_id=provider_org_id,
                provider_id=provider_uuid,
                error=str(e),
            )
            await db.commit()
            raise HTTPException(
                status_code=400, detail=f"OAuth authentication failed: {e}"
            )

        allowed_domains = _string_list(
            provider.allowed_domains, "provider.allowed_domains"
        )
        if allowed_domains:
            email_domain = (
                user_info.email.split("@")[-1] if "@" in user_info.email else ""
            )
            if email_domain not in allowed_domains:
                raise HTTPException(
                    status_code=403,
                    detail=f"Email domain '{email_domain}' is not allowed for this provider",
                )

        auth_provider_map = {
            OAuthProviderType.GITHUB: AuthProvider.GITHUB.value,
            OAuthProviderType.GITLAB: AuthProvider.GITLAB.value,
            OAuthProviderType.GOOGLE: AuthProvider.GOOGLE.value,
        }
        auth_provider_value = auth_provider_map[oauth_provider_type]

        user_stmt = select(User).where(getattr(User, "email") == user_info.email)
        user_result = await db.execute(user_stmt)
        user = user_result.scalar_one_or_none()

        if not user:
            if not _require_bool(
                provider.auto_provision_users, "provider.auto_provision_users"
            ):
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

            logger.info(
                "Auto-provisioned user %s via OAuth provider %s; onboarding required",
                sanitize_for_log(user_info.email),
                _require_str(provider.name, "provider.name"),
            )
        else:
            current_auth_provider = _optional_str(
                user.auth_provider, "user.auth_provider"
            )
            if current_auth_provider != auth_provider_value:
                setattr(user, "auth_provider", auth_provider_value)
                setattr(user, "auth_provider_id", user_info.provider_user_id)

            current_avatar_url = _present_str(user.avatar_url, "user.avatar_url")
            if user_info.avatar_url and not current_avatar_url:
                setattr(user, "avatar_url", user_info.avatar_url)
            current_full_name = _present_str(user.full_name, "user.full_name")
            if user_info.full_name and not current_full_name:
                setattr(user, "full_name", user_info.full_name)

        setattr(user, "last_login_at", datetime.now(timezone.utc))

        membership_stmt = select(Membership).where(
            getattr(Membership, "user_id") == user.id,
            getattr(Membership, "org_id") == provider.org_id,
        )
        membership_result = await db.execute(membership_stmt)
        membership = membership_result.scalar_one_or_none()

        if not membership:
            logger.info(
                "OAuth user %s authenticated without membership in org %s; onboarding required",
                sanitize_for_log(user_info.email),
                provider.org_id,
            )

        sso_service = SSOService(db)
        await sso_service.record_login(
            org_id=provider_org_id,
            provider_id=provider_uuid,
        )

        await db.commit()

        auth_service = get_auth_service()
        org_id = str(membership.org_id) if membership else ""
        role = str(membership.role) if membership else "member"
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=org_id,
            role=role,
            is_superuser=bool(user.is_superuser),
            username=_present_str(user.username, "user.username"),
            full_name=_present_str(user.full_name, "user.full_name"),
        )

        return SSOLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            user_id=str(user.id),
            email=str(user.email),
            org_id=org_id,
            role=role,
        )


@sso_router.get("/oauth/{provider_type}/authorize", response_model=OAuthAuthResponse)
@require_feature("sso_saml", required_tier="enterprise")
async def initiate_oauth_by_type(
    provider_type: str,
    org_id: str,
    redirect_uri: str | None = None,
) -> OAuthAuthResponse:
    import os
    import uuid as uuid_mod

    from dev_health_ops.api.services.oauth import (
        OAuthConfig,
    )
    from dev_health_ops.api.services.oauth import (
        create_oauth_provider as create_oauth_provider_instance,
    )
    from dev_health_ops.models.sso import SSOProtocol, SSOProvider

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
            getattr(SSOProvider, "org_id") == uuid_mod.UUID(org_id),
            getattr(SSOProvider, "protocol") == protocol,
            getattr(SSOProvider, "status") == "active",
            getattr(SSOProvider, "is_default").is_(True),  # noqa: E712
        )
        result = await db.execute(provider_stmt)
        provider = result.scalar_one_or_none()

        if not provider:
            provider_stmt = select(SSOProvider).where(
                getattr(SSOProvider, "org_id") == uuid_mod.UUID(org_id),
                getattr(SSOProvider, "protocol") == protocol,
                getattr(SSOProvider, "status") == "active",
            )
            result = await db.execute(provider_stmt)
            provider = result.scalar_one_or_none()

        if not provider:
            raise HTTPException(
                status_code=404,
                detail=f"No active {provider_type} OAuth provider found for this organization",
            )

        oauth_config = provider.get_oauth_config()
        provider_secret_values = _string_dict(
            provider.encrypted_secrets, "provider.encrypted_secrets"
        )

        # Use stored redirect_uri if available, otherwise generate from provider.id
        final_redirect_uri = oauth_config.get("redirect_uri")
        if not final_redirect_uri:
            final_redirect_uri = f"{base_url}/api/v1/auth/oauth/{provider.id}/callback"

        config = OAuthConfig(
            client_id=_require_str(oauth_config["client_id"], "oauth_config.client_id"),
            client_secret=_decrypt_secret(provider_secret_values, "client_secret"),
            redirect_uri=final_redirect_uri,
            scopes=_string_list(oauth_config.get("scopes", []), "oauth_config.scopes"),
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
