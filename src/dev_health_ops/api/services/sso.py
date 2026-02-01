from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from urllib.parse import urlencode

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.sso import (
    SSOProtocol,
    SSOProvider,
    SSOProviderStatus,
)
from dev_health_ops.api.utils.logging import sanitize_for_log

logger = logging.getLogger(__name__)


@dataclass
class SAMLConfig:
    entity_id: str
    sso_url: str
    certificate: str
    slo_url: Optional[str] = None
    sp_entity_id: Optional[str] = None
    sp_acs_url: Optional[str] = None
    name_id_format: str = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
    attribute_mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class OIDCConfig:
    client_id: str
    issuer: str
    authorization_endpoint: Optional[str] = None
    token_endpoint: Optional[str] = None
    userinfo_endpoint: Optional[str] = None
    jwks_uri: Optional[str] = None
    scopes: list[str] = field(default_factory=lambda: ["openid", "profile", "email"])
    claim_mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class SSOProviderEntry:
    id: str
    org_id: str
    name: str
    protocol: str
    status: str
    is_default: bool
    allow_idp_initiated: bool
    auto_provision_users: bool
    default_role: str
    config: dict[str, Any]
    allowed_domains: list[str]
    last_metadata_sync_at: Optional[datetime]
    last_login_at: Optional[datetime]
    last_error: Optional[str]
    last_error_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass
class OIDCAuthorizationRequest:
    authorization_url: str
    state: str
    nonce: str
    code_verifier: Optional[str] = None


class SSOService:
    def __init__(self, session: AsyncSession, base_url: str = ""):
        self.session = session
        self.base_url = base_url.rstrip("/")

    async def create_provider(
        self,
        org_id: uuid.UUID,
        name: str,
        protocol: SSOProtocol | str,
        config: dict[str, Any],
        encrypted_secrets: Optional[dict[str, Any]] = None,
        is_default: bool = False,
        allow_idp_initiated: bool = False,
        auto_provision_users: bool = True,
        default_role: str = "member",
        allowed_domains: Optional[list[str]] = None,
    ) -> SSOProvider:
        protocol_str = protocol.value if isinstance(protocol, SSOProtocol) else protocol

        if is_default:
            await self._clear_default_provider(org_id, protocol_str)

        provider = SSOProvider(
            org_id=org_id,
            name=name,
            protocol=protocol_str,
            config=config,
            encrypted_secrets=encrypted_secrets,
            status=SSOProviderStatus.PENDING_SETUP.value,
            is_default=is_default,
            allow_idp_initiated=allow_idp_initiated,
            auto_provision_users=auto_provision_users,
            default_role=default_role,
            allowed_domains=allowed_domains,
        )

        self.session.add(provider)
        await self.session.flush()

        logger.info(
            "SSO provider created: %s (%s) for org=%s",
            sanitize_for_log(name),
            sanitize_for_log(protocol_str),
            org_id,
        )

        return provider

    async def get_provider(
        self, org_id: uuid.UUID, provider_id: uuid.UUID
    ) -> Optional[SSOProvider]:
        stmt = select(SSOProvider).where(
            and_(SSOProvider.id == provider_id, SSOProvider.org_id == org_id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_provider_by_name(
        self, org_id: uuid.UUID, name: str
    ) -> Optional[SSOProvider]:
        stmt = select(SSOProvider).where(
            and_(SSOProvider.org_id == org_id, SSOProvider.name == name)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_default_provider(
        self, org_id: uuid.UUID, protocol: Optional[str] = None
    ) -> Optional[SSOProvider]:
        conditions = [
            SSOProvider.org_id == org_id,
            SSOProvider.is_default == True,  # noqa: E712
            SSOProvider.status == SSOProviderStatus.ACTIVE.value,
        ]
        if protocol:
            conditions.append(SSOProvider.protocol == protocol)

        stmt = select(SSOProvider).where(and_(*conditions))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_providers(
        self,
        org_id: uuid.UUID,
        protocol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[Sequence[SSOProvider], int]:
        conditions = [SSOProvider.org_id == org_id]

        if protocol:
            conditions.append(SSOProvider.protocol == protocol)
        if status:
            conditions.append(SSOProvider.status == status)

        count_stmt = select(SSOProvider).where(and_(*conditions))
        count_result = await self.session.execute(count_stmt)
        total = len(count_result.scalars().all())

        stmt = (
            select(SSOProvider)
            .where(and_(*conditions))
            .order_by(SSOProvider.created_at)
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(stmt)
        providers = result.scalars().all()

        return providers, total

    async def update_provider(
        self,
        org_id: uuid.UUID,
        provider_id: uuid.UUID,
        name: Optional[str] = None,
        config: Optional[dict[str, Any]] = None,
        encrypted_secrets: Optional[dict[str, Any]] = None,
        status: Optional[str] = None,
        is_default: Optional[bool] = None,
        allow_idp_initiated: Optional[bool] = None,
        auto_provision_users: Optional[bool] = None,
        default_role: Optional[str] = None,
        allowed_domains: Optional[list[str]] = None,
    ) -> Optional[SSOProvider]:
        provider = await self.get_provider(org_id, provider_id)
        if not provider:
            return None

        if name is not None:
            provider.name = name
        if config is not None:
            provider.config = config
        if encrypted_secrets is not None:
            provider.encrypted_secrets = encrypted_secrets
        if status is not None:
            provider.status = status
        if is_default is not None:
            if is_default:
                await self._clear_default_provider(org_id, str(provider.protocol))
            provider.is_default = is_default
        if allow_idp_initiated is not None:
            provider.allow_idp_initiated = allow_idp_initiated
        if auto_provision_users is not None:
            provider.auto_provision_users = auto_provision_users
        if default_role is not None:
            provider.default_role = default_role
        if allowed_domains is not None:
            provider.allowed_domains = allowed_domains

        provider.updated_at = datetime.now(timezone.utc)
        await self.session.flush()

        logger.info("SSO provider updated: %s for org=%s", provider_id, org_id)
        return provider

    async def delete_provider(self, org_id: uuid.UUID, provider_id: uuid.UUID) -> bool:
        provider = await self.get_provider(org_id, provider_id)
        if not provider:
            return False

        await self.session.delete(provider)
        await self.session.flush()

        logger.info("SSO provider deleted: %s for org=%s", provider_id, org_id)
        return True

    async def activate_provider(
        self, org_id: uuid.UUID, provider_id: uuid.UUID
    ) -> Optional[SSOProvider]:
        return await self.update_provider(
            org_id, provider_id, status=SSOProviderStatus.ACTIVE.value
        )

    async def deactivate_provider(
        self, org_id: uuid.UUID, provider_id: uuid.UUID
    ) -> Optional[SSOProvider]:
        return await self.update_provider(
            org_id, provider_id, status=SSOProviderStatus.INACTIVE.value
        )

    async def record_login(self, org_id: uuid.UUID, provider_id: uuid.UUID) -> None:
        provider = await self.get_provider(org_id, provider_id)
        if provider:
            provider.last_login_at = datetime.now(timezone.utc)
            await self.session.flush()

    async def record_error(
        self, org_id: uuid.UUID, provider_id: uuid.UUID, error: str
    ) -> None:
        provider = await self.get_provider(org_id, provider_id)
        if provider:
            provider.last_error = error
            provider.last_error_at = datetime.now(timezone.utc)
            provider.status = SSOProviderStatus.ERROR.value
            await self.session.flush()

    def generate_saml_sp_metadata(self, provider: SSOProvider) -> str:
        if not provider.is_saml:
            raise ValueError("Provider is not SAML")

        config = provider.get_saml_config()
        sp_entity_id = (
            config.get("sp_entity_id") or f"{self.base_url}/saml/{provider.id}/metadata"
        )
        sp_acs_url = (
            config.get("sp_acs_url") or f"{self.base_url}/saml/{provider.id}/acs"
        )

        return f"""<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
                     entityID="{sp_entity_id}">
    <md:SPSSODescriptor AuthnRequestsSigned="false"
                        WantAssertionsSigned="true"
                        protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
        <md:NameIDFormat>{config.get("name_id_format", "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress")}</md:NameIDFormat>
        <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
                                     Location="{sp_acs_url}"
                                     index="0"
                                     isDefault="true"/>
    </md:SPSSODescriptor>
</md:EntityDescriptor>"""

    def generate_saml_auth_request_url(
        self,
        provider: SSOProvider,
        relay_state: Optional[str] = None,
    ) -> str:
        if not provider.is_saml:
            raise ValueError("Provider is not SAML")

        config = provider.get_saml_config()
        sp_entity_id = (
            config.get("sp_entity_id") or f"{self.base_url}/saml/{provider.id}/metadata"
        )
        sp_acs_url = (
            config.get("sp_acs_url") or f"{self.base_url}/saml/{provider.id}/acs"
        )
        request_id = f"_id{secrets.token_hex(16)}"
        issue_instant = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        saml_request = f"""<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    ID="{request_id}"
    Version="2.0"
    IssueInstant="{issue_instant}"
    Destination="{config["sso_url"]}"
    ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
    AssertionConsumerServiceURL="{sp_acs_url}">
    <saml:Issuer>{sp_entity_id}</saml:Issuer>
    <samlp:NameIDPolicy Format="{config.get("name_id_format", "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress")}"
                        AllowCreate="true"/>
</samlp:AuthnRequest>"""

        import base64
        import zlib

        deflated = zlib.compress(saml_request.encode())[2:-4]
        encoded = base64.b64encode(deflated).decode()

        params = {"SAMLRequest": encoded}
        if relay_state:
            params["RelayState"] = relay_state

        return f"{config['sso_url']}?{urlencode(params)}"

    def generate_oidc_authorization_request(
        self,
        provider: SSOProvider,
        redirect_uri: Optional[str] = None,
        use_pkce: bool = True,
    ) -> OIDCAuthorizationRequest:
        if not provider.is_oidc:
            raise ValueError("Provider is not OIDC")

        config = provider.get_oidc_config()
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)

        redirect = redirect_uri or f"{self.base_url}/oidc/{provider.id}/callback"
        scopes = " ".join(config.get("scopes", ["openid", "profile", "email"]))

        params: dict[str, str] = {
            "response_type": "code",
            "client_id": config["client_id"],
            "redirect_uri": redirect,
            "scope": scopes,
            "state": state,
            "nonce": nonce,
        }

        code_verifier: Optional[str] = None
        if use_pkce:
            code_verifier = secrets.token_urlsafe(64)
            code_challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(code_verifier.encode()).digest()
                )
                .decode()
                .rstrip("=")
            )
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = "S256"

        auth_endpoint = (
            config.get("authorization_endpoint") or f"{config['issuer']}/authorize"
        )
        authorization_url = f"{auth_endpoint}?{urlencode(params)}"

        return OIDCAuthorizationRequest(
            authorization_url=authorization_url,
            state=state,
            nonce=nonce,
            code_verifier=code_verifier,
        )

    async def _clear_default_provider(self, org_id: uuid.UUID, protocol: str) -> None:
        stmt = select(SSOProvider).where(
            and_(
                SSOProvider.org_id == org_id,
                SSOProvider.protocol == protocol,
                SSOProvider.is_default == True,  # noqa: E712
            )
        )
        result = await self.session.execute(stmt)
        for provider in result.scalars().all():
            provider.is_default = False
        await self.session.flush()

    @staticmethod
    def to_entry(provider: SSOProvider) -> SSOProviderEntry:
        return SSOProviderEntry(
            id=str(provider.id),
            org_id=str(provider.org_id),
            name=str(provider.name),
            protocol=str(provider.protocol),
            status=str(provider.status),
            is_default=bool(provider.is_default),
            allow_idp_initiated=bool(provider.allow_idp_initiated),
            auto_provision_users=bool(provider.auto_provision_users),
            default_role=str(provider.default_role),
            config=dict(provider.config) if provider.config else {},
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
