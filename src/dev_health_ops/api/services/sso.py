from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urlencode, urlparse
from xml.etree import ElementTree

import httpx
import jwt
from jwt import PyJWKClient
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.sso import (
    SSOProtocol,
    SSOProvider,
    SSOProviderStatus,
)
from dev_health_ops.models.users import AuthProvider, Membership, User
from dev_health_ops.api.utils.logging import sanitize_for_log

logger = logging.getLogger(__name__)


class SSOProcessingError(Exception):
    pass


class SAMLProcessingError(SSOProcessingError):
    pass


class OIDCProcessingError(SSOProcessingError):
    pass


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
    # Security and timing constants
    SAML_TIMESTAMP_SKEW_MINUTES = 5
    OIDC_JWT_LEEWAY_SECONDS = 60
    HTTP_TIMEOUT_SECONDS = 30.0
    
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

    async def process_saml_response(
        self,
        provider: SSOProvider,
        saml_response: str,
        relay_state: str | None,
    ) -> dict[str, Any]:
        if not provider.is_saml:
            raise SAMLProcessingError("Provider is not SAML")

        config = provider.get_saml_config()
        sp_entity_id = (
            config.get("sp_entity_id") or f"{self.base_url}/saml/{provider.id}/metadata"
        )
        sp_acs_url = (
            config.get("sp_acs_url") or f"{self.base_url}/saml/{provider.id}/acs"
        )

        if not saml_response:
            raise SAMLProcessingError("Missing SAMLResponse payload")

        try:
            decoded_response = base64.b64decode(saml_response, validate=True)
        except (ValueError, TypeError) as exc:
            raise SAMLProcessingError("Invalid base64 SAMLResponse") from exc

        response_xml = self._parse_saml_xml(decoded_response)
        self._validate_saml_signature(response_xml, config.get("certificate"))

        namespaces = {
            "samlp": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml": "urn:oasis:names:tc:SAML:2.0:assertion",
        }

        status_code = response_xml.find("./samlp:Status/samlp:StatusCode", namespaces)
        if status_code is None or status_code.get("Value") != (
            "urn:oasis:names:tc:SAML:2.0:status:Success"
        ):
            raise SAMLProcessingError("SAML response status is not Success")

        issuer = self._find_text(
            response_xml, "./saml:Issuer", namespaces
        ) or self._find_text(response_xml, ".//saml:Issuer", namespaces)
        if issuer != config.get("entity_id"):
            raise SAMLProcessingError("SAML issuer mismatch")

        assertion = response_xml.find(".//saml:Assertion", namespaces)
        if assertion is None:
            raise SAMLProcessingError("SAML assertion missing")

        audience = self._find_text(
            assertion, ".//saml:AudienceRestriction/saml:Audience", namespaces
        )
        if audience and audience != sp_entity_id:
            raise SAMLProcessingError("SAML audience mismatch")

        subject_confirmation = assertion.find(
            ".//saml:SubjectConfirmationData", namespaces
        )
        if subject_confirmation is None:
            raise SAMLProcessingError("SAML subject confirmation missing")

        recipient = subject_confirmation.get("Recipient")
        if recipient and recipient != sp_acs_url:
            raise SAMLProcessingError("SAML recipient mismatch")

        self._validate_saml_timestamps(assertion, subject_confirmation, namespaces)

        name_id = self._find_text(assertion, ".//saml:Subject/saml:NameID", namespaces)
        attributes = self._extract_saml_attributes(assertion, namespaces)
        mapped = self._map_attributes(attributes, config.get("attribute_mapping", {}))

        email = mapped.get("email") or attributes.get("email") or None
        if not email and name_id and "@" in name_id:
            email = name_id

        if not email:
            raise SAMLProcessingError("SAML assertion missing email attribute")

        full_name = mapped.get("full_name") or mapped.get("name")

        return {
            "email": email,
            "full_name": full_name,
            "external_id": name_id,
            "relay_state": relay_state,
            "attributes": attributes,
        }

    async def process_oidc_callback(
        self,
        provider: SSOProvider,
        code: str,
        state: str,
        code_verifier: str | None,
    ) -> dict[str, Any]:
        if not provider.is_oidc:
            raise OIDCProcessingError("Provider is not OIDC")

        if not code:
            raise OIDCProcessingError("Missing authorization code")

        config = provider.get_oidc_config()
        oidc_metadata = await self._get_oidc_metadata(config)

        expected_state = self._get_expected_state(provider)
        if not state or not expected_state:
            raise OIDCProcessingError("Missing OIDC state")
        if state != expected_state:
            raise OIDCProcessingError("OIDC state mismatch")

        token_response = await self._exchange_oidc_code(
            provider=provider,
            code=code,
            code_verifier=code_verifier,
            token_endpoint=oidc_metadata["token_endpoint"],
        )

        id_token = token_response.get("id_token")
        if not id_token:
            raise OIDCProcessingError("OIDC token response missing id_token")

        id_claims = await self._validate_id_token(
            id_token=id_token,
            issuer=config.get("issuer") or oidc_metadata.get("issuer"),
            client_id=config.get("client_id"),
            jwks_uri=oidc_metadata.get("jwks_uri"),
            expected_nonce=self._get_expected_nonce(provider),
        )

        userinfo_claims = None
        if oidc_metadata.get("userinfo_endpoint") and token_response.get(
            "access_token"
        ):
            userinfo_claims = await self._fetch_userinfo(
                oidc_metadata["userinfo_endpoint"], token_response["access_token"]
            )

        claim_mapping = config.get("claim_mapping", {})
        merged_claims = {**(userinfo_claims or {}), **id_claims}
        mapped = self._map_attributes(merged_claims, claim_mapping)

        email = mapped.get("email") or merged_claims.get("email")
        if not email:
            raise OIDCProcessingError("OIDC claims missing email")

        full_name = mapped.get("full_name") or mapped.get("name")

        return {
            "email": email,
            "full_name": full_name,
            "external_id": merged_claims.get("sub"),
            "id_claims": id_claims,
        }

    async def provision_or_get_user(
        self,
        org_id: uuid.UUID,
        email: str,
        name: str | None,
        provider_id: uuid.UUID,
        external_id: str | None = None,
    ) -> tuple[User, Membership, SSOProvider]:
        provider = await self.get_provider(org_id, provider_id)
        if not provider:
            raise SSOProcessingError("SSO provider not found")

        stmt = select(User).where(User.email == email)
        result = await self.session.execute(stmt)
        user = result.scalar_one_or_none()

        auth_provider_value = (
            AuthProvider.SAML.value if provider.is_saml else AuthProvider.OIDC.value
        )

        if not user:
            if not provider.auto_provision_users:
                raise SSOProcessingError(
                    "User not found and auto-provisioning disabled"
                )

            user = User(
                email=email,
                full_name=name,
                auth_provider=auth_provider_value,
                auth_provider_id=external_id,
                is_active=True,
                is_verified=True,
            )
            self.session.add(user)
            await self.session.flush()

            membership = Membership(
                user_id=user.id,
                org_id=provider.org_id,
                role=provider.default_role,
                joined_at=datetime.now(timezone.utc),
            )
            self.session.add(membership)
            await self.session.flush()

            logger.info(
                "Auto-provisioned user %s via %s provider %s",
                sanitize_for_log(email),
                provider.protocol,
                provider.name,
            )
        else:
            if user.auth_provider != auth_provider_value:
                user.auth_provider = auth_provider_value
            if external_id and user.auth_provider_id != external_id:
                user.auth_provider_id = external_id
            if name and not user.full_name:
                user.full_name = name

            membership_stmt = select(Membership).where(
                Membership.user_id == user.id, Membership.org_id == provider.org_id
            )
            membership_result = await self.session.execute(membership_stmt)
            membership = membership_result.scalar_one_or_none()

            if not membership:
                if not provider.auto_provision_users:
                    raise SSOProcessingError(
                        "User is not a member of this organization"
                    )
                membership = Membership(
                    user_id=user.id,
                    org_id=provider.org_id,
                    role=provider.default_role,
                    joined_at=datetime.now(timezone.utc),
                )
                self.session.add(membership)
                await self.session.flush()

        user.last_login_at = datetime.now(timezone.utc)
        return user, membership, provider

    @staticmethod
    def _parse_saml_xml(xml_bytes: bytes) -> ElementTree.Element:
        try:
            try:
                from defusedxml import ElementTree as safe_tree

                return safe_tree.fromstring(xml_bytes)
            except ImportError as exc:
                logger.error(
                    "defusedxml is required for secure SAML XML parsing but is not installed"
                )
                raise SAMLProcessingError(
                    "SAML processing requires the 'defusedxml' package to be installed"
                ) from exc
        except ElementTree.ParseError as exc:
            raise SAMLProcessingError("Invalid SAML XML") from exc

    @staticmethod
    def _find_text(
        element: ElementTree.Element,
        path: str,
        namespaces: Mapping[str, str],
    ) -> str | None:
        node = element.find(path, namespaces)
        if node is None or node.text is None:
            return None
        return node.text.strip()

    def _validate_saml_signature(
        self, xml_root: ElementTree.Element, certificate: str | None
    ) -> None:
        if not certificate:
            raise SAMLProcessingError("SAML certificate is required for validation")

        try:
            from signxml import XMLVerifier  # type: ignore[import-not-found]
        except ImportError as exc:
            raise SAMLProcessingError(
                "SAML signature validation requires signxml"
            ) from exc

        try:
            XMLVerifier().verify(xml_root, x509_cert=certificate)
        except (ValueError, KeyError, AttributeError) as exc:
            raise SAMLProcessingError("SAML signature validation failed") from exc

    @staticmethod
    def _extract_saml_attributes(
        assertion: ElementTree.Element,
        namespaces: Mapping[str, str],
    ) -> dict[str, str]:
        attributes: dict[str, str] = {}
        for attr in assertion.findall(".//saml:Attribute", namespaces):
            name = attr.get("Name")
            if not name:
                continue
            value_node = attr.find("./saml:AttributeValue", namespaces)
            if value_node is not None and value_node.text:
                attributes[name] = value_node.text.strip()
        return attributes

    @staticmethod
    def _map_attributes(
        attributes: Mapping[str, Any],
        mapping: Mapping[str, str],
    ) -> dict[str, str]:
        mapped: dict[str, str] = {}
        for target_field, source_field in mapping.items():
            value = attributes.get(source_field)
            if isinstance(value, str) and value.strip():
                mapped[target_field] = value.strip()
        return mapped

    def _validate_saml_timestamps(
        self,
        assertion: ElementTree.Element,
        subject_confirmation: ElementTree.Element,
        namespaces: Mapping[str, str],
    ) -> None:
        now = datetime.now(timezone.utc)
        skew = timedelta(minutes=self.SAML_TIMESTAMP_SKEW_MINUTES)

        conditions = assertion.find("./saml:Conditions", namespaces)
        if conditions is not None:
            not_before = conditions.get("NotBefore")
            not_on_or_after = conditions.get("NotOnOrAfter")

            if not_before:
                not_before_dt = datetime.fromisoformat(
                    not_before.replace("Z", "+00:00")
                )
                if now + skew < not_before_dt:
                    raise SAMLProcessingError("SAML assertion not yet valid")

            if not_on_or_after:
                not_on_or_after_dt = datetime.fromisoformat(
                    not_on_or_after.replace("Z", "+00:00")
                )
                if now - skew >= not_on_or_after_dt:
                    raise SAMLProcessingError("SAML assertion expired")

        subject_not_on_or_after = subject_confirmation.get("NotOnOrAfter")
        if subject_not_on_or_after:
            subject_expiry = datetime.fromisoformat(
                subject_not_on_or_after.replace("Z", "+00:00")
            )
            if now - skew >= subject_expiry:
                raise SAMLProcessingError("SAML subject confirmation expired")

    @staticmethod
    def _get_expected_state(provider: SSOProvider) -> str | None:
        return (provider.encrypted_secrets or {}).get("expected_state") or (
            provider.config or {}
        ).get("expected_state")

    @staticmethod
    def _get_expected_nonce(provider: SSOProvider) -> str | None:
        return (provider.encrypted_secrets or {}).get("expected_nonce") or (
            provider.config or {}
        ).get("expected_nonce")

    async def _get_oidc_metadata(self, config: dict[str, Any]) -> dict[str, str]:
        metadata = {
            "issuer": config.get("issuer"),
            "token_endpoint": config.get("token_endpoint"),
            "userinfo_endpoint": config.get("userinfo_endpoint"),
            "jwks_uri": config.get("jwks_uri"),
        }

        if metadata["token_endpoint"] and metadata["jwks_uri"]:
            return metadata

        issuer = config.get("issuer")
        if not issuer:
            raise OIDCProcessingError("OIDC issuer is required")
        
        # Validate issuer URL to prevent SSRF attacks
        try:
            parsed = urlparse(issuer)
            if parsed.scheme not in ("https",):
                raise OIDCProcessingError("OIDC issuer must use HTTPS")
            if not parsed.netloc:
                raise OIDCProcessingError("OIDC issuer must be a valid URL")
        except ValueError as exc:
            raise OIDCProcessingError("Invalid OIDC issuer URL") from exc

        discovery_url = issuer.rstrip("/") + "/.well-known/openid-configuration"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    discovery_url, timeout=self.HTTP_TIMEOUT_SECONDS
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise OIDCProcessingError(
                    "Failed to fetch OIDC discovery document"
                ) from exc

        data = response.json()
        metadata.update(
            {
                "issuer": data.get("issuer", issuer),
                "token_endpoint": data.get("token_endpoint"),
                "userinfo_endpoint": data.get("userinfo_endpoint"),
                "jwks_uri": data.get("jwks_uri"),
            }
        )

        if not metadata["token_endpoint"] or not metadata["jwks_uri"]:
            raise OIDCProcessingError("OIDC discovery missing required endpoints")

        return metadata

    async def _exchange_oidc_code(
        self,
        provider: SSOProvider,
        code: str,
        code_verifier: str | None,
        token_endpoint: str,
    ) -> dict[str, Any]:
        config = provider.get_oidc_config()
        secrets = provider.encrypted_secrets or {}

        redirect_uri = f"{self.base_url}/oidc/{provider.id}/callback"
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": config.get("client_id"),
        }

        client_secret = secrets.get("client_secret")
        if client_secret:
            payload["client_secret"] = client_secret
        if code_verifier:
            payload["code_verifier"] = code_verifier

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    token_endpoint,
                    data=payload,
                    headers={"Accept": "application/json"},
                    timeout=self.HTTP_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise OIDCProcessingError("OIDC token exchange failed") from exc

        data = response.json()
        if "access_token" not in data:
            raise OIDCProcessingError("OIDC token response missing access_token")

        return data

    async def _validate_id_token(
        self,
        id_token: str,
        issuer: str | None,
        client_id: str | None,
        jwks_uri: str | None,
        expected_nonce: str | None,
    ) -> dict[str, Any]:
        if not issuer or not client_id or not jwks_uri:
            raise OIDCProcessingError(
                "OIDC configuration missing issuer/client_id/jwks"
            )

        try:
            jwk_client = PyJWKClient(jwks_uri)
            signing_key = jwk_client.get_signing_key_from_jwt(id_token)
            claims = jwt.decode(
                id_token,
                signing_key.key,
                algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
                audience=client_id,
                issuer=issuer,
                options={"require": ["exp", "iat", "iss", "aud"]},
                leeway=self.OIDC_JWT_LEEWAY_SECONDS,
            )
        except (jwt.PyJWTError, httpx.HTTPError, ValueError, KeyError) as exc:
            raise OIDCProcessingError("OIDC id_token validation failed") from exc

        if expected_nonce and claims.get("nonce") != expected_nonce:
            raise OIDCProcessingError("OIDC nonce mismatch")

        return claims

    async def _fetch_userinfo(
        self, userinfo_endpoint: str, access_token: str
    ) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    userinfo_endpoint,
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=self.HTTP_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise OIDCProcessingError("OIDC userinfo request failed") from exc

        return response.json()

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
