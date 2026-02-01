from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class SSOProtocol(str, Enum):
    SAML = "saml"
    OIDC = "oidc"
    OAUTH_GITHUB = "oauth_github"
    OAUTH_GITLAB = "oauth_gitlab"
    OAUTH_GOOGLE = "oauth_google"


class SSOProviderStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING_SETUP = "pending_setup"
    ERROR = "error"


class SSOProvider(Base):
    __tablename__ = "sso_providers"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    name = Column(Text, nullable=False)
    protocol = Column(Text, nullable=False, index=True)
    status = Column(
        Text,
        nullable=False,
        default=SSOProviderStatus.PENDING_SETUP.value,
    )

    # Common SSO settings
    is_default = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    allow_idp_initiated = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    auto_provision_users = Column(
        Boolean,
        nullable=False,
        default=True,
    )
    default_role = Column(
        Text,
        nullable=False,
        default="member",
    )

    # SAML-specific configuration (stored in config JSON)
    # - entity_id: IdP Entity ID
    # - sso_url: IdP SSO URL
    # - slo_url: IdP SLO URL (optional)
    # - certificate: IdP signing certificate (PEM)
    # - sp_entity_id: Service Provider Entity ID
    # - sp_acs_url: Assertion Consumer Service URL
    # - name_id_format: NameID format
    # - attribute_mapping: Map IdP attributes to user fields

    # OIDC-specific configuration (stored in config JSON)
    # - client_id: OIDC Client ID
    # - client_secret: OIDC Client Secret (encrypted)
    # - issuer: OIDC Issuer URL
    # - authorization_endpoint: Authorization endpoint (auto-discovered or manual)
    # - token_endpoint: Token endpoint
    # - userinfo_endpoint: UserInfo endpoint
    # - jwks_uri: JWKS URI
    # - scopes: Requested scopes
    # - claim_mapping: Map OIDC claims to user fields

    config = Column(
        JSON,
        nullable=False,
        default=dict,
    )
    encrypted_secrets = Column(
        JSON,
        nullable=True,
        default=dict,
    )

    # Domain restrictions for this provider
    allowed_domains = Column(
        JSON,
        nullable=True,
        default=list,
    )

    # Last sync/validation timestamps
    last_metadata_sync_at = Column(DateTime(timezone=True), nullable=True)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint("org_id", "name", name="uq_sso_provider_org_name"),
        Index("ix_sso_providers_org_protocol", "org_id", "protocol"),
        Index("ix_sso_providers_org_status", "org_id", "status"),
    )

    def __init__(
        self,
        org_id: uuid.UUID,
        name: str,
        protocol: str,
        config: Optional[dict[str, Any]] = None,
        encrypted_secrets: Optional[dict[str, Any]] = None,
        status: str = SSOProviderStatus.PENDING_SETUP.value,
        is_default: bool = False,
        allow_idp_initiated: bool = False,
        auto_provision_users: bool = True,
        default_role: str = "member",
        allowed_domains: Optional[list[str]] = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.name = name
        self.protocol = protocol
        self.config = config or {}
        self.encrypted_secrets = encrypted_secrets or {}
        self.status = status
        self.is_default = is_default
        self.allow_idp_initiated = allow_idp_initiated
        self.auto_provision_users = auto_provision_users
        self.default_role = default_role
        self.allowed_domains = allowed_domains or []
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return f"<SSOProvider {self.name} ({self.protocol}) org={self.org_id}>"

    @property
    def is_saml(self) -> bool:
        return self.protocol == SSOProtocol.SAML.value

    @property
    def is_oidc(self) -> bool:
        return self.protocol == SSOProtocol.OIDC.value

    def get_saml_config(self) -> dict[str, Any]:
        if not self.is_saml:
            raise ValueError("Provider is not SAML")
        return {
            "entity_id": self.config.get("entity_id"),
            "sso_url": self.config.get("sso_url"),
            "slo_url": self.config.get("slo_url"),
            "certificate": self.config.get("certificate"),
            "sp_entity_id": self.config.get("sp_entity_id"),
            "sp_acs_url": self.config.get("sp_acs_url"),
            "name_id_format": self.config.get(
                "name_id_format",
                "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
            ),
            "attribute_mapping": self.config.get("attribute_mapping", {}),
        }

    def get_oidc_config(self) -> dict[str, Any]:
        if not self.is_oidc:
            raise ValueError("Provider is not OIDC")
        return {
            "client_id": self.config.get("client_id"),
            "issuer": self.config.get("issuer"),
            "authorization_endpoint": self.config.get("authorization_endpoint"),
            "token_endpoint": self.config.get("token_endpoint"),
            "userinfo_endpoint": self.config.get("userinfo_endpoint"),
            "jwks_uri": self.config.get("jwks_uri"),
            "scopes": self.config.get("scopes", ["openid", "profile", "email"]),
            "claim_mapping": self.config.get("claim_mapping", {}),
        }

    @property
    def is_oauth(self) -> bool:
        return self.protocol in (
            SSOProtocol.OAUTH_GITHUB.value,
            SSOProtocol.OAUTH_GITLAB.value,
            SSOProtocol.OAUTH_GOOGLE.value,
        )

    @property
    def oauth_provider_type(self) -> Optional[str]:
        if self.protocol == SSOProtocol.OAUTH_GITHUB.value:
            return "github"
        elif self.protocol == SSOProtocol.OAUTH_GITLAB.value:
            return "gitlab"
        elif self.protocol == SSOProtocol.OAUTH_GOOGLE.value:
            return "google"
        return None

    def get_oauth_config(self) -> dict[str, Any]:
        if not self.is_oauth:
            raise ValueError("Provider is not OAuth")
        return {
            "client_id": self.config.get("client_id"),
            "redirect_uri": self.config.get("redirect_uri"),
            "scopes": self.config.get("scopes", []),
            "base_url": self.config.get("base_url"),
        }
