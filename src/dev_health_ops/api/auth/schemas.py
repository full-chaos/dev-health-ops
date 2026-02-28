from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SSOProviderResponse(BaseModel):
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
    last_metadata_sync_at: datetime | None
    last_login_at: datetime | None
    last_error: str | None
    last_error_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SSOProviderListResponse(BaseModel):
    items: list[SSOProviderResponse]
    total: int
    limit: int
    offset: int


class SAMLConfigInput(BaseModel):
    entity_id: str = Field(..., description="IdP Entity ID")
    sso_url: str = Field(..., description="IdP SSO URL")
    certificate: str = Field(..., description="IdP signing certificate (PEM format)")
    slo_url: str | None = Field(default=None, description="IdP SLO URL")
    name_id_format: str = Field(
        default="urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
        description="SAML NameID format",
    )
    attribute_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Map IdP attributes to user fields (e.g., {'email': 'emailAddress'})",
    )


class OIDCConfigInput(BaseModel):
    client_id: str = Field(..., description="OIDC Client ID")
    client_secret: str = Field(..., description="OIDC Client Secret")
    issuer: str = Field(..., description="OIDC Issuer URL")
    authorization_endpoint: str | None = Field(
        default=None, description="Authorization endpoint (auto-discovered if not set)"
    )
    token_endpoint: str | None = Field(
        default=None, description="Token endpoint (auto-discovered if not set)"
    )
    userinfo_endpoint: str | None = Field(
        default=None, description="UserInfo endpoint (auto-discovered if not set)"
    )
    jwks_uri: str | None = Field(
        default=None, description="JWKS URI (auto-discovered if not set)"
    )
    scopes: list[str] = Field(
        default_factory=lambda: ["openid", "profile", "email"],
        description="Requested OIDC scopes",
    )
    claim_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Map OIDC claims to user fields (e.g., {'email': 'email'})",
    )


class SSOProviderCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    protocol: str = Field(..., pattern="^(saml|oidc)$")
    saml_config: SAMLConfigInput | None = Field(
        default=None, description="SAML configuration (required if protocol=saml)"
    )
    oidc_config: OIDCConfigInput | None = Field(
        default=None, description="OIDC configuration (required if protocol=oidc)"
    )
    is_default: bool = False
    allow_idp_initiated: bool = False
    auto_provision_users: bool = True
    default_role: str = "member"
    allowed_domains: list[str] = Field(default_factory=list)


class SSOProviderUpdate(BaseModel):
    name: str | None = Field(default=None, max_length=255)
    saml_config: SAMLConfigInput | None = None
    oidc_config: OIDCConfigInput | None = None
    is_default: bool | None = None
    allow_idp_initiated: bool | None = None
    auto_provision_users: bool | None = None
    default_role: str | None = None
    allowed_domains: list[str] | None = None


class SSOProviderActivate(BaseModel):
    activate: bool = True


class SAMLMetadataResponse(BaseModel):
    metadata_xml: str
    entity_id: str
    acs_url: str


class SAMLAuthRequest(BaseModel):
    relay_state: str | None = None


class SAMLAuthResponse(BaseModel):
    redirect_url: str


class SAMLCallbackRequest(BaseModel):
    saml_response: str = Field(..., alias="SAMLResponse")
    relay_state: str | None = Field(default=None, alias="RelayState")

    model_config = ConfigDict(populate_by_name=True)


class OIDCAuthRequest(BaseModel):
    redirect_uri: str | None = None
    use_pkce: bool = True


class OIDCAuthResponse(BaseModel):
    authorization_url: str
    state: str


class OIDCCallbackRequest(BaseModel):
    code: str
    state: str
    code_verifier: str | None = None


class SSOLoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user_id: str
    email: str
    org_id: str
    role: str


class OAuthConfigInput(BaseModel):
    client_id: str = Field(..., description="OAuth Client ID")
    client_secret: str = Field(..., description="OAuth Client Secret")
    scopes: list[str] = Field(
        default_factory=list,
        description="OAuth scopes (uses provider defaults if empty)",
    )
    base_url: str | None = Field(
        default=None,
        description="Base URL for self-hosted instances (GitLab only)",
    )


class OAuthProviderCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    provider_type: str = Field(
        ...,
        pattern="^(github|gitlab|google)$",
        description="OAuth provider type",
    )
    oauth_config: OAuthConfigInput
    is_default: bool = False
    auto_provision_users: bool = True
    default_role: str = "member"
    allowed_domains: list[str] = Field(default_factory=list)


class OAuthProviderUpdate(BaseModel):
    name: str | None = Field(default=None, max_length=255)
    oauth_config: OAuthConfigInput | None = None
    is_default: bool | None = None
    auto_provision_users: bool | None = None
    default_role: str | None = None
    allowed_domains: list[str] | None = None


class OAuthAuthRequest(BaseModel):
    redirect_uri: str | None = Field(
        default=None,
        description="Custom redirect URI (uses default if not provided)",
    )


class OAuthAuthResponse(BaseModel):
    authorization_url: str
    state: str


class OAuthCallbackRequest(BaseModel):
    code: str
    state: str
