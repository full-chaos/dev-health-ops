"""PagerDuty OAuth setup endpoints.

This router deliberately owns only the authorization start and callback flow.
Status, disconnect, preflight, and alternate authentication modes belong in this
module as separately scoped follow-up endpoints.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import (  # noqa: F401
    get_admin_org_id,
    get_admin_user,
)
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.licensing import is_org_feature_enabled_async
from dev_health_ops.licensing.registry import CANONICAL_INCIDENT_INGESTION_FEATURE

router = APIRouter()

_FEATURE_DISABLED_DETAIL = (
    "Canonical incident ingestion is not enabled for this organization"
)


async def _require_canonical_incident_ingestion(
    session: AsyncSession, org_id: str
) -> None:
    try:
        parsed_org_id = uuid.UUID(org_id)
    except ValueError:
        allowed = False
    else:
        try:
            allowed = await is_org_feature_enabled_async(
                session,
                parsed_org_id,
                CANONICAL_INCIDENT_INGESTION_FEATURE,
            )
        except SQLAlchemyError:
            allowed = False
    if not allowed:
        raise HTTPException(status_code=403, detail=_FEATURE_DISABLED_DETAIL)


class PagerDutyAuthorizeRequest(BaseModel):
    """Empty published-app OAuth request; setup details are provider-derived."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class PagerDutyAuthorizeResponse(BaseModel):
    """Provider authorization URL without server-held PKCE data."""

    model_config = ConfigDict(frozen=True)

    authorize_url: str


class PagerDutyCallbackRequest(BaseModel):
    """Frontend-mediated PagerDuty OAuth callback inputs."""

    model_config = ConfigDict(extra="forbid")

    state: str = Field(min_length=1)
    code: str | None = None
    error: str | None = None


class PagerDutyCallbackResponse(BaseModel):
    """Non-secret PagerDuty OAuth connection result."""

    model_config = ConfigDict(frozen=True)

    connected: Literal[True]
    credential_name: str
    region: str
    subdomain: str
    granted_scopes: list[str]


class PagerDutyStatusResponse(BaseModel):
    """Non-secret PagerDuty credential status."""

    model_config = ConfigDict(frozen=True)

    connected: bool
    credential_name: str
    auth_mode: str | None
    region: str | None
    subdomain: str | None
    account_id: str | None
    account_display: str | None
    granted_scopes: list[str]
    expires_at: datetime | None
    has_refresh_token: bool


class PagerDutyDisconnectRequest(BaseModel):
    """Named PagerDuty credential to deactivate."""

    model_config = ConfigDict(extra="forbid")

    credential_name: str = "default"

    @field_validator("credential_name")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class PagerDutyDisconnectResponse(BaseModel):
    """Idempotent PagerDuty disconnection result."""

    model_config = ConfigDict(frozen=True)

    disconnected: Literal[True]
    credential_name: str


class PagerDutyPreflightRequest(BaseModel):
    """Requested PagerDuty datasets for a scope readiness check."""

    model_config = ConfigDict(extra="forbid")

    credential_name: str = "default"
    enabled_datasets: list[str]

    @field_validator("credential_name")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class PagerDutyDatasetPreflight(BaseModel):
    """Scope readiness for one requested PagerDuty dataset."""

    model_config = ConfigDict(frozen=True)

    requested: str
    required_scopes: list[str]
    granted: bool
    missing: list[str]


class PagerDutyPreflightResponse(BaseModel):
    """Non-secret scope readiness for the requested datasets."""

    model_config = ConfigDict(frozen=True)

    connected: bool
    credential_name: str
    datasets: list[PagerDutyDatasetPreflight]


class PagerDutyClientCredentialsRequest(BaseModel):
    """Machine-to-machine PagerDuty credential inputs."""

    model_config = ConfigDict(extra="forbid")

    credential_name: str = "default"
    client_id: str = Field(min_length=1)
    client_secret: str = Field(min_length=1)
    subdomain: str = Field(min_length=1)
    region: Literal["us", "eu"] = "us"

    @field_validator("credential_name", "subdomain")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class PagerDutyApiTokenRequest(BaseModel):
    """PagerDuty API-token credential inputs."""

    model_config = ConfigDict(extra="forbid")

    credential_name: str = "default"
    api_token: str = Field(min_length=1)
    subdomain: str = Field(min_length=1)
    region: Literal["us", "eu"] = "us"

    @field_validator("credential_name", "subdomain")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class PagerDutyConnectionResponse(BaseModel):
    """Non-secret result after saving a non-OAuth PagerDuty credential."""

    model_config = ConfigDict(frozen=True)

    connected: Literal[True]
    credential_name: str
    auth_mode: Literal["client_credentials", "api_token"]
    region: Literal["us", "eu"]
    subdomain: str


@router.post(
    "/integrations/pagerduty/authorize",
    response_model=PagerDutyAuthorizeResponse,
)
async def authorize_pagerduty(
    body: PagerDutyAuthorizeRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyAuthorizeResponse:
    """Create a one-time authorization context and return PagerDuty's URL."""
    raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/authorize", GO_API)


@router.post(
    "/integrations/pagerduty/callback",
    response_model=PagerDutyCallbackResponse,
)
async def complete_pagerduty_authorization(
    body: PagerDutyCallbackRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyCallbackResponse:
    """Consume a one-time state and persist the encrypted OAuth binding.

    The feature gate is enforced only at ``authorize`` (the true setup-mutation
    entry point). A flag flip between authorize and callback must not strand
    an already-consumed one-time state or a single-use PagerDuty authorization
    code;
    the gate having passed at authorize time is sufficient.
    """
    raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/callback", GO_API)


@router.get(
    "/integrations/pagerduty/status",
    response_model=PagerDutyStatusResponse,
)
async def get_pagerduty_status(
    credential_name: str = "default",
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyStatusResponse:
    """Return PagerDuty setup status without decrypting OAuth tokens."""
    raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/status", GO_API)


@router.post(
    "/integrations/pagerduty/disconnect",
    response_model=PagerDutyDisconnectResponse,
)
async def disconnect_pagerduty(
    body: PagerDutyDisconnectRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyDisconnectResponse:
    """Revoke PagerDuty secrets and retain only an inactive descriptor tombstone."""
    raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/disconnect", GO_API)


@router.post(
    "/integrations/pagerduty/preflight",
    response_model=PagerDutyPreflightResponse,
)
async def preflight_pagerduty(
    body: PagerDutyPreflightRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyPreflightResponse:
    """Report requested dataset scopes without imposing unrelated requirements."""
    raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/preflight", GO_API)


@router.post(
    "/integrations/pagerduty/client-credentials",
    response_model=PagerDutyConnectionResponse,
)
async def set_pagerduty_client_credentials(
    body: PagerDutyClientCredentialsRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyConnectionResponse:
    """Persist a non-OAuth PagerDuty client-credentials descriptor."""
    raise_served_by_go_api(
        "/api/v1/admin/integrations/pagerduty/client-credentials", GO_API
    )


@router.post(
    "/integrations/pagerduty/api-token",
    response_model=PagerDutyConnectionResponse,
)
async def set_pagerduty_api_token(
    body: PagerDutyApiTokenRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyConnectionResponse:
    """Persist a non-OAuth PagerDuty API-token descriptor."""
    raise_served_by_go_api("/api/v1/admin/integrations/pagerduty/api-token", GO_API)
