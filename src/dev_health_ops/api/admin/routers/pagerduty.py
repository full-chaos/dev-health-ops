"""PagerDuty OAuth setup endpoints.

This router deliberately owns only the authorization start and callback flow.
Status, disconnect, preflight, and alternate authentication modes belong in this
module as separately scoped follow-up endpoints.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id, get_admin_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.providers.pagerduty.oauth import (
    DATASET_SCOPES,
    OAuthCallbackValidationError,
    OAuthTokens,
    PagerDutyOAuthConfig,
    build_authorization_request,
    exchange_code,
    missing_read_scopes,
    revoke_token,
    validate_callback,
)
from dev_health_ops.providers.pagerduty.oauth_authorization_store import (
    PagerDutyAuthorizationRequestStore,
)
from dev_health_ops.providers.pagerduty.oauth_storage import (
    PagerDutyOAuthCredentialRepository,
)

from .common import get_session

router = APIRouter()


class PagerDutyAuthorizeRequest(BaseModel):
    """Inputs required to begin a server-bound PagerDuty PKCE flow."""

    model_config = ConfigDict(extra="forbid")

    credential_name: str = "default"
    region: Literal["us", "eu"] = "us"
    subdomain: str = Field(min_length=1)
    enabled_datasets: list[str]

    @field_validator("credential_name", "subdomain")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


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
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
    admin_user: AuthenticatedUser = Depends(get_admin_user),
) -> PagerDutyAuthorizeResponse:
    """Create a one-time authorization context and return PagerDuty's URL."""
    enabled_datasets = set(body.enabled_datasets)
    unknown_datasets = enabled_datasets.difference(DATASET_SCOPES)
    if unknown_datasets:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown PagerDuty datasets: {', '.join(sorted(unknown_datasets))}"
            ),
        )

    config = PagerDutyOAuthConfig.from_env()
    if config is None:
        raise HTTPException(
            status_code=400,
            detail="PAGER_DUTY_CLIENT_ID is not configured",
        )

    authorization_request = build_authorization_request(config, enabled_datasets)
    await PagerDutyAuthorizationRequestStore(session).create(
        org_id=org_id,
        state=authorization_request.state,
        credential_name=body.credential_name,
        code_verifier=authorization_request.code_verifier,
        enabled_datasets=body.enabled_datasets,
        region=body.region,
        subdomain=body.subdomain,
        initiated_by=admin_user.user_id or None,
    )
    return PagerDutyAuthorizeResponse(authorize_url=authorization_request.url)


@router.post(
    "/integrations/pagerduty/callback",
    response_model=PagerDutyCallbackResponse,
)
async def complete_pagerduty_authorization(
    body: PagerDutyCallbackRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyCallbackResponse:
    """Consume a one-time state and persist the encrypted OAuth binding."""
    consumed = await PagerDutyAuthorizationRequestStore(session).consume(
        org_id=org_id,
        state=body.state,
    )
    if consumed is None:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired PagerDuty OAuth state",
        )
    await session.commit()

    try:
        code = validate_callback(code=body.code or "", error=body.error)
    except OAuthCallbackValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    config = PagerDutyOAuthConfig.from_env()
    if config is None:
        raise HTTPException(
            status_code=500,
            detail="PagerDuty OAuth configuration is unavailable",
        )

    subdomain = consumed.subdomain
    if subdomain is None:
        raise HTTPException(
            status_code=400,
            detail="PagerDuty OAuth state is missing account context",
        )

    try:
        tokens = await exchange_code(
            config,
            code=code,
            code_verifier=consumed.code_verifier,
        )
    except httpx.HTTPStatusError as exc:
        if 400 <= exc.response.status_code < 500:
            raise HTTPException(
                status_code=400,
                detail="PagerDuty OAuth authorization code was rejected",
            ) from exc
        raise HTTPException(
            status_code=502,
            detail="PagerDuty OAuth service is unavailable",
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=503,
            detail="PagerDuty OAuth service is unavailable",
        ) from exc
    missing_scopes = missing_read_scopes(
        set(consumed.enabled_datasets),
        set(tokens.granted_scopes),
    )
    if missing_scopes:
        await _revoke_tokens(config, tokens)
        raise HTTPException(
            status_code=400,
            detail=(
                "Missing required PagerDuty OAuth scopes: "
                f"{', '.join(sorted(missing_scopes))}"
            ),
        )

    binding_id = uuid.uuid4().hex
    try:
        await PagerDutyOAuthCredentialRepository(
            session,
            org_id,
            consumed.credential_name,
        ).create_or_replace(
            tokens,
            binding_id=binding_id,
            account_id=subdomain,
            account_display=subdomain,
        )
        await IntegrationCredentialsService(session, org_id).set(
            provider="pagerduty",
            name=consumed.credential_name,
            credentials={
                "auth_mode": "oauth",
                "oauth_credential_name": consumed.credential_name,
                "oauth_binding_id": binding_id,
                "subdomain": subdomain,
                "region": consumed.region,
                "account_id": subdomain,
            },
            config={
                "auth_mode": "oauth",
                "region": consumed.region,
                "subdomain": subdomain,
                "account_id": subdomain,
                "granted_scopes": sorted(tokens.granted_scopes),
            },
            is_active=True,
        )
        await session.commit()
    except Exception:
        await session.rollback()
        await _revoke_tokens(config, tokens)
        raise

    return PagerDutyCallbackResponse(
        connected=True,
        credential_name=consumed.credential_name,
        region=consumed.region,
        subdomain=subdomain,
        granted_scopes=sorted(tokens.granted_scopes),
    )


async def _revoke_access_token(config: PagerDutyOAuthConfig, access_token: str) -> None:
    """Attempt revocation without obscuring the OAuth setup outcome."""
    try:
        await revoke_token(config, access_token)
    except httpx.HTTPError:
        return


async def _revoke_tokens(config: PagerDutyOAuthConfig, tokens: OAuthTokens) -> None:
    """Compensate a failed OAuth setup using the refresh token when available."""
    await _revoke_access_token(config, tokens.refresh_token or tokens.access_token)


async def _remove_oauth_binding(
    repository: PagerDutyOAuthCredentialRepository,
    config: PagerDutyOAuthConfig | None,
) -> str | None:
    """Delete the local OAuth binding unconditionally; return its revoke token.

    Local deletion never depends on decrypting the stored token or on a
    successful remote revocation, so corrupt ciphertext or a transport error
    cannot strand the row. The returned token (when app config is available)
    must be revoked by the caller only AFTER the local removal is committed.
    """
    token_to_revoke: str | None = None
    try:
        versioned = await repository.get()
        if versioned is not None:
            token_to_revoke = (
                versioned.tokens.refresh_token or versioned.tokens.access_token
            )
    except ValueError:
        # Corrupt/undecryptable stored token (decrypt_value and token JSON
        # validation both raise ValueError): we cannot recover a token to revoke
        # but must still delete the local row. Programming errors propagate.
        token_to_revoke = None
    finally:
        await repository.delete()
    return token_to_revoke if config is not None else None


def _config_string(config: Mapping[str, object] | None, key: str) -> str | None:
    """Return a non-secret string configuration value when present."""
    if config is None:
        return None
    value = config.get(key)
    return value if isinstance(value, str) else None


def _config_scopes(config: Mapping[str, object] | None) -> frozenset[str]:
    """Return persisted non-secret granted scopes without decrypting credentials."""
    if config is None:
        return frozenset()
    raw_scopes = config.get("granted_scopes")
    if not isinstance(raw_scopes, list):
        return frozenset()
    return frozenset(scope for scope in raw_scopes if isinstance(scope, str))


@router.get(
    "/integrations/pagerduty/status",
    response_model=PagerDutyStatusResponse,
)
async def get_pagerduty_status(
    credential_name: str = "default",
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyStatusResponse:
    """Return PagerDuty setup status without decrypting OAuth tokens."""
    credentials = IntegrationCredentialsService(session, org_id)
    descriptor = await credentials.get("pagerduty", credential_name)
    if descriptor is None:
        return PagerDutyStatusResponse(
            connected=False,
            credential_name=credential_name,
            auth_mode=None,
            region=None,
            subdomain=None,
            account_id=None,
            account_display=None,
            granted_scopes=[],
            expires_at=None,
            has_refresh_token=False,
        )

    config = descriptor.config
    metadata = await PagerDutyOAuthCredentialRepository(
        session,
        org_id,
        credential_name,
    ).get_status_metadata()
    auth_mode = _config_string(config, "auth_mode")
    granted_scopes = (
        metadata.granted_scopes if auth_mode == "oauth" and metadata else frozenset()
    )
    return PagerDutyStatusResponse(
        connected=descriptor.is_active,
        credential_name=credential_name,
        auth_mode=_config_string(config, "auth_mode"),
        region=_config_string(config, "region"),
        subdomain=_config_string(config, "subdomain"),
        account_id=(
            metadata.account_id
            if metadata is not None
            else _config_string(config, "account_id")
        ),
        account_display=metadata.account_display if metadata is not None else None,
        granted_scopes=sorted(granted_scopes),
        expires_at=metadata.expires_at if metadata is not None else None,
        has_refresh_token=(
            metadata.has_refresh_token if metadata is not None else False
        ),
    )


@router.post(
    "/integrations/pagerduty/disconnect",
    response_model=PagerDutyDisconnectResponse,
)
async def disconnect_pagerduty(
    body: PagerDutyDisconnectRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyDisconnectResponse:
    """Revoke PagerDuty secrets and retain only an inactive descriptor tombstone."""
    credentials = IntegrationCredentialsService(session, org_id)
    descriptor = await credentials.get("pagerduty", body.credential_name)
    repository = PagerDutyOAuthCredentialRepository(
        session, org_id, body.credential_name
    )
    config = PagerDutyOAuthConfig.from_env()
    token_to_revoke = await _remove_oauth_binding(repository, config)
    if descriptor is not None:
        descriptor.is_active = False
        descriptor.credentials_encrypted = None
        await session.flush()
    # Commit the local removal BEFORE remote revocation so a commit failure
    # cannot leave an active-looking local binding whose remote token is dead.
    await session.commit()
    if config is not None and token_to_revoke is not None:
        await _revoke_access_token(config, token_to_revoke)
    return PagerDutyDisconnectResponse(
        disconnected=True,
        credential_name=body.credential_name,
    )


@router.post(
    "/integrations/pagerduty/preflight",
    response_model=PagerDutyPreflightResponse,
)
async def preflight_pagerduty(
    body: PagerDutyPreflightRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyPreflightResponse:
    """Report requested dataset scopes without imposing unrelated requirements."""
    unknown_datasets = set(body.enabled_datasets).difference(DATASET_SCOPES)
    if unknown_datasets:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown PagerDuty datasets: {', '.join(sorted(unknown_datasets))}",
        )

    credentials = IntegrationCredentialsService(session, org_id)
    descriptor = await credentials.get("pagerduty", body.credential_name)
    config = descriptor.config if descriptor is not None else None
    metadata = await PagerDutyOAuthCredentialRepository(
        session,
        org_id,
        body.credential_name,
    ).get_status_metadata()
    auth_mode = _config_string(config, "auth_mode")
    granted_scopes = (
        metadata.granted_scopes if auth_mode == "oauth" and metadata else frozenset()
    )
    datasets = []
    for dataset in body.enabled_datasets:
        required_scopes = sorted(DATASET_SCOPES[dataset])
        grantable = auth_mode in {"api_token", "client_credentials"}
        missing_scopes = (
            frozenset()
            if grantable
            else missing_read_scopes({dataset}, set(granted_scopes))
        )
        datasets.append(
            PagerDutyDatasetPreflight(
                requested=dataset,
                required_scopes=required_scopes,
                granted=not missing_scopes,
                missing=sorted(missing_scopes),
            )
        )
    return PagerDutyPreflightResponse(
        connected=descriptor is not None
        and descriptor.is_active
        and (auth_mode != "oauth" or metadata is not None),
        credential_name=body.credential_name,
        datasets=datasets,
    )


@router.post(
    "/integrations/pagerduty/client-credentials",
    response_model=PagerDutyConnectionResponse,
)
async def set_pagerduty_client_credentials(
    body: PagerDutyClientCredentialsRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyConnectionResponse:
    """Persist a non-OAuth PagerDuty client-credentials descriptor."""
    config = PagerDutyOAuthConfig.from_env()
    token_to_revoke = await _remove_oauth_binding(
        PagerDutyOAuthCredentialRepository(session, org_id, body.credential_name),
        config,
    )
    await IntegrationCredentialsService(session, org_id).set(
        provider="pagerduty",
        name=body.credential_name,
        credentials={
            "auth_mode": "client_credentials",
            "client_id": body.client_id,
            "client_secret": body.client_secret,
            "subdomain": body.subdomain,
            "region": body.region,
        },
        config={
            "auth_mode": "client_credentials",
            "region": body.region,
            "subdomain": body.subdomain,
        },
        is_active=True,
    )
    await session.commit()
    if config is not None and token_to_revoke is not None:
        await _revoke_access_token(config, token_to_revoke)
    return PagerDutyConnectionResponse(
        connected=True,
        credential_name=body.credential_name,
        auth_mode="client_credentials",
        region=body.region,
        subdomain=body.subdomain,
    )


@router.post(
    "/integrations/pagerduty/api-token",
    response_model=PagerDutyConnectionResponse,
)
async def set_pagerduty_api_token(
    body: PagerDutyApiTokenRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyConnectionResponse:
    """Persist a non-OAuth PagerDuty API-token descriptor."""
    config = PagerDutyOAuthConfig.from_env()
    token_to_revoke = await _remove_oauth_binding(
        PagerDutyOAuthCredentialRepository(session, org_id, body.credential_name),
        config,
    )
    await IntegrationCredentialsService(session, org_id).set(
        provider="pagerduty",
        name=body.credential_name,
        credentials={
            "auth_mode": "api_token",
            "api_token": body.api_token,
            "subdomain": body.subdomain,
            "region": body.region,
        },
        config={
            "auth_mode": "api_token",
            "region": body.region,
            "subdomain": body.subdomain,
        },
        is_active=True,
    )
    await session.commit()
    if config is not None and token_to_revoke is not None:
        await _revoke_access_token(config, token_to_revoke)
    return PagerDutyConnectionResponse(
        connected=True,
        credential_name=body.credential_name,
        auth_mode="api_token",
        region=body.region,
        subdomain=body.subdomain,
    )
