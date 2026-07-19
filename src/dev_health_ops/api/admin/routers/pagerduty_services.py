from __future__ import annotations

from collections.abc import Mapping
from math import ceil
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.exceptions import (
    AuthenticationException,
    ConnectorException,
    RateLimitException,
)
from dev_health_ops.providers.pagerduty.auth import (
    ApiTokenAuth,
    OAuthBearerAuth,
    PagerDutyAuth,
)
from dev_health_ops.providers.pagerduty.client import PagerDutyClient
from dev_health_ops.providers.pagerduty.degradation import (
    PagerDutyInsufficientScopeError,
)
from dev_health_ops.providers.pagerduty.oauth import (
    READ_SCOPES,
    PagerDutyOAuthConfig,
    client_credentials,
)
from dev_health_ops.providers.pagerduty.oauth_lifecycle import get_valid_access_token
from dev_health_ops.providers.pagerduty.oauth_storage import (
    OAuthRotationConflictError,
    PagerDutyOAuthCredentialRepository,
)

from .common import get_session

router = APIRouter()


class PagerDutyServiceResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    external_id: str
    display_name: str
    name_resolved: bool
    status: str | None


class PagerDutyServicesResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    credential_name: str
    services: list[PagerDutyServiceResponse]


def _required_string(values: Mapping[str, Any], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value.strip():
        raise HTTPException(
            status_code=409,
            detail=f"PagerDuty credential is missing required {key}",
        )
    return value.strip()


async def _build_auth(
    values: Mapping[str, Any],
    *,
    session: AsyncSession,
    org_id: str,
) -> PagerDutyAuth:
    auth_mode = _required_string(values, "auth_mode")
    match auth_mode:
        case "api_token":
            return ApiTokenAuth(_required_string(values, "api_token"))
        case "oauth":
            config = PagerDutyOAuthConfig.from_env()
            if config is None:
                raise HTTPException(
                    status_code=409,
                    detail="PagerDuty OAuth app is not configured",
                )
            try:
                access_token = await get_valid_access_token(
                    PagerDutyOAuthCredentialRepository(
                        session,
                        org_id,
                        _required_string(values, "oauth_credential_name"),
                        expected_binding_id=_required_string(
                            values, "oauth_binding_id"
                        ),
                    ),
                    config,
                )
            except (OAuthRotationConflictError, ValueError) as exc:
                raise HTTPException(
                    status_code=409,
                    detail="PagerDuty OAuth credential must be reconnected",
                ) from exc
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=502,
                    detail="PagerDuty OAuth renewal is temporarily unavailable",
                ) from exc
            await session.commit()
            return OAuthBearerAuth(access_token)
        case "client_credentials":
            config = PagerDutyOAuthConfig(
                client_id=_required_string(values, "client_id"),
                client_secret=_required_string(values, "client_secret"),
                redirect_uri="",
            )
            try:
                tokens = await client_credentials(
                    config,
                    scopes=set(READ_SCOPES),
                    subdomain=_required_string(values, "subdomain"),
                    region=_required_string(values, "region"),
                )
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=502,
                    detail="PagerDuty authentication is temporarily unavailable",
                ) from exc
            return OAuthBearerAuth(tokens.access_token)
        case _:
            raise HTTPException(
                status_code=409,
                detail="PagerDuty credential uses an unsupported authentication mode",
            )


@router.get(
    "/integrations/pagerduty/services",
    response_model=PagerDutyServicesResponse,
)
async def list_pagerduty_services(
    credential_name: str = Query(default="default", min_length=1),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyServicesResponse:
    normalized_name = credential_name.strip()
    if not normalized_name:
        raise HTTPException(status_code=422, detail="credential_name must not be blank")

    credentials_service = IntegrationCredentialsService(session, org_id)
    descriptor = await credentials_service.get("pagerduty", normalized_name)
    values = await credentials_service.get_decrypted_credentials(
        "pagerduty", normalized_name
    )
    if descriptor is None or not descriptor.is_active or values is None:
        raise HTTPException(
            status_code=404, detail="PagerDuty credential was not found"
        )

    auth = await _build_auth(
        values,
        session=session,
        org_id=org_id,
    )
    region = _required_string(values, "region")
    client = PagerDutyClient(auth, region=region)
    try:
        services = await client.list_services()
    except PagerDutyInsufficientScopeError as exc:
        raise HTTPException(
            status_code=403,
            detail="PagerDuty credential is missing Services.read permission",
        ) from exc
    except AuthenticationException as exc:
        raise HTTPException(
            status_code=401,
            detail="PagerDuty credential is no longer authorized",
        ) from exc
    except RateLimitException as exc:
        headers = (
            {"Retry-After": str(max(0, ceil(exc.retry_after_seconds)))}
            if exc.retry_after_seconds is not None
            else None
        )
        raise HTTPException(
            status_code=429,
            detail="PagerDuty rate limit exceeded",
            headers=headers,
        ) from exc
    except ConnectorException as exc:
        raise HTTPException(
            status_code=502,
            detail="PagerDuty services are temporarily unavailable",
        ) from exc
    finally:
        await client.close()

    resolved = [
        PagerDutyServiceResponse(
            external_id=service.id,
            display_name=(
                service.name.strip()
                if service.name and service.name.strip()
                else f"PagerDuty service {service.id}"
            ),
            name_resolved=bool(service.name and service.name.strip()),
            status=service.status,
        )
        for service in services
    ]
    resolved.sort(
        key=lambda service: (service.display_name.casefold(), service.external_id)
    )
    return PagerDutyServicesResponse(
        credential_name=normalized_name,
        services=resolved,
    )
