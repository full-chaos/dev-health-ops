from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from limits import parse as parse_rate_limit
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.api.middleware.rate_limit import (
    INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT,
    get_forwarded_ip,
    limiter,
)
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing.gating import get_org_entitlements_from_db
from dev_health_ops.models.internal_service_credential import (
    INTERNAL_SERVICE_TOKEN_PREFIX,
    InternalServiceCredential,
    InternalServiceCredentialAudit,
    hash_internal_service_token,
)
from dev_health_ops.models.users import Organization

router = APIRouter(prefix="/api/v1/internal/acr", tags=["internal-acr"])
_REQUIRED_SCOPE = "entitlements:read"
_AUTH_ATTEMPT_LIMIT_ITEM = parse_rate_limit(INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT)


class ACREntitlementResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "acr_entitlement.v1"
    org_id: str
    agent_context_runtime: bool


class ACRServiceHealthResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "acr_service_health.v1"
    service: str = "dev-health-ops"
    status: str = "ok"


def _extract_token(request: Request) -> str | None:
    authorization = request.headers.get("Authorization")
    if authorization is None or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ")
    if not token.startswith(INTERNAL_SERVICE_TOKEN_PREFIX) or len(token) <= len(
        INTERNAL_SERVICE_TOKEN_PREFIX
    ):
        return None
    return token


def _reserve_auth_attempt_or_429(request: Request) -> None:
    rate_limiter = getattr(limiter, "limiter", None)
    if rate_limiter is None:
        return
    key = f"internal-acr-auth-attempt:{get_forwarded_ip(request)}"
    if not rate_limiter.hit(_AUTH_ATTEMPT_LIMIT_ITEM, key):
        raise HTTPException(status_code=429, detail="Too many authentication attempts")


async def _audit(
    credential_id: uuid.UUID | None,
    requested_org_id: str,
    outcome: str,
) -> None:
    async with get_postgres_session() as session:
        session.add(
            InternalServiceCredentialAudit(
                credential_id=credential_id,
                requested_org_id=requested_org_id,
                action="acr_entitlement_lookup",
                outcome=outcome,
            )
        )
        await session.commit()


@router.get("/health", response_model=ACRServiceHealthResponse)
async def get_acr_service_health(request: Request) -> ACRServiceHealthResponse:
    _reserve_auth_attempt_or_429(request)
    token = _extract_token(request)
    if token is None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with get_postgres_session() as session:
            result = await session.execute(
                select(InternalServiceCredential).where(
                    InternalServiceCredential.token_hash
                    == hash_internal_service_token(token)
                )
            )
            credential = result.scalar_one_or_none()
    except (RuntimeError, SQLAlchemyError):
        raise HTTPException(status_code=503, detail="Service unavailable") from None

    if credential is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    now = datetime.now(timezone.utc)
    if credential.service_name != "acr" or not credential.is_valid(now):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if _REQUIRED_SCOPE not in credential.scopes:
        raise HTTPException(status_code=403, detail="Forbidden")
    return ACRServiceHealthResponse()


@router.get("/entitlements/{org_id}", response_model=ACREntitlementResponse)
async def get_acr_entitlement(org_id: str, request: Request) -> ACREntitlementResponse:
    _reserve_auth_attempt_or_429(request)
    token = _extract_token(request)
    if token is None:
        await _audit(None, org_id, "missing_or_malformed_token")
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with get_postgres_session() as session:
        result = await session.execute(
            select(InternalServiceCredential).where(
                InternalServiceCredential.token_hash
                == hash_internal_service_token(token)
            )
        )
        credential = result.scalar_one_or_none()
        if credential is None:
            session.add(
                InternalServiceCredentialAudit(
                    credential_id=None,
                    requested_org_id=org_id,
                    action="acr_entitlement_lookup",
                    outcome="unknown_token",
                )
            )
            await session.commit()
            raise HTTPException(status_code=401, detail="Unauthorized")

        credential_id = credential.id
        now = datetime.now(timezone.utc)
        if credential.service_name != "acr" or not credential.is_valid(now):
            session.add(
                InternalServiceCredentialAudit(
                    credential_id=credential_id,
                    requested_org_id=org_id,
                    action="acr_entitlement_lookup",
                    outcome="inactive_token",
                )
            )
            await session.commit()
            raise HTTPException(status_code=401, detail="Unauthorized")

        credential.last_used_at = now
        if _REQUIRED_SCOPE not in credential.scopes:
            session.add(
                InternalServiceCredentialAudit(
                    credential_id=credential_id,
                    requested_org_id=org_id,
                    action="acr_entitlement_lookup",
                    outcome="insufficient_scope",
                )
            )
            await session.commit()
            raise HTTPException(status_code=403, detail="Forbidden")

        try:
            org_uuid = uuid.UUID(org_id)
        except ValueError:
            session.add(
                InternalServiceCredentialAudit(
                    credential_id=credential.id,
                    requested_org_id=org_id,
                    action="acr_entitlement_lookup",
                    outcome="organization_not_found",
                )
            )
            await session.commit()
            raise HTTPException(status_code=404, detail="Not found") from None

        org = await session.get(Organization, org_uuid)
        if org is None:
            session.add(
                InternalServiceCredentialAudit(
                    credential_id=credential.id,
                    requested_org_id=org_id,
                    action="acr_entitlement_lookup",
                    outcome="organization_not_found",
                )
            )
            await session.commit()
            raise HTTPException(status_code=404, detail="Not found")

        try:
            entitlements = await get_org_entitlements_from_db(org_uuid, session)
        except Exception:  # noqa: BLE001 - entitlement provider is an untrusted boundary.
            await session.rollback()
            session.add(
                InternalServiceCredentialAudit(
                    credential_id=credential_id,
                    requested_org_id=org_id,
                    action="acr_entitlement_lookup",
                    outcome="entitlement_lookup_failed",
                )
            )
            await session.commit()
            raise HTTPException(status_code=503, detail="Service unavailable") from None
        session.add(
            InternalServiceCredentialAudit(
                credential_id=credential.id,
                requested_org_id=org_id,
                action="acr_entitlement_lookup",
                outcome="success",
            )
        )
        await session.commit()
    features = entitlements["features"]
    return ACREntitlementResponse(
        org_id=org_id,
        agent_context_runtime=bool(features.get("agent_context_runtime", False)),
    )
