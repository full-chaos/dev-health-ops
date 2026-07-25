from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.routers.pagerduty import (
    _require_canonical_incident_ingestion,
)
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.providers.pagerduty.webhook_bindings import (
    CreatePagerDutyWebhookBinding,
    PagerDutyWebhookBindingInputError,
    PagerDutyWebhookBindingService,
)

from .common import get_session

router = APIRouter()


class PagerDutyWebhookBindingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_source_id: UUID
    credential_id: UUID
    provider_subscription_id: str = Field(min_length=1)
    signing_secret: str = Field(min_length=1)


class PagerDutyWebhookBindingResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    integration_source_id: UUID
    credential_id: UUID | None
    provider_subscription_id: str
    signing_secret_key_version: str
    status: str
    created_at: datetime
    rotated_at: datetime | None
    revoked_at: datetime | None


def _response(binding: PagerDutyWebhookBinding) -> PagerDutyWebhookBindingResponse:
    return PagerDutyWebhookBindingResponse(
        id=binding.id,
        integration_source_id=binding.integration_source_id,
        credential_id=binding.credential_id,
        provider_subscription_id=binding.provider_subscription_id,
        signing_secret_key_version=binding.signing_secret_key_version,
        status=binding.status,
        created_at=binding.created_at,
        rotated_at=binding.rotated_at,
        revoked_at=binding.revoked_at,
    )


def _org_id(org_id: str) -> UUID:
    try:
        return UUID(org_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid organization id") from exc


def _input(
    org_id: UUID,
    body: PagerDutyWebhookBindingRequest,
) -> CreatePagerDutyWebhookBinding:
    return CreatePagerDutyWebhookBinding(
        org_id=org_id,
        integration_source_id=body.integration_source_id,
        credential_id=body.credential_id,
        provider_subscription_id=body.provider_subscription_id,
        signing_secret=body.signing_secret,
    )


@router.post(
    "/integrations/pagerduty/webhook-bindings",
    response_model=PagerDutyWebhookBindingResponse,
    status_code=201,
)
async def create_pagerduty_webhook_binding(
    body: PagerDutyWebhookBindingRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    await _require_canonical_incident_ingestion(session, org_id)
    service = PagerDutyWebhookBindingService(session)
    try:
        binding = await service.create(_input(_org_id(org_id), body))
    except PagerDutyWebhookBindingInputError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _response(binding)


@router.post(
    "/integrations/pagerduty/webhook-bindings/{binding_id}/rotate",
    response_model=PagerDutyWebhookBindingResponse,
)
async def rotate_pagerduty_webhook_binding(
    binding_id: UUID,
    body: PagerDutyWebhookBindingRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    await _require_canonical_incident_ingestion(session, org_id)
    service = PagerDutyWebhookBindingService(session)
    try:
        candidate = await service.create_rotation_candidate(
            binding_id,
            _input(_org_id(org_id), body),
        )
    except PagerDutyWebhookBindingInputError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if candidate is None:
        raise HTTPException(status_code=404, detail="Active webhook binding not found")
    return _response(candidate)


@router.post(
    "/integrations/pagerduty/webhook-bindings/{binding_id}/activate",
    response_model=PagerDutyWebhookBindingResponse,
)
async def activate_pagerduty_webhook_binding(
    binding_id: UUID,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    await _require_canonical_incident_ingestion(session, org_id)
    service = PagerDutyWebhookBindingService(session)
    candidate = await service.cutover_ready_candidate(binding_id, _org_id(org_id))
    if candidate is None:
        raise HTTPException(
            status_code=404, detail="Ready webhook binding candidate not found"
        )
    return _response(candidate)


@router.post(
    "/integrations/pagerduty/webhook-bindings/{binding_id}/revoke",
    response_model=PagerDutyWebhookBindingResponse,
)
async def revoke_pagerduty_webhook_binding(
    binding_id: UUID,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    service = PagerDutyWebhookBindingService(session)
    binding = await service.revoke(binding_id, _org_id(org_id))
    if binding is None:
        raise HTTPException(status_code=404, detail="Active webhook binding not found")
    return _response(binding)


@router.get(
    "/integrations/pagerduty/webhook-bindings/{binding_id}",
    response_model=PagerDutyWebhookBindingResponse,
)
async def get_pagerduty_webhook_binding(
    binding_id: UUID,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    binding = await PagerDutyWebhookBindingService(session).load_by_id_for_org(
        binding_id,
        _org_id(org_id),
    )
    if binding is None:
        raise HTTPException(status_code=404, detail="Webhook binding not found")
    return _response(binding)
