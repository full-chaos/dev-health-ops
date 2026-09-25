from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api

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


@router.post(
    "/integrations/pagerduty/webhook-bindings",
    response_model=PagerDutyWebhookBindingResponse,
    status_code=201,
)
async def create_pagerduty_webhook_binding(
    body: PagerDutyWebhookBindingRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    raise_served_by_go_api(
        "/api/v1/admin/integrations/pagerduty/webhook-bindings", GO_API
    )


@router.post(
    "/integrations/pagerduty/webhook-bindings/{binding_id}/rotate",
    response_model=PagerDutyWebhookBindingResponse,
)
async def rotate_pagerduty_webhook_binding(
    binding_id: UUID,
    body: PagerDutyWebhookBindingRequest,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    raise_served_by_go_api(
        "/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate",
        GO_API,
    )


@router.post(
    "/integrations/pagerduty/webhook-bindings/{binding_id}/activate",
    response_model=PagerDutyWebhookBindingResponse,
)
async def activate_pagerduty_webhook_binding(
    binding_id: UUID,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    raise_served_by_go_api(
        "/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate",
        GO_API,
    )


@router.post(
    "/integrations/pagerduty/webhook-bindings/{binding_id}/revoke",
    response_model=PagerDutyWebhookBindingResponse,
)
async def revoke_pagerduty_webhook_binding(
    binding_id: UUID,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    raise_served_by_go_api(
        "/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke",
        GO_API,
    )


@router.get(
    "/integrations/pagerduty/webhook-bindings/{binding_id}",
    response_model=PagerDutyWebhookBindingResponse,
)
async def get_pagerduty_webhook_binding(
    binding_id: UUID,
    org_id: str = Depends(get_admin_org_id),
) -> PagerDutyWebhookBindingResponse:
    raise_served_by_go_api(
        "/api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}", GO_API
    )
