from __future__ import annotations

import importlib
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.api.services.auth import AuthenticatedUser

router = APIRouter(prefix="/subscriptions", tags=["billing-subscriptions"])


class SubscriptionView(BaseModel):
    id: str
    org_id: str
    stripe_subscription_id: str
    stripe_customer_id: str
    status: str
    current_period_start: str
    current_period_end: str
    cancel_at_period_end: bool
    canceled_at: str | None = None
    trial_start: str | None = None
    trial_end: str | None = None
    plan: dict[str, Any] | None = None
    price: dict[str, Any] | None = None


class SubscriptionHistoryItem(BaseModel):
    id: str
    stripe_event_id: str
    event_type: str
    previous_status: str | None
    new_status: str
    processed_at: str
    payload: dict[str, Any] = Field(default_factory=dict)


class SubscriptionHistoryResponse(BaseModel):
    items: list[SubscriptionHistoryItem]
    total: int
    limit: int
    offset: int


class SubscriptionListResponse(BaseModel):
    items: list[SubscriptionView]
    total: int
    limit: int
    offset: int


class ChangePlanRequest(BaseModel):
    price_id: str


class CancelSubscriptionRequest(BaseModel):
    immediately: bool = False


class MutationResponse(BaseModel):
    status: str


def _service(session: AsyncSession) -> Any:
    module = importlib.import_module("dev_health_ops.api.billing.subscription_service")
    return module.SubscriptionService(session)


@router.get("/list", response_model=SubscriptionListResponse)
async def list_subscriptions(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    org_id: uuid.UUID | None = Query(default=None),
) -> SubscriptionListResponse:
    raise_served_by_go_api("/api/v1/billing/subscriptions/list", GO_API)


@router.get("", response_model=SubscriptionView)
async def get_subscription(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    org_id: uuid.UUID | None = Query(default=None),
) -> SubscriptionView:
    raise_served_by_go_api("/api/v1/billing/subscriptions", GO_API)


@router.get("/history", response_model=SubscriptionHistoryResponse)
async def get_subscription_history(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    org_id: uuid.UUID | None = Query(default=None),
) -> SubscriptionHistoryResponse:
    raise_served_by_go_api("/api/v1/billing/subscriptions/history", GO_API)


@router.post("/change-plan", response_model=MutationResponse)
async def change_plan(
    body: ChangePlanRequest,
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    org_id: uuid.UUID | None = Query(default=None),
) -> MutationResponse:
    raise_served_by_go_api("/api/v1/billing/subscriptions/change-plan", GO_API)


@router.post("/cancel", response_model=MutationResponse)
async def cancel_subscription(
    body: CancelSubscriptionRequest,
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    org_id: uuid.UUID | None = Query(default=None),
) -> MutationResponse:
    raise_served_by_go_api("/api/v1/billing/subscriptions/cancel", GO_API)


@router.post("/reactivate", response_model=MutationResponse)
async def reactivate_subscription(
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    org_id: uuid.UUID | None = Query(default=None),
) -> MutationResponse:
    raise_served_by_go_api("/api/v1/billing/subscriptions/reactivate", GO_API)
