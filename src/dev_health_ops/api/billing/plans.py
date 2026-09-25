from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from dev_health_ops.api.admin.middleware import require_superuser
from dev_health_ops.api.auth.router import get_current_user, get_current_user_optional
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.billing import (
    BillingInterval,
)

from ._helpers import (
    BillingTier,
)

router = APIRouter(tags=["billing-plans"])


class BillingPriceInput(BaseModel):
    interval: BillingInterval
    amount: int = Field(ge=0)
    currency: str = "usd"
    is_active: bool = True
    stripe_price_id: str | None = None


class BillingPlanCreate(BaseModel):
    key: str
    name: str
    description: str | None = None
    tier: BillingTier
    is_active: bool = True
    display_order: int = 0
    stripe_product_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    prices: list[BillingPriceInput] = Field(default_factory=list)
    bundle_ids: list[str] = Field(default_factory=list)


class BillingPlanUpdate(BaseModel):
    key: str | None = None
    name: str | None = None
    description: str | None = None
    tier: BillingTier | None = None
    is_active: bool | None = None
    display_order: int | None = None
    stripe_product_id: str | None = None
    metadata: dict[str, Any] | None = None
    prices: list[BillingPriceInput] | None = None
    bundle_ids: list[str] | None = None


class FeatureBundleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    key: str
    name: str
    description: str | None
    features: list[str]


class BillingPriceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    plan_id: str
    interval: str
    amount: int
    currency: str
    is_active: bool
    stripe_price_id: str | None


class BillingPlanResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: str
    key: str
    name: str
    description: str | None
    tier: BillingTier
    is_active: bool
    display_order: int
    stripe_product_id: str | None
    metadata: dict[str, Any]
    prices: list[BillingPriceResponse] = Field(default_factory=list)
    bundles: list[FeatureBundleResponse] = Field(default_factory=list)


class PullStripeResponse(BaseModel):
    created: list[str]
    updated: list[str]
    skipped: list[str]
    errors: list[str]


def _parse_uuid(value: str, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}") from exc


@router.get("/plans", response_model=list[BillingPlanResponse])
async def list_billing_plans(
    include_inactive: Annotated[bool, Query()] = False,
    user: AuthenticatedUser | None = Depends(get_current_user_optional),
) -> list[BillingPlanResponse]:
    raise_served_by_go_api("/api/v1/billing/plans", GO_API)


@router.post("/plans/pull-stripe", response_model=PullStripeResponse)
async def pull_plans_from_stripe(
    user: AuthenticatedUser = Depends(require_superuser),
) -> PullStripeResponse:
    raise_served_by_go_api("/api/v1/billing/plans/pull-stripe", GO_API)


@router.get("/plans/{plan_id}", response_model=BillingPlanResponse)
async def get_billing_plan(
    plan_id: str,
    include_inactive_prices: Annotated[bool, Query()] = False,
    user: AuthenticatedUser | None = Depends(get_current_user_optional),
) -> BillingPlanResponse:
    raise_served_by_go_api("/api/v1/billing/plans/{plan_id}", GO_API)


@router.post("/plans", response_model=BillingPlanResponse)
async def create_billing_plan(
    payload: BillingPlanCreate,
    user: AuthenticatedUser = Depends(get_current_user),
) -> BillingPlanResponse:
    raise_served_by_go_api("/api/v1/billing/plans", GO_API)


@router.put("/plans/{plan_id}", response_model=BillingPlanResponse)
async def update_billing_plan(
    plan_id: str,
    payload: BillingPlanUpdate,
    user: AuthenticatedUser = Depends(get_current_user),
) -> BillingPlanResponse:
    raise_served_by_go_api("/api/v1/billing/plans/{plan_id}", GO_API)


@router.delete("/plans/{plan_id}")
async def delete_billing_plan(
    plan_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
) -> dict[str, bool]:
    raise_served_by_go_api("/api/v1/billing/plans/{plan_id}", GO_API)


@router.post("/plans/{plan_id}/sync-stripe", response_model=BillingPlanResponse)
async def sync_plan_to_stripe(
    plan_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
) -> BillingPlanResponse:
    raise_served_by_go_api("/api/v1/billing/plans/{plan_id}/sync-stripe", GO_API)
