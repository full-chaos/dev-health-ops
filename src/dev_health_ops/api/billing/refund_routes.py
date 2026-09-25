from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.api.services.auth import AuthenticatedUser

from ._helpers import RefundReason

router = APIRouter(prefix="/refunds", tags=["billing-refunds"])


class CreateRefundRequest(BaseModel):
    invoice_id: str
    amount: int | None = Field(default=None, ge=1)
    reason: RefundReason | None = None
    description: str | None = None


class RefundResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: str
    org_id: str
    invoice_id: str | None
    subscription_id: str | None
    stripe_refund_id: str
    stripe_charge_id: str
    stripe_payment_intent_id: str | None
    amount: int
    currency: str
    status: str
    reason: str | None
    description: str | None
    failure_reason: str | None
    initiated_by: str | None
    metadata: dict[str, Any]
    created_at: datetime | None
    updated_at: datetime | None


class RefundListResponse(BaseModel):
    items: list[RefundResponse]
    total: int
    limit: int
    offset: int


@router.post("", response_model=RefundResponse)
async def create_refund(
    payload: CreateRefundRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> RefundResponse:
    raise_served_by_go_api("/api/v1/billing/refunds", GO_API)


@router.get("", response_model=RefundListResponse)
async def list_refunds(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    limit: int = 20,
    offset: int = 0,
    org_id: uuid.UUID | None = Query(default=None),
) -> RefundListResponse:
    raise_served_by_go_api("/api/v1/billing/refunds", GO_API)


@router.get("/{refund_id}", response_model=RefundResponse)
async def get_refund(
    refund_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    org_id: uuid.UUID | None = Query(default=None),
) -> RefundResponse:
    raise_served_by_go_api("/api/v1/billing/refunds/{refund_id}", GO_API)
