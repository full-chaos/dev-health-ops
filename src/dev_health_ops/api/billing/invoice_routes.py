from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.go_served import GO_API, raise_served_by_go_api
from dev_health_ops.api.services.auth import AuthenticatedUser

from .invoice_service import InvoiceService

router = APIRouter(prefix="/invoices", tags=["billing"])
invoice_service = InvoiceService()


class InvoiceLineItemResponse(BaseModel):
    id: str
    stripe_line_item_id: str | None
    description: str | None
    amount: int
    quantity: int
    period_start: datetime | None
    period_end: datetime | None
    stripe_price_id: str | None


class InvoiceResponse(BaseModel):
    id: str
    org_id: str
    subscription_id: str | None
    stripe_invoice_id: str
    stripe_customer_id: str
    status: str
    amount_due: int
    amount_paid: int
    amount_remaining: int
    currency: str
    period_start: datetime | None
    period_end: datetime | None
    hosted_invoice_url: str | None
    pdf_url: str | None
    payment_intent_id: str | None
    finalized_at: datetime | None
    paid_at: datetime | None
    voided_at: datetime | None
    attempt_count: int
    metadata: dict[str, Any]
    created_at: datetime | None
    updated_at: datetime | None
    line_items: list[InvoiceLineItemResponse] = []


class InvoiceListResponse(BaseModel):
    items: list[InvoiceResponse]
    total: int
    limit: int
    offset: int


@router.get("", response_model=InvoiceListResponse)
async def list_invoices(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    org_id: uuid.UUID | None = Query(default=None),
) -> InvoiceListResponse:
    raise_served_by_go_api("/api/v1/billing/invoices", GO_API)


@router.get("/{invoice_id}", response_model=InvoiceResponse)
async def get_invoice(
    invoice_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    org_id: uuid.UUID | None = Query(default=None),
) -> InvoiceResponse:
    raise_served_by_go_api("/api/v1/billing/invoices/{invoice_id}", GO_API)


@router.post("/{invoice_id}/void", response_model=InvoiceResponse)
async def void_invoice(
    invoice_id: str,
    _: Annotated[AuthenticatedUser, Depends(require_admin)],
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    org_id: uuid.UUID | None = Query(default=None),
) -> InvoiceResponse:
    raise_served_by_go_api("/api/v1/billing/invoices/{invoice_id}/void", GO_API)
