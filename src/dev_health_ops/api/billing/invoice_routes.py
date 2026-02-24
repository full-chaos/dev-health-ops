from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, AsyncGenerator, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session

from .invoice_service import InvoiceService
from .stripe_client import get_stripe_client

router = APIRouter(prefix="/invoices", tags=["billing"])
invoice_service = InvoiceService()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_postgres_session() as session:
        yield session


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


def _to_invoice_response(
    invoice: Any, include_line_items: bool = False
) -> InvoiceResponse:
    line_items: list[InvoiceLineItemResponse] = []
    if include_line_items:
        line_items = [
            InvoiceLineItemResponse(
                id=str(line_item.id),
                stripe_line_item_id=line_item.stripe_line_item_id,
                description=line_item.description,
                amount=line_item.amount,
                quantity=line_item.quantity,
                period_start=line_item.period_start,
                period_end=line_item.period_end,
                stripe_price_id=line_item.stripe_price_id,
            )
            for line_item in invoice.line_items
        ]

    return InvoiceResponse(
        id=str(invoice.id),
        org_id=str(invoice.org_id),
        subscription_id=str(invoice.subscription_id)
        if invoice.subscription_id
        else None,
        stripe_invoice_id=invoice.stripe_invoice_id,
        stripe_customer_id=invoice.stripe_customer_id,
        status=invoice.status,
        amount_due=invoice.amount_due,
        amount_paid=invoice.amount_paid,
        amount_remaining=invoice.amount_remaining,
        currency=invoice.currency,
        period_start=invoice.period_start,
        period_end=invoice.period_end,
        hosted_invoice_url=invoice.hosted_invoice_url,
        pdf_url=invoice.pdf_url,
        payment_intent_id=invoice.payment_intent_id,
        finalized_at=invoice.finalized_at,
        paid_at=invoice.paid_at,
        voided_at=invoice.voided_at,
        attempt_count=invoice.attempt_count,
        metadata=invoice.metadata_ or {},
        created_at=invoice.created_at,
        updated_at=invoice.updated_at,
        line_items=line_items,
    )


@router.get("", response_model=InvoiceListResponse)
async def list_invoices(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: AsyncSession = Depends(get_session),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
) -> InvoiceListResponse:
    invoices, total = await invoice_service.list_invoices(
        db=session,
        org_id=uuid.UUID(user.org_id),
        limit=limit,
        offset=offset,
        status_filter=status,
    )
    return InvoiceListResponse(
        items=[_to_invoice_response(invoice) for invoice in invoices],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{invoice_id}", response_model=InvoiceResponse)
async def get_invoice(
    invoice_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: AsyncSession = Depends(get_session),
) -> InvoiceResponse:
    try:
        invoice_uuid = uuid.UUID(invoice_id)
        org_uuid = uuid.UUID(user.org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid invoice id")

    invoice = await invoice_service.get_invoice(session, invoice_uuid, org_uuid)
    if invoice is None:
        raise HTTPException(status_code=404, detail="Invoice not found")

    return _to_invoice_response(invoice, include_line_items=True)


@router.post("/{invoice_id}/void", response_model=InvoiceResponse)
async def void_invoice(
    invoice_id: str,
    _: Annotated[AuthenticatedUser, Depends(require_admin)],
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: AsyncSession = Depends(get_session),
) -> InvoiceResponse:
    try:
        invoice_uuid = uuid.UUID(invoice_id)
        org_uuid = uuid.UUID(user.org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid invoice id")

    invoice = await invoice_service.get_invoice(session, invoice_uuid, org_uuid)
    if invoice is None:
        raise HTTPException(status_code=404, detail="Invoice not found")
    if invoice.status == "paid":
        raise HTTPException(status_code=400, detail="Paid invoices cannot be voided")

    try:
        stripe_client = get_stripe_client()
        stripe_client.invoices.void_invoice(invoice.stripe_invoice_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to void invoice: {exc}")

    updated_invoice = await invoice_service.mark_voided(
        session, invoice.stripe_invoice_id
    )
    updated_invoice.amount_remaining = int(Decimal("0"))
    await session.commit()
    refreshed = await invoice_service.get_invoice(session, updated_invoice.id, org_uuid)
    if refreshed is None:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return _to_invoice_response(refreshed, include_line_items=True)
