from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.refunds import Refund

from .refund_service import refund_service

router = APIRouter(prefix="/refunds", tags=["billing-refunds"])


class CreateRefundRequest(BaseModel):
    invoice_id: str
    amount: int | None = Field(default=None, ge=1)
    reason: str | None = None
    description: str | None = None


class RefundResponse(BaseModel):
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


def _as_response(refund: Refund) -> RefundResponse:
    return RefundResponse(
        id=str(refund.id),
        org_id=str(refund.org_id),
        invoice_id=str(refund.invoice_id) if refund.invoice_id else None,
        subscription_id=str(refund.subscription_id) if refund.subscription_id else None,
        stripe_refund_id=refund.stripe_refund_id,
        stripe_charge_id=refund.stripe_charge_id,
        stripe_payment_intent_id=refund.stripe_payment_intent_id,
        amount=refund.amount,
        currency=refund.currency,
        status=refund.status,
        reason=refund.reason,
        description=refund.description,
        failure_reason=refund.failure_reason,
        initiated_by=str(refund.initiated_by) if refund.initiated_by else None,
        metadata=refund.metadata_ or {},
        created_at=refund.created_at,
        updated_at=refund.updated_at,
    )


@router.post("", response_model=RefundResponse)
async def create_refund(
    payload: CreateRefundRequest,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> RefundResponse:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")

    try:
        invoice_id = uuid.UUID(payload.invoice_id)
        actor_id = uuid.UUID(user.user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid identifier") from exc

    async with get_postgres_session() as db:
        try:
            refund = await refund_service.create_refund(
                db=db,
                invoice_id=invoice_id,
                amount=payload.amount,
                reason=payload.reason,
                description=payload.description,
                actor_id=actor_id,
            )
            return _as_response(refund)
        except ValueError as exc:
            detail = str(exc)
            if detail == "Invoice not found":
                raise HTTPException(status_code=404, detail=detail) from exc
            raise HTTPException(status_code=400, detail=detail) from exc


@router.get("", response_model=RefundListResponse)
async def list_refunds(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    limit: int = 20,
    offset: int = 0,
    org_id: uuid.UUID | None = Query(default=None),
) -> RefundListResponse:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")

    safe_limit = min(max(limit, 1), 100)
    safe_offset = max(offset, 0)

    async with get_postgres_session() as db:
        refunds, total = await refund_service.list_refunds(
            db=db,
            org_id=org_id,
            limit=safe_limit,
            offset=safe_offset,
        )
        return RefundListResponse(
            items=[_as_response(item) for item in refunds],
            total=total,
            limit=safe_limit,
            offset=safe_offset,
        )


@router.get("/{refund_id}", response_model=RefundResponse)
async def get_refund(
    refund_id: str,
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    org_id: uuid.UUID | None = Query(default=None),
) -> RefundResponse:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")

    try:
        parsed_refund_id = uuid.UUID(refund_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid identifier") from exc

    async with get_postgres_session() as db:
        refund = await refund_service.get_refund(
            db=db,
            refund_id=parsed_refund_id,
            org_id=org_id,
        )
        if refund is None:
            raise HTTPException(status_code=404, detail="Refund not found")
        return _as_response(refund)
