from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.refunds import Refund

from .stripe_client import get_stripe_client


def _obj_get(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


class RefundService:
    async def create_refund(
        self,
        db: AsyncSession,
        invoice_id: uuid.UUID,
        amount: int | None = None,
        reason: str | None = None,
        description: str | None = None,
        actor_id: uuid.UUID | None = None,
    ) -> Refund:
        invoice = await self._get_invoice(db, invoice_id=invoice_id)
        if invoice is None:
            raise ValueError("Invoice not found")

        org_id = uuid.UUID(str(invoice["org_id"]))

        invoice_status = str(invoice.get("status") or "")
        if invoice_status.lower() != "paid":
            raise ValueError("Invoice is not paid")

        amount_paid = int(invoice.get("amount_paid") or 0)
        already_refunded = await self._get_existing_refunds_total(
            db, invoice_id=invoice_id
        )
        refundable_amount = amount_paid - already_refunded
        if refundable_amount <= 0:
            raise ValueError("Invoice has already been fully refunded")

        refund_amount = refundable_amount if amount is None else amount
        if refund_amount <= 0:
            raise ValueError("Refund amount must be greater than zero")
        if refund_amount > refundable_amount:
            raise ValueError("Refund amount exceeds refundable balance")

        charge_id = str(invoice.get("stripe_charge_id") or "").strip()
        if not charge_id:
            raise ValueError("Invoice does not have a Stripe charge ID")

        stripe_client = get_stripe_client()
        params: dict[str, Any] = {
            "charge": charge_id,
            "amount": refund_amount,
            "metadata": {
                "org_id": str(org_id),
                "invoice_id": str(invoice_id),
            },
        }
        if reason:
            params["reason"] = reason

        stripe_refund = stripe_client.refunds.create(params=params)

        refund = Refund(
            org_id=org_id,
            invoice_id=invoice_id,
            subscription_id=invoice.get("subscription_id"),
            stripe_refund_id=str(_obj_get(stripe_refund, "id")),
            stripe_charge_id=charge_id,
            stripe_payment_intent_id=invoice.get("stripe_payment_intent_id"),
            amount=refund_amount,
            currency=str(invoice.get("currency") or "usd").lower(),
            status=str(_obj_get(stripe_refund, "status") or "pending"),
            reason=reason,
            description=description,
            failure_reason=_obj_get(stripe_refund, "failure_reason"),
            initiated_by=actor_id,
            metadata_=_obj_get(stripe_refund, "metadata") or {},
        )
        db.add(refund)
        await db.commit()
        await db.refresh(refund)
        return refund

    async def process_webhook(self, db: AsyncSession, event: Any) -> None:
        event_type = _obj_get(event, "type", "")
        event_data = _obj_get(event, "data", SimpleNamespace(object=None))
        data_object = _obj_get(event_data, "object")
        if data_object is None:
            return

        if event_type == "charge.refunded":
            refunds = _obj_get(_obj_get(data_object, "refunds", {}), "data", [])
            charge_id = _obj_get(data_object, "id")
            for stripe_refund in refunds or []:
                await self._upsert_from_stripe_refund(
                    db=db,
                    stripe_refund=stripe_refund,
                    charge_id=charge_id,
                )
            return

        if event_type == "charge.refund.updated":
            await self._upsert_from_stripe_refund(db=db, stripe_refund=data_object)

    async def get_refund(
        self,
        db: AsyncSession,
        refund_id: uuid.UUID,
        org_id: uuid.UUID | None,
    ) -> Refund | None:
        query = select(Refund).where(Refund.id == refund_id)
        if org_id is not None:
            query = query.where(Refund.org_id == org_id)

        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def list_refunds(
        self,
        db: AsyncSession,
        org_id: uuid.UUID | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Refund], int]:
        items_query = (
            select(Refund)
            .order_by(Refund.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        count_query = select(func.count(Refund.id))
        if org_id is not None:
            items_query = items_query.where(Refund.org_id == org_id)
            count_query = count_query.where(Refund.org_id == org_id)

        items_result = await db.execute(items_query)
        count_result = await db.execute(count_query)
        return list(items_result.scalars().all()), int(count_result.scalar_one() or 0)

    async def _get_invoice(
        self,
        db: AsyncSession,
        invoice_id: uuid.UUID,
    ) -> dict[str, Any] | None:
        query = text(
            """
            SELECT
                id,
                org_id,
                status,
                amount_paid,
                currency,
                stripe_charge_id,
                stripe_payment_intent_id,
                subscription_id
            FROM invoices
            WHERE id = :invoice_id
            LIMIT 1
            """
        )
        result = await db.execute(
            query,
            {"invoice_id": str(invoice_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def _get_existing_refunds_total(
        self,
        db: AsyncSession,
        invoice_id: uuid.UUID,
    ) -> int:
        result = await db.execute(
            select(func.coalesce(func.sum(Refund.amount), 0)).where(
                Refund.invoice_id == invoice_id,
                Refund.status.in_(["pending", "succeeded"]),
            )
        )
        return int(result.scalar_one() or 0)

    async def _upsert_from_stripe_refund(
        self,
        db: AsyncSession,
        stripe_refund: Any,
        charge_id: str | None = None,
    ) -> None:
        stripe_refund_id = _obj_get(stripe_refund, "id")
        if not stripe_refund_id:
            return

        result = await db.execute(
            select(Refund).where(Refund.stripe_refund_id == str(stripe_refund_id))
        )
        refund = result.scalar_one_or_none()

        metadata = _obj_get(stripe_refund, "metadata") or {}
        metadata_org_id = _obj_get(metadata, "org_id")
        resolved_org_id: uuid.UUID | None = None
        if metadata_org_id:
            try:
                resolved_org_id = uuid.UUID(str(metadata_org_id))
            except ValueError:
                resolved_org_id = None
        if resolved_org_id is None and refund is not None:
            resolved_org_id = refund.org_id

        if resolved_org_id is None:
            return

        metadata_invoice_id = _obj_get(metadata, "invoice_id")
        invoice_id: uuid.UUID | None = None
        if metadata_invoice_id:
            try:
                invoice_id = uuid.UUID(str(metadata_invoice_id))
            except ValueError:
                invoice_id = None

        if refund is None:
            refund = Refund(
                org_id=resolved_org_id,
                invoice_id=invoice_id,
                subscription_id=None,
                stripe_refund_id=str(stripe_refund_id),
                stripe_charge_id=str(
                    charge_id or _obj_get(stripe_refund, "charge") or ""
                ),
                stripe_payment_intent_id=_obj_get(stripe_refund, "payment_intent"),
                amount=int(_obj_get(stripe_refund, "amount") or 0),
                currency=str(_obj_get(stripe_refund, "currency") or "usd").lower(),
                status=str(_obj_get(stripe_refund, "status") or "pending"),
                reason=_obj_get(stripe_refund, "reason"),
                description=_obj_get(stripe_refund, "description"),
                failure_reason=_obj_get(stripe_refund, "failure_reason"),
                initiated_by=None,
                metadata_=metadata,
            )
            db.add(refund)
        else:
            refund.stripe_charge_id = str(
                charge_id
                or _obj_get(stripe_refund, "charge")
                or refund.stripe_charge_id
            )
            refund.stripe_payment_intent_id = _obj_get(
                stripe_refund, "payment_intent", refund.stripe_payment_intent_id
            )
            refund.amount = int(_obj_get(stripe_refund, "amount") or refund.amount)
            refund.currency = str(
                _obj_get(stripe_refund, "currency") or refund.currency or "usd"
            ).lower()
            refund.status = str(_obj_get(stripe_refund, "status") or refund.status)
            refund.reason = _obj_get(stripe_refund, "reason", refund.reason)
            stripe_description = _obj_get(stripe_refund, "description")
            if stripe_description and not refund.description:
                refund.description = stripe_description
            refund.failure_reason = _obj_get(
                stripe_refund, "failure_reason", refund.failure_reason
            )
            refund.metadata_ = metadata
            if invoice_id and refund.invoice_id is None:
                refund.invoice_id = invoice_id

        await db.commit()


refund_service = RefundService()
