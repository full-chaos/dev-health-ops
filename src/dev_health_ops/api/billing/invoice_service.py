from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dev_health_ops.models.invoices import Invoice, InvoiceLineItem


def _get_attr(payload: Any, key: str, default: Any = None) -> Any:
    if isinstance(payload, dict):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return None


class InvoiceService:
    async def is_duplicate_event(
        self,
        db: AsyncSession,
        stripe_event_id: str,
        event_type: str,
        payload: Any,
    ) -> bool:
        try:
            subscription_module = import_module("dev_health_ops.models.subscriptions")
            subscription_event_model = getattr(
                subscription_module, "SubscriptionEvent", None
            )
        except Exception:
            return False

        if subscription_event_model is None:
            return False

        table = subscription_event_model.__table__
        identifier_column = table.c.get("stripe_event_id") or table.c.get("event_id")
        if identifier_column is None:
            return False

        exists = await db.execute(
            select(subscription_event_model).where(identifier_column == stripe_event_id)
        )
        if exists.scalar_one_or_none() is not None:
            return True

        event_row: dict[str, Any] = {}
        if "stripe_event_id" in table.c:
            event_row["stripe_event_id"] = stripe_event_id
        if "event_id" in table.c:
            event_row["event_id"] = stripe_event_id
        if "event_type" in table.c:
            event_row["event_type"] = event_type
        if "payload" in table.c:
            event_row["payload"] = payload if isinstance(payload, dict) else {}
        if "event_payload" in table.c:
            event_row["event_payload"] = payload if isinstance(payload, dict) else {}
        if "processed_at" in table.c:
            event_row["processed_at"] = datetime.now(timezone.utc)

        db.add(subscription_event_model(**event_row))
        await db.flush()
        return False

    async def upsert_invoice(self, db: AsyncSession, stripe_invoice: Any) -> Invoice:
        stripe_invoice_id = _get_attr(stripe_invoice, "id")
        if not stripe_invoice_id:
            raise ValueError("Stripe invoice id is required")

        existing = await db.execute(
            select(Invoice).where(Invoice.stripe_invoice_id == stripe_invoice_id)
        )
        invoice = existing.scalar_one_or_none()

        status_transitions = _get_attr(stripe_invoice, "status_transitions", {}) or {}
        period_start = _to_datetime(_get_attr(stripe_invoice, "period_start"))
        period_end = _to_datetime(_get_attr(stripe_invoice, "period_end"))
        metadata = _get_attr(stripe_invoice, "metadata", {}) or {}

        data = {
            "stripe_customer_id": _get_attr(stripe_invoice, "customer", "") or "",
            "status": _get_attr(stripe_invoice, "status", "draft") or "draft",
            "amount_due": _get_attr(stripe_invoice, "amount_due", 0) or 0,
            "amount_paid": _get_attr(stripe_invoice, "amount_paid", 0) or 0,
            "amount_remaining": _get_attr(stripe_invoice, "amount_remaining", 0) or 0,
            "currency": (_get_attr(stripe_invoice, "currency", "usd") or "usd").lower(),
            "period_start": period_start,
            "period_end": period_end,
            "hosted_invoice_url": _get_attr(stripe_invoice, "hosted_invoice_url"),
            "pdf_url": _get_attr(stripe_invoice, "invoice_pdf"),
            "payment_intent_id": _get_attr(stripe_invoice, "payment_intent"),
            "finalized_at": _to_datetime(_get_attr(status_transitions, "finalized_at")),
            "paid_at": _to_datetime(_get_attr(status_transitions, "paid_at")),
            "voided_at": _to_datetime(_get_attr(status_transitions, "voided_at")),
            "attempt_count": _get_attr(stripe_invoice, "attempt_count", 0) or 0,
            "metadata_": metadata if isinstance(metadata, dict) else {},
        }

        if invoice is None:
            org_id = _get_attr(metadata, "org_id")
            subscription_id = _get_attr(stripe_invoice, "subscription")
            if not org_id:
                raise ValueError("Stripe invoice metadata.org_id is required")

            now = datetime.now(timezone.utc)
            invoice = Invoice(
                org_id=UUID(org_id),
                subscription_id=UUID(subscription_id) if subscription_id else None,
                stripe_invoice_id=stripe_invoice_id,
                created_at=now,
                updated_at=now,
                **data,
            )
            db.add(invoice)
        else:
            subscription_id = _get_attr(stripe_invoice, "subscription")
            invoice.subscription_id = UUID(subscription_id) if subscription_id else None
            for key, value in data.items():
                setattr(invoice, key, value)

        invoice.updated_at = datetime.now(timezone.utc)
        await db.flush()
        return invoice

    async def upsert_line_items(
        self,
        db: AsyncSession,
        invoice_id: UUID,
        stripe_lines: Any,
    ) -> list[InvoiceLineItem]:
        await db.execute(
            delete(InvoiceLineItem).where(InvoiceLineItem.invoice_id == invoice_id)
        )

        if stripe_lines is None:
            await db.flush()
            return []

        line_data = _get_attr(stripe_lines, "data", stripe_lines)
        rows: list[InvoiceLineItem] = []
        for line in line_data or []:
            period = _get_attr(line, "period", {}) or {}
            price = _get_attr(line, "price", {}) or {}
            row = InvoiceLineItem(
                invoice_id=invoice_id,
                stripe_line_item_id=_get_attr(line, "id"),
                description=_get_attr(line, "description"),
                amount=_get_attr(line, "amount", 0) or 0,
                quantity=_get_attr(line, "quantity", 1) or 1,
                period_start=_to_datetime(_get_attr(period, "start")),
                period_end=_to_datetime(_get_attr(period, "end")),
                stripe_price_id=_get_attr(price, "id"),
            )
            db.add(row)
            rows.append(row)

        await db.flush()
        return rows

    async def mark_paid(
        self,
        db: AsyncSession,
        stripe_invoice_id: str,
        payment_intent: Any,
    ) -> Invoice:
        result = await db.execute(
            select(Invoice).where(Invoice.stripe_invoice_id == stripe_invoice_id)
        )
        invoice = result.scalar_one_or_none()
        if invoice is None:
            raise ValueError(f"Invoice not found: {stripe_invoice_id}")

        invoice.status = "paid"
        invoice.payment_intent_id = _get_attr(payment_intent, "id", payment_intent)
        invoice.amount_paid = _get_attr(
            payment_intent, "amount_received", invoice.amount_paid
        )
        invoice.amount_remaining = 0
        if invoice.paid_at is None:
            invoice.paid_at = datetime.now(timezone.utc)
        invoice.updated_at = datetime.now(timezone.utc)
        await db.flush()
        return invoice

    async def mark_voided(self, db: AsyncSession, stripe_invoice_id: str) -> Invoice:
        result = await db.execute(
            select(Invoice).where(Invoice.stripe_invoice_id == stripe_invoice_id)
        )
        invoice = result.scalar_one_or_none()
        if invoice is None:
            raise ValueError(f"Invoice not found: {stripe_invoice_id}")

        invoice.status = "void"
        invoice.voided_at = datetime.now(timezone.utc)
        invoice.updated_at = datetime.now(timezone.utc)
        await db.flush()
        return invoice

    async def get_invoice(
        self,
        db: AsyncSession,
        invoice_id: UUID,
        org_id: UUID,
    ) -> Invoice | None:
        result = await db.execute(
            select(Invoice)
            .options(selectinload(Invoice.line_items))
            .where(Invoice.id == invoice_id, Invoice.org_id == org_id)
        )
        return result.scalar_one_or_none()

    async def list_invoices(
        self,
        db: AsyncSession,
        org_id: UUID,
        limit: int,
        offset: int,
        status_filter: str | None,
    ) -> tuple[list[Invoice], int]:
        count_stmt = select(func.count(Invoice.id)).where(Invoice.org_id == org_id)
        list_stmt = (
            select(Invoice)
            .where(Invoice.org_id == org_id)
            .order_by(Invoice.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        if status_filter:
            count_stmt = count_stmt.where(Invoice.status == status_filter)
            list_stmt = list_stmt.where(Invoice.status == status_filter)

        total_result = await db.execute(count_stmt)
        list_result = await db.execute(list_stmt)

        return list(list_result.scalars().all()), int(total_result.scalar_one() or 0)
