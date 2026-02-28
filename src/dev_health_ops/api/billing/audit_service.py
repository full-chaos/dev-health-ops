from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.billing_audit import BillingAuditLog

logger = logging.getLogger(__name__)


class BillingAuditService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def log(
        self,
        org_id: uuid.UUID,
        action: str,
        resource_type: str,
        resource_id: uuid.UUID,
        description: str,
        actor_id: uuid.UUID | None = None,
        stripe_event_id: str | None = None,
        local_state: dict[str, Any] | None = None,
        stripe_state: dict[str, Any] | None = None,
        reconciliation_status: str | None = None,
    ) -> BillingAuditLog | None:
        try:
            async with self.db.begin_nested():
                entry = BillingAuditLog(
                    org_id=org_id,
                    actor_id=actor_id,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    description=description,
                    stripe_event_id=stripe_event_id,
                    local_state=local_state,
                    stripe_state=stripe_state,
                    reconciliation_status=reconciliation_status,
                    created_at=datetime.now(timezone.utc),
                )
                self.db.add(entry)
                await self.db.flush()
                return entry
        except Exception:
            logger.exception("Failed to write audit log")
            return None

    async def log_webhook(
        self,
        event: Any,
        resource_type: str,
        resource_id: uuid.UUID,
        org_id: uuid.UUID,
        local_state: dict[str, Any] | None = None,
    ) -> BillingAuditLog | None:
        event_type = str(getattr(event, "type", "webhook.received"))
        event_id = getattr(event, "id", None)
        description = f"Stripe webhook received: {event_type}"
        return await self.log(
            org_id=org_id,
            action=event_type,
            resource_type=resource_type,
            resource_id=resource_id,
            description=description,
            stripe_event_id=str(event_id) if event_id else None,
            local_state=local_state,
        )

    async def query(
        self,
        org_id: uuid.UUID,
        resource_type: str | None = None,
        resource_id: uuid.UUID | None = None,
        action: str | None = None,
        reconciliation_status: str | None = None,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[BillingAuditLog], int]:
        filters = [BillingAuditLog.org_id == org_id]
        if resource_type:
            filters.append(BillingAuditLog.resource_type == resource_type)
        if resource_id:
            filters.append(BillingAuditLog.resource_id == resource_id)
        if action:
            filters.append(BillingAuditLog.action == action)
        if reconciliation_status:
            filters.append(
                BillingAuditLog.reconciliation_status == reconciliation_status
            )
        if from_date:
            filters.append(BillingAuditLog.created_at >= from_date)
        if to_date:
            filters.append(BillingAuditLog.created_at <= to_date)

        where_clause = and_(*filters)

        query_stmt = (
            select(BillingAuditLog)
            .where(where_clause)
            .order_by(BillingAuditLog.created_at.desc())
            .limit(max(1, limit))
            .offset(max(0, offset))
        )
        rows = await self.db.execute(query_stmt)
        items = list(rows.scalars().all())

        total_stmt = (
            select(func.count()).select_from(BillingAuditLog).where(where_clause)
        )
        total_result = await self.db.execute(total_stmt)
        total = int(total_result.scalar() or 0)
        return items, total
