from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import JSON, DateTime, ForeignKey, Index, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class BillingAuditLog(Base):
    __tablename__ = "billing_audit_log"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("organizations.id"), nullable=False, index=True
    )
    actor_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("users.id"), nullable=True
    )
    action: Mapped[str] = mapped_column(Text, nullable=False)
    resource_type: Mapped[str] = mapped_column(Text, nullable=False)
    resource_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    stripe_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    local_state: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    stripe_state: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    reconciliation_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=sa.text("now()")
    )

    __table_args__ = (
        Index("ix_billing_audit_log_org_created", "org_id", "created_at"),
        Index("ix_billing_audit_log_resource", "resource_type", "resource_id"),
        Index("ix_billing_audit_log_reconciliation_status", "reconciliation_status"),
        Index("ix_billing_audit_log_action", "action"),
        Index("ix_billing_audit_log_stripe_event_id", "stripe_event_id"),
    )
