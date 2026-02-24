from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, ForeignKey, Index, JSON, Text

from dev_health_ops.models.git import Base, GUID


class BillingAuditLog(Base):
    __tablename__ = "billing_audit_log"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(GUID(), ForeignKey("organizations.id"), nullable=False, index=True)
    actor_id = Column(GUID(), ForeignKey("users.id"), nullable=True)
    action = Column(Text, nullable=False)
    resource_type = Column(Text, nullable=False)
    resource_id = Column(GUID(), nullable=False)
    description = Column(Text, nullable=False)
    stripe_event_id = Column(Text, nullable=True)
    local_state = Column(JSON, nullable=True)
    stripe_state = Column(JSON, nullable=True)
    reconciliation_status = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))

    __table_args__ = (
        Index("ix_billing_audit_log_org_created", "org_id", "created_at"),
        Index("ix_billing_audit_log_resource", "resource_type", "resource_id"),
        Index("ix_billing_audit_log_reconciliation_status", "reconciliation_status"),
        Index("ix_billing_audit_log_action", "action"),
        Index("ix_billing_audit_log_stripe_event_id", "stripe_event_id"),
    )
