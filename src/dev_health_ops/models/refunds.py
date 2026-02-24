from __future__ import annotations

import uuid
from enum import Enum

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, Text

from dev_health_ops.models.git import Base, GUID


class RefundStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class Refund(Base):
    __tablename__ = "refunds"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(GUID(), ForeignKey("organizations.id"), nullable=False, index=True)
    invoice_id = Column(GUID(), ForeignKey("invoices.id"), nullable=True, index=True)
    subscription_id = Column(
        GUID(), ForeignKey("subscriptions.id"), nullable=True, index=True
    )
    stripe_refund_id = Column(Text, unique=True, nullable=False, index=True)
    stripe_charge_id = Column(Text, nullable=False, index=True)
    stripe_payment_intent_id = Column(Text, nullable=True)
    amount = Column(Integer, nullable=False)
    currency = Column(Text, server_default="usd", nullable=False)
    status = Column(Text, nullable=False, server_default="pending")
    reason = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    failure_reason = Column(Text, nullable=True)
    initiated_by = Column(GUID(), ForeignKey("users.id"), nullable=True)
    metadata_ = Column("metadata", JSON, server_default="{}", nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))
    updated_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))
