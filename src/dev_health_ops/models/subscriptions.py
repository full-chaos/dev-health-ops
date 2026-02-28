from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy import JSON, Boolean, Column, DateTime, ForeignKey, Text

from dev_health_ops.models.git import GUID, Base


class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(GUID(), ForeignKey("organizations.id"), nullable=False, index=True)
    billing_plan_id = Column(GUID(), ForeignKey("billing_plans.id"), nullable=False)
    billing_price_id = Column(GUID(), ForeignKey("billing_prices.id"), nullable=False)
    stripe_subscription_id = Column(Text, unique=True, nullable=False, index=True)
    stripe_customer_id = Column(Text, nullable=False, index=True)
    status = Column(Text, nullable=False)
    current_period_start = Column(DateTime(timezone=True), nullable=False)
    current_period_end = Column(DateTime(timezone=True), nullable=False)
    cancel_at_period_end = Column(Boolean, server_default="false")
    canceled_at = Column(DateTime(timezone=True), nullable=True)
    trial_start = Column(DateTime(timezone=True), nullable=True)
    trial_end = Column(DateTime(timezone=True), nullable=True)
    metadata_ = Column("metadata", JSON, server_default="{}", nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))
    updated_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))


class SubscriptionEvent(Base):
    __tablename__ = "subscription_events"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    subscription_id = Column(
        GUID(),
        ForeignKey("subscriptions.id"),
        nullable=False,
        index=True,
    )
    stripe_event_id = Column(Text, unique=True, nullable=False)
    event_type = Column(Text, nullable=False)
    previous_status = Column(Text, nullable=True)
    new_status = Column(Text, nullable=False)
    payload = Column(JSON, server_default="{}", nullable=False)
    processed_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))
