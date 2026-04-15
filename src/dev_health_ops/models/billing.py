from __future__ import annotations

import uuid
from enum import Enum

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
)

from dev_health_ops.models.git import GUID, Base


class BillingInterval(str, Enum):
    MONTHLY = "monthly"
    YEARLY = "yearly"


class BillingPlan(Base):
    __tablename__ = "billing_plans"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    key = Column(Text, nullable=False, unique=True, index=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    tier = Column(Text, nullable=False)
    is_active = Column(Boolean, server_default="true", nullable=False)
    display_order = Column(Integer, server_default="0", nullable=False)
    stripe_product_id = Column(Text, nullable=True, unique=True)
    metadata_ = Column("metadata", JSON, server_default="{}", nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class BillingPrice(Base):
    __tablename__ = "billing_prices"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    plan_id = Column(
        GUID(),
        ForeignKey("billing_plans.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    interval = Column(Text, nullable=False)
    amount = Column(Integer, nullable=False)
    currency = Column(Text, server_default="usd", nullable=False)
    is_active = Column(Boolean, server_default="true", nullable=False)
    stripe_price_id = Column(Text, nullable=True, unique=True)
    created_at = Column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class FeatureBundle(Base):
    __tablename__ = "feature_bundles"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    key = Column(Text, nullable=False, unique=True, index=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    features = Column(JSON, nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class PlanFeatureBundle(Base):
    __tablename__ = "plan_feature_bundles"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    plan_id = Column(GUID(), ForeignKey("billing_plans.id"), nullable=False, index=True)
    bundle_id = Column(
        GUID(), ForeignKey("feature_bundles.id"), nullable=False, index=True
    )

    __table_args__ = (
        UniqueConstraint("plan_id", "bundle_id", name="uq_plan_feature_bundle"),
    )
