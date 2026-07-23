"""Durable references for payload-bearing operational work.

The queue is transport only: workers receive these row identifiers, never the
provider payload or rendered notification contents.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .git import GUID, Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    provider: Mapped[str] = mapped_column(String(16), nullable=False)
    delivery_key: Mapped[str] = mapped_column(String(256), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_event_type: Mapped[str] = mapped_column(String(256), nullable=False)
    org_ref: Mapped[str | None] = mapped_column(String(256), nullable=True)
    repo_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        UniqueConstraint("provider", "delivery_key", name="uq_webhook_delivery_key"),
        CheckConstraint(
            "provider IN ('github', 'gitlab', 'jira')",
            name="ck_webhook_delivery_provider",
        ),
        Index("ix_webhook_delivery_created", "created_at"),
    )


class BillingNotification(Base):
    __tablename__ = "billing_notifications"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    notification_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_billing_notification_key"),
        Index("ix_billing_notification_org_created", "org_id", "created_at"),
    )
