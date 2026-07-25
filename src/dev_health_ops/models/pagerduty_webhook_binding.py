from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class PagerDutyWebhookBinding(Base):
    """Immutable-secret PagerDuty webhook routing record."""

    __tablename__ = "pagerduty_webhook_bindings"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    integration_source_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("integration_sources.id"),
        nullable=False,
        index=True,
    )
    credential_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID,
        ForeignKey("integration_credentials.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    provider_subscription_id: Mapped[str] = mapped_column(Text, nullable=False)
    signing_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    signing_secret_key_version: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="candidate")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    rotated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('candidate', 'ready', 'active', 'inactive')",
            name="ck_pagerduty_webhook_bindings_status",
        ),
        CheckConstraint(
            "status != 'active' OR length(trim(provider_subscription_id)) > 0",
            name="ck_pagerduty_webhook_bindings_active_subscription_required",
        ),
        CheckConstraint(
            "status != 'active' OR credential_id IS NOT NULL",
            name="ck_pagerduty_webhook_bindings_active_credential_required",
        ),
        Index(
            "uq_pagerduty_webhook_bindings_active_source",
            "org_id",
            "integration_source_id",
            unique=True,
            postgresql_where=(status == "active"),
            sqlite_where=(status == "active"),
        ),
        Index(
            "uq_pagerduty_webhook_bindings_active_subscription",
            "org_id",
            "provider_subscription_id",
            unique=True,
            postgresql_where=(status == "active"),
            sqlite_where=(status == "active"),
        ),
    )
