from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dev_health_ops.models.git import GUID, Base

if TYPE_CHECKING:
    from .users import Organization, User


class RetentionResourceType(str, Enum):
    AUDIT_LOGS = "audit_logs"
    METRICS_DAILY = "metrics_daily"
    WORK_ITEMS = "work_items"
    GIT_COMMITS = "git_commits"
    SYNC_LOGS = "sync_logs"


class OrgRetentionPolicy(Base):
    __tablename__ = "org_retention_policies"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    resource_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )
    retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=90)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    last_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_deleted_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_by_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    organization: Mapped[Organization] = relationship(
        "Organization", back_populates="retention_policies"
    )
    created_by: Mapped[User | None] = relationship(
        "User", foreign_keys=[created_by_id]
    )

    __table_args__ = (
        UniqueConstraint("org_id", "resource_type", name="uq_org_retention_resource"),
        Index("ix_retention_policies_org_active", "org_id", "is_active"),
    )

    def __repr__(self) -> str:
        return (
            f"<OrgRetentionPolicy(id={self.id}, org_id={self.org_id}, "
            f"resource_type={self.resource_type}, retention_days={self.retention_days})>"
        )
