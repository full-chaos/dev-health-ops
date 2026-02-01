from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class RetentionResourceType(str, Enum):
    AUDIT_LOGS = "audit_logs"
    METRICS_DAILY = "metrics_daily"
    WORK_ITEMS = "work_items"
    GIT_COMMITS = "git_commits"
    SYNC_LOGS = "sync_logs"


class OrgRetentionPolicy(Base):
    __tablename__ = "org_retention_policies"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    resource_type = Column(
        String(50),
        nullable=False,
    )
    retention_days = Column(Integer, nullable=False, default=90)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_run_deleted_count = Column(Integer, nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)

    created_by_id = Column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    organization = relationship("Organization", back_populates="retention_policies")
    created_by = relationship("User", foreign_keys=[created_by_id])

    __table_args__ = (
        UniqueConstraint("org_id", "resource_type", name="uq_org_retention_resource"),
        Index("ix_retention_policies_org_active", "org_id", "is_active"),
    )

    def __repr__(self) -> str:
        return (
            f"<OrgRetentionPolicy(id={self.id}, org_id={self.org_id}, "
            f"resource_type={self.resource_type}, retention_days={self.retention_days})>"
        )
