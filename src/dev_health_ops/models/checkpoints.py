"""Metric checkpoint models for tracking computation watermarks.

Checkpoints enable:
- Resume-on-failure: skip repos that already completed for a given day
- Distributed coordination: prevent duplicate computation across workers
- Backfill tracking: know exactly which (repo, day) pairs have been computed
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import IntEnum

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class CheckpointStatus(IntEnum):
    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class MetricCheckpoint(Base):
    """Tracks completion state of metric computations per (org, repo, type, day).

    Used by the partitioned metrics pipeline to:
    - Skip already-completed repos on retry/resume
    - Track which worker is processing a given scope
    - Record errors for failed computations
    - Enable the ``metrics rebuild`` CLI to target specific gaps

    The ``repo_id`` column is nullable: NULL represents a "finalize" checkpoint
    that covers cross-repo aggregations (e.g. IC landscape rolling).
    """

    __tablename__ = "metric_checkpoints"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    repo_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    metric_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Computation scope: daily_batch, daily_finalize, rebuild",
    )
    day: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="Target date"
    )
    status: Mapped[int | None] = mapped_column(
        Integer,
        nullable=False,
        default=CheckpointStatus.PENDING,
        comment="0=pending, 1=running, 2=completed, 3=failed",
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Celery task ID for distributed locking"
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

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "repo_id",
            "metric_type",
            "day",
            name="uq_checkpoint_scope",
        ),
        Index("ix_checkpoint_status_day", "status", "day"),
        Index("ix_checkpoint_org_type_day", "org_id", "metric_type", "day"),
    )

    def __repr__(self) -> str:
        return (
            f"<MetricCheckpoint("
            f"org={self.org_id!r}, repo={self.repo_id}, "
            f"type={self.metric_type!r}, day={self.day}, "
            f"status={self.status})>"
        )
