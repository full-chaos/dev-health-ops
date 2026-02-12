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
    Column,
    DateTime,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)

from dev_health_ops.models.git import Base, GUID


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

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(Text, nullable=False, default="default")
    repo_id = Column(GUID, nullable=True)
    metric_type = Column(
        Text,
        nullable=False,
        comment="Computation scope: daily_batch, daily_finalize, rebuild",
    )
    day = Column(DateTime(timezone=True), nullable=False, comment="Target date")
    status = Column(
        Integer,
        nullable=False,
        default=CheckpointStatus.PENDING,
        comment="0=pending, 1=running, 2=completed, 3=failed",
    )
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error = Column(Text, nullable=True)
    worker_id = Column(
        Text, nullable=True, comment="Celery task ID for distributed locking"
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
