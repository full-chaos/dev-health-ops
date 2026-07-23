"""Durable domain state for Go worker idempotency claims."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .git import GUID, Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


class WorkerJobRun(Base):
    """One logical external-effect execution, independent of River attempts.

    Queue rows remain transport state. This table is the durable claim and
    completion record that handlers use before invoking a provider, email, or
    destructive maintenance operation.
    """

    __tablename__ = "worker_job_runs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    job_kind: Mapped[str] = mapped_column(String(96), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    org_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    domain_type: Mapped[str] = mapped_column(String(64), nullable=False)
    domain_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    claim_token: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    result: Mapped[str | None] = mapped_column(String(16), nullable=True)
    error_category: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now
    )

    __table_args__ = (
        UniqueConstraint("job_kind", "idempotency_key", name="uq_worker_job_run_key"),
        CheckConstraint(
            "status IN ('running', 'retryable', 'succeeded', 'terminal')",
            name="ck_worker_job_run_status",
        ),
        CheckConstraint("attempt_count >= 1", name="ck_worker_job_run_attempt_count"),
        CheckConstraint(
            "(status = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL AND finished_at IS NULL) OR (status <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL AND finished_at IS NOT NULL)",
            name="ck_worker_job_run_claim_state",
        ),
        CheckConstraint(
            "(result IS NULL AND error_category IS NULL) OR (result IS NOT NULL AND error_category IS NOT NULL)",
            name="ck_worker_job_run_result_state",
        ),
        Index("ix_worker_job_run_reclaim", "status", "lease_expires_at"),
    )
