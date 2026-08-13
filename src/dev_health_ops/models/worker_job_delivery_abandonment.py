"""Minimal evidence retained after a worker outbox delivery is abandoned."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .git import Base


class WorkerJobDeliveryAbandonment(Base):
    """A terminal delivery fact that survives full outbox-row retention."""

    __tablename__ = "worker_job_delivery_abandonments"

    dedupe_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    job_kind: Mapped[str] = mapped_column(String(96), nullable=False)
    abandoned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_worker_job_delivery_abandonments_attempt_count",
        ),
        Index(
            "ix_worker_job_delivery_abandonments_kind_time",
            "job_kind",
            "abandoned_at",
        ),
    )
