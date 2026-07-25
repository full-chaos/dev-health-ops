"""Durable per-kind execution transport selected by authenticated operators."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import BigInteger, Boolean, CheckConstraint, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from .git import Base


def _utc_now() -> datetime:
    return datetime.now(UTC)


class WorkerJobRoute(Base):
    __tablename__ = "worker_job_routes"

    job_kind: Mapped[str] = mapped_column(String(96), primary_key=True)
    transport: Mapped[str] = mapped_column(String(16), nullable=False)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "transport IN ('celery', 'shadow', 'river_canary', 'river')",
            name="ck_worker_job_route_transport",
        ),
        CheckConstraint("generation >= 1", name="ck_worker_job_route_generation"),
    )
