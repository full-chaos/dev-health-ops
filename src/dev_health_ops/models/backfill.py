from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class BackfillJob(Base):
    __tablename__ = "backfill_jobs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    sync_config_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("sync_configurations.id", ondelete="CASCADE"),
        nullable=False,
    )
    celery_task_id: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str | None] = mapped_column(
        String, nullable=False, default="pending"
    )
    since_date: Mapped[date] = mapped_column(Date, nullable=False)
    before_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_chunks: Mapped[int | None] = mapped_column(Integer, nullable=False, default=0)
    completed_chunks: Mapped[int | None] = mapped_column(
        Integer, nullable=False, default=0
    )
    failed_chunks: Mapped[int | None] = mapped_column(
        Integer, nullable=False, default=0
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
