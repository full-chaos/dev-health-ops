from __future__ import annotations

import uuid

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String, Text, func

from dev_health_ops.models.git import GUID, Base


class BackfillJob(Base):
    __tablename__ = "backfill_jobs"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    org_id = Column(String, nullable=False, index=True)
    sync_config_id = Column(
        GUID,
        ForeignKey("sync_configurations.id", ondelete="CASCADE"),
        nullable=False,
    )
    celery_task_id = Column(String, nullable=True)
    status = Column(String, nullable=False, default="pending")
    since_date = Column(Date, nullable=False)
    before_date = Column(Date, nullable=False)
    total_chunks = Column(Integer, nullable=False, default=0)
    completed_chunks = Column(Integer, nullable=False, default=0)
    failed_chunks = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
