from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class SyncCoverageProjection(Base):
    """Atomically replaceable, compact coverage summary for one sync config."""

    __tablename__ = "sync_coverage_projections"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(String, nullable=False)
    sync_config_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("sync_configurations.id", ondelete="CASCADE"),
        nullable=False,
    )
    history_lookback_days: Mapped[int] = mapped_column(Integer, nullable=False)
    projection_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    source_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    backfill_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    invalidated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "sync_config_id",
            "history_lookback_days",
            name="uq_sync_coverage_projection_org_config_window",
        ),
        Index(
            "ix_sync_coverage_projection_org_config",
            "org_id",
            "sync_config_id",
        ),
        Index(
            "ix_sync_coverage_projection_refresh_order",
            "invalidated_at",
            "updated_at",
        ),
    )
