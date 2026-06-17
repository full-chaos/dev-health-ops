from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import (
    JSON,
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


class SyncRunUnitStatus(str, Enum):
    """Sync run unit statuses.

    Terminal states: success, failed.
    """

    PLANNED = "planned"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


class SyncRunStatus(str, Enum):
    """Sync run statuses.

    Terminal states: success, partial_failed, failed.
    """

    PLANNED = "planned"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"


class SyncRunMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"
    FULL_RESYNC = "full_resync"


class Integration(Base):
    __tablename__ = "integrations"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    provider: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    credential_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID, ForeignKey("integration_credentials.id"), nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    schedule_cron: Mapped[str | None] = mapped_column(Text, nullable=True)
    timezone: Mapped[str | None] = mapped_column(Text, nullable=True)
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

    sources: Mapped[list[IntegrationSource]] = relationship(
        back_populates="integration", cascade="all, delete-orphan"
    )
    datasets: Mapped[list[IntegrationDataset]] = relationship(
        back_populates="integration", cascade="all, delete-orphan"
    )
    sync_runs: Mapped[list[SyncRun]] = relationship(back_populates="integration")


class IntegrationSource(Base):
    __tablename__ = "integration_sources"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    integration_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("integrations.id"), nullable=False
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON, nullable=False, default=dict
    )
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    last_sync_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_sync_success: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    integration: Mapped[Integration] = relationship(back_populates="sources")
    run_units: Mapped[list[SyncRunUnit]] = relationship(back_populates="source")

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "integration_id",
            "provider",
            "external_id",
            name="uq_integration_sources_org_integration_provider_external",
        ),
        Index(
            "ix_integration_sources_org_integration_enabled",
            "org_id",
            "integration_id",
            "is_enabled",
        ),
    )


class IntegrationDataset(Base):
    __tablename__ = "integration_datasets"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    integration_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("integrations.id"), nullable=False
    )
    dataset_key: Mapped[str] = mapped_column(String, nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    options: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    integration: Mapped[Integration] = relationship(back_populates="datasets")

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "integration_id",
            "dataset_key",
            name="uq_integration_datasets_org_integration_dataset",
        ),
    )


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    integration_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("integrations.id"), nullable=False
    )
    triggered_by: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    total_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    integration: Mapped[Integration] = relationship(back_populates="sync_runs")
    units: Mapped[list[SyncRunUnit]] = relationship(
        back_populates="sync_run", cascade="all, delete-orphan"
    )
    post_dispatches: Mapped[list[SyncRunPostDispatch]] = relationship(
        back_populates="sync_run", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index(
            "ix_sync_runs_org_integration_status", "org_id", "integration_id", "status"
        ),
    )


class SyncRunUnit(Base):
    __tablename__ = "sync_run_units"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("sync_runs.id"), nullable=False
    )
    integration_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("integration_sources.id"), nullable=False
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_key: Mapped[str] = mapped_column(String, nullable=False)
    cost_class: Mapped[str] = mapped_column(String, nullable=False)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    since_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    before_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(String, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
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

    sync_run: Mapped[SyncRun] = relationship(back_populates="units")
    source: Mapped[IntegrationSource] = relationship(back_populates="run_units")

    __table_args__ = (
        Index("ix_sync_run_units_run_status", "sync_run_id", "status"),
        Index("ix_sync_run_units_source_id", "source_id"),
        Index("ix_sync_run_units_dataset_key", "dataset_key"),
    )


class SyncRunPostDispatch(Base):
    __tablename__ = "sync_run_post_dispatches"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("sync_runs.id"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String, nullable=False)
    dispatched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    sync_run: Mapped[SyncRun] = relationship(back_populates="post_dispatches")

    __table_args__ = (
        UniqueConstraint(
            "sync_run_id", "kind", name="uq_sync_run_post_dispatches_run_kind"
        ),
    )
