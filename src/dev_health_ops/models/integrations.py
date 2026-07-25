from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
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
    # Run-auth freeze (CHAOS-2755): the credential resolved once at plan time and
    # frozen for the whole run. ``credential_id`` is a PLAIN UUID with NO foreign
    # key — a stamped credential deleted mid-run must not be blocked by an FK and
    # instead surfaces as the existing "Credential not found" unit failure.
    # ``credential_fingerprint`` is a safe-scope content witness (no raw secret).
    # ``auth_source`` is 'integration_credential' | 'environment'; NULL marks a
    # legacy/pre-migration or in-flight-at-deploy run that falls back to the
    # mutable ``Integration.credential_id`` resolution path.
    credential_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    credential_fingerprint: Mapped[str | None] = mapped_column(Text, nullable=True)
    auth_source: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    dispatch_outbox: Mapped[list[SyncDispatchOutbox]] = relationship(
        back_populates="sync_run", cascade="all, delete-orphan"
    )
    reference_discovery: Mapped[SyncRunReferenceDiscovery | None] = relationship(
        back_populates="sync_run", cascade="all, delete-orphan", uselist=False
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
    available_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rate_limit_deferrals: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    rate_limit_first_seen_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expired_lease_retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_retry_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_exhausted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    processor_flags: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
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

    sync_run: Mapped[SyncRun] = relationship(back_populates="units")
    source: Mapped[IntegrationSource] = relationship(back_populates="run_units")

    __table_args__ = (
        Index("ix_sync_run_units_run_status", "sync_run_id", "status"),
        Index(
            "ix_sync_run_units_status_available",
            "sync_run_id",
            "status",
            "available_at",
        ),
        Index("ix_sync_run_units_source_id", "source_id"),
        Index("ix_sync_run_units_dataset_key", "dataset_key"),
        Index(
            "ix_sync_run_units_bucket_status_lease",
            "org_id",
            "provider",
            "cost_class",
            "status",
            "lease_expires_at",
        ),
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


class SyncRunReferenceDiscovery(Base):
    __tablename__ = "sync_run_reference_discoveries"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("sync_runs.id"), nullable=False, unique=True
    )
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    lease_owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
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

    sync_run: Mapped[SyncRun] = relationship(back_populates="reference_discovery")

    __table_args__ = (
        Index(
            "ix_sync_run_reference_discoveries_status_available",
            "status",
            "available_at",
        ),
        Index("ix_sync_run_reference_discoveries_org", "org_id"),
    )


class SyncDispatchOutbox(Base):
    __tablename__ = "sync_dispatch_outbox"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("sync_runs.id"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    dispatched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    claim_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    claim_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    claim_transport: Mapped[str | None] = mapped_column(Text, nullable=True)
    claim_route_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    dispatched_transport: Mapped[str | None] = mapped_column(Text, nullable=True)
    dispatched_route_generation: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    transport_job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
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

    sync_run: Mapped[SyncRun] = relationship(back_populates="dispatch_outbox")

    __table_args__ = (
        UniqueConstraint(
            "sync_run_id", "kind", name="uq_sync_dispatch_outbox_run_kind"
        ),
        CheckConstraint(
            "("
            "claim_token IS NULL AND claim_expires_at IS NULL "
            "AND claim_transport IS NULL AND claim_route_generation IS NULL"
            ") OR ("
            "claim_token IS NOT NULL AND claim_expires_at IS NOT NULL "
            "AND claim_transport IS NOT NULL "
            "AND claim_route_generation IS NOT NULL"
            ")",
            name="ck_sync_dispatch_outbox_claim_route_coherence",
        ),
        CheckConstraint(
            "("
            "status = 'dispatched' AND ("
            "("
            "last_error = 'feature_disabled' "
            "AND dispatched_transport IS NULL "
            "AND dispatched_route_generation IS NULL "
            "AND transport_job_id IS NULL"
            ") OR ("
            "(last_error IS NULL OR last_error <> 'feature_disabled') "
            "AND "
            "dispatched_transport IS NOT NULL "
            "AND dispatched_route_generation IS NOT NULL"
            ")"
            ")"
            ") OR ("
            "status <> 'dispatched' AND dispatched_transport IS NULL "
            "AND dispatched_route_generation IS NULL "
            "AND transport_job_id IS NULL"
            ")",
            name="ck_sync_dispatch_outbox_dispatched_route_coherence",
        ),
        Index("ix_sync_dispatch_outbox_due", "status", "available_at"),
        Index("ix_sync_dispatch_outbox_org", "org_id"),
    )


class SyncDispatchTransportRoute(Base):
    __tablename__ = "sync_dispatch_transport_routes"

    kind: Mapped[str] = mapped_column(String, primary_key=True)
    transport: Mapped[str] = mapped_column(String, nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    paused_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rollback_transport: Mapped[str] = mapped_column(
        String, nullable=False, default="celery"
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
        CheckConstraint(
            "kind IN ('dispatch_sync_run', 'finalize_sync_run', "
            "'post_sync', 'reference_discovery')",
            name="ck_sync_dispatch_transport_routes_kind",
        ),
        CheckConstraint(
            "transport IN ('celery', 'river')",
            name="ck_sync_dispatch_transport_routes_transport",
        ),
        CheckConstraint(
            "rollback_transport = 'celery'",
            name="ck_sync_dispatch_transport_routes_rollback",
        ),
        CheckConstraint(
            "generation >= 1",
            name="ck_sync_dispatch_transport_routes_generation",
        ),
        CheckConstraint(
            "(paused AND paused_at IS NOT NULL) OR (NOT paused AND paused_at IS NULL)",
            name="ck_sync_dispatch_transport_routes_pause_timestamp",
        ),
    )
