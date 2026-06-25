from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dev_health_ops.models.git import GUID, Base


class InvestmentBatchJobStatus(str, Enum):
    CREATED = "created"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class InvestmentBatchItemStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PROVIDER_SUCCEEDED = "provider_succeeded"
    PROVIDER_FAILED = "provider_failed"
    VALIDATED = "validated"
    REPAIRING = "repairing"
    REPAIRED = "repaired"
    FALLBACK = "fallback"
    REUSED = "reused"
    FAILED = "failed"


class InvestmentBatchJob(Base):
    __tablename__ = "investment_batch_jobs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default=InvestmentBatchJobStatus.CREATED.value,
        index=True,
    )
    provider_job_id: Mapped[str | None] = mapped_column(
        String, nullable=True, index=True
    )
    local_correlation_id: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    prompt_version: Mapped[str] = mapped_column(String, nullable=False)
    contract_version: Mapped[str] = mapped_column(String, nullable=False)
    total_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_metadata: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
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

    items: Mapped[list[InvestmentBatchItem]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index(
            "uq_investment_batch_jobs_org_correlation",
            "org_id",
            "local_correlation_id",
            unique=True,
        ),
        Index("ix_investment_batch_jobs_org_status", "org_id", "status"),
        Index("ix_investment_batch_jobs_org_provider_job", "org_id", "provider_job_id"),
    )


class InvestmentBatchItem(Base):
    __tablename__ = "investment_batch_items"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        GUID, ForeignKey("investment_batch_jobs.id", ondelete="CASCADE"), nullable=False
    )
    work_unit_id: Mapped[str] = mapped_column(Text, nullable=False)
    component_index: Mapped[int] = mapped_column(Integer, nullable=False)
    custom_id: Mapped[str] = mapped_column(String, nullable=False)
    input_hash: Mapped[str] = mapped_column(String, nullable=False)
    provider: Mapped[str] = mapped_column(String, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    prompt_version: Mapped[str] = mapped_column(String, nullable=False)
    contract_version: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default=InvestmentBatchItemStatus.PENDING.value,
        index=True,
    )
    validation_status: Mapped[str | None] = mapped_column(String, nullable=True)
    repair_status: Mapped[str | None] = mapped_column(String, nullable=True)
    fallback_status: Mapped[str | None] = mapped_column(String, nullable=True)
    provider_response: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    provider_error: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    audit: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
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

    job: Mapped[InvestmentBatchJob] = relationship(back_populates="items")

    __table_args__ = (
        Index(
            "ix_investment_batch_items_idempotency_lookup",
            "org_id",
            "work_unit_id",
            "component_index",
            "input_hash",
            "provider",
            "model",
            "prompt_version",
            "contract_version",
        ),
        Index(
            "uq_investment_batch_items_job_custom",
            "org_id",
            "job_id",
            "custom_id",
            unique=True,
        ),
        Index("ix_investment_batch_items_org_job", "org_id", "job_id"),
        Index("ix_investment_batch_items_org_custom", "org_id", "custom_id"),
        Index("ix_investment_batch_items_org_status", "org_id", "status"),
    )
