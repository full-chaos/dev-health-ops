"""Durable Python-to-Go job dispatch outbox."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .git import GUID, Base

WORKER_OUTBOX_PENDING = "pending"
WORKER_OUTBOX_CLAIMED = "claimed"
WORKER_OUTBOX_DELIVERED = "delivered"
WORKER_OUTBOX_DEAD = "dead"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class WorkerJobOutbox(Base):
    """One immutable logical dispatch plus mutable relay state."""

    __tablename__ = "worker_job_outbox"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    dedupe_key: Mapped[str] = mapped_column(String(256), nullable=False)
    job_kind: Mapped[str] = mapped_column(String(96), nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    args: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(71), nullable=False)
    queue: Mapped[str] = mapped_column(String(96), nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    max_attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=WORKER_OUTBOX_PENDING
    )
    claim_token: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    claim_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error_detail: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_error_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    river_job_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'claimed', 'delivered', 'dead')",
            name="ck_worker_job_outbox_status",
        ),
        CheckConstraint(
            "contract_version > 0", name="ck_worker_job_outbox_contract_version"
        ),
        CheckConstraint(
            "priority BETWEEN 1 AND 4", name="ck_worker_job_outbox_priority"
        ),
        CheckConstraint(
            "max_attempts BETWEEN 1 AND 25", name="ck_worker_job_outbox_max_attempts"
        ),
        CheckConstraint(
            "attempt_count >= 0", name="ck_worker_job_outbox_attempt_count"
        ),
        CheckConstraint(
            "length(payload_hash) = 71 AND payload_hash LIKE 'sha256:%'",
            name="ck_worker_job_outbox_payload_hash",
        ),
        CheckConstraint(
            "length(CAST(args AS TEXT)) <= 16384",
            name="ck_worker_job_outbox_args_size",
        ),
        CheckConstraint(
            "(status = 'claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL "
            "AND claim_expires_at IS NOT NULL) OR "
            "(status <> 'claimed' AND claim_token IS NULL AND claimed_at IS NULL "
            "AND claim_expires_at IS NULL)",
            name="ck_worker_job_outbox_claim_state",
        ),
        CheckConstraint(
            "(status = 'delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL) "
            "OR (status <> 'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)",
            name="ck_worker_job_outbox_delivery_state",
        ),
        CheckConstraint(
            "(last_error_code IS NULL AND last_error_detail IS NULL AND last_error_at IS NULL) "
            "OR (last_error_code IS NOT NULL AND last_error_detail IS NOT NULL "
            "AND last_error_at IS NOT NULL)",
            name="ck_worker_job_outbox_error_state",
        ),
        UniqueConstraint("dedupe_key", name="uq_worker_job_outbox_dedupe_key"),
        UniqueConstraint("river_job_id", name="uq_worker_job_outbox_river_job_id"),
        Index(
            "ix_worker_job_outbox_due",
            "status",
            "next_attempt_at",
            "scheduled_at",
            "created_at",
            postgresql_where=text("status IN ('pending', 'claimed')"),
        ),
        Index(
            "ix_worker_job_outbox_claim_expiry",
            "claim_expires_at",
            postgresql_where=text("status = 'claimed'"),
        ),
        Index(
            "ix_worker_job_outbox_terminal",
            "status",
            "delivered_at",
            "updated_at",
            postgresql_where=text("status IN ('delivered', 'dead')"),
        ),
    )
