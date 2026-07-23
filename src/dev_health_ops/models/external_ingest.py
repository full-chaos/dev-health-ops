"""External customer-push ingestion status store (CHAOS-2690 / CHAOS-2694).

These declarative classes exist ONLY as the schema-of-record for Alembic and
for ``Base.metadata.create_all()`` in sqlite-backed unit tests. All actual
reads/writes go through parameterized ``text()`` SQL in
``dev_health_ops.api.external_ingest.status`` -- per the plan's "use direct
SQL for API persistence, avoid ORM-only paths" directive. Do not add
``session.add()``/``session.query()`` call sites against ``ExternalIngestBatch``
/``ExternalIngestRejection``; extend ``status.py`` instead.

Postgres, not ClickHouse: transactional, joins the ingest-token/source model
(CHAOS-2696) in the same database, and must support strongly-consistent
read-after-write for CLI polling (``dev-hops push batch --poll``) immediately
after 202 Accepted -- ClickHouse's async merge semantics on ReplacingMergeTree
would make "poll immediately after accept" flaky.

``ExternalIngestBatchPayload`` is CHAOS-2693's transient raw-payload table --
hosted here (DDL/model) so wave 3 needs no migration of its own; 2693 owns the
``payload_store.py`` accessors and the orphan-prune sweep for this table. It
has no FK to ``external_ingest_batches``: it is written before the batch row
exists (payload row lands first in CHAOS-2695's accept sequence) and deleted
independently by the worker on terminal status, per master-spec CC9/CC22.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class BatchStatus(str, Enum):
    ACCEPTED = "accepted"
    STREAM_UNAVAILABLE = "stream_unavailable"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


#: Master-spec CC12: terminal outcomes. ``accepted``/``stream_unavailable``
#: are non-terminal (retryable); ``processing`` is non-terminal/in-flight.
TERMINAL_STATUSES = frozenset(
    {BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED}
)

#: Cap on stored per-record rejection diagnostics for a single batch (brief
#: Design Decision 4/9). ``items_rejected``/``error_summary.total_rejected``
#: on the batch row always reflect the TRUE total even beyond this cap.
MAX_STORED_REJECTIONS_PER_BATCH = 1000


class ExternalIngestBatch(Base):
    __tablename__ = "external_ingest_batches"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        GUID, primary_key=True, default=uuid.uuid4
    )
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    source_instance: Mapped[str] = mapped_column(Text, nullable=False)
    entity_family: Mapped[str] = mapped_column(
        Text, nullable=False, default="legacy", server_default="legacy"
    )
    producer: Mapped[str | None] = mapped_column(Text, nullable=True)
    producer_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    window_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    window_ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(
        Text, nullable=False, default=BatchStatus.ACCEPTED.value
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    items_received: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_accepted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_rejected: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Per-record-kind counts (e.g. {"pull_request.v1": 12, ...}) for
    # Screen 6/detail. NOT populated by CHAOS-2694 v1 write paths (no caller
    # exists yet -- CHAOS-2697's worker is the eventual writer); the column
    # exists now so the migration doesn't need to change later.
    record_counts: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_summary: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # CHAOS-2699 (master-spec CC21): bounded-recompute visibility, added by
    # migration 0034 which ALTERs this table (owned by CHAOS-2694). Enum
    # pinned epic-wide: not_applicable | pending | dispatched |
    # skipped_no_scope | failed. Written by recompute_status.py, never by
    # this file's own status.py CRUD helpers.
    recompute_status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="not_applicable"
    )
    recompute_scope: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    recompute_dispatched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    recompute_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    recompute_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "source_system",
            "source_instance",
            "entity_family",
            "idempotency_key",
            name="uq_external_ingest_batches_idem",
        ),
        Index("ix_external_ingest_batches_org_status", "org_id", "status"),
        Index("ix_external_ingest_batches_org_created", "org_id", "created_at"),
        Index(
            "ix_external_ingest_batches_org_source",
            "org_id",
            "source_system",
            "source_instance",
        ),
    )


class ExternalIngestRejection(Base):
    __tablename__ = "external_ingest_rejections"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("external_ingest_batches.ingestion_id", ondelete="CASCADE"),
        nullable=False,
    )
    record_index: Mapped[int] = mapped_column(Integer, nullable=False)
    record_kind: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    path: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        # Unique, not just an ordering index: a single position in a
        # customer's submitted batch is rejected at most once, so this also
        # backstops complete_batch() against ever double-inserting
        # diagnostics for the same record.
        Index(
            "uq_external_ingest_rejections_ingestion_order",
            "ingestion_id",
            "record_index",
            unique=True,
        ),
        Index("ix_external_ingest_rejections_org_id", "org_id"),
    )


class ExternalIngestBatchPayload(Base):
    """Transient raw-batch-JSON store (CHAOS-2693's ``payload_store.py``
    accessors; DDL/model hosted here per master-spec CC19)."""

    __tablename__ = "external_ingest_batch_payloads"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True)
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (Index("ix_external_ingest_batch_payloads_org_id", "org_id"),)


class ExternalIngestRecomputeJob(Base):
    """Per-dispatch recompute job log (CHAOS-2699, migration 0034).

    A single debounced flush can fan out to N per-repo
    ``run_daily_metrics``/``run_work_graph_build`` chains plus one
    ``dispatch_investment_materialize_partitioned`` call; this table logs
    each individual Celery dispatch for observability (the acceptance
    criteria explicitly ask for "observability around recompute
    scheduling"). FK-less by design (mirrors ``provider_rate_limit_observations``,
    migration 0031): a flush coalesces N ingestion_ids (debounce key grain is
    ``(org_id, source_system, source_instance)``, not per-ingestion), so
    there is no single ``external_ingest_batches`` row to own a job row --
    the affected batches' own ``recompute_status``/``recompute_scope``
    columns are updated separately, keyed by org/source/time range.

    The dormant Go external-ingest runner also uses a deterministic row in
    this ledger as a fixed compatibility-bridge identity. That row is claimed
    by the allowlisted Python bridge task before the existing planner emits
    the ordinary per-job observability rows described above.
    """

    __tablename__ = "external_ingest_recompute_jobs"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    source_instance: Mapped[str] = mapped_column(Text, nullable=False)
    celery_task_name: Mapped[str] = mapped_column(Text, nullable=False)
    # Nullable: a per-repo daily-metrics job's captured id can be None when
    # its chain AsyncResult has no `.parent` -- the Celery dispatch itself
    # still succeeded (see recompute.py's dispatch_recompute()).
    celery_task_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    queue: Mapped[str] = mapped_column(Text, nullable=False)
    repo_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="dispatched")
    dispatched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index(
            "ix_external_ingest_recompute_jobs_scope",
            "org_id",
            "source_system",
            "source_instance",
            "dispatched_at",
        ),
    )


def terminal_status_for(
    items_received: int, items_accepted: int, items_rejected: int
) -> BatchStatus:
    """Pure terminal-status derivation (brief Design Decision 5).

    ``completed`` iff nothing was rejected; ``failed`` iff nothing was
    accepted (and the batch was non-empty); else ``partial``.
    """
    assert items_received > 0, (
        "empty batches must be rejected by CHAOS-2691 schema validation "
        "before reaching the status store"
    )
    if items_rejected == 0:
        return BatchStatus.COMPLETED
    if items_accepted == 0:
        return BatchStatus.FAILED
    return BatchStatus.PARTIAL


__all__ = [
    "MAX_STORED_REJECTIONS_PER_BATCH",
    "TERMINAL_STATUSES",
    "BatchStatus",
    "ExternalIngestBatch",
    "ExternalIngestBatchPayload",
    "ExternalIngestRecomputeJob",
    "ExternalIngestRejection",
    "terminal_status_for",
]
