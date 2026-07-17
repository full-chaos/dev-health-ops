"""Ingest batch status store + data-plane GET endpoints (CHAOS-2694).

Direct-SQL read/write layer other external-ingest sub-issues call into
(CHAOS-2691's ``POST /batches``, CHAOS-2695's idempotency/conflict policy,
CHAOS-2697's worker) plus this issue's own GET endpoints. Defines its OWN
``APIRouter`` (``status_router``), mounted directly in ``api/main.py`` -- it
does NOT append to CHAOS-2691's ``router.py``/``schemas.py`` (deliberate,
keeps wave-2 files disjoint from CHAOS-2692/2712; see
``docs/architecture/external-ingest-status-store.md``).

# nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
All reads/writes go through ``session.execute(text(...), params)`` -- no
``session.add()``/ORM query paths -- per the plan's "use direct SQL for API
persistence" mandate (``dev_health_ops.models.external_ingest`` holds the
declarative classes purely as the Alembic/sqlite-test schema-of-record). SQL
is dialect-portable (no ``RETURNING``/``ON CONFLICT``): UUIDs are bound as
``str(...)`` and JSON columns as ``json.dumps(...)``/``json.loads(...)``
# nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
manually (neither SQLAlchemy's ``text()`` nor asyncpg apply column-type-aware
bind/result processing to untyped raw-SQL parameters), matching the
``api/billing/reconciliation_service.py``/``refund_service.py`` precedent.

CHAOS-2699 (wave 3) extends ``BatchStatusResponse``/``get_batch_status`` with
a ``recompute_status`` block -- deliberately NOT present in this file yet
(master-spec CC21); the response model's trailing comment marks the seam.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.middleware.rate_limit import (
    INGEST_READ_LIMIT,
    get_ingest_token_key,
    limiter,
)
from dev_health_ops.external_ingest.recompute_status import (
    RecomputeJobRow,
    get_recompute_jobs,
)
from dev_health_ops.models.external_ingest import (
    MAX_STORED_REJECTIONS_PER_BATCH,
    TERMINAL_STATUSES,
    BatchStatus,
    terminal_status_for,
)

from .auth import IngestAuthContext, require_ingest_scope
from .errors import ExternalIngestError

__all__ = [
    "BatchRow",
    "RejectedRecord",
    "RejectionRow",
    "DuplicateIdempotencyKeyError",
    "find_existing_batch",
    "create_batch",
    "mark_processing",
    "mark_stream_unavailable",
    "reset_for_retry",
    "complete_batch",
    "mark_failed",
    "get_batch",
    "list_batches",
    "list_rejections",
    "status_router",
]

_BATCHES_TABLE = "external_ingest_batches"
_REJECTIONS_TABLE = "external_ingest_rejections"
_TERMINAL_STATUS_VALUES = {s.value for s in TERMINAL_STATUSES}


class DuplicateIdempotencyKeyError(Exception):
    """Unique-constraint collision on (org_id, source_system, source_instance,
    idempotency_key). Raised by ``create_batch()`` when a concurrent insert
    wins the race after the caller's own ``find_existing_batch()`` pre-check
    missed it. Callers (CHAOS-2691/2695) should catch this, re-run
    ``find_existing_batch()``, and apply the same-hash-200 /
    different-hash-409 policy.
    """

    def __init__(
        self,
        org_id: str,
        source_system: str,
        source_instance: str,
        idempotency_key: str,
    ) -> None:
        super().__init__(
            f"duplicate idempotency key for org={org_id!r} "
            f"source={source_system}/{source_instance} key={idempotency_key!r}"
        )
        self.org_id = org_id
        self.source_system = source_system
        self.source_instance = source_instance
        self.idempotency_key = idempotency_key


@dataclass(frozen=True)
class BatchRow:
    ingestion_id: uuid.UUID
    org_id: str
    idempotency_key: str
    payload_hash: str
    source_system: str
    source_instance: str
    entity_family: str
    producer: str | None
    producer_version: str | None
    schema_version: str
    window_started_at: datetime | None
    window_ended_at: datetime | None
    status: str
    attempts: int
    items_received: int
    items_accepted: int
    items_rejected: int
    record_counts: dict[str, Any] | None
    error_summary: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    # CHAOS-2699 (master-spec CC21): bounded-recompute visibility, added by
    # migration 0034.
    recompute_status: str
    recompute_scope: dict[str, Any] | None
    recompute_dispatched_at: datetime | None
    recompute_completed_at: datetime | None
    recompute_error: str | None


@dataclass(frozen=True)
class RejectedRecord:
    """Input shape the worker (CHAOS-2697/2698) passes to ``complete_batch()``."""

    record_index: int
    record_kind: str
    external_id: str | None
    code: str
    message: str
    path: str | None


@dataclass(frozen=True)
class RejectionRow(RejectedRecord):
    id: uuid.UUID
    created_at: datetime


# ---------------------------------------------------------------------------
# Row <-> Python value marshaling helpers (dialect-portable raw-SQL bind/read)
# ---------------------------------------------------------------------------


def _parse_uuid(value: Any) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _parse_dt_required(value: Any) -> datetime:
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    return None if value is None else _parse_dt_required(value)


def _parse_json(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return dict(json.loads(value))


def _row_to_batch(m: RowMapping) -> BatchRow:
    return BatchRow(
        ingestion_id=_parse_uuid(m["ingestion_id"]),
        org_id=m["org_id"],
        idempotency_key=m["idempotency_key"],
        payload_hash=m["payload_hash"],
        source_system=m["source_system"],
        source_instance=m["source_instance"],
        entity_family=m["entity_family"],
        producer=m["producer"],
        producer_version=m["producer_version"],
        schema_version=m["schema_version"],
        window_started_at=_parse_dt(m["window_started_at"]),
        window_ended_at=_parse_dt(m["window_ended_at"]),
        status=m["status"],
        attempts=int(m["attempts"]),
        items_received=int(m["items_received"]),
        items_accepted=int(m["items_accepted"]),
        items_rejected=int(m["items_rejected"]),
        record_counts=_parse_json(m["record_counts"]),
        error_summary=_parse_json(m["error_summary"]),
        created_at=_parse_dt_required(m["created_at"]),
        updated_at=_parse_dt_required(m["updated_at"]),
        completed_at=_parse_dt(m["completed_at"]),
        recompute_status=m["recompute_status"],
        recompute_scope=_parse_json(m["recompute_scope"]),
        recompute_dispatched_at=_parse_dt(m["recompute_dispatched_at"]),
        recompute_completed_at=_parse_dt(m["recompute_completed_at"]),
        recompute_error=m["recompute_error"],
    )


def _row_to_rejection(m: RowMapping) -> RejectionRow:
    return RejectionRow(
        record_index=int(m["record_index"]),
        record_kind=m["record_kind"],
        external_id=m["external_id"],
        code=m["code"],
        message=m["message"],
        path=m["path"],
        id=_parse_uuid(m["id"]),
        created_at=_parse_dt_required(m["created_at"]),
    )


def _build_error_summary(
    total_rejected: int, stored: list[RejectedRecord], truncated: bool
) -> dict[str, Any] | None:
    if total_rejected == 0:
        return None
    counts: dict[str, int] = {}
    for rejection in stored:
        counts[rejection.code] = counts.get(rejection.code, 0) + 1
    top_codes = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return {
        "total_rejected": total_rejected,
        "stored_rejections": len(stored),
        "truncated": truncated,
        "top_codes": [{"code": code, "count": count} for code, count in top_codes],
    }


# ---------------------------------------------------------------------------
# CRUD / state machine
# ---------------------------------------------------------------------------


async def find_existing_batch(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    idempotency_key: str,
    entity_family: str = "legacy",
) -> BatchRow | None:
    """Idempotency-key lookup. Consumed by CHAOS-2695's conflict policy
    BEFORE calling ``create_batch()`` -- this is the primary dedupe path; the
    unique constraint + ``DuplicateIdempotencyKeyError`` is the race-safety
    backstop, not the primary mechanism."""
    result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"SELECT * FROM {_BATCHES_TABLE} WHERE org_id = :org_id "
            "AND source_system = :source_system AND source_instance = :source_instance "
            "AND entity_family = :entity_family "
            "AND idempotency_key = :idempotency_key"
        ),
        {
            "org_id": org_id,
            "source_system": source_system,
            "source_instance": source_instance,
            "entity_family": entity_family,
            "idempotency_key": idempotency_key,
        },
    )
    row = result.mappings().first()
    return _row_to_batch(row) if row is not None else None


async def create_batch(
    session: AsyncSession,
    *,
    ingestion_id: uuid.UUID,
    org_id: str,
    idempotency_key: str,
    payload_hash: str,
    source_system: str,
    source_instance: str,
    producer: str | None,
    producer_version: str | None,
    schema_version: str,
    window_started_at: datetime | None,
    window_ended_at: datetime | None,
    items_received: int,
    entity_family: str = "legacy",
) -> BatchRow:
    """INSERT a new ``status='accepted'`` row. This is the FIRST write in
    the accept sequence (master-spec CC22: idempotency row -> payload row ->
    COMMIT -> stream enqueue) -- the row is written BEFORE the enqueue is
    attempted, so a failed enqueue leaves a durable row the caller must
    transition via ``mark_stream_unavailable()`` (commit-before-raise), and
    the client's same-key retry resolves as RETRY instead of vanishing.
    Callers go through ``external_ingest/idempotency.py``'s
    ``resolve_batch_idempotency()``, which runs the ``find_existing_batch()``
    pre-check and maps the ``DuplicateIdempotencyKeyError`` race. Does NOT
    commit -- caller commits once the accept sequence's other writes (the
    payload row) also succeed."""
    now = datetime.now(timezone.utc)
    params: dict[str, Any] = {
        "ingestion_id": str(ingestion_id),
        "org_id": org_id,
        "idempotency_key": idempotency_key,
        "payload_hash": payload_hash,
        "source_system": source_system,
        "source_instance": source_instance,
        "entity_family": entity_family,
        "producer": producer,
        "producer_version": producer_version,
        "schema_version": schema_version,
        "window_started_at": window_started_at,
        "window_ended_at": window_ended_at,
        "status": BatchStatus.ACCEPTED.value,
        "attempts": 1,
        "items_received": items_received,
        "items_accepted": 0,
        "items_rejected": 0,
        "record_counts": None,
        "error_summary": None,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        # CHAOS-2699: explicit, not relied-upon-via-server_default (matches
        # this file's created_at/updated_at/completed_at convention) --
        # every INSERT this function issues starts a batch with no
        # recompute decision made yet.
        "recompute_status": "not_applicable",
        "recompute_scope": None,
        "recompute_dispatched_at": None,
        "recompute_completed_at": None,
        "recompute_error": None,
    }
    # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    insert_sql = text(
        f"""
        INSERT INTO {_BATCHES_TABLE} (
            ingestion_id, org_id, idempotency_key, payload_hash, source_system,
            source_instance, entity_family, producer, producer_version, schema_version,
            window_started_at, window_ended_at, status, attempts,
            items_received, items_accepted, items_rejected, record_counts,
            error_summary, created_at, updated_at, completed_at,
            recompute_status, recompute_scope, recompute_dispatched_at,
            recompute_completed_at, recompute_error
        ) VALUES (
            :ingestion_id, :org_id, :idempotency_key, :payload_hash, :source_system,
            :source_instance, :entity_family, :producer, :producer_version, :schema_version,
            :window_started_at, :window_ended_at, :status, :attempts,
            :items_received, :items_accepted, :items_rejected, :record_counts,
            :error_summary, :created_at, :updated_at, :completed_at,
            :recompute_status, :recompute_scope, :recompute_dispatched_at,
            :recompute_completed_at, :recompute_error
        )
        """
    )
    try:
        async with session.begin_nested():
            await session.execute(insert_sql, params)
    except IntegrityError as exc:
        raise DuplicateIdempotencyKeyError(
            org_id=org_id,
            source_system=source_system,
            source_instance=source_instance,
            idempotency_key=idempotency_key,
        ) from exc

    return BatchRow(
        ingestion_id=ingestion_id,
        org_id=org_id,
        idempotency_key=idempotency_key,
        payload_hash=payload_hash,
        source_system=source_system,
        source_instance=source_instance,
        entity_family=entity_family,
        producer=producer,
        producer_version=producer_version,
        schema_version=schema_version,
        window_started_at=window_started_at,
        window_ended_at=window_ended_at,
        status=BatchStatus.ACCEPTED.value,
        attempts=1,
        items_received=items_received,
        items_accepted=0,
        items_rejected=0,
        record_counts=None,
        error_summary=None,
        created_at=now,
        updated_at=now,
        completed_at=None,
        recompute_status="not_applicable",  # DB server_default (migration 0034)
        recompute_scope=None,
        recompute_dispatched_at=None,
        recompute_completed_at=None,
        recompute_error=None,
    )


async def _transition_status(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    from_status: BatchStatus,
    to_status: BatchStatus,
) -> None:
    now = datetime.now(timezone.utc)
    await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"UPDATE {_BATCHES_TABLE} SET status = :new_status, updated_at = :updated_at "
            "WHERE org_id = :org_id AND ingestion_id = :ingestion_id AND status = :old_status"
        ),
        {
            "new_status": to_status.value,
            "updated_at": now,
            "org_id": org_id,
            "ingestion_id": str(ingestion_id),
            "old_status": from_status.value,
        },
    )


async def mark_processing(
    session: AsyncSession, *, org_id: str, ingestion_id: uuid.UUID
) -> None:
    """``accepted|stream_unavailable -> processing``. Idempotent: a no-op
    UPDATE if already processing/terminal, so redelivered stream entries
    (at-least-once) never regress a terminal status back to processing.

    ``stream_unavailable`` is included (CHAOS-2697): a 503'd accept whose
    XADD actually landed before the error surfaced leaves a live pointer for
    a ``stream_unavailable`` row (the expected-duplicate-pointer case,
    docs/architecture/external-ingest-idempotency-ownership.md). Processing
    it is strictly better than wedging: the payload row is durable, and the
    client's same-key retry then REPLAYs the terminal outcome instead of
    re-accepting. A concurrent ``reset_for_retry`` serializes against this
    CAS on the row lock — whichever loses re-reads and yields."""
    now = datetime.now(timezone.utc)
    await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"UPDATE {_BATCHES_TABLE} SET status = :new_status, updated_at = :updated_at "
            "WHERE org_id = :org_id AND ingestion_id = :ingestion_id "
            "AND status IN (:accepted, :stream_unavailable)"
        ),
        {
            "new_status": BatchStatus.PROCESSING.value,
            "updated_at": now,
            "org_id": org_id,
            "ingestion_id": str(ingestion_id),
            "accepted": BatchStatus.ACCEPTED.value,
            "stream_unavailable": BatchStatus.STREAM_UNAVAILABLE.value,
        },
    )


async def mark_stream_unavailable(
    session: AsyncSession, *, org_id: str, ingestion_id: uuid.UUID
) -> None:
    """``accepted -> stream_unavailable`` (master-spec CC12/CC22): the
    Postgres commit for the accept row succeeded but the stream enqueue
    failed. Callers MUST commit this write before raising the 503 to the
    client ("commit-before-raise") -- a rolled-back status row would leave
    the client's 503 with no corresponding durable state to resubmit
    against."""
    await _transition_status(
        session,
        org_id=org_id,
        ingestion_id=ingestion_id,
        from_status=BatchStatus.ACCEPTED,
        to_status=BatchStatus.STREAM_UNAVAILABLE,
    )


async def reset_for_retry(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    from_status: str,
) -> bool:
    """Re-accept an existing batch row for a RETRY outcome (CHAOS-2695).

    ``status -> accepted``, ``attempts += 1``, and the previous attempt's
    outcome fields are cleared -- including its rejection rows, which MUST
    be deleted here or the retry's own ``complete_batch()`` would violate
    the ``(ingestion_id, record_index)`` unique index when it re-inserts
    diagnostics for the same records.

    Atomic CAS on the caller's observed ``from_status`` (same pattern as
    ``complete_batch``): returns ``False`` without touching anything if the
    row already moved (a concurrent retry won, or a live worker completed a
    stale-looking batch first) -- the caller should re-read and treat the
    fresh row as a REPLAY. ``recompute_*`` fields are deliberately left
    alone (CHAOS-2699 owns them; the worker overwrites them on the retry's
    completion). Does NOT commit.
    """
    now = datetime.now(timezone.utc)
    cas_result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"""
            UPDATE {_BATCHES_TABLE}
            SET status = :status, attempts = attempts + 1,
                items_accepted = 0, items_rejected = 0,
                record_counts = NULL, error_summary = NULL,
                completed_at = NULL, updated_at = :updated_at
            WHERE org_id = :org_id AND ingestion_id = :ingestion_id
                AND status = :expected_status
            """
        ),
        {
            "status": BatchStatus.ACCEPTED.value,
            "updated_at": now,
            "org_id": org_id,
            "ingestion_id": str(ingestion_id),
            "expected_status": from_status,
        },
    )
    if int(getattr(cas_result, "rowcount", 0) or 0) != 1:
        return False

    await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"DELETE FROM {_REJECTIONS_TABLE} "
            "WHERE org_id = :org_id AND ingestion_id = :ingestion_id"
        ),
        {"org_id": org_id, "ingestion_id": str(ingestion_id)},
    )
    return True


async def complete_batch(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    items_accepted: int,
    items_rejected: int,
    rejections: list[RejectedRecord],
    record_counts: dict[str, int] | None = None,
) -> BatchRow:
    """``processing -> {completed, partial, failed}`` (derived from counts,
    see ``terminal_status_for``). Idempotent: if called twice for the same
    ``ingestion_id`` (worker redelivery) once the batch is already terminal,
    this is a pure no-op -- the second call's (expected-identical) inputs are
    discarded and the already-persisted row is returned unchanged. Only a
    batch currently ``processing`` may transition to a terminal status
    (adversarial review: completing directly from ``accepted``/
    ``stream_unavailable`` would let an unprocessed or never-enqueued batch
    masquerade as successfully completed) -- raises ``ValueError`` for any
    other non-terminal starting state, and for counters that are negative or
    don't sum to the batch's recorded ``items_received``.

    Concurrency (adversarial review): the status/counter UPDATE is an atomic
    compare-and-swap (``WHERE status = 'processing'``), not a check-then-write
    -- if two callers race to complete the same batch, the DB row lock the
    UPDATE takes serializes them, and only the winner (the one whose UPDATE
    actually matched a row) proceeds to insert rejection rows; the loser
    re-reads and returns whatever the winner persisted. This is what makes it
    safe even though the documented deployment topology (single
    ``worker-external-ingest`` replica at concurrency=1, consumer-level
    idempotent-skip guard -- master-spec CC11) shouldn't produce concurrent
    calls in practice; the DB-level guarantee doesn't rely on that discipline
    holding. Diagnostics and the terminal status/counters land in the same
    UPDATE/INSERT sequence inside one uncommitted transaction, so a caller
    that commits afterward never exposes a terminal status without its
    rejection rows already durable, and the ``(ingestion_id, record_index)``
    unique index is a second, DB-enforced backstop against ever
    double-inserting a rejection row.

    ``record_counts`` (CHAOS-2697): per-kind accepted counts keyed by FULL
    kind name (``{"pull_request.v1": 12}``) — the worker is this column's
    only writer (see the model comment in ``models/external_ingest.py``)."""
    current = await get_batch(session, org_id=org_id, ingestion_id=ingestion_id)
    if current is None:
        raise ValueError(f"external ingest batch not found: {ingestion_id}")

    if current.status in _TERMINAL_STATUS_VALUES:
        return current

    if current.status != BatchStatus.PROCESSING.value:
        raise ValueError(
            f"cannot complete external ingest batch {ingestion_id}: "
            f"status is {current.status!r}, expected {BatchStatus.PROCESSING.value!r}"
        )
    if items_accepted < 0 or items_rejected < 0:
        raise ValueError("items_accepted/items_rejected must be non-negative")
    if items_accepted + items_rejected != current.items_received:
        raise ValueError(
            f"items_accepted ({items_accepted}) + items_rejected "
            f"({items_rejected}) must equal items_received "
            f"({current.items_received})"
        )

    new_status = terminal_status_for(
        current.items_received, items_accepted, items_rejected
    )
    now = datetime.now(timezone.utc)
    stored = rejections[:MAX_STORED_REJECTIONS_PER_BATCH]
    truncated = len(rejections) > MAX_STORED_REJECTIONS_PER_BATCH
    error_summary = _build_error_summary(items_rejected, stored, truncated)

    cas_result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"""
            UPDATE {_BATCHES_TABLE}
            SET status = :status, items_accepted = :items_accepted,
                items_rejected = :items_rejected, error_summary = :error_summary,
                record_counts = :record_counts,
                completed_at = :completed_at, updated_at = :updated_at
            WHERE org_id = :org_id AND ingestion_id = :ingestion_id
                AND status = :expected_status
            """
        ),
        {
            "status": new_status.value,
            "items_accepted": items_accepted,
            "items_rejected": items_rejected,
            "error_summary": (
                json.dumps(error_summary) if error_summary is not None else None
            ),
            "record_counts": (
                json.dumps(record_counts) if record_counts is not None else None
            ),
            "completed_at": now,
            "updated_at": now,
            "org_id": org_id,
            "ingestion_id": str(ingestion_id),
            "expected_status": BatchStatus.PROCESSING.value,
        },
    )
    if int(getattr(cas_result, "rowcount", 0) or 0) != 1:
        # Lost the race: a concurrent completion (or some other transition)
        # already moved this row out of 'processing' between our read above
        # and this UPDATE. Never insert our own rejection rows in that case
        # -- return whatever the winner actually persisted.
        refreshed = await get_batch(session, org_id=org_id, ingestion_id=ingestion_id)
        assert refreshed is not None
        return refreshed

    if stored:
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        insert_sql = text(
            f"""
            INSERT INTO {_REJECTIONS_TABLE} (
                id, org_id, ingestion_id, record_index, record_kind,
                external_id, code, message, path, created_at
            ) VALUES (
                :id, :org_id, :ingestion_id, :record_index, :record_kind,
                :external_id, :code, :message, :path, :created_at
            )
            """
        )
        for rejection in stored:
            await session.execute(
                insert_sql,
                {
                    "id": str(uuid.uuid4()),
                    "org_id": org_id,
                    "ingestion_id": str(ingestion_id),
                    "record_index": rejection.record_index,
                    "record_kind": rejection.record_kind,
                    "external_id": rejection.external_id,
                    "code": rejection.code,
                    "message": rejection.message,
                    "path": rejection.path,
                    "created_at": now,
                },
            )

    updated = await get_batch(session, org_id=org_id, ingestion_id=ingestion_id)
    assert updated is not None
    return updated


async def mark_failed(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    reason: str,
) -> bool:
    """Force a batch to terminal ``failed`` from ANY non-terminal status —
    the consumer's give-up path (CHAOS-2697 ``mark_batch_failed``, master-spec
    CC11: max_deliveries exhausted or a ``PermanentProcessingError`` DLQ'd the
    entry). Unlike ``complete_batch`` this deliberately does not require
    ``processing`` or count reconciliation: the whole point is that the worker
    never got far enough to produce counts (schema-version mismatch and
    missing-payload failures raise before ``mark_processing``, leaving
    ``accepted``). The published counter invariant still holds (adversarial
    round 2): ``failed`` means zero accepted, so ``items_rejected`` is forced
    to ``items_received`` (a system failure rejects the whole batch as far as
    GET/list consumers are concerned; no per-record rejection rows exist --
    ``error_summary.system_failure`` carries the cause) and ``record_counts``
    is cleared. Idempotent: returns ``False`` (untouched row) when already
    terminal or missing. Does NOT commit."""
    now = datetime.now(timezone.utc)
    result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"""
            UPDATE {_BATCHES_TABLE}
            SET status = :status, error_summary = :error_summary,
                items_accepted = 0, items_rejected = items_received,
                record_counts = NULL,
                completed_at = :completed_at, updated_at = :updated_at
            WHERE org_id = :org_id AND ingestion_id = :ingestion_id
                AND status NOT IN (:completed, :partial, :failed)
            """
        ),
        {
            "status": BatchStatus.FAILED.value,
            "error_summary": json.dumps(
                {"system_failure": True, "reason": reason[:500]}
            ),
            "completed_at": now,
            "updated_at": now,
            "org_id": org_id,
            "ingestion_id": str(ingestion_id),
            "completed": BatchStatus.COMPLETED.value,
            "partial": BatchStatus.PARTIAL.value,
            "failed": BatchStatus.FAILED.value,
        },
    )
    return int(getattr(result, "rowcount", 0) or 0) == 1


async def get_batch(
    session: AsyncSession, *, org_id: str, ingestion_id: uuid.UUID
) -> BatchRow | None:
    """Tenant-scoped single lookup. Returns ``None`` (never raises) for both
    "does not exist" and "exists but belongs to a different org" -- callers
    MUST turn both into an identical 404, never a 403 (avoid leaking
    cross-org existence)."""
    result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"SELECT * FROM {_BATCHES_TABLE} "
            "WHERE org_id = :org_id AND ingestion_id = :ingestion_id"
        ),
        {"org_id": org_id, "ingestion_id": str(ingestion_id)},
    )
    row = result.mappings().first()
    return _row_to_batch(row) if row is not None else None


async def list_batches(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str | None = None,
    source_instance: str | None = None,
    status: str | None = None,
    producer: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[BatchRow], int]:
    """Returns ``(page, total_count)``. Ordered by ``created_at DESC,
    ingestion_id DESC`` -- the ``ingestion_id`` tiebreaker keeps pagination
    stable across requests since ``created_at`` alone is not unique.
    ``producer`` is an admin-proxy-only filter (master-spec CC25); the
    data-plane list endpoint does not expose it."""
    where = ["org_id = :org_id"]
    params: dict[str, Any] = {"org_id": org_id}
    if source_system is not None:
        where.append("source_system = :source_system")
        params["source_system"] = source_system
    if source_instance is not None:
        where.append("source_instance = :source_instance")
        params["source_instance"] = source_instance
    if status is not None:
        where.append("status = :status")
        params["status"] = status
    if producer is not None:
        where.append("producer = :producer")
        params["producer"] = producer
    if created_after is not None:
        where.append("created_at >= :created_after")
        params["created_after"] = created_after
    if created_before is not None:
        where.append("created_at <= :created_before")
        params["created_before"] = created_before
    where_clause = " AND ".join(where)

    count_result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(f"SELECT COUNT(*) AS total FROM {_BATCHES_TABLE} WHERE {where_clause}"),
        params,
    )
    total = int(count_result.scalar_one())

    page_params = dict(params)
    page_params["limit"] = limit
    page_params["offset"] = offset
    rows_result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"SELECT * FROM {_BATCHES_TABLE} WHERE {where_clause} "
            # ingestion_id tiebreaker: created_at alone is not unique, so a
            # plain ORDER BY created_at DESC leaves ties free to reorder
            # between requests -- adversarial-review finding -- which can
            # duplicate or skip rows across pages under concurrent inserts.
            "ORDER BY created_at DESC, ingestion_id DESC LIMIT :limit OFFSET :offset"
        ),
        page_params,
    )
    rows = [_row_to_batch(m) for m in rows_result.mappings().all()]
    return rows, total


async def list_rejections(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[RejectionRow], int]:
    """Returns ``(page, total_stored_count)`` ordered by ``record_index
    ASC``. ``total_stored_count`` is capped at
    ``MAX_STORED_REJECTIONS_PER_BATCH`` -- for the TRUE total_rejected count
    (which may exceed what's stored), read ``BatchRow.items_rejected`` /
    ``error_summary['total_rejected']`` instead. ``org_id``-scoped even
    though the caller has typically already tenant-checked the parent batch
    (defense in depth: no rejection query in this module is ever
    org-unscoped)."""
    params = {"org_id": org_id, "ingestion_id": str(ingestion_id)}
    count_result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"SELECT COUNT(*) AS total FROM {_REJECTIONS_TABLE} "
            "WHERE org_id = :org_id AND ingestion_id = :ingestion_id"
        ),
        params,
    )
    total = int(count_result.scalar_one())

    page_params: dict[str, Any] = dict(params)
    page_params["limit"] = limit
    page_params["offset"] = offset
    rows_result = await session.execute(
        # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
        text(
            f"SELECT * FROM {_REJECTIONS_TABLE} "
            "WHERE org_id = :org_id AND ingestion_id = :ingestion_id "
            "ORDER BY record_index ASC LIMIT :limit OFFSET :offset"
        ),
        page_params,
    )
    rows = [_row_to_rejection(m) for m in rows_result.mappings().all()]
    return rows, total


# ---------------------------------------------------------------------------
# Data-plane response models (camelCase, local to this file -- CHAOS-2694
# does not append to CHAOS-2691's schemas.py; see module docstring)
# ---------------------------------------------------------------------------


class SourceRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    system: str
    instance: str


class WindowRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    started_at: datetime | None = Field(default=None, alias="startedAt")
    ended_at: datetime | None = Field(default=None, alias="endedAt")


class RejectedRecordResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    index: int
    kind: str
    external_id: str | None = Field(default=None, alias="externalId")
    code: str
    message: str
    path: str | None = None


class RecomputeJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    task: str
    task_id: str | None = Field(default=None, alias="taskId")
    queue: str
    repo_id: str | None = Field(default=None, alias="repoId")


class RecomputeScopeResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    repo_ids: list[str] = Field(default_factory=list, alias="repoIds")
    team_ids: list[str] = Field(default_factory=list, alias="teamIds")
    window_started_at: datetime | None = Field(default=None, alias="windowStartedAt")
    window_ended_at: datetime | None = Field(default=None, alias="windowEndedAt")
    capped_days: bool = Field(default=False, alias="cappedDays")
    capped_repos: bool = Field(default=False, alias="cappedRepos")


class RecomputeStatusResponse(BaseModel):
    """CHAOS-2699 (master-spec CC21). Enum pinned epic-wide:
    ``not_applicable | pending | dispatched | skipped_no_scope | failed``."""

    model_config = ConfigDict(populate_by_name=True)
    status: str
    scope: RecomputeScopeResponse | None = None
    dispatched_at: datetime | None = Field(default=None, alias="dispatchedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    error: str | None = None
    jobs: list[RecomputeJobResponse] = Field(default_factory=list)


class BatchStatusResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    ingestion_id: uuid.UUID = Field(alias="ingestionId")
    status: str
    attempts: int
    items_received: int = Field(alias="itemsReceived")
    items_accepted: int = Field(alias="itemsAccepted")
    items_rejected: int = Field(alias="itemsRejected")
    source: SourceRef
    window: WindowRef
    producer: str | None = None
    producer_version: str | None = Field(default=None, alias="producerVersion")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    error_summary: dict[str, Any] | None = Field(default=None, alias="errorSummary")
    errors: list[RejectedRecordResponse]
    errors_total: int = Field(alias="errorsTotal")
    errors_limit: int = Field(alias="errorsLimit")
    errors_offset: int = Field(alias="errorsOffset")
    # CHAOS-2699, master-spec CC21: cross-wave extension -- 2694 ships wave
    # 2 with no recompute references; this block is added in wave 3.
    recompute: RecomputeStatusResponse


class BatchListItemResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    ingestion_id: uuid.UUID = Field(alias="ingestionId")
    status: str
    items_received: int = Field(alias="itemsReceived")
    items_accepted: int = Field(alias="itemsAccepted")
    items_rejected: int = Field(alias="itemsRejected")
    source: SourceRef
    window: WindowRef
    producer: str | None = None
    created_at: datetime = Field(alias="createdAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")


class BatchListResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    items: list[BatchListItemResponse]
    total: int
    limit: int
    offset: int


def _batch_to_list_item(row: BatchRow) -> BatchListItemResponse:
    return BatchListItemResponse(
        ingestion_id=row.ingestion_id,
        status=row.status,
        items_received=row.items_received,
        items_accepted=row.items_accepted,
        items_rejected=row.items_rejected,
        source=SourceRef(system=row.source_system, instance=row.source_instance),
        window=WindowRef(
            started_at=row.window_started_at, ended_at=row.window_ended_at
        ),
        producer=row.producer,
        created_at=row.created_at,
        completed_at=row.completed_at,
    )


def _recompute_scope_response(
    scope: dict[str, Any] | None,
) -> RecomputeScopeResponse | None:
    if scope is None:
        return None
    return RecomputeScopeResponse(
        repo_ids=list(scope.get("repoIds") or []),
        team_ids=list(scope.get("teamIds") or []),
        window_started_at=_parse_dt(scope.get("windowStartedAt")),
        window_ended_at=_parse_dt(scope.get("windowEndedAt")),
        capped_days=bool(scope.get("cappedDays", False)),
        capped_repos=bool(scope.get("cappedRepos", False)),
    )


def _batch_to_recompute_response(
    row: BatchRow, jobs: list[RecomputeJobRow]
) -> RecomputeStatusResponse:
    return RecomputeStatusResponse(
        status=row.recompute_status,
        scope=_recompute_scope_response(row.recompute_scope),
        dispatched_at=row.recompute_dispatched_at,
        completed_at=row.recompute_completed_at,
        error=row.recompute_error,
        jobs=[
            RecomputeJobResponse(
                task=j.task, task_id=j.task_id, queue=j.queue, repo_id=j.repo_id
            )
            for j in jobs
        ],
    )


def _batch_to_status_response(
    row: BatchRow,
    errors: list[RejectionRow],
    errors_total: int,
    errors_limit: int,
    errors_offset: int,
    recompute_jobs: list[RecomputeJobRow],
) -> BatchStatusResponse:
    return BatchStatusResponse(
        ingestion_id=row.ingestion_id,
        status=row.status,
        attempts=row.attempts,
        items_received=row.items_received,
        items_accepted=row.items_accepted,
        items_rejected=row.items_rejected,
        source=SourceRef(system=row.source_system, instance=row.source_instance),
        window=WindowRef(
            started_at=row.window_started_at, ended_at=row.window_ended_at
        ),
        producer=row.producer,
        producer_version=row.producer_version,
        created_at=row.created_at,
        updated_at=row.updated_at,
        completed_at=row.completed_at,
        error_summary=row.error_summary,
        errors=[
            RejectedRecordResponse(
                index=e.record_index,
                kind=e.record_kind,
                external_id=e.external_id,
                code=e.code,
                message=e.message,
                path=e.path,
            )
            for e in errors
        ],
        errors_total=errors_total,
        errors_limit=errors_limit,
        errors_offset=errors_offset,
        recompute=_batch_to_recompute_response(row, recompute_jobs),
    )


# ---------------------------------------------------------------------------
# Router (mounted directly in api/main.py, not via CHAOS-2691's router.py)
# ---------------------------------------------------------------------------

status_router = APIRouter(prefix="/api/v1/external-ingest", tags=["external-ingest"])

# Bound once at import time (matches router.py's convention) so tests can
# target this exact object via app.dependency_overrides.
_require_ingest_status = require_ingest_scope("ingest:status")


@status_router.get("/batches", response_model=BatchListResponse)
@limiter.limit(INGEST_READ_LIMIT, key_func=get_ingest_token_key)
async def list_batch_statuses(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    ctx: Annotated[IngestAuthContext, Depends(_require_ingest_status)],
    source_system: Annotated[str | None, Query(alias="sourceSystem")] = None,
    source_instance: Annotated[str | None, Query(alias="sourceInstance")] = None,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
    created_after: Annotated[datetime | None, Query(alias="createdAfter")] = None,
    created_before: Annotated[datetime | None, Query(alias="createdBefore")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> BatchListResponse:
    rows, total = await list_batches(
        session,
        org_id=ctx.org_id,
        source_system=source_system,
        source_instance=source_instance,
        status=status_filter,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
        offset=offset,
    )
    return BatchListResponse(
        items=[_batch_to_list_item(row) for row in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@status_router.get("/batches/{ingestion_id}", response_model=BatchStatusResponse)
@limiter.limit(INGEST_READ_LIMIT, key_func=get_ingest_token_key)
async def get_batch_status(
    ingestion_id: uuid.UUID,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    ctx: Annotated[IngestAuthContext, Depends(_require_ingest_status)],
    error_limit: Annotated[int, Query(ge=1, le=200, alias="errorLimit")] = 50,
    error_offset: Annotated[int, Query(ge=0, alias="errorOffset")] = 0,
) -> BatchStatusResponse:
    batch = await get_batch(session, org_id=ctx.org_id, ingestion_id=ingestion_id)
    if batch is None:
        raise ExternalIngestError(404, "not_found", "ingestion batch not found")
    errors, errors_total = await list_rejections(
        session,
        org_id=ctx.org_id,
        ingestion_id=ingestion_id,
        limit=error_limit,
        offset=error_offset,
    )
    recompute_jobs = await get_recompute_jobs(
        session,
        org_id=ctx.org_id,
        source_system=batch.source_system,
        source_instance=batch.source_instance,
        dispatched_at=batch.recompute_dispatched_at,
    )
    return _batch_to_status_response(
        batch, errors, errors_total, error_limit, error_offset, recompute_jobs
    )
