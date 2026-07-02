"""Batch idempotency resolution for external customer-push ingestion (CHAOS-2695).

Implements the NEW / REPLAY / CONFLICT / RETRY policy over CHAOS-2694's
direct-SQL status store (``status.py``) — this module owns the DECISION,
the store owns the rows. Lives in the API package (deliberate deviation
from the brief's ``dev_health_ops/external_ingest/`` placement): the policy
is accept-time-only and its store dependency lives here — importing the
store from the sibling package would recurse through this package's
``__init__`` (which imports ``router``, which imports this module) into a
circular ImportError. ``external_ingest/ownership.py`` (models-only
imports, worker-shareable) keeps the brief's placement. Keys are unique
FOREVER per
``(org_id, source_system, source_instance, idempotency_key)`` (no TTL —
deliberate deviation from the legacy ``/api/v1/ingest`` 24h Redis cache;
"reprocessing must be safe" requires durable dedup, brief §2).

Outcome semantics (brief decision 7 + post-critique CC13):

- ``NEW``       — no row existed; one was inserted (``status='accepted'``,
  ``attempts=1``). Caller persists the payload row and enqueues.
- ``REPLAY``    — same key, same payload hash, batch not retryable → return
  the CURRENT status (200, brief decision 8), do NOT re-enqueue.
- ``CONFLICT``  — same key, different payload hash → 409
  ``idempotency_conflict``. Never overwrites the original row.
- ``RETRY``     — same key, same hash, and the existing row is safe to
  re-accept: ``status`` in ``RETRYABLE_STATUSES`` ({stream_unavailable,
  failed}), OR ``accepted`` gone stale (``updated_at`` older than
  ``EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES``, default 15 — closes the
  crash-before-XADD / stream-trim fail-open where ``accepted`` was otherwise
  unrecoverable; stale ``processing`` deliberately REPLAYs, see
  ``_STALE_ELIGIBLE_STATUSES``). Caller resets the row (attempts+=1,
  status→accepted), refreshes the payload, and re-enqueues the SAME
  ingestion_id.

See docs/architecture/external-ingest-idempotency-ownership.md.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.external_ingest import BatchStatus

from .status import (
    BatchRow,
    DuplicateIdempotencyKeyError,
    create_batch,
    find_existing_batch,
)

RETRYABLE_STATUSES = frozenset(
    {BatchStatus.STREAM_UNAVAILABLE.value, BatchStatus.FAILED.value}
)

#: accepted rows younger than this are REPLAY (the enqueue may still be
#: in flight); older ones are presumed lost and RETRY.
ACCEPTED_STALE_MINUTES_DEFAULT = 15

#: Only ``accepted`` is stale-retryable — a DELIBERATE narrowing of the
#: reconciliation header's literal "accepted/processing" (adversarial-review
#: finding). A row stuck in ``accepted`` means the pointer never became
#: visible on the stream (crash between COMMIT and XADD, or trimmed before
#: first delivery): no worker can ever see it, so the client's same-key
#: resubmit is the ONLY recovery path — that is the fail-open CC13 exists to
#: close. A row in ``processing`` proves a worker HAS the pointer; its
#: recovery paths are the stream reclaim machinery (CHAOS-2693) and the
#: ``failed`` terminal status (already RETRYABLE), and a client-driven retry
#: there would race a slow-but-alive worker's terminal CAS with no attempt
#: fence — the retried attempt and the original could both apply, with the
#: superseded attempt's counters winning. Stale ``processing`` therefore
#: REPLAYs. If operational need appears, CHAOS-2697+ can extend this with an
#: attempt-fenced design (attempt number in the stream entry, CAS'd through
#: mark_processing/complete_batch).
_STALE_ELIGIBLE_STATUSES = frozenset({BatchStatus.ACCEPTED.value})

__all__ = [
    "ACCEPTED_STALE_MINUTES_DEFAULT",
    "RETRYABLE_STATUSES",
    "IdempotencyOutcome",
    "IdempotencyOutcomeKind",
    "IngestTemporarilyUnavailableError",
    "accepted_stale_minutes",
    "compute_payload_hash",
    "resolve_batch_idempotency",
]


class IngestTemporarilyUnavailableError(RuntimeError):
    """True concurrent same-key race: our INSERT hit the unique constraint
    but the winning row is not visible yet (uncommitted, or itself rolled
    back). Maps to ``503 ingest_temporarily_unavailable`` (master-spec CC16)
    — the client's idempotency-safe retry resolves it on the next attempt."""


class IdempotencyOutcomeKind(str, Enum):
    NEW = "new"
    REPLAY = "replay"
    CONFLICT = "conflict"
    RETRY = "retry"


@dataclass(frozen=True)
class IdempotencyOutcome:
    kind: IdempotencyOutcomeKind
    batch: BatchRow


def accepted_stale_minutes() -> int:
    return int(
        os.environ.get(
            "EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES",
            str(ACCEPTED_STALE_MINUTES_DEFAULT),
        )
    )


def compute_payload_hash(envelope: BaseModel) -> str:
    """SHA-256 hex digest of the canonicalized, schema-validated envelope.

    MUST be called on the already-validated Pydantic model (post envelope
    validation in the router), never on raw request bytes — canonicalization
    relies on ``model_dump(mode="json")`` normalizing timestamp formats
    (``...Z`` vs ``...+00:00``) and on ``sort_keys``/``separators`` removing
    field-order and whitespace variance (brief decisions 1-2). The full
    envelope is hashed, ``records`` in given order — record order is
    position-significant by design (brief decisions 3-4).
    """
    canonical = json.dumps(
        envelope.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _classify_existing(
    existing: BatchRow, payload_hash: str, now: datetime
) -> IdempotencyOutcome:
    if existing.payload_hash != payload_hash:
        return IdempotencyOutcome(IdempotencyOutcomeKind.CONFLICT, existing)
    if existing.status in RETRYABLE_STATUSES:
        return IdempotencyOutcome(IdempotencyOutcomeKind.RETRY, existing)
    if existing.status in _STALE_ELIGIBLE_STATUSES:
        age = now - existing.updated_at
        if age > timedelta(minutes=accepted_stale_minutes()):
            return IdempotencyOutcome(IdempotencyOutcomeKind.RETRY, existing)
    return IdempotencyOutcome(IdempotencyOutcomeKind.REPLAY, existing)


async def resolve_batch_idempotency(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    idempotency_key: str,
    payload_hash: str,
    schema_version: str,
    producer: str | None,
    producer_version: str | None,
    window_started_at: datetime | None,
    window_ended_at: datetime | None,
    items_received: int,
) -> IdempotencyOutcome:
    """Resolve NEW / REPLAY / CONFLICT / RETRY for a batch identity.

    MUST be the first Postgres write in the accept sequence (master-spec
    CC22: idempotency row → payload row → COMMIT → enqueue) — the unique
    index on ``(org_id, source_system, source_instance, idempotency_key)``
    is the serialization point for concurrent same-key accepts. The insert
    runs inside ``create_batch``'s SAVEPOINT, so a losing racer only rolls
    back the savepoint, not the caller's session. Does NOT commit.

    Raises :class:`IngestTemporarilyUnavailableError` when the insert loses
    the race but the winner's row is not yet visible (map to 503).
    """
    now = datetime.now(timezone.utc)
    existing = await find_existing_batch(
        session,
        org_id=org_id,
        source_system=source_system,
        source_instance=source_instance,
        idempotency_key=idempotency_key,
    )
    if existing is not None:
        return _classify_existing(existing, payload_hash, now)

    try:
        created = await create_batch(
            session,
            ingestion_id=uuid.uuid4(),
            org_id=org_id,
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            source_system=source_system,
            source_instance=source_instance,
            producer=producer,
            producer_version=producer_version,
            schema_version=schema_version,
            window_started_at=window_started_at,
            window_ended_at=window_ended_at,
            items_received=items_received,
        )
    except DuplicateIdempotencyKeyError:
        raced = await find_existing_batch(
            session,
            org_id=org_id,
            source_system=source_system,
            source_instance=source_instance,
            idempotency_key=idempotency_key,
        )
        if raced is None:
            raise IngestTemporarilyUnavailableError(
                "A concurrent request for the same idempotency key is in "
                "progress. Retry."
            ) from None
        return _classify_existing(raced, payload_hash, now)

    return IdempotencyOutcome(IdempotencyOutcomeKind.NEW, created)
