"""Worker-side batch processor for external ingest (CHAOS-2697).

The pinned worker contract (master-spec CC23, consumed by CHAOS-2693's
``api/external_ingest/consumer.py`` — its ``_processor_available()``
deployment-order guard keys on this module's importability, so merging this
file is what arms the beat-scheduled consumer):

- ``process_batch(*, ingestion_id, org_id, source_system, source_instance,
  schema_version) -> int`` — fetch payload → re-validate per record
  (``validate.py`` unchanged + the CC6 matrix, via ``normalize.py``) →
  ``sinks.write_batch()`` with the CC11 in-process retry ladder → terminal
  status + rejection diagnostics via ``api/external_ingest/status.py`` →
  delete the payload row → ``schedule_or_coalesce(...)`` once (best-effort).
- ``mark_batch_failed(*, ingestion_id, org_id, reason)`` — the consumer's
  give-up path (max_deliveries exhausted / permanent DLQ). MUST raise on
  failure: the consumer's ACK gate leaves the entry un-ACKed when this
  raises, so a transient Postgres outage retries the status write instead of
  stranding the batch in a non-terminal status with the entry gone.

Failure classification (consumer contract):

- ``PermanentProcessingError`` — unsupported schema version, corrupt/missing
  payload, unregistered source, pointer/payload disagreement. The consumer
  DLQs immediately and marks the batch failed.
- Any other exception — treated as transient; the entry stays in the PEL for
  the reclaim ladder. ``TransientSinkWriteError`` (sink writes still failing
  after the retry ladder) is deliberately in this class.

Replay safety: every path here tolerates a second invocation for the same
``ingestion_id`` (CC11 post-critique). The terminal-status check short-
circuits redelivered pointers; ``mark_processing``/``complete_batch`` are
CAS transitions; sink writes are ReplacingMergeTree upserts on natural keys;
recompute is debounce-coalesced. See
docs/architecture/external-ingest-worker.md.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.external_ingest.schemas import (
    OPERATIONAL_RECORD_KINDS,
    SCHEMA_VERSION,
    BatchEnvelope,
)
from dev_health_ops.api.external_ingest.status import (
    RejectedRecord,
    complete_batch,
    get_batch,
    mark_failed,
    mark_processing,
)
from dev_health_ops.db import get_postgres_session, require_clickhouse_uri
from dev_health_ops.external_ingest.errors import PermanentProcessingError
from dev_health_ops.external_ingest.feature_gate import (
    CanonicalIncidentIngestionDisabledError,
    external_operational_ingestion_allowed,
)
from dev_health_ops.external_ingest.normalize import (
    ALLOWED_KINDS_BY_SYSTEM,
    NormalizationResult,
    normalize_batch,
)
from dev_health_ops.external_ingest.payload_store import delete_payload, fetch_payload
from dev_health_ops.external_ingest.recompute import schedule_or_coalesce
from dev_health_ops.external_ingest.sinks import write_batch
from dev_health_ops.external_ingest.types import NormalizedBatch, SinkWriteResult
from dev_health_ops.models.external_ingest import TERMINAL_STATUSES, BatchStatus
from dev_health_ops.models.ingest_auth import IngestSource

logger = logging.getLogger(__name__)

#: CC11 in-process retry ladder for sink-write transients: initial attempt
#: plus one retry per backoff value (2s/4s/8s — 14s worst-case sleep, which
#: is why 2693 raised ``reclaim_idle_ms`` to 15 minutes).
SINK_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (2.0, 4.0, 8.0)

_TERMINAL_STATUS_VALUES = {s.value for s in TERMINAL_STATUSES}


async def _operational_ingestion_allowed(
    session: AsyncSession,
    org_id: str,
) -> bool:
    return await external_operational_ingestion_allowed(session, org_id)


class TransientSinkWriteError(RuntimeError):
    """Sink writes still failing after the in-process retry ladder.

    Deliberately NOT a ``PermanentProcessingError``: the consumer treats it
    as transient, leaving the entry in the PEL for the reclaim ladder
    (up to ``max_deliveries``) before the give-up path DLQs it — ClickHouse
    outages should get minutes of runway, not an instant DLQ."""


def _parse_ingestion_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError, TypeError) as exc:
        raise PermanentProcessingError(
            f"stream entry carries a non-UUID ingestion_id: {value!r}"
        ) from exc


async def _resolve_source_id(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    entity_family: str = "legacy",
) -> uuid.UUID:
    """The registered source's UUID, stamped as row provenance (CC8
    ``source_id`` column). Case-insensitive match on both system and
    instance, consistent with CHAOS-2695's ownership predicates (case-variant
    duplicates are blocked at registration, so at most one logical source
    matches). A source disabled AFTER accept still resolves — the batch was
    accepted while write-eligible and its data is already durable; prefer the
    write-eligible row only as a tiebreak against pre-2695 legacy duplicates.
    """
    rows = (
        (
            await session.execute(
                select(IngestSource)
                .where(
                    IngestSource.org_id == org_id,
                    func.lower(IngestSource.system) == source_system.strip().lower(),
                    func.lower(IngestSource.instance)
                    == source_instance.strip().lower(),
                    IngestSource.entity_family == entity_family,
                )
                .order_by(IngestSource.created_at)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        raise PermanentProcessingError(
            f"no registered ingest source for org={org_id!r} "
            f"source={source_system}/{source_instance} — cannot attribute "
            "provenance (source_id) for this batch"
        )
    for row in rows:
        if row.is_write_eligible():
            return row.id
    return rows[0].id


def _parse_envelope(payload_bytes: bytes, *, ingestion_id: str) -> BatchEnvelope:
    try:
        data = json.loads(payload_bytes)
    except (ValueError, UnicodeDecodeError) as exc:
        raise PermanentProcessingError(
            f"payload for batch {ingestion_id} is not valid JSON: {exc}"
        ) from exc
    try:
        return BatchEnvelope.model_validate(data)
    except ValidationError as exc:
        raise PermanentProcessingError(
            f"payload for batch {ingestion_id} is not a valid "
            f"{SCHEMA_VERSION} envelope: {exc.error_count()} validation error(s)"
        ) from exc


async def _write_with_retries(
    batch: NormalizedBatch, *, clickhouse_dsn: str
) -> SinkWriteResult:
    """``write_batch`` never raises — per-kind failures come back in
    ``result.errors`` (batch-call granularity). Retrying the WHOLE batch is
    safe (RMT upserts on natural keys), so the ladder re-runs everything and
    only the final attempt's outcome counts."""
    result = await write_batch(batch, clickhouse_dsn=clickhouse_dsn)
    for delay in SINK_RETRY_BACKOFF_SECONDS:
        if not result.errors:
            break
        logger.warning(
            "external_ingest.processor.sink_retry ingestion_id=%s errors=%d "
            "retry_in=%.0fs",
            batch.ingestion_id,
            len(result.errors),
            delay,
        )
        await asyncio.sleep(delay)
        result = await write_batch(batch, clickhouse_dsn=clickhouse_dsn)
    if result.errors:
        first = result.errors[0]
        raise TransientSinkWriteError(
            f"{len(result.errors)} sink write failure(s) persisted through "
            f"{1 + len(SINK_RETRY_BACKOFF_SECONDS)} attempts for batch "
            f"{batch.ingestion_id} (first: {first.kind}: {first.message})"
        )
    for warning in result.warnings:
        logger.warning(
            "external_ingest.processor.sink_warning ingestion_id=%s kind=%s "
            "index=%d code=%s: %s",
            batch.ingestion_id,
            warning.kind,
            warning.record_index,
            warning.code,
            warning.message,
        )
    return result


def _rejections_for_store(norm: NormalizationResult) -> list[RejectedRecord]:
    return [
        RejectedRecord(
            record_index=r.index,
            record_kind=r.kind,
            external_id=r.external_id,
            code=r.code,
            message=r.message,
            path=r.path,
        )
        for r in norm.rejections
    ]


async def _dispatch_recompute_best_effort(
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    ingestion_id: str,
    norm: NormalizationResult,
    sink_result: SinkWriteResult,
    window_started_at,
    window_ended_at,
) -> None:
    """CC23: ``schedule_or_coalesce`` ONCE at the end; a dispatch failure
    never fails ingestion (the batch is already terminal + durable).

    ``record_kinds`` uses the FULL kind names from normalization
    (``pull_request.v1``) — the planner's ``_GIT_KINDS``/``_WORK_ITEM_KINDS``
    vocabularies are ``.v1``-suffixed, so the sink scope's bare-kind names
    would silently plan zero recompute. ``schedule_or_coalesce`` is sync
    (Valkey pipeline + ``apply_async`` + a possible synchronous fallback into
    ``get_postgres_session_sync``) — run in a thread, matching the sink
    layer's ``asyncio.to_thread`` convention for sync clients."""
    scope = sink_result.affected_scope
    try:
        await asyncio.to_thread(
            schedule_or_coalesce,
            org_id=org_id,
            source_system=source_system,
            source_instance=source_instance,
            ingestion_id=ingestion_id,
            repo_ids={str(repo_id) for repo_id in scope.repo_ids},
            team_ids=set(scope.team_ids),
            window_start=scope.min_timestamp or window_started_at,
            window_end=scope.max_timestamp or window_ended_at,
            record_kinds=set(norm.record_counts),
        )
    except Exception:
        logger.exception(
            "external_ingest.processor.recompute_dispatch_failed "
            "ingestion_id=%s org_id=%s (best-effort; batch is already terminal)",
            ingestion_id,
            org_id,
        )


async def process_batch(
    *,
    ingestion_id: str,
    org_id: str,
    source_system: str,
    source_instance: str,
    schema_version: str,
) -> int:
    """Process one accepted batch end to end. Returns ``items_accepted``
    (0 for an idempotent skip). See the module docstring for the failure
    classification the consumer relies on."""
    if schema_version != SCHEMA_VERSION:
        raise PermanentProcessingError(
            f"unsupported schema version {schema_version!r} on stream entry "
            f"for batch {ingestion_id} (worker speaks {SCHEMA_VERSION!r} only)"
        )
    if source_system not in ALLOWED_KINDS_BY_SYSTEM:
        raise PermanentProcessingError(
            f"unknown source system {source_system!r} on stream entry for "
            f"batch {ingestion_id}"
        )
    batch_uuid = _parse_ingestion_uuid(ingestion_id)
    # Resolve config before touching batch status: a missing CLICKHOUSE_URI
    # raises RuntimeError (transient — operator-fixable) without moving the
    # row out of its retryable status.
    clickhouse_dsn = require_clickhouse_uri()

    async with get_postgres_session() as session:
        row = await get_batch(session, org_id=org_id, ingestion_id=batch_uuid)
        if row is None:
            # The accept path commits the batch row before the pointer ever
            # reaches the stream, and rows are never deleted — a missing row
            # is cross-environment pollution or corruption, not a race.
            raise PermanentProcessingError(
                f"no status row for batch {ingestion_id} (org={org_id!r})"
            )
        if row.status in _TERMINAL_STATUS_VALUES:
            logger.info(
                "external_ingest.processor.skip_terminal ingestion_id=%s status=%s",
                ingestion_id,
                row.status,
            )
            return 0

        await mark_processing(session, org_id=org_id, ingestion_id=batch_uuid)
        # Commit the processing transition BEFORE any sink write (CHAOS-2498
        # commit-before-risky pattern): a crash mid-write must leave a row
        # that says a worker attempted it, and GET /batches must not report
        # 'accepted' for a batch whose rows are landing in ClickHouse.
        await session.commit()

        row = await get_batch(session, org_id=org_id, ingestion_id=batch_uuid)
        if row is None or row.status != BatchStatus.PROCESSING.value:
            # Lost the CAS: a concurrent stale-RETRY re-accepted the row (its
            # fresh pointer owns it now) or another actor completed it. With
            # the single-replica deployment invariant this is razor-thin, but
            # yielding is always safe — ACK this pointer and move on.
            logger.info(
                "external_ingest.processor.yield_lost_cas ingestion_id=%s status=%s",
                ingestion_id,
                "<missing>" if row is None else row.status,
            )
            return 0

        payload = await fetch_payload(session, ingestion_id=batch_uuid, org_id=org_id)
        if payload is None:
            # enqueue_batch's fail-closed invariant guarantees the payload was
            # durable before the pointer became visible; a missing row means
            # the prune sweep (CC9, 168h) or a prior terminal cleanup beat us.
            raise PermanentProcessingError(
                f"payload row for batch {ingestion_id} is missing (pruned or "
                "already cleaned up) — batch cannot be processed"
            )
        envelope = _parse_envelope(payload, ingestion_id=ingestion_id)
        if any(record.kind in OPERATIONAL_RECORD_KINDS for record in envelope.records):
            if not await _operational_ingestion_allowed(session, org_id):
                error = CanonicalIncidentIngestionDisabledError()
                await mark_failed(
                    session,
                    org_id=org_id,
                    ingestion_id=batch_uuid,
                    reason=str(error),
                )
                await delete_payload(session, ingestion_id=batch_uuid)
                await session.commit()
                raise error
        source_id = await _resolve_source_id(
            session,
            org_id=org_id,
            source_system=source_system,
            source_instance=source_instance,
            entity_family=envelope.source.entity_family,
        )
        if (
            envelope.source.system != source_system
            or envelope.source.instance != source_instance
        ):
            raise PermanentProcessingError(
                f"stream pointer for batch {ingestion_id} says "
                f"{source_system}/{source_instance} but the stored payload "
                f"says {envelope.source.system}/{envelope.source.instance}"
            )
        if len(envelope.records) != row.items_received:
            raise PermanentProcessingError(
                f"stored payload for batch {ingestion_id} has "
                f"{len(envelope.records)} records but the batch row recorded "
                f"items_received={row.items_received}"
            )

        norm = normalize_batch(
            org_id=org_id,
            source_id=source_id,
            source_system=source_system,
            source_instance=source_instance,
            ingestion_id=batch_uuid,
            records=envelope.records,
        )

        sink_result: SinkWriteResult | None = None
        if norm.items_accepted > 0:
            # Raises TransientSinkWriteError past the ladder -> the session
            # context rolls back nothing (processing is committed), the entry
            # stays in the PEL, and the reclaim ladder retries us whole.
            sink_result = await _write_with_retries(
                norm.batch, clickhouse_dsn=clickhouse_dsn
            )

        await complete_batch(
            session,
            org_id=org_id,
            ingestion_id=batch_uuid,
            items_accepted=norm.items_accepted,
            items_rejected=norm.items_rejected,
            rejections=_rejections_for_store(norm),
            record_counts=norm.record_counts or None,
        )
        # CC9: the worker deletes the payload row on terminal status, in the
        # same transaction — a failed-batch resubmission (RETRY) re-upserts it.
        await delete_payload(session, ingestion_id=batch_uuid)
        await session.commit()

    if sink_result is not None:
        await _dispatch_recompute_best_effort(
            org_id=org_id,
            source_system=source_system,
            source_instance=source_instance,
            ingestion_id=ingestion_id,
            norm=norm,
            sink_result=sink_result,
            window_started_at=row.window_started_at,
            window_ended_at=row.window_ended_at,
        )

    logger.info(
        "external_ingest.processor.completed ingestion_id=%s org_id=%s "
        "accepted=%d rejected=%d",
        ingestion_id,
        org_id,
        norm.items_accepted,
        norm.items_rejected,
    )
    return norm.items_accepted


async def mark_batch_failed(*, ingestion_id: str, org_id: str, reason: str) -> None:
    """Consumer give-up path (CC11/CC23). Forces the batch terminal-``failed``
    from any non-terminal status and cleans up its payload row.

    RAISES on failure (never swallow — the consumer's ACK gate): if the
    status write cannot land, the entry must stay un-ACKed so a later
    redelivery retries it; ACKing past a lost write would strand the batch
    non-terminal with the pointer gone. Idempotent for already-terminal or
    unknown batches (a DLQ re-drive or duplicate pointer is a no-op).
    """
    try:
        batch_uuid = uuid.UUID(ingestion_id)
    except (ValueError, AttributeError, TypeError):
        # Nothing addressable to mark; the DLQ entry itself is the record.
        logger.warning(
            "external_ingest.processor.mark_failed_unaddressable "
            "ingestion_id=%r org_id=%s",
            ingestion_id,
            org_id,
        )
        return

    async with get_postgres_session() as session:
        transitioned = await mark_failed(
            session, org_id=org_id, ingestion_id=batch_uuid, reason=reason
        )
        if transitioned:
            await delete_payload(session, ingestion_id=batch_uuid)
            logger.warning(
                "external_ingest.processor.marked_failed ingestion_id=%s "
                "org_id=%s reason=%s",
                ingestion_id,
                org_id,
                reason,
            )
        else:
            logger.info(
                "external_ingest.processor.mark_failed_noop ingestion_id=%s "
                "org_id=%s (already terminal or unknown)",
                ingestion_id,
                org_id,
            )
        # get_postgres_session commits on clean exit; an exception anywhere
        # above propagates (rollback + re-raise) per the RAISES contract.


__all__ = [
    "SINK_RETRY_BACKOFF_SECONDS",
    "TransientSinkWriteError",
    "mark_batch_failed",
    "process_batch",
]
