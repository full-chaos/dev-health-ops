"""Durable-stream writer for external-ingest batches (CHAOS-2691 D6, hardened
by CHAOS-2693).

Real, not mocked: raises ``StreamUnavailableError`` on any failure so the
router can fail closed with a 503 -- never accept-and-warn (contrast with
``api/ingest/streams.py``'s legacy accept-and-warn behavior, which the epic
explicitly rejects for the durability-focused external-ingest path).

Pointer-only transport (master-spec CC9/D2): the stream entry carries batch
*metadata* only; the full payload lives in Postgres
(``external_ingest_batch_payloads``, accessed via
``dev_health_ops.external_ingest.payload_store``). The wave-1 interim
inline-``payload`` field is dropped from the XADD fields here.

**Fail-closed payload invariant (standing correctness guarantee, not just a
deployment-sequencing note):** ``enqueue_batch()`` is ``async`` and, before
writing the pointer, verifies via a cheap indexed
``payload_store.payload_exists()`` check that the payload row is already
durable. Absent row -> ``StreamUnavailableError`` (503), same as a Redis
outage. This makes "a pointer must never become visible before its payload
is durable" true regardless of caller ordering mistakes -- the router
(CHAOS-2691, and its wave-4 CC22 rewire owned by CHAOS-2695) is expected to
call ``payload_store.upsert_payload()`` and commit *before* calling this
function, but the invariant holds even if a future caller gets that
ordering wrong.

Per-org stream/DLQ naming (master-spec CC10/D1): a single huge/bursty org's
backlog must not delay another org's consumer-group progress, since Redis
Streams delivers entries in strict ID order regardless of logical owner.
``StreamConsumer.discover_streams()`` (``api/_stream_consumer.py``)
resolves these via ``SCAN ... TYPE stream`` wildcard discovery
(``external-ingest:*:batches``), so per-org keys need no separate registry.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

#: Consumer-group name shared by producer (documented here) and consumer
#: (``consumer.py``) -- kept as one constant (master-spec CC10).
CONSUMER_GROUP = "external-ingest-consumers"

#: Matches the existing ingest/product-telemetry stream backpressure
#: convention -- bounds by entry count, not bytes (D2's rationale for
#: keeping payloads off the stream entirely).
STREAM_MAXLEN = 100_000


class StreamUnavailableError(Exception):
    """Raised when the durable ingest stream cannot accept a write.

    Callers MUST map this to HTTP 503, never accept-and-warn (master-spec
    CC11/D3) -- mirrors product-telemetry's ``raise ConnectionError(...)``,
    stricter than legacy ``/api/v1/ingest``'s silent-drop-on-``False``. Also
    raised when the payload-durability precondition (see module docstring)
    cannot be verified or is not satisfied.
    """


def stream_name(org_id: str) -> str:
    return f"external-ingest:{org_id}:batches"


def dlq_name(org_id: str) -> str:
    return f"external-ingest:{org_id}:dlq"


def get_redis_client():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import valkey as redis

        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Redis unavailable for external-ingest streams")
        return None


async def _require_payload_durable(*, org_id: str, ingestion_id: str) -> None:
    """Fail-closed precondition check: raises unless the payload row exists.

    Opens its own short-lived session (rather than requiring callers to pass
    one) so ``enqueue_batch()`` stays a self-contained precondition check --
    correct regardless of whether the caller's own write transaction has
    already committed by the time this runs (it must have, or this raises).
    A Postgres error here (not just "row absent") is ALSO fail-closed: if we
    cannot verify durability, we must not make the pointer visible either.
    """
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.external_ingest import payload_store

    try:
        async with get_postgres_session() as session:
            present = await payload_store.payload_exists(
                session, ingestion_id=ingestion_id, org_id=org_id
            )
    except Exception as exc:
        logger.exception(
            "Failed to verify payload durability for ingestion_id=%s", ingestion_id
        )
        raise StreamUnavailableError(str(exc)) from exc

    if not present:
        raise StreamUnavailableError(
            f"payload row missing for ingestion_id={ingestion_id!r} "
            f"org_id={org_id!r} -- refusing to enqueue a pointer with no "
            "durable payload behind it"
        )


async def enqueue_batch(
    *,
    org_id: str,
    ingestion_id: str,
    source_system: str,
    source_instance: str,
    schema_version: str,
    idempotency_key: str,
    record_count: int,
    window_started_at: datetime | None = None,
    window_ended_at: datetime | None = None,
) -> str:
    """Durably enqueue a pointer to an already-persisted batch.

    Returns the stream key written to. Raises ``StreamUnavailableError`` if
    the payload row is not yet durable (see module docstring), Redis/Valkey
    is unavailable, or the write fails -- CALLERS MUST map this to HTTP 503,
    never accept-and-warn.
    """
    await _require_payload_durable(org_id=org_id, ingestion_id=ingestion_id)

    client = get_redis_client()
    if client is None:
        raise StreamUnavailableError("Redis/Valkey unavailable")
    stream = stream_name(org_id)
    fields = {
        "ingestion_id": ingestion_id,
        "org_id": org_id,
        "source_system": source_system,
        "source_instance": source_instance,
        "schema_version": schema_version,
        "idempotency_key": idempotency_key,
        "record_count": str(record_count),
        "window_started_at": window_started_at.isoformat() if window_started_at else "",
        "window_ended_at": window_ended_at.isoformat() if window_ended_at else "",
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        client.xadd(stream, fields, maxlen=STREAM_MAXLEN, approximate=True)
    except Exception as exc:
        logger.exception("Failed to enqueue external-ingest batch %s", ingestion_id)
        raise StreamUnavailableError(str(exc)) from exc
    return stream


async def reenqueue_batch(
    *,
    org_id: str,
    ingestion_id: str,
    source_system: str,
    source_instance: str,
    schema_version: str,
    idempotency_key: str,
    record_count: int,
    window_started_at: datetime | None = None,
    window_ended_at: datetime | None = None,
) -> str:
    """Re-drive a batch whose Postgres row exists but has no live consumer.

    Thin wrapper around :func:`enqueue_batch` (brief Design Decision D2 /
    Gap G3): a batch can reach Postgres (``status='accepted'``) while its
    stream ``XADD`` fails or is trimmed before a consumer ever reads it.
    Exposed here as the seam a future reconciler (CHAOS-2769, and
    CHAOS-2695's wave-4 RETRY-idempotency-outcome path -- same
    ``ingestion_id`` reused) calls; this issue does not itself schedule any
    re-enqueue. The payload row is expected to already exist (that's the
    premise of "re-drive"); :func:`enqueue_batch`'s fail-closed check still
    applies.
    """
    return await enqueue_batch(
        org_id=org_id,
        ingestion_id=ingestion_id,
        source_system=source_system,
        source_instance=source_instance,
        schema_version=schema_version,
        idempotency_key=idempotency_key,
        record_count=record_count,
        window_started_at=window_started_at,
        window_ended_at=window_ended_at,
    )


__all__ = [
    "CONSUMER_GROUP",
    "STREAM_MAXLEN",
    "StreamUnavailableError",
    "stream_name",
    "dlq_name",
    "get_redis_client",
    "enqueue_batch",
    "reenqueue_batch",
]
