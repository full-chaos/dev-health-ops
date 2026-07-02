"""Interim durable-stream writer for external-ingest batches (CHAOS-2691 D6).

Real, not mocked: raises ``StreamUnavailableError`` on any failure so the
router can fail closed with a 503 — never accept-and-warn (contrast with
``api/ingest/streams.py``'s legacy accept-and-warn behavior, which the epic
explicitly rejects for the durability-focused external-ingest path).

CHAOS-2693 hardens this into the full DLQ/consumer-group system and must
preserve ``enqueue_batch()``'s signature and the stream-naming convention
below without updating ``router.py``'s call site out from under it.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class StreamUnavailableError(Exception):
    """Raised when the durable ingest stream cannot accept a write."""


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


def enqueue_batch(
    *,
    org_id: str,
    ingestion_id: str,
    source_system: str,
    source_instance: str,
    schema_version: str,
    idempotency_key: str,
    payload_json: str,
    record_count: int,
    window_started_at: datetime | None = None,
    window_ended_at: datetime | None = None,
) -> str:
    """Write one batch to the durable stream. Returns the stream key.

    Raises StreamUnavailableError if Redis/Valkey is unavailable or the
    write fails — CALLERS MUST map this to HTTP 503, never accept-and-warn.

    ``record_count``/``window_started_at``/``window_ended_at`` are pointer
    fields the CHAOS-2693 consumer/status-store need without deserializing
    ``payload`` (master-spec CC9). Wave-1 interim: ``payload`` is still
    XADD'd inline (nothing consumes it yet) — CHAOS-2693's PR drops that
    param once the Postgres-backed payload store lands, updating this call
    site as an approved, planned change.
    """
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
        "payload": payload_json,
    }
    try:
        client.xadd(stream, fields, maxlen=100000, approximate=True)
    except Exception as exc:
        raise StreamUnavailableError(str(exc)) from exc
    return stream


__all__ = [
    "StreamUnavailableError",
    "stream_name",
    "dlq_name",
    "get_redis_client",
    "enqueue_batch",
]
