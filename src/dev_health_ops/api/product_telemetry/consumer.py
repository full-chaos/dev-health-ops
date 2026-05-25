from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, cast

from pydantic import ValidationError

from .persist import ProductTelemetryPayloadError, persist_product_telemetry_events
from .schemas import ProductTelemetryEvent
from .streams import CONSUMER_GROUP, DLQ_STREAM

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
BLOCK_MS = 5000


def _ensure_group(redis_client, stream_key: str) -> None:
    try:
        redis_client.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            return
        logger.exception(
            "failed to ensure consumer group",
            extra={"stream_key": stream_key, "consumer_group": CONSUMER_GROUP},
        )
        raise


def _move_to_dlq(redis_client, stream_key: str, entry_id: str, reason: str) -> None:
    try:
        redis_client.xadd(
            DLQ_STREAM,
            {
                "original_stream": stream_key,
                "entry_id": entry_id,
                "reason": reason,
                "moved_at": str(time.time()),
            },
        )
    except Exception:
        logger.exception("Failed to move product telemetry entry %s to DLQ", entry_id)


def _events_from_entry(data: dict[str, str]) -> list[ProductTelemetryEvent]:
    raw_events = json.loads(data.get("events", "[]"))
    return [ProductTelemetryEvent.model_validate(raw_event) for raw_event in raw_events]


def consume_product_telemetry_streams(
    stream_patterns: list[str] | None = None,
    max_iterations: int | None = None,
    consumer_name: str | None = None,
) -> int:
    from .streams import get_redis_client

    redis_client = cast(Any, get_redis_client())
    if not redis_client:
        logger.warning("Redis unavailable, product telemetry consumer cannot start")
        return 0

    if consumer_name is None:
        consumer_name = f"product-telemetry-{uuid.uuid4().hex[:8]}"
    if stream_patterns is None:
        stream_patterns = ["product-telemetry:*:events"]

    streams: dict[str, str] = {}
    for pattern in stream_patterns:
        if "*" in pattern:
            for key in redis_client.scan_iter(match=pattern, _type="stream"):
                streams[key] = ">"
        else:
            streams[pattern] = ">"

    if not streams:
        return 0

    for stream_key in streams:
        _ensure_group(redis_client, stream_key)

    total_processed = 0
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        results = redis_client.xreadgroup(
            CONSUMER_GROUP,
            consumer_name,
            streams=streams,
            count=BATCH_SIZE,
            block=BLOCK_MS,
        )
        if not results:
            continue

        for stream_key, entries in results:
            for entry_id, data in entries:
                try:
                    events = _events_from_entry(data)
                    source = data.get("source", "dev-health-web")
                    persisted = asyncio.run(
                        persist_product_telemetry_events(events, source)
                    )
                    total_processed += persisted
                except (
                    json.JSONDecodeError,
                    ValidationError,
                    ProductTelemetryPayloadError,
                ) as exc:
                    logger.warning(
                        "Rejecting product telemetry entry %s: %s", entry_id, exc
                    )
                    _move_to_dlq(redis_client, stream_key, entry_id, str(exc))
                except Exception as exc:
                    logger.exception(
                        "Failed to persist product telemetry entry %s", entry_id
                    )
                    _move_to_dlq(redis_client, stream_key, entry_id, str(exc))
                finally:
                    redis_client.xack(stream_key, CONSUMER_GROUP, entry_id)

    return total_processed
