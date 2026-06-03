from __future__ import annotations

import asyncio
import json
import logging
import uuid  # noqa: F401  (retained for backward compatibility)
from typing import Any

from pydantic import ValidationError

from .._stream_consumer import StreamConsumer
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


def _events_from_entry(data: dict[str, str]) -> list[ProductTelemetryEvent]:
    raw_events = json.loads(data.get("events", "[]"))
    return [ProductTelemetryEvent.model_validate(raw_event) for raw_event in raw_events]


class ProductTelemetryStreamConsumer(StreamConsumer):
    """Drains ``product-telemetry:<org>:events`` streams into ClickHouse.

    Uses the base per-entry handler: each entry carries a JSON ``events`` array
    that is validated, sanitized, and persisted; poison entries are routed to
    the DLQ and ACKed so the group does not stall.
    """

    consumer_group = CONSUMER_GROUP
    dlq_stream = DLQ_STREAM
    consumer_name_prefix = "product-telemetry"
    reject_exceptions = (
        json.JSONDecodeError,
        ValidationError,
        ProductTelemetryPayloadError,
    )

    def __init__(self, stream_patterns: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._stream_patterns = stream_patterns

    def stream_patterns(self) -> list[str]:
        if self._stream_patterns is not None:
            return self._stream_patterns
        return ["product-telemetry:*:events"]

    def ensure_group(self, rc, stream_key: str) -> None:
        _ensure_group(rc, stream_key)

    def process_entry(
        self, stream_key: str, entry_id: str, data: dict[str, str]
    ) -> int:
        events = _events_from_entry(data)
        source = data.get("source", "dev-health-web")
        return asyncio.run(persist_product_telemetry_events(events, source))


def consume_product_telemetry_streams(
    stream_patterns: list[str] | None = None,
    max_iterations: int | None = None,
    consumer_name: str | None = None,
) -> int:
    consumer: Any = ProductTelemetryStreamConsumer(
        stream_patterns=stream_patterns,
        consumer_name=consumer_name,
        block_ms=BLOCK_MS,
        batch_size=BATCH_SIZE,
    )
    return consumer.consume(max_iterations=max_iterations)
