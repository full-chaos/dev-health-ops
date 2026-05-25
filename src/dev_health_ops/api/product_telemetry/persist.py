from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.metrics.sinks.factory import create_sink

from .schemas import ProductTelemetryEvent

BLOCKED_PAYLOAD_KEYS = {
    "email",
    "name",
    "userId",
    "orgId",
    "url",
    "query",
    "search",
    "stack",
    "message",
    "title",
    "body",
}

PRODUCT_TELEMETRY_COLUMNS = [
    "org_id_hash",
    "event_id",
    "name",
    "schema_version",
    "session_id",
    "anonymous_user_id",
    "route_pattern",
    "payload_json",
    "occurred_at",
    "ingested_at",
    "source",
]


class ProductTelemetryPayloadError(ValueError):
    pass


def _payload_json(payload: dict[str, str | int | float | bool | None]) -> str:
    blocked = BLOCKED_PAYLOAD_KEYS.intersection(payload)
    if blocked:
        blocked_list = ", ".join(sorted(blocked))
        raise ProductTelemetryPayloadError(
            f"Blocked product telemetry payload keys: {blocked_list}"
        )
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _to_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


async def persist_product_telemetry_events(
    events: list[ProductTelemetryEvent], source: str
) -> int:
    if not events:
        return 0
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[list[Any]] = []
    for event in events:
        rows.append(
            [
                event.org_id_hash,
                event.event_id,
                event.name,
                event.schema_version,
                event.session_id,
                event.anonymous_user_id,
                event.route_pattern,
                _payload_json(event.payload),
                _to_utc_naive(event.ts),
                ingested_at,
                source,
            ]
        )

    sink = create_sink()
    try:
        client = cast(Any, sink).client
        await asyncio.to_thread(
            client.insert,
            "product_telemetry_events",
            rows,
            column_names=PRODUCT_TELEMETRY_COLUMNS,
        )
    finally:
        sink.close()
    return len(rows)
