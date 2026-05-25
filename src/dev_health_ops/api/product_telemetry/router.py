from __future__ import annotations

import json
import logging
import os
from uuid import uuid4

from fastapi import APIRouter, status
from redis.asyncio import Redis

from .schemas import ProductTelemetryAccepted, ProductTelemetryBatch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/product-telemetry", tags=["product-telemetry"])


def product_telemetry_stream_name(org_id_hash: str | None) -> str:
    org_key = org_id_hash or "anonymous"
    return f"product-telemetry:{org_key}:events"


async def write_product_telemetry_batch(
    batch: ProductTelemetryBatch, ingestion_id: str
) -> str:
    stream = product_telemetry_stream_name(batch.org_id_hash)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.xadd(
            stream,
            {
                "ingestion_id": ingestion_id,
                "source": batch.source,
                "org_id_hash": batch.org_id_hash or "",
                "events": json.dumps(
                    [
                        event.model_dump(mode="json", by_alias=True)
                        for event in batch.events
                    ]
                ),
            },
        )
    finally:
        await client.aclose()
    return stream


@router.post(
    "/events",
    response_model=ProductTelemetryAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def accept_product_telemetry_events(
    batch: ProductTelemetryBatch,
) -> ProductTelemetryAccepted:
    ingestion_id = str(uuid4())
    try:
        stream = await write_product_telemetry_batch(batch, ingestion_id)
    except Exception as exc:  # noqa: BLE001 - ingestion must remain best-effort in local/dev
        logger.warning("Product telemetry stream unavailable", exc_info=exc)
        stream = "disabled"

    return ProductTelemetryAccepted(
        ingestion_id=ingestion_id,
        items_received=len(batch.events),
        stream=stream,
    )
