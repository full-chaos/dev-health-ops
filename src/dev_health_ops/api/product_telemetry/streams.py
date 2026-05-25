from __future__ import annotations

import json
import logging
import os

from .schemas import ProductTelemetryBatch

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "product-telemetry-consumers"
DLQ_STREAM = "product-telemetry:dlq"


def product_telemetry_stream_name(org_id_hash: str | None) -> str:
    org_key = org_id_hash or "anonymous"
    return f"product-telemetry:{org_key}:events"


def get_redis_client():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import valkey as redis

        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Redis unavailable for product telemetry streams")
        return None


async def write_product_telemetry_batch(
    batch: ProductTelemetryBatch, ingestion_id: str
) -> str:
    stream = product_telemetry_stream_name(batch.org_id_hash)
    redis_client = get_redis_client()
    if not redis_client:
        raise ConnectionError("Redis unavailable for product telemetry streams")
    redis_client.xadd(
        stream,
        {
            "ingestion_id": ingestion_id,
            "source": batch.source,
            "org_id_hash": batch.org_id_hash or "",
            "events": json.dumps(
                [event.model_dump(mode="json", by_alias=True) for event in batch.events]
            ),
        },
        maxlen=100000,
        approximate=True,
    )
    return stream
