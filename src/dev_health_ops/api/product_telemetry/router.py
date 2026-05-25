from __future__ import annotations

import logging
from uuid import uuid4

from fastapi import APIRouter, status

from .schemas import ProductTelemetryAccepted, ProductTelemetryBatch
from .streams import write_product_telemetry_batch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/product-telemetry", tags=["product-telemetry"])


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
    except Exception as exc:  # noqa: BLE001 - local/dev accept-and-warn path
        logger.warning("Product telemetry stream unavailable", exc_info=exc)
        stream = "disabled"

    return ProductTelemetryAccepted(
        ingestion_id=ingestion_id,
        items_received=len(batch.events),
        stream=stream,
    )
