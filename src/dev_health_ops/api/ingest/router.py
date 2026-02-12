from __future__ import annotations

import logging
import os
import uuid

from fastapi import APIRouter

from .auth import IngestAuthContext, IngestIdempotencyKey
from .schemas import (
    IngestAcceptedResponse,
    IngestCommitsRequest,
    IngestDeploymentsRequest,
    IngestIncidentsRequest,
    IngestPullRequestsRequest,
    IngestWorkItemsRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])


def _get_redis():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis

        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Redis unavailable for ingest streams")
        return None


def _write_to_stream(redis_client, stream_name: str, data: dict) -> bool:
    if not redis_client:
        return False
    try:
        redis_client.xadd(stream_name, data, maxlen=100000, approximate=True)
        return True
    except Exception:
        logger.exception("Failed to write to stream %s", stream_name)
        return False


@router.post("/commits", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_commits(
    payload: IngestCommitsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    stream_name = f"ingest:{payload.org_id}:commits"

    rc = _get_redis()
    written = _write_to_stream(
        rc,
        stream_name,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=stream_name,
    )


@router.post("/pull-requests", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_pull_requests(
    payload: IngestPullRequestsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    stream_name = f"ingest:{payload.org_id}:pull-requests"

    rc = _get_redis()
    written = _write_to_stream(
        rc,
        stream_name,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=stream_name,
    )


@router.post("/work-items", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_work_items(
    payload: IngestWorkItemsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    stream_name = f"ingest:{payload.org_id}:work-items"

    rc = _get_redis()
    written = _write_to_stream(
        rc,
        stream_name,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=stream_name,
    )


@router.post("/deployments", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_deployments(
    payload: IngestDeploymentsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    stream_name = f"ingest:{payload.org_id}:deployments"

    rc = _get_redis()
    written = _write_to_stream(
        rc,
        stream_name,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=stream_name,
    )


@router.post("/incidents", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_incidents(
    payload: IngestIncidentsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    stream_name = f"ingest:{payload.org_id}:incidents"

    rc = _get_redis()
    written = _write_to_stream(
        rc,
        stream_name,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=stream_name,
    )
