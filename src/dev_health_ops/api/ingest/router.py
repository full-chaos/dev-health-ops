from __future__ import annotations

import logging
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
from .streams import get_redis_client, stream_name, write_to_stream

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])


@router.post("/commits", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_commits(
    payload: IngestCommitsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    sname = stream_name(payload.org_id, "commits")

    rc = get_redis_client()
    written = write_to_stream(
        rc,
        sname,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=sname,
    )


@router.post("/pull-requests", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_pull_requests(
    payload: IngestPullRequestsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    sname = stream_name(payload.org_id, "pull-requests")

    rc = get_redis_client()
    written = write_to_stream(
        rc,
        sname,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=sname,
    )


@router.post("/work-items", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_work_items(
    payload: IngestWorkItemsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    sname = stream_name(payload.org_id, "work-items")

    rc = get_redis_client()
    written = write_to_stream(
        rc,
        sname,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=sname,
    )


@router.post("/deployments", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_deployments(
    payload: IngestDeploymentsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    sname = stream_name(payload.org_id, "deployments")

    rc = get_redis_client()
    written = write_to_stream(
        rc,
        sname,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=sname,
    )


@router.post("/incidents", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_incidents(
    payload: IngestIncidentsRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    ingestion_id = str(uuid.uuid4())
    sname = stream_name(payload.org_id, "incidents")

    rc = get_redis_client()
    written = write_to_stream(
        rc,
        sname,
        {"ingestion_id": ingestion_id, "payload": payload.model_dump_json()},
    )
    if not written:
        logger.warning(
            "Ingest %s: Redis unavailable, payload not streamed", ingestion_id
        )

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=sname,
    )
