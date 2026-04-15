from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter

from .auth import IngestAuthContext, IngestIdempotencyKey
from .schemas import (
    IngestAcceptedResponse,
    IngestCommitsRequest,
    IngestDeploymentsRequest,
    IngestIncidentsRequest,
    IngestPullRequestsRequest,
    IngestTelemetryRequest,
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


@router.post("/telemetry", status_code=202, response_model=IngestAcceptedResponse)
async def ingest_telemetry(
    payload: IngestTelemetryRequest,
    auth: IngestAuthContext,
    idempotency_key: IngestIdempotencyKey,
) -> IngestAcceptedResponse:
    from dev_health_ops.metrics.schemas import TelemetrySignalBucketRecord

    ingestion_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    records = [
        TelemetrySignalBucketRecord(
            signal_type=item.signal_type,
            signal_count=item.signal_count,
            session_count=item.session_count,
            unique_pseudonymous_count=item.unique_pseudonymous_count,
            endpoint_group=item.endpoint_group or None,
            environment=item.environment,
            repo_id=uuid.UUID(item.repo_id) if item.repo_id else None,
            release_ref=item.release_ref or None,
            bucket_start=item.bucket_start,
            bucket_end=item.bucket_end,
            ingested_at=now,
            is_sampled=item.is_sampled,
            schema_version=item.schema_version,
            dedupe_key=item.dedupe_key,
            org_id=payload.org_id,
        )
        for item in payload.items
    ]

    await _persist_telemetry(records)

    return IngestAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(payload.items),
        stream=f"ingest:{payload.org_id}:telemetry",
    )


async def _persist_telemetry(
    records: list,
) -> None:
    import os

    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    ch_url = os.getenv("CLICKHOUSE_URI") or os.getenv("DATABASE_URI") or ""
    if not ch_url:
        logger.warning("No ClickHouse URI configured, skipping telemetry persistence")
        return

    sink = ClickHouseMetricsSink(ch_url)
    try:
        sink.write_telemetry_signal_buckets(records)
    finally:
        sink.close()
