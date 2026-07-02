"""External-ingest REST contract: 4 endpoints (CHAOS-2691).

``POST /batches`` only checks envelope shape + the record-kind allowlist
(400 on any unknown kind) — deep per-record validation happens eagerly in
``POST /validate`` and durably in the CHAOS-2697 worker, so a customer's
momentary schema drift on a handful of records doesn't drop an entire batch
(see docs/architecture/external-ingest-rest-contract.md). ``GET /schemas*``
(CHAOS-2692) is generated from the same ``schemas.py`` Pydantic models via
``schema_registry.py`` — a versioned bundle with ``$defs``, per-record-kind
``$ref``s + examples, and an ETag; this module never redeclares those
models. See docs/architecture/adr-005-external-ingest-schema-discovery.md.
"""

from __future__ import annotations

import logging
import os
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, Request, Response
from pydantic import ValidationError

from dev_health_ops.api.middleware.rate_limit import (
    INGEST_BATCH_LIMIT,
    INGEST_READ_LIMIT,
    INGEST_VALIDATE_LIMIT,
    get_forwarded_ip,
    get_ingest_token_key,
    limiter,
)
from dev_health_ops.external_ingest.validate import validate_records

from .auth import IngestAuthContext, require_ingest_scope
from .errors import ExternalIngestError
from .schema_registry import compute_etag, get_bundle, list_versions
from .schemas import (
    MAX_BODY_BYTES_DEFAULT,
    MAX_RECORDS_DEFAULT,
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
    BatchAcceptedResponse,
    BatchEnvelope,
    ValidationResponse,
)
from .streams import StreamUnavailableError, enqueue_batch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/external-ingest", tags=["external-ingest"])

# Bound once at import time (not called inline in each Depends(...)) so tests
# can target these exact objects via app.dependency_overrides — overriding
# the require_ingest_scope factory itself does not intercept already-bound
# closures, since FastAPI matches overrides by the specific callable passed
# to Depends().
_require_schema_read = require_ingest_scope("schema:read")
_require_ingest_write = require_ingest_scope("ingest:write")


def _max_records() -> int:
    return int(os.environ.get("EXTERNAL_INGEST_MAX_RECORDS", str(MAX_RECORDS_DEFAULT)))


def _max_body_bytes() -> int:
    return int(
        os.environ.get("EXTERNAL_INGEST_MAX_BODY_BYTES", str(MAX_BODY_BYTES_DEFAULT))
    )


def _limits_payload() -> dict[str, int]:
    return {"maxRecordsPerBatch": _max_records(), "maxBodyBytes": _max_body_bytes()}


async def _read_body_enforcing_size_limit(request: Request) -> bytes:
    """Read the raw request body, enforcing EXTERNAL_INGEST_MAX_BODY_BYTES.

    Checks Content-Length first (reject fast) and falls back to counting
    streamed bytes when Content-Length is absent/chunked (brief D4). Reading
    raw bytes ourselves (rather than a typed Pydantic body param) is also
    what lets ``_parse_envelope_or_400`` map malformed JSON to 400 instead of
    FastAPI's app-wide RequestValidationError -> 422 convention (brief D2).
    """
    max_bytes = _max_body_bytes()
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                raise ExternalIngestError(
                    413, "payload_too_large", f"Request body exceeds {max_bytes} bytes"
                )
        except ValueError:
            pass  # malformed Content-Length header: fall through to streamed count

    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > max_bytes:
            raise ExternalIngestError(
                413, "payload_too_large", f"Request body exceeds {max_bytes} bytes"
            )
    return bytes(body)


def _parse_envelope_or_400(raw: bytes) -> BatchEnvelope:
    try:
        return BatchEnvelope.model_validate_json(raw)
    except ValidationError as exc:
        raise ExternalIngestError(
            400,
            "invalid_envelope",
            "Malformed batch envelope",
            errors=[dict(e) for e in exc.errors()],
        ) from exc


def _check_idempotency_header_matches_body(
    envelope: BatchEnvelope, header_value: str | None
) -> None:
    # Body idempotencyKey is canonical (brief D1); the header is an optional
    # Stripe-style alias that must agree, kept for generic cURL/CI ergonomics.
    if header_value is not None and header_value != envelope.idempotency_key:
        raise ExternalIngestError(
            400,
            "idempotency_key_mismatch",
            "Idempotency-Key header does not match body idempotencyKey",
        )


def _check_schema_version_or_400(envelope: BatchEnvelope) -> None:
    if envelope.schema_version != SCHEMA_VERSION:
        raise ExternalIngestError(
            400,
            "unsupported_schema_version",
            f"Unsupported schemaVersion: {envelope.schema_version!r}",
        )


def _check_batch_size_or_400(envelope: BatchEnvelope) -> None:
    max_records = _max_records()
    if len(envelope.records) > max_records:
        raise ExternalIngestError(
            400,
            "batch_too_large",
            f"Batch has {len(envelope.records)} records; max is {max_records}",
        )


def _check_all_kinds_known_or_400(envelope: BatchEnvelope) -> None:
    # /batches rejects the entire batch on any unsupported kind (no partial
    # acceptance in v1); /validate instead reports per-record errors via
    # validate_records (dev_health_ops.external_ingest.validate).
    for index, record in enumerate(envelope.records):
        if record.kind not in RECORD_KIND_MODELS:
            raise ExternalIngestError(
                400,
                "unknown_record_kind",
                f"Unknown record kind at index {index}: {record.kind!r}",
            )


# Force schema generation (models_json_schema + example loading) to run at
# import time, not lazily on a customer's first request — a bad example
# fixture or a models_json_schema regression should fail app startup, not a
# random GET (schema_registry D6).
get_bundle(SCHEMA_VERSION)


def _schema_discovery_rate_limit_key(request: Request) -> str:
    """IP-only rate-limit key for the public GET /schemas* routes.

    Deliberately does NOT reuse ``get_ingest_token_key``: that function
    hashes any ``Authorization: Bearer <value>`` header into a distinct
    limiter bucket, which is correct once a real bearer token is validated
    (POST /batches, POST /validate) but wrong here — these two routes never
    validate the bearer value at all (D2, public discovery), so a caller
    could rotate an arbitrary string per request and get a fresh bucket
    every time, defeating INGEST_READ_LIMIT (adversarial-review finding).
    The forwarded IP is the only real identity available for an
    unauthenticated route.
    """
    return f"ingest-ip:{get_forwarded_ip(request)}"


@router.get("/schemas")
@limiter.limit(INGEST_READ_LIMIT, key_func=_schema_discovery_rate_limit_key)
async def list_schemas(request: Request) -> dict[str, object]:
    return {
        "schemaVersions": [version["schemaVersion"] for version in list_versions()],
        "recordKinds": sorted(RECORD_KIND_MODELS),
        "limits": _limits_payload(),
    }


@router.get("/schemas/{schema_version}")
@limiter.limit(INGEST_READ_LIMIT, key_func=_schema_discovery_rate_limit_key)
async def get_schema(
    schema_version: str, request: Request, response: Response
) -> dict[str, object]:
    bundle = get_bundle(schema_version)
    if bundle is None:
        raise ExternalIngestError(
            404,
            "unsupported_schema_version",
            f"Unknown schema version: {schema_version!r}",
        )

    # ETag over the full served representation (schema + live limits), not
    # just bundle.etag's schema-only hash — otherwise a limits change could
    # be masked behind a 304 for a client holding the old ETag
    # (adversarial-review finding; see schema_registry.compute_etag).
    body = {**bundle.document, "limits": _limits_payload()}
    etag = compute_etag(body)

    if request.headers.get("if-none-match") == etag:
        response.status_code = 304
        response.headers["ETag"] = etag
        return {}

    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "public, max-age=3600, must-revalidate"
    return body


@router.post("/validate", response_model=ValidationResponse)
@limiter.limit(INGEST_VALIDATE_LIMIT, key_func=get_ingest_token_key)
async def validate_batch(
    request: Request,
    ctx: IngestAuthContext = Depends(_require_schema_read),
) -> ValidationResponse:
    raw = await _read_body_enforcing_size_limit(request)
    envelope = _parse_envelope_or_400(raw)
    _check_schema_version_or_400(envelope)
    _check_batch_size_or_400(envelope)
    errors = validate_records(envelope.records)
    rejected_indices = {item.index for item in errors}
    return ValidationResponse(
        valid=not errors,
        items_accepted=len(envelope.records) - len(rejected_indices),
        items_rejected=len(rejected_indices),
        errors=errors,
    )


@router.post("/batches", response_model=BatchAcceptedResponse, status_code=202)
@limiter.limit(INGEST_BATCH_LIMIT, key_func=get_ingest_token_key)
async def accept_batch(
    request: Request,
    ctx: IngestAuthContext = Depends(_require_ingest_write),
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
) -> BatchAcceptedResponse:
    raw = await _read_body_enforcing_size_limit(request)
    envelope = _parse_envelope_or_400(raw)
    _check_idempotency_header_matches_body(envelope, idempotency_key_header)
    _check_schema_version_or_400(envelope)
    _check_all_kinds_known_or_400(envelope)
    _check_batch_size_or_400(envelope)

    ingestion_id = str(uuid4())
    window = envelope.window
    try:
        stream = enqueue_batch(
            org_id=ctx.org_id,
            ingestion_id=ingestion_id,
            source_system=envelope.source.system,
            source_instance=envelope.source.instance,
            schema_version=envelope.schema_version,
            idempotency_key=envelope.idempotency_key,
            payload_json=raw.decode("utf-8"),
            record_count=len(envelope.records),
            window_started_at=window.started_at if window else None,
            window_ended_at=window.ended_at if window else None,
        )
    except StreamUnavailableError as exc:
        raise ExternalIngestError(
            503, "stream_unavailable", "Ingest stream unavailable"
        ) from exc

    return BatchAcceptedResponse(
        ingestion_id=ingestion_id,
        items_received=len(envelope.records),
        stream=stream,
    )


__all__ = ["router"]
