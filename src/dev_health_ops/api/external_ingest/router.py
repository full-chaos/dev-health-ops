"""External-ingest REST contract: 4 endpoints (CHAOS-2691 + CHAOS-2695).

``POST /batches`` checks envelope shape + the record-kind allowlist (400 on
any unknown kind), then runs the full CC22 accept sequence — token source
binding, one-active-owner check, NEW/REPLAY/CONFLICT/RETRY idempotency
(``idempotency.py``/``external_ingest/ownership.py``, CHAOS-2695) — before
the durable payload write + pointer enqueue. Deep per-record validation
happens eagerly in ``POST /validate`` and durably in the CHAOS-2697 worker,
so a customer's momentary schema drift on a handful of records doesn't drop
an entire batch (see docs/architecture/external-ingest-rest-contract.md and
docs/architecture/external-ingest-idempotency-ownership.md). ``GET /schemas*``
(CHAOS-2692) is generated from the same ``schemas.py`` Pydantic models via
``schema_registry.py`` — a versioned bundle with ``$defs``, per-record-kind
``$ref``s + examples, and an ETag; this module never redeclares those
models. See docs/architecture/adr-005-external-ingest-schema-discovery.md.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.middleware.rate_limit import (
    INGEST_BATCH_LIMIT,
    INGEST_READ_LIMIT,
    INGEST_VALIDATE_LIMIT,
    get_forwarded_ip,
    get_ingest_token_key,
    limiter,
)
from dev_health_ops.external_ingest.ownership import (
    EffectiveMode,
    resolve_effective_mode,
)
from dev_health_ops.external_ingest.payload_store import upsert_payload
from dev_health_ops.external_ingest.recompute_status import get_recompute_jobs
from dev_health_ops.external_ingest.validate import validate_records

from .auth import IngestAuthContext, require_ingest_scope, require_matching_source
from .errors import ExternalIngestError
from .idempotency import (
    IdempotencyOutcomeKind,
    IngestTemporarilyUnavailableError,
    compute_payload_hash,
    resolve_batch_idempotency,
)
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
from .status import (
    BatchRow,
    _batch_to_status_response,
    get_batch,
    list_rejections,
    mark_stream_unavailable,
    reset_for_retry,
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


def _ownership_error(
    mode: EffectiveMode, system: str, instance: str
) -> ExternalIngestError:
    """Map a non-customer_push effective mode to its brief §7 error."""
    if mode == "unclaimed":
        return ExternalIngestError(
            403,
            "source_not_registered",
            f"No ingest source is registered for system='{system}' "
            f"instance='{instance}' in this organization. Register it under "
            "/org/admin/integrations before pushing.",
        )
    if mode == "disabled":
        return ExternalIngestError(
            403,
            "source_disabled",
            f"Ingest source '{system}:{instance}' is disabled for this organization.",
        )
    return ExternalIngestError(
        403,
        "source_owned_by_fullchaos_sync",
        f"Source '{system}:{instance}' is currently managed by "
        "FullChaos-hosted sync. Disable managed sync for this source before "
        "pushing customer data, or contact support.",
    )


async def _replay_status_response(
    session: AsyncSession, org_id: str, batch: BatchRow
) -> JSONResponse:
    """REPLAY -> 200 OK with the FULL current-status envelope (brief decision
    8): the replayed batch may already be completed/partial, and the narrow
    202-accepted shape would misreport it. Same body as GET /batches/{id}
    (first error page, default limits) so ``dev-hops push batch --poll`` can
    short-circuit in one round trip."""
    errors, errors_total = await list_rejections(
        session, org_id=org_id, ingestion_id=batch.ingestion_id, limit=50, offset=0
    )
    recompute_jobs = await get_recompute_jobs(
        session,
        org_id=org_id,
        source_system=batch.source_system,
        source_instance=batch.source_instance,
        dispatched_at=batch.recompute_dispatched_at,
    )
    body = _batch_to_status_response(batch, errors, errors_total, 50, 0, recompute_jobs)
    return JSONResponse(
        status_code=200, content=json.loads(body.model_dump_json(by_alias=True))
    )


@router.post("/batches", response_model=BatchAcceptedResponse, status_code=202)
@limiter.limit(INGEST_BATCH_LIMIT, key_func=get_ingest_token_key)
async def accept_batch(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    ctx: IngestAuthContext = Depends(_require_ingest_write),
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
) -> Response:
    """Full CC22 accept sequence (CHAOS-2695): envelope checks -> token
    source binding -> ownership -> idempotency (FIRST Postgres write) ->
    payload upsert -> COMMIT -> pointer enqueue. See
    docs/architecture/external-ingest-idempotency-ownership.md."""
    raw = await _read_body_enforcing_size_limit(request)
    envelope = _parse_envelope_or_400(raw)
    _check_idempotency_header_matches_body(envelope, idempotency_key_header)
    _check_schema_version_or_400(envelope)
    _check_all_kinds_known_or_400(envelope)
    _check_batch_size_or_400(envelope)
    # Adversarial-review fix: require_ingest_scope resolves before the body
    # is parsed, so it can't check payload source vs. token-bound source
    # itself -- a source-bound ingest:write token must not be able to push
    # data for a different source instance in the same org (CC16
    # source_mismatch / source_disabled).
    require_matching_source(ctx, envelope.source.system, envelope.source.instance)

    # One-active-owner re-check at accept time (CC5/CC14 defense in depth):
    # require_matching_source only proves the token binds to a registered,
    # write-eligible source row -- it cannot see a managed sync source that
    # was connected to the SAME instance AFTER registration (nothing on the
    # api/admin/routers/sync.py side knows about external_ingest_sources).
    mode = await resolve_effective_mode(
        session,
        org_id=ctx.org_id,
        system=envelope.source.system,
        instance=envelope.source.instance,
    )
    if mode != "customer_push":
        raise _ownership_error(mode, envelope.source.system, envelope.source.instance)

    window = envelope.window
    payload_hash = compute_payload_hash(envelope)
    try:
        # FIRST Postgres write of the accept sequence (CC22) -- the unique
        # idempotency index is the serialization point for concurrent
        # same-key accepts.
        outcome = await resolve_batch_idempotency(
            session,
            org_id=ctx.org_id,
            source_system=envelope.source.system,
            source_instance=envelope.source.instance,
            idempotency_key=envelope.idempotency_key,
            payload_hash=payload_hash,
            schema_version=envelope.schema_version,
            producer=envelope.source.producer,
            producer_version=envelope.source.producer_version,
            window_started_at=window.started_at if window else None,
            window_ended_at=window.ended_at if window else None,
            items_received=len(envelope.records),
        )
    except IngestTemporarilyUnavailableError as exc:
        raise ExternalIngestError(
            503,
            "ingest_temporarily_unavailable",
            "A concurrent request for the same idempotency key is in progress. Retry.",
        ) from exc

    if outcome.kind is IdempotencyOutcomeKind.CONFLICT:
        raise ExternalIngestError(
            409,
            "idempotency_conflict",
            f"Idempotency key '{envelope.idempotency_key}' was already used "
            f"for source '{envelope.source.system}:{envelope.source.instance}' "
            "with a different payload. Use a new idempotencyKey, or retry "
            "with the exact original payload to get the cached status.",
        )
    if outcome.kind is IdempotencyOutcomeKind.REPLAY:
        return await _replay_status_response(session, ctx.org_id, outcome.batch)

    batch = outcome.batch
    ingestion_id = str(batch.ingestion_id)

    if outcome.kind is IdempotencyOutcomeKind.RETRY:
        # Re-accept the SAME ingestion_id (attempts += 1, prior attempt's
        # outcome fields cleared). CAS on the status we classified against:
        # losing it means a concurrent retry won or a live worker finished a
        # stale-looking batch first -- either way the fresh row is the truth,
        # answer as a REPLAY instead of double-accepting.
        won = await reset_for_retry(
            session,
            org_id=ctx.org_id,
            ingestion_id=batch.ingestion_id,
            from_status=batch.status,
        )
        if not won:
            fresh = await get_batch(
                session, org_id=ctx.org_id, ingestion_id=batch.ingestion_id
            )
            assert fresh is not None  # the row existed; transitions never delete
            return await _replay_status_response(session, ctx.org_id, fresh)

    # Persist the raw payload in the SAME transaction as the status row
    # (D2/CC9/CC22): the stream entry never carries payload bytes, so a
    # worker fetching by ingestion_id must always find a durable row. The
    # commit lands BEFORE enqueue_batch()'s own fail-closed
    # payload-durability check (streams.py), which opens an independent
    # read. On RETRY this refreshes the (hash-identical) bytes -- the
    # ``failed`` case may have had its payload already deleted by the
    # worker's terminal cleanup (D7).
    await upsert_payload(
        session,
        ingestion_id=ingestion_id,
        org_id=ctx.org_id,
        schema_version=envelope.schema_version,
        payload_bytes=raw,
    )
    await session.commit()

    try:
        stream = await enqueue_batch(
            org_id=ctx.org_id,
            ingestion_id=ingestion_id,
            source_system=envelope.source.system,
            source_instance=envelope.source.instance,
            schema_version=envelope.schema_version,
            idempotency_key=envelope.idempotency_key,
            record_count=len(envelope.records),
            window_started_at=window.started_at if window else None,
            window_ended_at=window.ended_at if window else None,
        )
    except StreamUnavailableError as exc:
        # The batch row is durable, so record the failed enqueue on it and
        # commit BEFORE raising (mark_stream_unavailable's contract) -- the
        # client's same-key retry then resolves as RETRY against the same
        # ingestion_id. The payload row is deliberately KEPT (supersedes
        # CHAOS-2693's interim orphan-delete: it is now referenced by the
        # status row, the retry reuses it, and deleting it could black-hole
        # an enqueue whose XADD actually landed before the error surfaced);
        # never-retried leftovers are CHAOS-2769's reconciler's job.
        await mark_stream_unavailable(
            session, org_id=ctx.org_id, ingestion_id=batch.ingestion_id
        )
        await session.commit()
        raise ExternalIngestError(
            503,
            "stream_unavailable",
            "The durable ingest stream is temporarily unavailable. The batch "
            f"was recorded as '{ingestion_id}'; retry with the same "
            "idempotencyKey once available.",
        ) from exc

    return JSONResponse(
        status_code=202,
        content=BatchAcceptedResponse(
            ingestion_id=ingestion_id,
            items_received=len(envelope.records),
            stream=stream,
        ).model_dump(by_alias=True),
    )


__all__ = ["router"]
