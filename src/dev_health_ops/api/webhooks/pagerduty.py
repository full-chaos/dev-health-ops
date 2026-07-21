"""PagerDuty V3 webhook receiver and durable hand-off.

Subscriptions may be scoped to a PagerDuty account, a team, or selected
services. Subscription management remains manual in PagerDuty V1; this
receiver maps one configured subscription to one canonical provider instance.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.api.ingest.streams import get_redis_client
from dev_health_ops.api.middleware.rate_limit import limiter
from dev_health_ops.db import get_postgres_session
from dev_health_ops.licensing import is_org_feature_enabled_async
from dev_health_ops.licensing.registry import CANONICAL_INCIDENT_INGESTION_FEATURE
from dev_health_ops.workers.system_webhooks import process_pagerduty_webhook_event

from .pagerduty_models import (
    PagerDutyV3Webhook,
    PagerDutyWebhookConfiguration,
    PagerDutyWebhookResponse,
)

logger = logging.getLogger(__name__)

MAX_WEBHOOK_BODY_BYTES = 1_048_576
MAX_SIGNATURE_CANDIDATES = 8
router = APIRouter(prefix="/pagerduty")

_FEATURE_DISABLED_DETAIL = (
    "Canonical incident ingestion is not enabled for this organization"
)


async def _canonical_incident_ingestion_allowed(org_id: str) -> bool:
    try:
        parsed_org_id = uuid.UUID(org_id)
    except ValueError:
        return False
    try:
        async with get_postgres_session() as session:
            return await is_org_feature_enabled_async(
                session,
                parsed_org_id,
                CANONICAL_INCIDENT_INGESTION_FEATURE,
            )
    except SQLAlchemyError:
        return False


def _configuration() -> tuple[str | None, str | None, str | None]:
    return (
        os.getenv("PAGERDUTY_WEBHOOK_SECRET"),
        os.getenv("PAGERDUTY_WEBHOOK_ORG_ID"),
        os.getenv("PAGERDUTY_WEBHOOK_PROVIDER_INSTANCE_ID"),
    )


def _verify_signature(body: bytes, header: str | None, secret: str) -> bool:
    if header is None:
        return False
    header_candidates = header.split(",")
    if len(header_candidates) > MAX_SIGNATURE_CANDIDATES:
        return False

    candidates: list[str] = []
    for header_candidate in header_candidates:
        candidate = header_candidate.strip().removeprefix("v1=")
        if (
            header_candidate.strip().startswith("v1=")
            and len(candidate) == 64
            and all(character in "0123456789abcdefABCDEF" for character in candidate)
        ):
            candidates.append(candidate.lower())
    if not candidates:
        return False

    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    valid = False
    for candidate in candidates:
        matches = hmac.compare_digest(expected, candidate)
        valid = matches or valid
    return valid


def _parse_webhook(body: bytes) -> PagerDutyV3Webhook:
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Malformed JSON"
        ) from exc
    try:
        return PagerDutyV3Webhook.model_validate(decoded)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid PagerDuty V3 event"
        ) from exc


def _stream_name(org_id: str, provider_instance_id: str) -> str:
    return f"pagerduty-webhooks:{org_id}:{provider_instance_id}"


def _enqueue_event(
    *,
    webhook: PagerDutyV3Webhook,
    org_id: str,
    provider_instance_id: str,
    received_at: datetime,
) -> str:
    client = get_redis_client()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook queue unavailable",
        )
    fields = {
        "event_id": webhook.event.id,
        "event_type": webhook.event.event_type.value,
        "occurred_at": webhook.event.occurred_at.astimezone(UTC).isoformat(),
        "received_at": received_at.isoformat(),
        "org_id": org_id,
        "provider_instance_id": provider_instance_id,
        "payload": webhook.model_dump_json(),
    }
    # Never cap this stream with MAXLEN: entries stay until a worker persists
    # them and xdel's the entry (or dead-letters it). A MAXLEN trim would drop
    # the OLDEST *unpersisted* events under backlog — silent data loss.

    try:
        return str(
            client.xadd(
                _stream_name(org_id, provider_instance_id),
                fields,
            )
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook queue unavailable",
        ) from exc


@router.get("/configuration", response_model=PagerDutyWebhookConfiguration)
def pagerduty_configuration() -> PagerDutyWebhookConfiguration:
    secret, org_id, provider_instance_id = _configuration()
    return PagerDutyWebhookConfiguration(
        configured=bool(secret and org_id and provider_instance_id),
        org_id=org_id,
        provider_instance_id=provider_instance_id,
    )


async def _validated_webhook(
    request: Request, signature: str | None
) -> tuple[PagerDutyV3Webhook, str, str]:
    secret, org_id, provider_instance_id = _configuration()
    if not secret or not org_id or not provider_instance_id:
        logger.warning("pagerduty_webhook.audit rejected reason=unconfigured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook unconfigured",
        )
    body = await _read_body_limited(request)
    if not _verify_signature(body, signature, secret):
        logger.warning("pagerduty_webhook.audit rejected reason=invalid_signature")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature"
        )
    webhook = _parse_webhook(body)
    return webhook, org_id, provider_instance_id


async def _read_body_limited(request: Request) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_size = int(content_length)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Content-Length",
            ) from exc
        if declared_size < 0 or declared_size > MAX_WEBHOOK_BODY_BYTES:
            logger.warning("pagerduty_webhook.audit rejected reason=oversized")
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="Payload too large",
            )

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_WEBHOOK_BODY_BYTES:
            logger.warning("pagerduty_webhook.audit rejected reason=oversized")
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="Payload too large",
            )
        body.extend(chunk)
    return bytes(body)


@router.post(
    "", response_model=PagerDutyWebhookResponse, status_code=status.HTTP_202_ACCEPTED
)
@limiter.limit("60/minute")
async def pagerduty_webhook(
    request: Request,
    x_pagerduty_signature: str | None = Header(default=None),
) -> PagerDutyWebhookResponse:
    webhook, org_id, provider_instance_id = await _validated_webhook(
        request, x_pagerduty_signature
    )
    if not await _canonical_incident_ingestion_allowed(org_id):
        logger.warning("pagerduty_webhook.audit rejected reason=feature_disabled")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=_FEATURE_DISABLED_DETAIL,
        )
    received_at = datetime.now(UTC)
    try:
        stream_entry_id = _enqueue_event(
            webhook=webhook,
            org_id=org_id,
            provider_instance_id=provider_instance_id,
            received_at=received_at,
        )
        getattr(process_pagerduty_webhook_event, "delay")(
            org_id=org_id,
            provider_instance_id=provider_instance_id,
            stream_entry_id=stream_entry_id,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook queue unavailable",
        ) from exc
    logger.info("pagerduty_webhook.audit accepted event_id=%s", webhook.event.id)
    return PagerDutyWebhookResponse(
        status="accepted", event_id=webhook.event.id, message="Event accepted"
    )


@router.post("/test-event", response_model=PagerDutyWebhookResponse)
@limiter.limit("60/minute")
async def validate_pagerduty_test_event(
    request: Request,
    x_pagerduty_signature: str | None = Header(default=None),
) -> PagerDutyWebhookResponse:
    webhook, _, _ = await _validated_webhook(request, x_pagerduty_signature)
    return PagerDutyWebhookResponse(
        status="validated", event_id=webhook.event.id, message="Event validated"
    )
