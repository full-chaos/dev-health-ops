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
from datetime import UTC, datetime

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import ValidationError

from dev_health_ops.api.ingest.streams import get_redis_client
from dev_health_ops.api.middleware.rate_limit import limiter
from dev_health_ops.workers.system_webhooks import process_pagerduty_webhook_event

from .pagerduty_models import (
    PagerDutyV3Webhook,
    PagerDutyWebhookConfiguration,
    PagerDutyWebhookResponse,
)

logger = logging.getLogger(__name__)

MAX_WEBHOOK_BODY_BYTES = 1_048_576
REPLAY_TTL_SECONDS = 86_400
STREAM_MAXLEN = 100_000
router = APIRouter(prefix="/pagerduty")


def _configuration() -> tuple[str | None, str | None, str | None]:
    return (
        os.getenv("PAGERDUTY_WEBHOOK_SECRET"),
        os.getenv("PAGERDUTY_WEBHOOK_ORG_ID"),
        os.getenv("PAGERDUTY_WEBHOOK_PROVIDER_INSTANCE_ID"),
    )


def _verify_signature(body: bytes, header: str | None, secret: str) -> bool:
    if header is None or not header.startswith("v1="):
        return False
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"v1={digest}", header)


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


def _replay_key(org_id: str, provider_instance_id: str, event_id: str) -> str:
    return f"pagerduty-webhook:{org_id}:{provider_instance_id}:{event_id}"


def _claim_event(org_id: str, provider_instance_id: str, event_id: str) -> bool:
    client = get_redis_client()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook queue unavailable",
        )
    try:
        return bool(
            client.set(
                _replay_key(org_id, provider_instance_id, event_id),
                "received",
                nx=True,
                ex=REPLAY_TTL_SECONDS,
            )
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook queue unavailable",
        ) from exc


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
    try:
        return str(
            client.xadd(
                _stream_name(org_id, provider_instance_id),
                fields,
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook queue unavailable",
        ) from exc


def _release_claim(org_id: str, provider_instance_id: str, event_id: str) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        client.delete(_replay_key(org_id, provider_instance_id, event_id))
    except Exception:
        logger.warning(
            "pagerduty_webhook.audit claim_release_failed event_id=%s", event_id
        )


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
    body = await request.body()
    if len(body) > MAX_WEBHOOK_BODY_BYTES:
        logger.warning("pagerduty_webhook.audit rejected reason=oversized")
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail="Payload too large",
        )
    if not _verify_signature(body, signature, secret):
        logger.warning("pagerduty_webhook.audit rejected reason=invalid_signature")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature"
        )
    webhook = _parse_webhook(body)
    return webhook, org_id, provider_instance_id


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
    if not _claim_event(org_id, provider_instance_id, webhook.event.id):
        logger.info(
            "pagerduty_webhook.audit accepted duplicate event_id=%s", webhook.event.id
        )
        return PagerDutyWebhookResponse(
            status="accepted",
            event_id=webhook.event.id,
            message="Duplicate event accepted",
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
            webhook=webhook.model_dump(mode="json"),
            org_id=org_id,
            provider_instance_id=provider_instance_id,
            received_at=received_at.isoformat(),
            stream_entry_id=stream_entry_id,
        )
    except HTTPException:
        _release_claim(org_id, provider_instance_id, webhook.event.id)
        raise
    except Exception as exc:
        _release_claim(org_id, provider_instance_id, webhook.event.id)
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
