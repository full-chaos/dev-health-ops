"""Binding-scoped PagerDuty V3 webhook receiver.

PagerDuty V3 webhooks carry no timestamp header. Authenticity is proven by
an HMAC-SHA256 signature over the raw request body (one or more `v1=<hex>`
candidates in `X-PagerDuty-Signature`, comma-separated during secret
rotation) plus a `X-Webhook-Subscription` header that must match the
persisted `provider_subscription_id` on the binding addressed by the route.
Replay protection retains claims for 30 days, is keyed by the canonical binding
UUID and a SHA-256 of the event ID, and falls back to the raw-body SHA-256 when
an event carries no usable ID.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, assert_never
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from kombu.exceptions import KombuError
from pydantic import ValidationError
from valkey.exceptions import ValkeyError

from dev_health_ops.api.ingest.streams import get_redis_client
from dev_health_ops.api.middleware.rate_limit import limiter
from dev_health_ops.workers.system_webhooks import process_pagerduty_webhook_event

from .pagerduty_models import PagerDutyEventType, PagerDutyV3Webhook

if TYPE_CHECKING:
    from dev_health_ops.providers.pagerduty.webhook_bindings import (
        ResolvedPagerDutyWebhookBinding,
    )

logger = logging.getLogger(__name__)

MAX_WEBHOOK_BODY_BYTES = 1_048_576
MAX_SIGNATURE_CANDIDATES = 8
REPLAY_RETENTION_SECONDS = 30 * 24 * 60 * 60
PENDING_REPLAY_CLAIM_TTL_SECONDS = 5 * 60
RECEIVER_STREAM_MAXLEN = 10_000
router = APIRouter(prefix="/pagerduty")

_FEATURE_DISABLED_DETAIL = (
    "Canonical incident ingestion is not enabled for this organization"
)
_QUEUE_UNAVAILABLE_DETAIL = "Webhook queue unavailable"
_INVALID_SIGNATURE_DETAIL = "Invalid signature"


class ReplayClaimOutcome(StrEnum):
    CLAIMED = "claimed"
    PENDING = "pending"
    REPLAYED = "replayed"


async def _canonical_incident_ingestion_allowed(org_id: str) -> bool:
    from sqlalchemy.exc import SQLAlchemyError

    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.licensing import is_org_feature_enabled_async
    from dev_health_ops.licensing.registry import CANONICAL_INCIDENT_INGESTION_FEATURE

    try:
        parsed_org_id = UUID(org_id)
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


def _parse_binding_id(binding_id: str) -> UUID:
    try:
        return UUID(binding_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Webhook binding not found"
        ) from exc


async def _load_receivable_binding(
    binding_id: UUID,
) -> ResolvedPagerDutyWebhookBinding:
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.providers.pagerduty.webhook_bindings import (
        PagerDutyWebhookBindingService,
    )

    async with get_postgres_session() as session:
        binding = await PagerDutyWebhookBindingService(session).load_receivable_by_id(
            binding_id
        )
    if binding is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Webhook binding not found"
        )
    return binding


async def _mark_candidate_ready_from_verified_ping(
    binding_id: UUID,
    org_id: UUID,
) -> None:
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.providers.pagerduty.webhook_bindings import (
        PagerDutyWebhookBindingService,
    )

    async with get_postgres_session() as session:
        marked_binding = await PagerDutyWebhookBindingService(
            session
        ).mark_candidate_ready_from_verified_ping(binding_id, org_id)
    if marked_binding is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Webhook binding not found"
        )


def _subscription_matches(
    resolved_binding: ResolvedPagerDutyWebhookBinding, header_value: str | None
) -> bool:
    if header_value is None:
        return False
    return header_value == resolved_binding.binding.provider_subscription_id


def _verify_signature(body: bytes, header: str | None, secret: str) -> bool:
    if header is None:
        return False
    header_candidates = header.split(",")
    if len(header_candidates) > MAX_SIGNATURE_CANDIDATES:
        return False

    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    valid = False
    for candidate in header_candidates:
        signature = candidate.strip().removeprefix("v1=")
        if (
            candidate.strip().startswith("v1=")
            and len(signature) == 64
            and all(character in "0123456789abcdefABCDEF" for character in signature)
        ):
            valid = hmac.compare_digest(expected, signature.lower()) or valid
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


async def _read_body_limited(request: Request) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_size = int(content_length)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-Length"
            ) from exc
        if declared_size < 0 or declared_size > MAX_WEBHOOK_BODY_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="Payload too large",
            )

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_WEBHOOK_BODY_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="Payload too large",
            )
        body.extend(chunk)
    return bytes(body)


def _stream_name(binding_id: str) -> str:
    return f"pagerduty-webhooks:{binding_id}"


def _replay_key(binding_id: str, replay_identity: str) -> str:
    identity_hash = hashlib.sha256(replay_identity.encode()).hexdigest()
    return f"pagerduty-webhook-replay:{binding_id}:{identity_hash}"


def _replay_identity(provider_subscription_id: str, event_id: str, body: bytes) -> str:
    event_identity = event_id.strip() or hashlib.sha256(body).hexdigest()
    return f"{provider_subscription_id}\x1f{event_identity}"


def _claim_delivery(
    binding_id: str,
    provider_subscription_id: str,
    event_id: str,
    body: bytes,
) -> ReplayClaimOutcome:
    client = get_redis_client()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        )
    try:
        replay_identity = _replay_identity(provider_subscription_id, event_id, body)
        replay_key = _replay_key(binding_id, replay_identity)
        body_hash = hashlib.sha256(body).hexdigest()
        pending_state = f"pending:{body_hash}"
        claimed = client.set(
            replay_key,
            pending_state,
            nx=True,
            ex=PENDING_REPLAY_CLAIM_TTL_SECONDS,
        )
        if claimed:
            return ReplayClaimOutcome.CLAIMED
        stored_body_hash = client.get(replay_key)
    except (ValkeyError, KombuError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        ) from exc
    accepted_state = f"accepted:{body_hash}"
    match stored_body_hash:
        case str() if stored_body_hash == accepted_state:
            return ReplayClaimOutcome.REPLAYED
        case str() if stored_body_hash == pending_state:
            return ReplayClaimOutcome.PENDING
        case bytes() if stored_body_hash.decode() == accepted_state:
            return ReplayClaimOutcome.REPLAYED
        case bytes() if stored_body_hash.decode() == pending_state:
            return ReplayClaimOutcome.PENDING
    logger.warning(
        "pagerduty_webhook.audit rejected binding_id=%s reason=replay_body_conflict",
        binding_id,
    )
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT, detail="Webhook delivery body conflict"
    )


def _release_replay_claim(
    binding_id: str,
    provider_subscription_id: str,
    event_id: str,
    body: bytes,
) -> None:
    """Compensate a claimed delivery after a downstream failure."""
    client = get_redis_client()
    if client is None:
        return
    try:
        client.delete(
            _replay_key(
                binding_id, _replay_identity(provider_subscription_id, event_id, body)
            )
        )
    except (ValkeyError, KombuError):
        logger.exception(
            "pagerduty_webhook.replay_release_failed binding_id=%s",
            binding_id,
        )


_PROMOTE_REPLAY_CLAIM_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3], 'XX')
end
return false
"""


def _accept_replay_claim(
    binding_id: str,
    provider_subscription_id: str,
    event_id: str,
    body: bytes,
) -> None:
    """Mark a delivery accepted only after durable dispatch has succeeded."""
    client = get_redis_client()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        )
    body_hash = hashlib.sha256(body).hexdigest()
    try:
        replay_key = _replay_key(
            binding_id, _replay_identity(provider_subscription_id, event_id, body)
        )
        promote = getattr(client, "eval", None)
        if callable(promote):
            accepted = promote(
                _PROMOTE_REPLAY_CLAIM_SCRIPT,
                1,
                replay_key,
                f"pending:{body_hash}",
                f"accepted:{body_hash}",
                str(REPLAY_RETENTION_SECONDS),
            )
        else:
            accepted = client.set(
                replay_key,
                f"accepted:{body_hash}",
                xx=True,
                ex=REPLAY_RETENTION_SECONDS,
            )
    except (ValkeyError, KombuError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        ) from exc
    if not accepted:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        )


def _enqueue_event(
    *, binding_id: str, webhook: PagerDutyV3Webhook, raw_body_sha256: str
) -> str:
    client = get_redis_client()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        )
    fields = {
        "binding_id": binding_id,
        "event_id": webhook.event.id,
        "event_type": str(webhook.event.event_type),
        "occurred_at": webhook.event.occurred_at.astimezone(UTC).isoformat(),
        "received_at": datetime.now(UTC).isoformat(),
        "raw_body_sha256": raw_body_sha256,
        "payload": webhook.model_dump_json(),
    }
    try:
        return str(
            client.xadd(
                _stream_name(binding_id),
                fields,
                "*",
                RECEIVER_STREAM_MAXLEN,
                True,
            )
        )
    except (ValkeyError, KombuError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        ) from exc


def _compensate_stream_write(binding_id: str, stream_entry_id: str) -> None:
    """Undo a durable stream write after the dispatch that would consume it fails."""
    client = get_redis_client()
    if client is None:
        return
    try:
        client.xdel(_stream_name(binding_id), stream_entry_id)
    except (ValkeyError, KombuError):
        logger.exception(
            "pagerduty_webhook.stream_compensation_failed binding_id=%s "
            "stream_entry_id=%s",
            binding_id,
            stream_entry_id,
        )


@router.post("/{binding_id}", status_code=status.HTTP_202_ACCEPTED)
@limiter.limit("60/minute")
async def pagerduty_webhook(
    request: Request,
    binding_id: str,
    x_pagerduty_signature: str | None = Header(default=None),
    x_webhook_subscription: str | None = Header(default=None),
) -> Response:
    """Authenticate and enqueue one binding-scoped PagerDuty V3 webhook."""
    parsed_binding_id = _parse_binding_id(binding_id)
    canonical_binding_id = str(parsed_binding_id)
    if x_webhook_subscription is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_SIGNATURE_DETAIL
        )
    resolved_binding = await _load_receivable_binding(parsed_binding_id)
    binding = resolved_binding.binding
    if str(binding.id) != canonical_binding_id or not _subscription_matches(
        resolved_binding, x_webhook_subscription
    ):
        logger.warning(
            "pagerduty_webhook.audit rejected binding_id=%s reason=subscription_mismatch",
            canonical_binding_id,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_SIGNATURE_DETAIL
        )
    body = await _read_body_limited(request)
    if not _verify_signature(
        body, x_pagerduty_signature, resolved_binding.signing_secret
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_SIGNATURE_DETAIL
        )
    webhook = _parse_webhook(body)
    match binding.status:
        case "active":
            match webhook.event.event_type:
                case "pagey.ping":
                    return Response(status_code=status.HTTP_204_NO_CONTENT)
                case PagerDutyEventType():
                    pass
                case unreachable:
                    assert_never(unreachable)
        case "candidate" | "ready":
            match webhook.event.event_type:
                case "pagey.ping":
                    if not await _canonical_incident_ingestion_allowed(
                        str(binding.org_id)
                    ):
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=_FEATURE_DISABLED_DETAIL,
                        )
                    await _mark_candidate_ready_from_verified_ping(
                        parsed_binding_id, UUID(str(binding.org_id))
                    )
                    return Response(status_code=status.HTTP_204_NO_CONTENT)
                case PagerDutyEventType():
                    logger.warning(
                        "pagerduty_webhook.audit rejected binding_id=%s "
                        "reason=candidate_event",
                        canonical_binding_id,
                    )
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="Candidate webhook binding accepts only pagey.ping",
                    )
                case unreachable:
                    assert_never(unreachable)
        case _:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Webhook binding not found",
            )
    if not await _canonical_incident_ingestion_allowed(str(binding.org_id)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=_FEATURE_DISABLED_DETAIL
        )

    provider_subscription_id = binding.provider_subscription_id
    replay_claim = _claim_delivery(
        canonical_binding_id, provider_subscription_id, webhook.event.id, body
    )
    if replay_claim is ReplayClaimOutcome.REPLAYED:
        logger.info(
            "pagerduty_webhook.audit replayed binding_id=%s event_id=%s",
            canonical_binding_id,
            webhook.event.id,
        )
        return Response(status_code=status.HTTP_202_ACCEPTED)
    if replay_claim is ReplayClaimOutcome.PENDING:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        )
    try:
        stream_entry_id = _enqueue_event(
            binding_id=canonical_binding_id,
            webhook=webhook,
            raw_body_sha256=hashlib.sha256(body).hexdigest(),
        )
    except HTTPException:
        _release_replay_claim(
            canonical_binding_id, provider_subscription_id, webhook.event.id, body
        )
        raise
    try:
        getattr(process_pagerduty_webhook_event, "delay")(
            binding_id=canonical_binding_id, stream_entry_id=stream_entry_id
        )
    except (ValkeyError, KombuError) as exc:
        _compensate_stream_write(canonical_binding_id, stream_entry_id)
        _release_replay_claim(
            canonical_binding_id, provider_subscription_id, webhook.event.id, body
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_QUEUE_UNAVAILABLE_DETAIL,
        ) from exc
    _accept_replay_claim(
        canonical_binding_id, provider_subscription_id, webhook.event.id, body
    )
    logger.info(
        "pagerduty_webhook.audit accepted binding_id=%s event_id=%s",
        canonical_binding_id,
        webhook.event.id,
    )
    return Response(status_code=status.HTTP_202_ACCEPTED)
