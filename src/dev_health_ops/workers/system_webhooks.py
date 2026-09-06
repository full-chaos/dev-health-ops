from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import NoReturn

from dev_health_ops.providers.pagerduty.webhook_transport import (
    WebhookTransport,
    resolve_webhook_transport,
)
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

# Upper bound for a provider-supplied rate-limit retry delay. Caps absurd or
# adversarial ``Retry-After`` values so an oversized (but finite) countdown
# can't park a webhook retry for an unreasonable span; real rate limits clear
# well within an hour.
_MAX_RETRY_COUNTDOWN_SECONDS = 3600
_PAGERDUTY_WEBHOOK_DLQ_MAXLEN = 100_000
_PAGERDUTY_WEBHOOK_RECEIPT_TTL_SECONDS = 604_800


class CanonicalIncidentIngestionDisabledError(RuntimeError):
    __slots__ = ()


def _canonical_incident_ingestion_allowed(org_id: str) -> bool:
    from sqlalchemy.exc import SQLAlchemyError

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.licensing import is_org_feature_enabled_sync
    from dev_health_ops.licensing.registry import CANONICAL_INCIDENT_INGESTION_FEATURE

    try:
        parsed_org_id = uuid.UUID(org_id)
    except ValueError:
        return False
    try:
        with get_postgres_session_sync() as session:
            return is_org_feature_enabled_sync(
                session,
                parsed_org_id,
                CANONICAL_INCIDENT_INGESTION_FEATURE,
            )
    except SQLAlchemyError:
        return False


@celery_app.task(
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
    queue="webhooks",
    name="dev_health_ops.workers.tasks.process_pagerduty_webhook_event",
)
def process_pagerduty_webhook_event(
    self,
    *,
    binding_id: str,
    stream_entry_id: str,
) -> dict:
    import hashlib
    import json
    from datetime import datetime

    from dev_health_ops.api.ingest.streams import get_redis_client
    from dev_health_ops.api.webhooks.pagerduty_models import PagerDutyV3Webhook
    from dev_health_ops.providers.pagerduty.webhook_worker import (
        reconcile_pagerduty_webhook_with_locked_graph,
        resolve_pagerduty_webhook_binding,
    )

    # Re-read the switch here, at execution time, rather than trusting the
    # producer's decision: a task queued before the flip, or dispatched by a
    # rolling API pod that had not yet picked up the new env, arrives after the
    # stream entry already belongs to the Go consumer. Reconciling it would
    # double-write, and the XDEL below would strip an entry still pending in
    # Go's consumer group. Stand down before touching Valkey at all so the
    # entry, the receipt, and the ACK stay wholly Go's.
    transport = resolve_webhook_transport()
    if transport is WebhookTransport.RIVER:
        logger.info(
            "pagerduty_webhook.task_delegated binding_id=%s stream_entry_id=%s "
            "transport=%s",
            binding_id,
            stream_entry_id,
            transport.value,
        )
        return {
            "processed": False,
            "reason": "transport_delegated",
            "transport": transport.value,
            "binding_id": binding_id,
            "stream_entry_id": stream_entry_id,
        }

    redis_client = get_redis_client()
    stream_name = f"pagerduty-webhooks:{binding_id}"
    if redis_client is None:
        raise self.retry(
            exc=RuntimeError("pagerduty webhook stream unavailable"), countdown=30
        )
    clickhouse_url = os.getenv("CLICKHOUSE_URI")
    # Reading and parsing the pending entry is a pre-persistence failure
    # surface: a Redis error, a trimmed/missing entry, or a malformed payload.
    # Route ALL of them through the shared retry-then-dead-letter path so none
    # escape as a silent task failure that drops the webhook.
    fields: dict[str, str] | None = None
    try:
        entries = getattr(redis_client, "xrange")(
            stream_name, min=stream_entry_id, max=stream_entry_id
        )
        if not entries:
            raise RuntimeError("pagerduty webhook stream entry missing")
        _, entry_fields = entries[0]
        fields = dict(entry_fields)
        parsed = PagerDutyV3Webhook.model_validate(json.loads(entry_fields["payload"]))
        processed_at = datetime.fromisoformat(entry_fields["received_at"])
        receipt_key = _pagerduty_webhook_receipt_key(
            stream_name,
            parsed.event.id,
            entry_fields.get("raw_body_sha256")
            or hashlib.sha256(entry_fields["payload"].encode()).hexdigest(),
        )
        if _pagerduty_webhook_receipt_completed(redis_client, receipt_key):
            redis_client.xdel(stream_name, stream_entry_id)
            return {
                "processed": False,
                "receipt": "completed",
                "event_id": parsed.event.id,
                "stream_entry_id": stream_entry_id,
            }
        context = run_async(resolve_pagerduty_webhook_binding(binding_id))
        if not _canonical_incident_ingestion_allowed(context.org_id):
            raise CanonicalIncidentIngestionDisabledError()
        if not clickhouse_url:
            raise RuntimeError("pagerduty webhook persistence is unconfigured")

        processed = run_async(
            reconcile_pagerduty_webhook_with_locked_graph(
                binding_id=binding_id,
                expected_context=context,
                clickhouse_url=clickhouse_url,
                webhook=parsed,
                received_at=processed_at,
            )
        )
        _record_pagerduty_webhook_receipt(redis_client, receipt_key)
        redis_client.xdel(stream_name, stream_entry_id)
        return {
            "processed": processed,
            "event_id": parsed.event.id,
            "stream_entry_id": stream_entry_id,
        }
    except CanonicalIncidentIngestionDisabledError as exc:
        _retry_or_dead_letter_pagerduty_webhook(
            task=self,
            redis_client=redis_client,
            stream_name=stream_name,
            stream_entry_id=stream_entry_id,
            fields=fields,
            error=exc,
            retryable=False,
        )
    except Exception as exc:
        _retry_or_dead_letter_pagerduty_webhook(
            task=self,
            redis_client=redis_client,
            stream_name=stream_name,
            stream_entry_id=stream_entry_id,
            fields=fields,
            error=exc,
        )


def _pagerduty_webhook_receipt_key(
    stream_name: str, event_id: str, raw_body_sha256: str
) -> str:
    receipt_identity = event_id if event_id.strip() else raw_body_sha256
    return f"{stream_name}:receipts:{receipt_identity}"


def _pagerduty_webhook_receipt_completed(redis_client, receipt_key: str) -> bool:
    get = getattr(redis_client, "get", None)
    return bool(get(receipt_key)) if callable(get) else False


def _record_pagerduty_webhook_receipt(redis_client, receipt_key: str) -> None:
    set_receipt = getattr(redis_client, "set", None)
    if callable(set_receipt):
        set_receipt(
            receipt_key,
            "completed",
            ex=_PAGERDUTY_WEBHOOK_RECEIPT_TTL_SECONDS,
            nx=True,
        )


def _retry_or_dead_letter_pagerduty_webhook(
    *,
    task,
    redis_client,
    stream_name: str,
    stream_entry_id: str,
    fields: dict[str, str] | None,
    error: Exception,
    retryable: bool = True,
) -> NoReturn:
    if retryable and task.request.retries < task.max_retries:
        raise task.retry(
            exc=error,
            countdown=30 * (2**task.request.retries),
        )

    event_id = (fields or {}).get("event_id", "unknown")
    dlq_name = f"{stream_name}:dlq"
    redis_client.xadd(
        dlq_name,
        {
            "event_id": event_id,
            "stream_entry_id": stream_entry_id,
            "task_id": str(task.request.id or ""),
            "retry_count": str(task.request.retries),
            "failure_type": type(error).__name__,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "payload": (fields or {}).get("payload", ""),
        },
        maxlen=_PAGERDUTY_WEBHOOK_DLQ_MAXLEN,
        approximate=True,
    )
    redis_client.xdel(stream_name, stream_entry_id)
    if retryable:
        logger.error(
            "pagerduty_webhook.persistence_failed event_id=%s stream_entry_id=%s retries=%s",
            event_id,
            stream_entry_id,
            task.request.retries,
        )
    else:
        logger.warning(
            "pagerduty_webhook.feature_disabled event_id=%s stream_entry_id=%s",
            event_id,
            stream_entry_id,
        )
    raise RuntimeError("pagerduty webhook persistence exhausted") from error
