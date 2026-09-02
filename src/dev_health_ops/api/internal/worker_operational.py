"""Authenticated internal bridge invoked by LIVE Go operational handlers.

CHAOS-4440: this module's own docstring previously said "dormant" — backwards.
`operational.billing_notification`, `operational.webhook_delivery`, and
`system.heartbeat` are registered and running in production today
(`cmd/dev-health-worker/operational.go:120-167`); each route below is called
from the Go handler that owns the durable row, the River attempt, and retry
classification, while this bridge performs the compatibility side effect
(email dispatch, webhook processing, telemetry POST) during coexistence. The
`/pagerduty` route is a different shape: it reconciles one Go-owned Valkey
stream delivery per call rather than a durable Postgres row — see
`PagerDutyDelivery` below.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError
from starlette.concurrency import run_in_threadpool

from dev_health_ops.api.internal.worker_auth import authorize_worker_bridge
from dev_health_ops.workers.system_ops import (
    phone_home_heartbeat,
    send_billing_notification,
)
from dev_health_ops.workers.system_webhooks import process_webhook_event

router = APIRouter(prefix="/api/internal/worker-operational", include_in_schema=False)

# The Go stream consumer classifies on this exact string set, not on the HTTP
# status: every member is returned with 200 so a durable-but-unprocessed
# outcome is never mistaken for a transport failure. Only genuinely retryable
# dependency faults are allowed to escape as 5xx.
PAGERDUTY_BRIDGE_STATUSES = frozenset(
    {
        "processed",  # reconciliation committed
        "skipped",  # already reconciled; bounded no-op replay
        "feature_disabled",  # canonical incident ingestion off for the org
        "revoked_binding",  # binding missing, inactive, or revoked
        "malformed",  # payload failed PagerDuty V3 validation
        "rejected",  # any other terminal rejection
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WebhookReference(_StrictModel):
    delivery_id: uuid.UUID
    provider: str
    event_type: str


class BillingReference(_StrictModel):
    notification_id: uuid.UUID
    organization_id: uuid.UUID
    notification_type: str
    # CHAOS-3952: Go's own copy of the durable row's idempotency key. Cross-
    # checked against the row itself so the two sides' identity can never
    # silently drift; the actual duplicate-send guard is the row's own
    # completion fence (system_ops._claim_billing_notification_completion).
    # Optional, not required: a REQUIRED field on a strict (extra="forbid")
    # bridge model is a rolling-deploy hazard — an old Go binary that omits
    # it hits a 422 that http.go classifies as permanent (codex round 2,
    # P1, EXECUTED), terminalizing the River job with no email ever sent.
    # `send_billing_notification` itself already treats a missing key as
    # "skip the cross-check" (see system_ops.py), so the wire contract
    # matches. This does not cover new-Go-against-old-Python during a
    # rollout (an old strict model rejects the unknown field outright) —
    # that direction needs deploy ORDER (bridge before worker), not a code
    # change here; see the PR's RISK-NOTES.
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=256)


class HeartbeatReference(_StrictModel):
    scheduled_for: datetime


class PagerDutyDelivery(_StrictModel):
    """One Go-owned PagerDuty delivery, carrying its own verified payload.

    Unlike the other references on this router, this body is not a durable
    pointer: the Go stream consumer owns the Valkey stream entry, so the
    payload it already read is the only input this bridge may reconcile from.
    """

    binding_id: uuid.UUID
    # PagerDuty guarantees neither a usable event id nor a unique one, so the
    # receipt identity Go actually deduplicates on is carried separately.
    event_id: str = Field(max_length=512)
    receipt_id: str = Field(min_length=1, max_length=512)
    received_at: datetime
    payload: dict[str, JsonValue]


def _bridge_result(result: object, *, success: frozenset[str]) -> dict[str, str]:
    if not isinstance(result, dict):
        raise HTTPException(status_code=502, detail="Operational result unavailable")
    status = str(result.get("status", "unknown"))
    if status not in success:
        # These tasks encode invalid durable references and unsupported domain
        # values as error/dropped. Retrying cannot repair either condition.
        raise HTTPException(status_code=422, detail="Operational delivery rejected")
    return {"status": status}


def _authorize(authorization: Annotated[str | None, Header()] = None) -> None:
    authorize_worker_bridge(authorization)


@router.post("/webhook", dependencies=[])
async def process_webhook_reference(
    reference: WebhookReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict:
    _authorize(authorization)
    result = await run_in_threadpool(
        process_webhook_event.run,
        durable_delivery_id=str(reference.delivery_id),
    )
    return _bridge_result(result, success=frozenset({"success", "skipped"}))


@router.post("/billing", dependencies=[])
async def process_billing_reference(
    reference: BillingReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict:
    _authorize(authorization)
    result = await run_in_threadpool(
        send_billing_notification.run,
        durable_notification_id=str(reference.notification_id),
        idempotency_key=reference.idempotency_key,
    )
    return _bridge_result(result, success=frozenset({"sent"}))


@router.post("/heartbeat", dependencies=[])
async def process_heartbeat_reference(
    reference: HeartbeatReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict:
    _authorize(authorization)
    if reference.scheduled_for.utcoffset() != timedelta(0):
        raise HTTPException(status_code=422, detail="Heartbeat occurrence must be UTC")
    result = await run_in_threadpool(phone_home_heartbeat.run)
    return _bridge_result(result, success=frozenset({"ok"}))


@router.post("/pagerduty", dependencies=[])
async def process_pagerduty_delivery(
    delivery: PagerDutyDelivery,
    authorization: Annotated[str | None, Header()] = None,
) -> dict:
    """Reconcile one PagerDuty delivery from the payload the Go consumer read.

    Go owns the Valkey stream entry, the receipt, and the ACK for this
    delivery. This route therefore reconciles strictly from ``delivery.payload``
    and must never read the stream, never ``XDEL`` an entry, and never write the
    Python Valkey receipt key: doing any of those would make two runtimes
    consume the same entry and drop events the other still has pending.

    Every value in ``PAGERDUTY_BRIDGE_STATUSES`` answers with HTTP 200 because
    Go classifies on the status string. A retryable dependency failure (Postgres
    or ClickHouse unreachable) is deliberately left to escape as 5xx so the
    delivery stays in the pending list instead of being quarantined.
    """
    _authorize(authorization)
    if delivery.received_at.utcoffset() != timedelta(0):
        raise HTTPException(status_code=422, detail="Delivery receipt must be UTC")
    status = await _reconcile_pagerduty_delivery(delivery)
    if status not in PAGERDUTY_BRIDGE_STATUSES:
        raise HTTPException(status_code=502, detail="Operational result unavailable")
    return {"status": status}


async def _reconcile_pagerduty_delivery(delivery: PagerDutyDelivery) -> str:
    # Imported lazily, like the other worker bodies on this router. This module
    # is pulled in by api.main, and binding the PagerDuty reconciliation graph
    # at import time would drag the provider and ClickHouse stack into every
    # API import and pre-bind symbols other suites patch.
    from dev_health_ops.api.webhooks.pagerduty_models import PagerDutyV3Webhook
    from dev_health_ops.providers.pagerduty.webhook_worker import (
        reconcile_pagerduty_webhook_with_locked_graph,
        resolve_pagerduty_webhook_binding,
    )
    from dev_health_ops.workers.system_webhooks import (
        CanonicalIncidentIngestionDisabledError,
        _canonical_incident_ingestion_allowed,
    )

    binding_id = str(delivery.binding_id)
    try:
        webhook = PagerDutyV3Webhook.model_validate(delivery.payload)
    except ValidationError:
        # ``malformed`` is scoped to this step alone. Only the webhook payload
        # is immutable bad data that a redelivery can never repair, so only its
        # validation may be classified permanent.
        return "malformed"
    if delivery.event_id and delivery.event_id != webhook.event.id:
        # Go's receipt identity is derived from the stream's event id. A payload
        # that disagrees with it would let one receipt stand for a different
        # event, so the delivery is refused rather than reconciled.
        return "rejected"
    if webhook.event.event_type == "pagey.ping":
        # Verification pings are answered by the receiver; they carry no
        # operational state and are terminal here.
        return "rejected"
    try:
        try:
            context = await resolve_pagerduty_webhook_binding(binding_id)
        except RuntimeError:
            # This resolver raises RuntimeError only for an unusable binding
            # identity or an absent/inactive/revoked binding; a database fault
            # surfaces as SQLAlchemyError and stays retryable.
            return "revoked_binding"
        if not await run_in_threadpool(
            _canonical_incident_ingestion_allowed, context.org_id
        ):
            raise CanonicalIncidentIngestionDisabledError()
        clickhouse_url = os.getenv("CLICKHOUSE_URI")
        if not clickhouse_url:
            # Operator-fixable configuration gap: keep the delivery pending
            # rather than quarantining a valid event.
            raise HTTPException(
                status_code=503, detail="Operational persistence unconfigured"
            )
        # Reconciliation hydrates from the PagerDuty REST API, so a
        # ``ValidationError`` raised in here describes a bad *upstream response*,
        # not the webhook. That is a transient provider condition: it is left to
        # propagate as 5xx, which the Go client classifies as retryable, so a
        # valid webhook keeps the retries the Celery path performs instead of
        # being dead-lettered on one bad hydration.
        processed = await reconcile_pagerduty_webhook_with_locked_graph(
            binding_id=binding_id,
            expected_context=context,
            clickhouse_url=clickhouse_url,
            webhook=webhook,
            received_at=delivery.received_at,
        )
    except CanonicalIncidentIngestionDisabledError:
        return "feature_disabled"
    return "processed" if processed else "skipped"
