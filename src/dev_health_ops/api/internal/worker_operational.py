"""Authenticated internal bridge invoked by LIVE Go operational handlers.

CHAOS-4440: this module's own docstring previously said "dormant" — backwards.
`operational.billing_notification` and `system.heartbeat` are registered and
running in production today (`cmd/dev-health-worker/operational.go`); each
route below is called from the Go handler that owns the durable row, the
River attempt, and retry classification, while this bridge performs the
compatibility side effect (email dispatch, telemetry POST) during
coexistence. CHAOS-5320 deleted the `/webhook` route this module used to
carry (`operational.webhook_delivery`'s Python fallback) -- native Go
handling (CHAOS-5318/5319/5320) plus its own explicit-ignore path fully
replaced it, so no webhook delivery ever reaches this bridge anymore.
CHAOS-4105 then deleted `/pagerduty` on the same grounds: PagerDuty webhook
reconciliation is native Go (`internal/jobs/pagerduty/reconcile_native.go`
writing through the providersync effect sinks), so no PagerDuty delivery
reaches this bridge either. What remains is billing and heartbeat.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from dev_health_ops.api.internal.worker_auth import authorize_worker_bridge
from dev_health_ops.workers.system_ops import (
    phone_home_heartbeat,
    send_billing_notification,
)

router = APIRouter(prefix="/api/internal/worker-operational", include_in_schema=False)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


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
