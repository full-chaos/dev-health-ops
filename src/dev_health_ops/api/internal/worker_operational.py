"""Authenticated internal bridge invoked by LIVE Go operational handlers.

CHAOS-4440: this module's own docstring previously said "dormant" — backwards.
`system.heartbeat` is the ONLY route this module still carries. It is called
from the Go handler that owns the River attempt and retry classification,
while this bridge performs the telemetry POST.

Every other compatibility route is gone, each replaced by native Go rather
than merely disabled. CHAOS-5320 deleted `/webhook`
(`operational.webhook_delivery`'s Python fallback), replaced by native
handling plus an explicit-ignore path. CHAOS-4105 deleted `/pagerduty`:
PagerDuty webhook reconciliation is native Go
(`internal/jobs/pagerduty/reconcile_native.go` writing through the
providersync effect sinks). CHAOS-5353 deleted `/billing`: the Go
BillingHandler owns the completion fence, the owner-email lookup, all seven
email renderings and the provider send.

Once heartbeat's own phone-home effect is ported, this module has no reason
to exist at all.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool

from dev_health_ops.api.internal.worker_auth import authorize_worker_bridge
from dev_health_ops.workers.system_ops import phone_home_heartbeat

router = APIRouter(prefix="/api/internal/worker-operational", include_in_schema=False)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


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
