"""Authenticated internal bridge for dormant Go operational handlers."""

from __future__ import annotations

import hmac
import os
import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool

from dev_health_ops.workers.system_ops import (
    phone_home_heartbeat,
    send_billing_notification,
)
from dev_health_ops.workers.system_webhooks import process_webhook_event

router = APIRouter(prefix="/api/internal/worker-operational", include_in_schema=False)


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
    expected = os.environ.get("WORKER_OPERATIONAL_BRIDGE_TOKEN", "")
    supplied = authorization or ""
    if (
        not expected
        or not supplied.startswith("Bearer ")
        or not hmac.compare_digest(supplied[7:], expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")


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
    )
    return _bridge_result(result, success=frozenset({"sent"}))


@router.post("/heartbeat", dependencies=[])
async def process_heartbeat_reference(
    reference: HeartbeatReference,
    authorization: Annotated[str | None, Header()] = None,
) -> dict:
    _authorize(authorization)
    if reference.scheduled_for.tzinfo is None:
        raise HTTPException(status_code=422, detail="Heartbeat occurrence must be UTC")
    result = await run_in_threadpool(phone_home_heartbeat.run)
    return _bridge_result(result, success=frozenset({"ok"}))
