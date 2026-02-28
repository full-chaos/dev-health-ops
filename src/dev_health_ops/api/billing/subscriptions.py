from __future__ import annotations

import importlib
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import require_admin
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing.stripe_client import get_stripe_client
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency

router = APIRouter(prefix="/subscriptions", tags=["billing-subscriptions"])


class SubscriptionView(BaseModel):
    id: str
    org_id: str
    stripe_subscription_id: str
    stripe_customer_id: str
    status: str
    current_period_start: str
    current_period_end: str
    cancel_at_period_end: bool
    canceled_at: str | None = None
    trial_start: str | None = None
    trial_end: str | None = None
    plan: dict[str, Any] | None = None
    price: dict[str, Any] | None = None


class SubscriptionHistoryItem(BaseModel):
    id: str
    stripe_event_id: str
    event_type: str
    previous_status: str | None
    new_status: str
    processed_at: str
    payload: dict[str, Any] = Field(default_factory=dict)


class SubscriptionHistoryResponse(BaseModel):
    items: list[SubscriptionHistoryItem]
    total: int
    limit: int
    offset: int


class ChangePlanRequest(BaseModel):
    price_id: str


class CancelSubscriptionRequest(BaseModel):
    immediately: bool = False


class MutationResponse(BaseModel):
    status: str


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


async def _load_plan_price(
    subscription: Any, db: AsyncSession
) -> tuple[Any | None, Any | None]:
    try:
        billing_module = importlib.import_module("dev_health_ops.models.billing")
    except ImportError:
        return None, None

    BillingPlan = getattr(billing_module, "BillingPlan", None)
    BillingPrice = getattr(billing_module, "BillingPrice", None)
    if BillingPlan is None or BillingPrice is None:
        return None, None

    price = await db.get(BillingPrice, subscription.billing_price_id)
    plan = await db.get(BillingPlan, subscription.billing_plan_id)
    return plan, price


def _serialize_record(record: Any) -> dict[str, Any]:
    if record is None:
        return {}
    out: dict[str, Any] = {}
    for key, value in vars(record).items():
        if key.startswith("_"):
            continue
        out[key] = _to_iso(value) if "at" in key or "period" in key else value
    return out


def _service(session: AsyncSession) -> Any:
    module = importlib.import_module("dev_health_ops.api.billing.subscription_service")
    return module.SubscriptionService(session)


@router.get("", response_model=SubscriptionView)
async def get_subscription(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: AsyncSession = Depends(postgres_session_dependency),
) -> SubscriptionView:
    service = _service(session)
    subscription = await service.get_for_org(uuid.UUID(user.org_id))
    if subscription is None:
        raise HTTPException(status_code=404, detail="No active subscription")

    plan, price = await _load_plan_price(subscription, session)
    return SubscriptionView(
        id=str(subscription.id),
        org_id=str(subscription.org_id),
        stripe_subscription_id=subscription.stripe_subscription_id,
        stripe_customer_id=subscription.stripe_customer_id,
        status=subscription.status,
        current_period_start=_to_iso(subscription.current_period_start) or "",
        current_period_end=_to_iso(subscription.current_period_end) or "",
        cancel_at_period_end=bool(subscription.cancel_at_period_end),
        canceled_at=_to_iso(subscription.canceled_at),
        trial_start=_to_iso(subscription.trial_start),
        trial_end=_to_iso(subscription.trial_end),
        plan=_serialize_record(plan) or None,
        price=_serialize_record(price) or None,
    )


@router.get("/history", response_model=SubscriptionHistoryResponse)
async def get_subscription_history(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
    session: AsyncSession = Depends(postgres_session_dependency),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> SubscriptionHistoryResponse:
    service = _service(session)
    events, total = await service.get_history(uuid.UUID(user.org_id), limit, offset)
    return SubscriptionHistoryResponse(
        items=[
            SubscriptionHistoryItem(
                id=str(event.id),
                stripe_event_id=event.stripe_event_id,
                event_type=event.event_type,
                previous_status=event.previous_status,
                new_status=event.new_status,
                processed_at=_to_iso(event.processed_at) or "",
                payload=event.payload or {},
            )
            for event in events
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post("/change-plan", response_model=MutationResponse)
async def change_plan(
    body: ChangePlanRequest,
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    session: AsyncSession = Depends(postgres_session_dependency),
) -> MutationResponse:
    service = _service(session)
    subscription = await service.get_for_org(uuid.UUID(user.org_id))
    if subscription is None:
        raise HTTPException(status_code=404, detail="No subscription found")

    client = get_stripe_client()
    stripe_sub = client.subscriptions.retrieve(subscription.stripe_subscription_id)
    item_id = None
    items = getattr(getattr(stripe_sub, "items", None), "data", []) or []
    if items:
        item_id = getattr(items[0], "id", None)
    if item_id is None:
        raise HTTPException(status_code=400, detail="Stripe subscription has no items")

    client.subscriptions.update(
        subscription.stripe_subscription_id,
        params={
            "items": [{"id": item_id, "price": body.price_id}],
            "proration_behavior": "create_prorations",
        },
    )
    return MutationResponse(status="ok")


@router.post("/cancel", response_model=MutationResponse)
async def cancel_subscription(
    body: CancelSubscriptionRequest,
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    session: AsyncSession = Depends(postgres_session_dependency),
) -> MutationResponse:
    service = _service(session)
    subscription = await service.get_for_org(uuid.UUID(user.org_id))
    if subscription is None:
        raise HTTPException(status_code=404, detail="No subscription found")

    client = get_stripe_client()
    if body.immediately:
        client.subscriptions.cancel(subscription.stripe_subscription_id)
    else:
        client.subscriptions.update(
            subscription.stripe_subscription_id,
            params={"cancel_at_period_end": True},
        )
    return MutationResponse(status="ok")


@router.post("/reactivate", response_model=MutationResponse)
async def reactivate_subscription(
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    session: AsyncSession = Depends(postgres_session_dependency),
) -> MutationResponse:
    service = _service(session)
    subscription = await service.get_for_org(uuid.UUID(user.org_id))
    if subscription is None:
        raise HTTPException(status_code=404, detail="No subscription found")

    client = get_stripe_client()
    client.subscriptions.update(
        subscription.stripe_subscription_id,
        params={"cancel_at_period_end": False},
    )
    return MutationResponse(status="ok")
