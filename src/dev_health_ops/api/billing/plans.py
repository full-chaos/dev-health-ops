from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from stripe.params._price_create_params import PriceCreateParams
from stripe.params._product_create_params import ProductCreateParams

from dev_health_ops.api.admin.middleware import require_superuser
from dev_health_ops.api.auth.router import get_current_user, get_current_user_optional
from dev_health_ops.api.billing.plan_sync_service import pull_from_stripe
from dev_health_ops.api.billing.stripe_client import get_stripe_client
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency
from dev_health_ops.models.billing import (
    BillingInterval,
    BillingPlan,
    BillingPrice,
    FeatureBundle,
    PlanFeatureBundle,
)

from ._helpers import (
    BillingTier,
    assign_attr,
    ensure_dict,
    ensure_str_list,
    normalize_billing_tier,
    require_int,
    require_str,
    require_uuid,
)

router = APIRouter(tags=["billing-plans"])


class BillingPriceInput(BaseModel):
    interval: BillingInterval
    amount: int = Field(ge=0)
    currency: str = "usd"
    is_active: bool = True
    stripe_price_id: str | None = None


class BillingPlanCreate(BaseModel):
    key: str
    name: str
    description: str | None = None
    tier: BillingTier
    is_active: bool = True
    display_order: int = 0
    stripe_product_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    prices: list[BillingPriceInput] = Field(default_factory=list)
    bundle_ids: list[str] = Field(default_factory=list)


class BillingPlanUpdate(BaseModel):
    key: str | None = None
    name: str | None = None
    description: str | None = None
    tier: BillingTier | None = None
    is_active: bool | None = None
    display_order: int | None = None
    stripe_product_id: str | None = None
    metadata: dict[str, Any] | None = None
    prices: list[BillingPriceInput] | None = None
    bundle_ids: list[str] | None = None


class FeatureBundleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    key: str
    name: str
    description: str | None
    features: list[str]


class BillingPriceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    plan_id: str
    interval: str
    amount: int
    currency: str
    is_active: bool
    stripe_price_id: str | None


class BillingPlanResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: str
    key: str
    name: str
    description: str | None
    tier: BillingTier
    is_active: bool
    display_order: int
    stripe_product_id: str | None
    metadata: dict[str, Any]
    prices: list[BillingPriceResponse] = Field(default_factory=list)
    bundles: list[FeatureBundleResponse] = Field(default_factory=list)


class PullStripeResponse(BaseModel):
    created: list[str]
    updated: list[str]
    skipped: list[str]
    errors: list[str]


async def _require_superadmin(user: AuthenticatedUser) -> None:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superadmin access required")


def _parse_uuid(value: str, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}") from exc


async def _load_prices(
    db: AsyncSession, plan_id: uuid.UUID, include_inactive: bool
) -> list[BillingPrice]:
    stmt = select(BillingPrice).where(BillingPrice.plan_id == plan_id)
    if not include_inactive:
        stmt = stmt.where(BillingPrice.is_active.is_(True))
    result = await db.execute(stmt.order_by(BillingPrice.amount.asc()))
    return list(result.scalars().all())


async def _load_bundles(db: AsyncSession, plan_id: uuid.UUID) -> list[FeatureBundle]:
    stmt = (
        select(FeatureBundle)
        .join(PlanFeatureBundle, PlanFeatureBundle.bundle_id == FeatureBundle.id)
        .where(PlanFeatureBundle.plan_id == plan_id)
        .order_by(FeatureBundle.key.asc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


def _price_to_response(price: BillingPrice) -> BillingPriceResponse:
    return BillingPriceResponse(
        id=str(require_uuid(price.id, "price.id")),
        plan_id=str(require_uuid(price.plan_id, "price.plan_id")),
        interval=require_str(price.interval, "price.interval"),
        amount=require_int(price.amount, "price.amount"),
        currency=require_str(price.currency, "price.currency"),
        is_active=bool(price.is_active),
        stripe_price_id=price.stripe_price_id
        if isinstance(price.stripe_price_id, str)
        else None,
    )


def _bundle_to_response(bundle: FeatureBundle) -> FeatureBundleResponse:
    return FeatureBundleResponse(
        id=str(require_uuid(bundle.id, "bundle.id")),
        key=require_str(bundle.key, "bundle.key"),
        name=require_str(bundle.name, "bundle.name"),
        description=bundle.description if isinstance(bundle.description, str) else None,
        features=ensure_str_list(bundle.features),
    )


async def _plan_to_response(
    db: AsyncSession,
    plan: BillingPlan,
    include_inactive_prices: bool,
) -> BillingPlanResponse:
    plan_id = require_uuid(plan.id, "plan.id")
    prices = await _load_prices(db, plan_id, include_inactive=include_inactive_prices)
    bundles = await _load_bundles(db, plan_id)
    return BillingPlanResponse(
        id=str(plan_id),
        key=require_str(plan.key, "plan.key"),
        name=require_str(plan.name, "plan.name"),
        description=plan.description if isinstance(plan.description, str) else None,
        tier=normalize_billing_tier(plan.tier),
        is_active=bool(plan.is_active),
        display_order=require_int(plan.display_order, "plan.display_order"),
        stripe_product_id=(
            plan.stripe_product_id if isinstance(plan.stripe_product_id, str) else None
        ),
        metadata=ensure_dict(plan.metadata_),
        prices=[_price_to_response(price) for price in prices],
        bundles=[_bundle_to_response(bundle) for bundle in bundles],
    )


async def _replace_prices(
    db: AsyncSession,
    plan_id: uuid.UUID,
    prices: list[BillingPriceInput],
) -> None:
    existing_result = await db.execute(
        select(BillingPrice).where(BillingPrice.plan_id == plan_id)
    )
    existing_prices = list(existing_result.scalars().all())
    existing_by_key = {
        (
            require_str(price.interval, "price.interval"),
            require_str(price.currency, "price.currency"),
        ): price
        for price in existing_prices
    }

    now = datetime.now(timezone.utc)
    incoming_keys: set[tuple[str, str]] = set()

    for price in prices:
        key = (price.interval.value, price.currency)
        incoming_keys.add(key)
        existing = existing_by_key.get(key)

        if existing is None:
            db.add(
                BillingPrice(
                    plan_id=plan_id,
                    interval=price.interval.value,
                    amount=price.amount,
                    currency=price.currency,
                    is_active=price.is_active,
                    stripe_price_id=price.stripe_price_id,
                    created_at=now,
                    updated_at=now,
                )
            )
            continue

        assign_attr(existing, "amount", price.amount)
        assign_attr(existing, "is_active", price.is_active)
        if price.stripe_price_id:
            assign_attr(existing, "stripe_price_id", price.stripe_price_id)
        assign_attr(existing, "updated_at", now)

    to_delete_ids = [
        price.id for key, price in existing_by_key.items() if key not in incoming_keys
    ]
    if to_delete_ids:
        await db.execute(delete(BillingPrice).where(BillingPrice.id.in_(to_delete_ids)))


async def _replace_bundles(
    db: AsyncSession,
    plan_id: uuid.UUID,
    bundle_ids: list[str],
) -> None:
    await db.execute(
        delete(PlanFeatureBundle).where(PlanFeatureBundle.plan_id == plan_id)
    )
    parsed_bundle_ids = [
        _parse_uuid(bundle_id, "bundle_id") for bundle_id in bundle_ids
    ]
    if not parsed_bundle_ids:
        return
    bundle_check = await db.execute(
        select(FeatureBundle.id).where(FeatureBundle.id.in_(parsed_bundle_ids))
    )
    existing_ids = {row[0] for row in bundle_check.all()}
    missing = [
        bundle_id for bundle_id in parsed_bundle_ids if bundle_id not in existing_ids
    ]
    if missing:
        raise HTTPException(
            status_code=404, detail="One or more feature bundles were not found"
        )
    for bundle_id in parsed_bundle_ids:
        db.add(PlanFeatureBundle(plan_id=plan_id, bundle_id=bundle_id))


@router.get("/plans", response_model=list[BillingPlanResponse])
async def list_billing_plans(
    include_inactive: Annotated[bool, Query()] = False,
    db: AsyncSession = Depends(postgres_session_dependency),
    user: AuthenticatedUser | None = Depends(get_current_user_optional),
) -> list[BillingPlanResponse]:
    if include_inactive:
        if user is None or not user.is_superuser:
            raise HTTPException(status_code=403, detail="Superadmin access required")
    stmt = select(BillingPlan)
    if not include_inactive:
        stmt = stmt.where(BillingPlan.is_active.is_(True))
    result = await db.execute(
        stmt.order_by(BillingPlan.display_order.asc(), BillingPlan.name.asc())
    )
    plans = list(result.scalars().all())
    return [
        await _plan_to_response(db, plan, include_inactive_prices=include_inactive)
        for plan in plans
    ]


@router.post("/plans/pull-stripe", response_model=PullStripeResponse)
async def pull_plans_from_stripe(
    user: AuthenticatedUser = Depends(require_superuser),
    db: AsyncSession = Depends(postgres_session_dependency),
) -> PullStripeResponse:
    report = await pull_from_stripe(db)
    return PullStripeResponse(**report.to_dict())


@router.get("/plans/{plan_id}", response_model=BillingPlanResponse)
async def get_billing_plan(
    plan_id: str,
    include_inactive_prices: Annotated[bool, Query()] = False,
    db: AsyncSession = Depends(postgres_session_dependency),
    user: AuthenticatedUser | None = Depends(get_current_user_optional),
) -> BillingPlanResponse:
    plan_uuid = _parse_uuid(plan_id, "plan_id")
    result = await db.execute(select(BillingPlan).where(BillingPlan.id == plan_uuid))
    plan = result.scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    if (
        isinstance(plan.is_active, bool)
        and not plan.is_active
        and (user is None or not user.is_superuser)
    ):
        raise HTTPException(status_code=404, detail="Plan not found")
    if include_inactive_prices and (user is None or not user.is_superuser):
        raise HTTPException(status_code=403, detail="Superadmin access required")
    return await _plan_to_response(
        db, plan, include_inactive_prices=include_inactive_prices
    )


@router.post("/plans", response_model=BillingPlanResponse)
async def create_billing_plan(
    payload: BillingPlanCreate,
    user: AuthenticatedUser = Depends(get_current_user),
    db: AsyncSession = Depends(postgres_session_dependency),
) -> BillingPlanResponse:
    await _require_superadmin(user)
    now = datetime.now(timezone.utc)
    plan = BillingPlan(
        key=payload.key,
        name=payload.name,
        description=payload.description,
        tier=payload.tier,
        is_active=payload.is_active,
        display_order=payload.display_order,
        stripe_product_id=payload.stripe_product_id,
        metadata_=payload.metadata,
        created_at=now,
        updated_at=now,
    )
    db.add(plan)
    await db.flush()
    created_plan_id = require_uuid(plan.id, "plan.id")
    await _replace_prices(db, created_plan_id, payload.prices)
    await _replace_bundles(db, created_plan_id, payload.bundle_ids)
    await db.flush()
    return await _plan_to_response(db, plan, include_inactive_prices=True)


@router.put("/plans/{plan_id}", response_model=BillingPlanResponse)
async def update_billing_plan(
    plan_id: str,
    payload: BillingPlanUpdate,
    user: AuthenticatedUser = Depends(get_current_user),
    db: AsyncSession = Depends(postgres_session_dependency),
) -> BillingPlanResponse:
    await _require_superadmin(user)
    plan_uuid = _parse_uuid(plan_id, "plan_id")
    result = await db.execute(select(BillingPlan).where(BillingPlan.id == plan_uuid))
    plan = result.scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=404, detail="Plan not found")

    resolved_plan_id = require_uuid(plan.id, "plan.id")
    if "metadata" in payload.model_fields_set:
        assign_attr(plan, "metadata_", payload.metadata or {})
    if "prices" in payload.model_fields_set and payload.prices is not None:
        await _replace_prices(db, resolved_plan_id, payload.prices)
    if "bundle_ids" in payload.model_fields_set and payload.bundle_ids is not None:
        await _replace_bundles(db, resolved_plan_id, payload.bundle_ids)
    updates = payload.model_dump(
        exclude_unset=True,
        exclude={"metadata", "prices", "bundle_ids"},
    )
    for field_name, value in updates.items():
        setattr(plan, field_name, value)
    assign_attr(plan, "updated_at", datetime.now(timezone.utc))
    await db.flush()
    return await _plan_to_response(db, plan, include_inactive_prices=True)


@router.delete("/plans/{plan_id}")
async def delete_billing_plan(
    plan_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
    db: AsyncSession = Depends(postgres_session_dependency),
) -> dict[str, bool]:
    await _require_superadmin(user)
    plan_uuid = _parse_uuid(plan_id, "plan_id")
    result = await db.execute(select(BillingPlan).where(BillingPlan.id == plan_uuid))
    plan = result.scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    assign_attr(plan, "is_active", False)
    assign_attr(plan, "updated_at", datetime.now(timezone.utc))
    await db.flush()
    return {"deleted": True}


@router.post("/plans/{plan_id}/sync-stripe", response_model=BillingPlanResponse)
async def sync_plan_to_stripe(
    plan_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
    db: AsyncSession = Depends(postgres_session_dependency),
) -> BillingPlanResponse:
    await _require_superadmin(user)
    plan_uuid = _parse_uuid(plan_id, "plan_id")
    result = await db.execute(select(BillingPlan).where(BillingPlan.id == plan_uuid))
    plan = result.scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=404, detail="Plan not found")

    client = get_stripe_client()
    if isinstance(plan.stripe_product_id, str) and plan.stripe_product_id:
        product_id = require_str(plan.stripe_product_id, "plan.stripe_product_id")
    else:
        plan_description = plan.description if isinstance(plan.description, str) else ""
        product_params: ProductCreateParams = {
            "name": require_str(plan.name, "plan.name"),
            "description": plan_description,
            "metadata": {
                "plan_key": require_str(plan.key, "plan.key"),
                "tier": normalize_billing_tier(plan.tier),
            },
        }
        product = client.products.create(params=product_params)
        product_id = product.id
        assign_attr(plan, "stripe_product_id", product_id)

    stripe_plan_id = require_uuid(plan.id, "plan.id")
    prices = await _load_prices(db, stripe_plan_id, include_inactive=True)
    for price in prices:
        if isinstance(price.stripe_price_id, str) and price.stripe_price_id:
            continue
        recurring_interval: Literal["month", "year"] = (
            "month"
            if require_str(price.interval, "price.interval")
            == BillingInterval.MONTHLY.value
            else "year"
        )
        price_params: PriceCreateParams = {
            "product": product_id,
            "unit_amount": require_int(price.amount, "price.amount"),
            "currency": require_str(price.currency, "price.currency"),
            "recurring": {"interval": recurring_interval},
            "metadata": {
                "plan_key": require_str(plan.key, "plan.key"),
                "interval": require_str(price.interval, "price.interval"),
            },
        }
        stripe_price = client.prices.create(params=price_params)
        assign_attr(price, "stripe_price_id", stripe_price.id)
        assign_attr(price, "updated_at", datetime.now(timezone.utc))

    assign_attr(plan, "updated_at", datetime.now(timezone.utc))
    await db.flush()
    return await _plan_to_response(db, plan, include_inactive_prices=True)
