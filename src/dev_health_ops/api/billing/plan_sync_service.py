"""Shared service for syncing billing plans between the local DB and Stripe.

Used by both the admin CLI and the API endpoint to avoid logic duplication.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, TypedDict

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from stripe.params._price_create_params import PriceCreateParams
from stripe.params._product_create_params import ProductCreateParams

from dev_health_ops.models.billing import BillingInterval, BillingPlan, BillingPrice

from ._helpers import (
    assign_attr,
    ensure_str_dict,
    normalize_billing_tier,
    require_int,
    require_str,
)
from .stripe_client import get_stripe_client

logger = logging.getLogger(__name__)


class StripeProductRecord(TypedDict):
    id: str
    name: str
    description: str | None
    metadata: dict[str, str]


class StripeRecurringRecord(TypedDict):
    interval: str


class StripePriceRecord(TypedDict):
    id: str
    unit_amount: int
    currency: str
    active: bool
    recurring: StripeRecurringRecord


@dataclass
class SyncReport:
    """Summary of a pull/sync operation."""

    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, list[str]]:
        return {
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
        }


def _slugify(name: str) -> str:
    """Convert a product name to a plan key slug."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _stripe_interval_to_billing(interval: str) -> str | None:
    """Map Stripe interval string to BillingInterval value."""
    if interval == "month":
        return BillingInterval.MONTHLY.value
    if interval == "year":
        return BillingInterval.YEARLY.value
    return None


async def _load_existing_plans(
    db: AsyncSession,
) -> tuple[dict[str, BillingPlan], dict[str, BillingPlan], dict[str, BillingPlan]]:
    """Load all billing plans keyed by stripe_product_id, key, and slugified name."""
    result = await db.execute(select(BillingPlan))
    plans = list(result.scalars().all())

    by_stripe_id: dict[str, BillingPlan] = {}
    by_key: dict[str, BillingPlan] = {}
    by_slug: dict[str, BillingPlan] = {}

    for plan in plans:
        if isinstance(plan.stripe_product_id, str) and plan.stripe_product_id:
            by_stripe_id[
                require_str(plan.stripe_product_id, "plan.stripe_product_id")
            ] = plan
        by_key[require_str(plan.key, "plan.key")] = plan
        by_slug[_slugify(require_str(plan.name, "plan.name"))] = plan

    return by_stripe_id, by_key, by_slug


def _match_plan(
    product_id: str,
    metadata: dict[str, str],
    name: str,
    by_stripe_id: dict[str, BillingPlan],
    by_key: dict[str, BillingPlan],
    by_slug: dict[str, BillingPlan],
) -> BillingPlan | None:
    """Try to match a Stripe product to an existing local plan."""
    if product_id in by_stripe_id:
        return by_stripe_id[product_id]

    plan_key = metadata.get("plan_key", "")
    if plan_key and plan_key in by_key:
        return by_key[plan_key]

    slug = _slugify(name)
    if slug in by_slug:
        return by_slug[slug]

    return None


async def _upsert_prices(
    db: AsyncSession,
    plan: BillingPlan,
    stripe_prices: list[StripePriceRecord],
) -> None:
    """Create or update BillingPrice rows from Stripe price data."""
    result = await db.execute(
        select(BillingPrice).where(BillingPrice.plan_id == plan.id)
    )
    all_prices = list(result.scalars().all())
    existing = {
        require_str(p.stripe_price_id, "price.stripe_price_id"): p
        for p in all_prices
        if isinstance(p.stripe_price_id, str) and p.stripe_price_id
    }
    existing_by_interval = {
        require_str(p.interval, "price.interval"): p for p in all_prices
    }

    now = datetime.now(timezone.utc)
    for sp in stripe_prices:
        billing_interval = _stripe_interval_to_billing(
            sp.get("recurring", {}).get("interval", "")
        )
        if not billing_interval:
            continue

        stripe_price_id = sp["id"]

        if stripe_price_id in existing:
            # Update existing price matched by stripe_price_id
            price = existing[stripe_price_id]
            assign_attr(price, "amount", sp["unit_amount"])
            assign_attr(price, "currency", sp["currency"])
            assign_attr(price, "is_active", sp["active"])
            assign_attr(price, "updated_at", now)
        elif billing_interval in existing_by_interval:
            # Match by interval and attach stripe_price_id
            price = existing_by_interval[billing_interval]
            assign_attr(price, "stripe_price_id", stripe_price_id)
            assign_attr(price, "amount", sp["unit_amount"])
            assign_attr(price, "currency", sp["currency"])
            assign_attr(price, "is_active", sp["active"])
            assign_attr(price, "updated_at", now)
        else:
            # Create new price
            db.add(
                BillingPrice(
                    plan_id=plan.id,
                    interval=billing_interval,
                    amount=sp["unit_amount"],
                    currency=sp["currency"],
                    is_active=sp["active"],
                    stripe_price_id=stripe_price_id,
                    created_at=now,
                    updated_at=now,
                )
            )


def _product_to_record(product: object) -> StripeProductRecord | None:
    product_id = getattr(product, "id", None)
    name = getattr(product, "name", None)
    if not isinstance(product_id, str) or not isinstance(name, str):
        return None

    metadata_obj = getattr(product, "metadata", None)
    to_dict = getattr(metadata_obj, "to_dict", None)
    metadata_source = to_dict() if callable(to_dict) else metadata_obj
    metadata = ensure_str_dict(metadata_source)
    description = getattr(product, "description", None)
    return {
        "id": product_id,
        "name": name,
        "description": description if isinstance(description, str) else None,
        "metadata": metadata,
    }


def _fetch_all_products(client) -> list[StripeProductRecord]:
    """Fetch all active Stripe products with pagination."""
    products: list[StripeProductRecord] = []
    params: dict = {"active": True, "limit": 100}
    while True:
        response = client.products.list(params=params)
        for product in response.data:
            record = _product_to_record(product)
            if record is not None:
                products.append(record)
        if not response.has_more:
            break
        params["starting_after"] = response.data[-1].id
    return products


def _price_to_record(stripe_price: object) -> StripePriceRecord | None:
    price_id = getattr(stripe_price, "id", None)
    unit_amount = getattr(stripe_price, "unit_amount", None)
    currency = getattr(stripe_price, "currency", None)
    active = getattr(stripe_price, "active", None)
    recurring = getattr(stripe_price, "recurring", None)
    interval = getattr(recurring, "interval", None) if recurring is not None else None
    if (
        not isinstance(price_id, str)
        or not isinstance(unit_amount, int)
        or not isinstance(currency, str)
        or not isinstance(active, bool)
        or not isinstance(interval, str)
    ):
        return None
    return {
        "id": price_id,
        "unit_amount": unit_amount,
        "currency": currency,
        "active": active,
        "recurring": {"interval": interval},
    }


def _fetch_prices_for_product(client, product_id: str) -> list[StripePriceRecord]:
    """Fetch all prices for a Stripe product."""
    prices: list[StripePriceRecord] = []
    params: dict = {"product": product_id, "active": True, "limit": 100}
    while True:
        response = client.prices.list(params=params)
        for stripe_price in response.data:
            record = _price_to_record(stripe_price)
            if record is not None:
                prices.append(record)
        if not response.has_more:
            break
        params["starting_after"] = response.data[-1].id
    return prices


async def pull_from_stripe(
    db: AsyncSession,
    dry_run: bool = False,
) -> SyncReport:
    """Pull plans from Stripe into the local database.

    For each active Stripe product with recurring prices:
    - Match to existing BillingPlan by stripe_product_id, then plan_key metadata, then slugified name
    - Create or update the plan and upsert prices by stripe_price_id
    """
    report = SyncReport()
    client = get_stripe_client()

    by_stripe_id, by_key, by_slug = await _load_existing_plans(db)
    products = _fetch_all_products(client)

    for product in products:
        product_id = product["id"]
        name = product["name"]
        metadata = product["metadata"]

        try:
            prices = _fetch_prices_for_product(client, product_id)
            recurring_prices = prices

            if not recurring_prices:
                report.skipped.append(f"{name} ({product_id}): no recurring prices")
                continue

            existing_plan = _match_plan(
                product_id, metadata, name, by_stripe_id, by_key, by_slug
            )

            now = datetime.now(timezone.utc)

            if existing_plan:
                if not dry_run:
                    assign_attr(existing_plan, "stripe_product_id", product_id)
                    assign_attr(
                        existing_plan,
                        "name",
                        name or require_str(existing_plan.name, "plan.name"),
                    )
                    if metadata.get("tier"):
                        assign_attr(
                            existing_plan,
                            "tier",
                            normalize_billing_tier(metadata["tier"]),
                        )
                    assign_attr(existing_plan, "updated_at", now)
                    await _upsert_prices(db, existing_plan, recurring_prices)
                report.updated.append(f"{existing_plan.key} ← {product_id}")
            else:
                plan_key = metadata.get("plan_key") or _slugify(name)
                tier = normalize_billing_tier(metadata.get("tier"))
                if not dry_run:
                    plan = BillingPlan(
                        key=plan_key,
                        name=name,
                        description=product["description"],
                        tier=tier,
                        stripe_product_id=product_id,
                        metadata_=metadata,
                        created_at=now,
                        updated_at=now,
                    )
                    db.add(plan)
                    await db.flush()
                    await _upsert_prices(db, plan, recurring_prices)
                    # Keep lookup dicts current so later products can
                    # match plans created earlier in this same loop.
                    by_stripe_id[product_id] = plan
                    by_key[plan_key] = plan
                    by_slug[_slugify(name)] = plan
                report.created.append(f"{plan_key} ← {product_id}")

        except IntegrityError as exc:
            await db.rollback()
            report.errors.append(f"{name} ({product_id}): {exc}")
            logger.exception("Error pulling product %s", product_id)
            # Re-load lookups since rollback may have invalidated state
            by_stripe_id, by_key, by_slug = await _load_existing_plans(db)
        except Exception as exc:
            report.errors.append(f"{name} ({product_id}): {exc}")
            logger.exception("Error pulling product %s", product_id)

    if not dry_run:
        await db.flush()

    return report


async def sync_all_to_stripe(db: AsyncSession) -> SyncReport:
    """Push all local plans without a stripe_product_id to Stripe.

    Creates Stripe products and prices, then stores the IDs locally.
    """
    report = SyncReport()
    client = get_stripe_client()

    result = await db.execute(
        select(BillingPlan).where(
            BillingPlan.is_active.is_(True),
            BillingPlan.stripe_product_id.is_(None),
        )
    )
    plans = list(result.scalars().all())

    for plan in plans:
        try:
            plan_description = (
                plan.description if isinstance(plan.description, str) else ""
            )
            product_params: ProductCreateParams = {
                "name": require_str(plan.name, "plan.name"),
                "description": plan_description,
                "metadata": {
                    "plan_key": require_str(plan.key, "plan.key"),
                    "tier": normalize_billing_tier(plan.tier),
                },
            }
            product = client.products.create(params=product_params)
            assign_attr(plan, "stripe_product_id", product.id)

            price_result = await db.execute(
                select(BillingPrice).where(BillingPrice.plan_id == plan.id)
            )
            prices = list(price_result.scalars().all())
            now = datetime.now(timezone.utc)

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
                    "product": product.id,
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
                assign_attr(price, "updated_at", now)

            assign_attr(plan, "updated_at", now)
            report.created.append(f"{plan.key} → {product.id}")

        except Exception as exc:
            report.errors.append(f"{plan.key}: {exc}")
            logger.exception("Error syncing plan %s to Stripe", plan.key)

    if plans:
        await db.flush()

    return report
