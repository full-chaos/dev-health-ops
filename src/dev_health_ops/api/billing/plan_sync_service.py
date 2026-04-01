"""Shared service for syncing billing plans between the local DB and Stripe.

Used by both the admin CLI and the API endpoint to avoid logic duplication.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.billing import BillingInterval, BillingPlan, BillingPrice

from .stripe_client import get_stripe_client

logger = logging.getLogger(__name__)


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


async def _load_existing_plans(db: AsyncSession) -> dict[str, BillingPlan]:
    """Load all billing plans keyed by stripe_product_id, key, and slugified name."""
    result = await db.execute(select(BillingPlan))
    plans = list(result.scalars().all())

    by_stripe_id: dict[str, BillingPlan] = {}
    by_key: dict[str, BillingPlan] = {}
    by_slug: dict[str, BillingPlan] = {}

    for plan in plans:
        if plan.stripe_product_id:
            by_stripe_id[plan.stripe_product_id] = plan
        by_key[plan.key] = plan
        by_slug[_slugify(plan.name)] = plan

    return by_stripe_id, by_key, by_slug  # type: ignore[return-value]


def _match_plan(
    product_id: str,
    metadata: dict,
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
    stripe_prices: list[dict],
) -> None:
    """Create or update BillingPrice rows from Stripe price data."""
    result = await db.execute(
        select(BillingPrice).where(BillingPrice.plan_id == plan.id)
    )
    all_prices = list(result.scalars().all())
    existing = {p.stripe_price_id: p for p in all_prices if p.stripe_price_id}
    existing_by_interval = {p.interval: p for p in all_prices}

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
            price.amount = sp["unit_amount"]
            price.currency = sp["currency"]
            price.is_active = sp["active"]
            price.updated_at = now
        elif billing_interval in existing_by_interval:
            # Match by interval and attach stripe_price_id
            price = existing_by_interval[billing_interval]
            price.stripe_price_id = stripe_price_id
            price.amount = sp["unit_amount"]
            price.currency = sp["currency"]
            price.is_active = sp["active"]
            price.updated_at = now
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


def _fetch_all_products(client) -> list[dict]:
    """Fetch all active Stripe products with pagination."""
    products = []
    params: dict = {"active": True, "limit": 100}
    while True:
        response = client.products.list(params=params)
        products.extend(response.data)
        if not response.has_more:
            break
        params["starting_after"] = response.data[-1].id
    return products


def _fetch_prices_for_product(client, product_id: str) -> list[dict]:
    """Fetch all prices for a Stripe product."""
    prices = []
    params: dict = {"product": product_id, "active": True, "limit": 100}
    while True:
        response = client.prices.list(params=params)
        prices.extend(response.data)
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
        product_id = product.id
        name = product.name or ""
        metadata = product.metadata.to_dict() if product.metadata else {}

        try:
            prices = _fetch_prices_for_product(client, product_id)
            recurring_prices = [
                p for p in prices if getattr(p, "recurring", None) is not None
            ]

            if not recurring_prices:
                report.skipped.append(f"{name} ({product_id}): no recurring prices")
                continue

            existing_plan = _match_plan(
                product_id, metadata, name, by_stripe_id, by_key, by_slug
            )

            now = datetime.now(timezone.utc)

            if existing_plan:
                if not dry_run:
                    existing_plan.stripe_product_id = product_id
                    existing_plan.name = name or existing_plan.name
                    if metadata.get("tier"):
                        existing_plan.tier = metadata["tier"]
                    existing_plan.updated_at = now
                    await _upsert_prices(
                        db,
                        existing_plan,
                        [_price_to_dict(p) for p in recurring_prices],
                    )
                report.updated.append(f"{existing_plan.key} ← {product_id}")
            else:
                plan_key = metadata.get("plan_key") or _slugify(name)
                tier = metadata.get("tier", "team")
                if not dry_run:
                    plan = BillingPlan(
                        key=plan_key,
                        name=name,
                        description=product.description or None,
                        tier=tier,
                        stripe_product_id=product_id,
                        metadata_=metadata,
                        created_at=now,
                        updated_at=now,
                    )
                    db.add(plan)
                    await db.flush()
                    await _upsert_prices(
                        db,
                        plan,
                        [_price_to_dict(p) for p in recurring_prices],
                    )
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


def _price_to_dict(stripe_price) -> dict:
    """Normalize a Stripe price object to a plain dict."""
    return {
        "id": stripe_price.id,
        "unit_amount": stripe_price.unit_amount,
        "currency": stripe_price.currency,
        "active": stripe_price.active,
        "recurring": {
            "interval": stripe_price.recurring.interval,
        }
        if stripe_price.recurring
        else {},
    }


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
            product = client.products.create(
                params={
                    "name": plan.name,
                    "description": plan.description or "",
                    "metadata": {"plan_key": plan.key, "tier": plan.tier},
                }
            )
            plan.stripe_product_id = product.id

            price_result = await db.execute(
                select(BillingPrice).where(BillingPrice.plan_id == plan.id)
            )
            prices = list(price_result.scalars().all())
            now = datetime.now(timezone.utc)

            for price in prices:
                if price.stripe_price_id:
                    continue
                stripe_price = client.prices.create(
                    params={
                        "product": product.id,
                        "unit_amount": price.amount,
                        "currency": price.currency,
                        "recurring": {
                            "interval": "month"
                            if price.interval == BillingInterval.MONTHLY.value
                            else "year"
                        },
                        "metadata": {
                            "plan_key": plan.key,
                            "interval": price.interval,
                        },
                    }
                )
                price.stripe_price_id = stripe_price.id
                price.updated_at = now

            plan.updated_at = now
            report.created.append(f"{plan.key} → {product.id}")

        except Exception as exc:
            report.errors.append(f"{plan.key}: {exc}")
            logger.exception("Error syncing plan %s to Stripe", plan.key)

    if plans:
        await db.flush()

    return report
