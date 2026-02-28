from __future__ import annotations

import importlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class SubscriptionService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def upsert_from_stripe(
        self,
        stripe_sub: Any,
        org_id: uuid.UUID,
    ) -> Any:
        stripe_subscription_id = str(getattr(stripe_sub, "id", ""))
        stripe_customer_id = str(getattr(stripe_sub, "customer", ""))
        if not stripe_subscription_id or not stripe_customer_id:
            raise ValueError("Stripe subscription payload missing required identifiers")

        existing = await self._get_by_stripe_subscription_id(stripe_subscription_id)

        billing_price_id, billing_plan_id = await self._resolve_plan_and_price_ids(
            stripe_sub,
            existing,
        )

        if existing is None:
            subscription_cls = self._subscription_cls()
            existing = subscription_cls(
                org_id=org_id,
                billing_plan_id=billing_plan_id,
                billing_price_id=billing_price_id,
                stripe_subscription_id=stripe_subscription_id,
                stripe_customer_id=stripe_customer_id,
                status=str(getattr(stripe_sub, "status", "incomplete")),
                current_period_start=self._to_dt(
                    getattr(stripe_sub, "current_period_start", None)
                ),
                current_period_end=self._to_dt(
                    getattr(stripe_sub, "current_period_end", None)
                ),
            )
            self.db.add(existing)
        else:
            existing.org_id = org_id
            existing.billing_plan_id = billing_plan_id
            existing.billing_price_id = billing_price_id
            existing.stripe_customer_id = stripe_customer_id
            existing.status = str(getattr(stripe_sub, "status", existing.status))
            existing.current_period_start = self._to_dt(
                getattr(
                    stripe_sub,
                    "current_period_start",
                    existing.current_period_start,
                )
            )
            existing.current_period_end = self._to_dt(
                getattr(
                    stripe_sub,
                    "current_period_end",
                    existing.current_period_end,
                )
            )

        existing.cancel_at_period_end = bool(
            getattr(stripe_sub, "cancel_at_period_end", False)
        )
        existing.canceled_at = self._to_dt(
            getattr(stripe_sub, "canceled_at", None), True
        )
        existing.trial_start = self._to_dt(
            getattr(stripe_sub, "trial_start", None), True
        )
        existing.trial_end = self._to_dt(getattr(stripe_sub, "trial_end", None), True)
        existing.metadata_ = self._as_dict(getattr(stripe_sub, "metadata", {}))
        existing.updated_at = datetime.now(timezone.utc)

        await self.db.flush()
        return existing

    async def process_event(self, event: Any) -> None:
        event_id = str(getattr(event, "id", ""))
        if not event_id:
            raise ValueError("Stripe event is missing id")

        if await self._event_exists(event_id):
            return

        event_type = str(getattr(event, "type", ""))
        if not event_type.startswith("customer.subscription."):
            return

        stripe_sub = getattr(getattr(event, "data", None), "object", None)
        if stripe_sub is None:
            raise ValueError("Stripe event data.object is missing")

        metadata = self._as_dict(getattr(stripe_sub, "metadata", {}))
        org_id_value = metadata.get("org_id")
        if not org_id_value:
            logger.warning(
                "Subscription event %s has no org_id in metadata, skipping", event_id
            )
            return

        org_id = uuid.UUID(str(org_id_value))
        previous_status = await self._get_previous_status(getattr(stripe_sub, "id", ""))
        subscription = await self.upsert_from_stripe(stripe_sub, org_id)

        try:
            self.db.add(
                self._subscription_event_cls()(
                    subscription_id=subscription.id,
                    stripe_event_id=event_id,
                    event_type=event_type,
                    previous_status=previous_status,
                    new_status=subscription.status,
                    payload=self._serialize_payload(event),
                )
            )
            await self.db.flush()
        except IntegrityError:
            await self.db.rollback()
            return

    async def get_for_org(self, org_id: uuid.UUID) -> Any | None:
        subscription_cls = self._subscription_cls()
        result = await self.db.execute(
            select(subscription_cls)
            .where(subscription_cls.org_id == org_id)
            .order_by(subscription_cls.updated_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_history(
        self,
        org_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> tuple[list[Any], int]:
        subscription_cls = self._subscription_cls()
        subscription_event_cls = self._subscription_event_cls()
        base_query: Select[Any] = (
            select(subscription_event_cls)
            .join(
                subscription_cls,
                subscription_cls.id == subscription_event_cls.subscription_id,
            )
            .where(subscription_cls.org_id == org_id)
        )
        result = await self.db.execute(
            base_query.order_by(subscription_event_cls.processed_at.desc())
            .limit(limit)
            .offset(offset)
        )

        count_result = await self.db.execute(
            select(func.count())
            .select_from(subscription_event_cls)
            .join(
                subscription_cls,
                subscription_cls.id == subscription_event_cls.subscription_id,
            )
            .where(subscription_cls.org_id == org_id)
        )
        return list(result.scalars().all()), int(count_result.scalar_one())

    async def _event_exists(self, stripe_event_id: str) -> bool:
        subscription_event_cls = self._subscription_event_cls()
        result = await self.db.execute(
            select(subscription_event_cls.id).where(
                subscription_event_cls.stripe_event_id == stripe_event_id
            )
        )
        return result.scalar_one_or_none() is not None

    async def _get_previous_status(self, stripe_subscription_id: str) -> str | None:
        if not stripe_subscription_id:
            return None
        subscription_cls = self._subscription_cls()
        result = await self.db.execute(
            select(subscription_cls.status).where(
                subscription_cls.stripe_subscription_id == stripe_subscription_id
            )
        )
        return result.scalar_one_or_none()

    async def _get_by_stripe_subscription_id(
        self,
        stripe_subscription_id: str,
    ) -> Any | None:
        subscription_cls = self._subscription_cls()
        result = await self.db.execute(
            select(subscription_cls).where(
                subscription_cls.stripe_subscription_id == stripe_subscription_id
            )
        )
        return result.scalar_one_or_none()

    async def _resolve_plan_and_price_ids(
        self,
        stripe_sub: Any,
        existing: Any | None,
    ) -> tuple[uuid.UUID, uuid.UUID]:
        stripe_price_id = self._extract_stripe_price_id(stripe_sub)
        if stripe_price_id:
            price = await self._lookup_billing_price(stripe_price_id)
            if price is not None:
                return price.id, price.plan_id

        if existing is not None:
            return existing.billing_price_id, existing.billing_plan_id

        raise ValueError("Unable to resolve billing plan and price for subscription")

    async def _lookup_billing_price(self, stripe_price_id: str) -> Any | None:
        try:
            billing_module = importlib.import_module("dev_health_ops.models.billing")
        except ImportError:
            return None
        BillingPrice = getattr(billing_module, "BillingPrice", None)
        if BillingPrice is None:
            return None
        stripe_id_field = None
        for candidate in (
            "stripe_price_id",
            "stripe_id",
            "provider_price_id",
            "external_price_id",
        ):
            field = getattr(BillingPrice, candidate, None)
            if field is not None:
                stripe_id_field = field
                break

        if stripe_id_field is None:
            return None

        result = await self.db.execute(
            select(BillingPrice).where(stripe_id_field == stripe_price_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    def _extract_stripe_price_id(stripe_sub: Any) -> str | None:
        items = getattr(stripe_sub, "items", None)
        for item in getattr(items, "data", []) or []:
            price = getattr(item, "price", None)
            if price is not None and getattr(price, "id", None):
                return str(price.id)
        return None

    @staticmethod
    def _subscription_cls() -> Any:
        module = importlib.import_module("dev_health_ops.models.subscriptions")
        return module.Subscription

    @staticmethod
    def _subscription_event_cls() -> Any:
        module = importlib.import_module("dev_health_ops.models.subscriptions")
        return module.SubscriptionEvent

    @staticmethod
    def _to_dt(value: Any, nullable: bool = False) -> datetime | None:
        if value is None:
            if nullable:
                return None
            return datetime.now(timezone.utc)
        if isinstance(value, datetime):
            return value
        if isinstance(value, (float, int)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                if nullable:
                    return None
                return datetime.now(timezone.utc)
        if nullable:
            return None
        return datetime.now(timezone.utc)

    @staticmethod
    def _as_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if hasattr(value, "to_dict"):
            out = value.to_dict()
            return out if isinstance(out, dict) else {}
        if hasattr(value, "__dict__"):
            out = dict(value.__dict__)
            out.pop("_requestor", None)
            out.pop("_last_response", None)
            return out
        return {}

    @classmethod
    def _serialize_payload(cls, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            payload = value
        elif hasattr(value, "to_dict"):
            maybe = value.to_dict()
            payload = maybe if isinstance(maybe, dict) else cls._as_dict(value)
        else:
            payload = cls._as_dict(value)

        try:
            json.dumps(payload)
            return payload
        except TypeError:
            return json.loads(json.dumps(payload, default=str))
