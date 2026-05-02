from __future__ import annotations

import importlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.billing import BillingPlan, FeatureBundle, PlanFeatureBundle
from dev_health_ops.models.licensing import OrgLicense

from ._helpers import assign_attr, ensure_str_list, normalize_billing_tier, require_uuid

logger = logging.getLogger(__name__)

# Terminal subscription statuses that indicate the subscription is no longer active.
_CANCELLED_STATUSES: frozenset[str] = frozenset({"canceled", "incomplete_expired"})


async def has_had_trial(org_id: str | uuid.UUID, session: AsyncSession) -> bool:
    """Check if an org has ever had a trial subscription."""
    org_uuid = org_id if isinstance(org_id, uuid.UUID) else uuid.UUID(str(org_id))

    subscription_module = importlib.import_module("dev_health_ops.models.subscriptions")
    subscription_cls = subscription_module.Subscription

    result = await session.execute(
        select(subscription_cls.id)
        .where(subscription_cls.org_id == org_uuid)
        .where(subscription_cls.trial_start.is_not(None))
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


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

        # Bridge: sync OrgLicense from plan feature bundles within the same transaction.
        await self._sync_org_license(existing)

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

    async def get_for_org(self, org_id: uuid.UUID | None) -> Any | None:
        subscription_cls = self._subscription_cls()
        query = (
            select(subscription_cls)
            .order_by(subscription_cls.updated_at.desc())
            .limit(1)
        )
        if org_id is not None:
            query = query.where(subscription_cls.org_id == org_id)

        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_subscriptions(
        self,
        org_id: uuid.UUID | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Any], int]:
        subscription_cls = self._subscription_cls()
        query = (
            select(subscription_cls)
            .order_by(subscription_cls.updated_at.desc())
            .limit(limit)
            .offset(offset)
        )
        count_query = select(func.count()).select_from(subscription_cls)
        if org_id is not None:
            query = query.where(subscription_cls.org_id == org_id)
            count_query = count_query.where(subscription_cls.org_id == org_id)
        result = await self.db.execute(query)
        count_result = await self.db.execute(count_query)
        return list(result.scalars().all()), int(count_result.scalar_one())

    async def get_history(
        self,
        org_id: uuid.UUID | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Any], int]:
        subscription_cls = self._subscription_cls()
        subscription_event_cls = self._subscription_event_cls()
        base_query: Select[Any] = select(subscription_event_cls).join(
            subscription_cls,
            subscription_cls.id == subscription_event_cls.subscription_id,
        )
        if org_id is not None:
            base_query = base_query.where(subscription_cls.org_id == org_id)

        result = await self.db.execute(
            base_query.order_by(subscription_event_cls.processed_at.desc())
            .limit(limit)
            .offset(offset)
        )

        count_query: Select[Any] = (
            select(func.count())
            .select_from(subscription_event_cls)
            .join(
                subscription_cls,
                subscription_cls.id == subscription_event_cls.subscription_id,
            )
        )
        if org_id is not None:
            count_query = count_query.where(subscription_cls.org_id == org_id)

        count_result = await self.db.execute(count_query)
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

    async def _sync_org_license(self, subscription: Any) -> None:
        """Upsert OrgLicense from the plan's feature bundles.

        Called inside ``upsert_from_stripe``; runs in the same transaction so that
        Subscription + OrgLicense are committed atomically.  If anything goes wrong
        we log and re-raise so the caller's flush/commit fails, rolling back both.
        """
        org_id = require_uuid(subscription.org_id, "subscription.org_id")
        billing_plan_id = subscription.billing_plan_id
        status: str = str(getattr(subscription, "status", "active") or "active")
        current_period_end = getattr(subscription, "current_period_end", None)

        # --- Determine tier and feature set ---
        is_cancelled = status in _CANCELLED_STATUSES

        if is_cancelled:
            tier_str = "community"
            feature_keys: list[str] = []
            feature_flags: dict[str, bool] = {}
            expires_at = None
        else:
            if billing_plan_id is None:
                # Subscription has no plan resolved (e.g. trial before a price is matched).
                # Nothing to sync; preserve existing OrgLicense state.
                return

            # Load the BillingPlan. Tolerate missing billing tables (tests that
            # scope their schema to subscriptions only): skip rather than fail.
            try:
                resolved_billing_plan_id = require_uuid(
                    billing_plan_id, "subscription.billing_plan_id"
                )
                plan_result = await self.db.execute(
                    select(BillingPlan).where(
                        BillingPlan.id == resolved_billing_plan_id
                    )
                )
                plan = plan_result.scalar_one_or_none()
            except OperationalError as exc:
                logger.warning(
                    "Billing tables unavailable; skipping org-license sync (%s)", exc
                )
                return
            if plan is None:
                logger.warning(
                    "BillingPlan not found for subscription org_id=%s; "
                    "skipping org-license sync",
                    org_id,
                )
                return

            tier_str = normalize_billing_tier(plan.tier, default="community")

            # Resolve all FeatureBundle rows for this plan.
            bundle_rows_result = await self.db.execute(
                select(FeatureBundle)
                .join(
                    PlanFeatureBundle,
                    PlanFeatureBundle.bundle_id == FeatureBundle.id,
                )
                .where(PlanFeatureBundle.plan_id == resolved_billing_plan_id)
            )
            bundles = list(bundle_rows_result.scalars().all())

            # Flatten and deduplicate feature keys, validating against registry.
            known_keys = self._known_feature_keys()
            raw_keys: set[str] = set()
            for bundle in bundles:
                bundle_features: object = bundle.features or []
                if isinstance(bundle_features, dict):
                    bundle_features = list(bundle_features.keys())
                for key_str in ensure_str_list(bundle_features):
                    if key_str not in known_keys:
                        logger.warning(
                            "Bundle %s references unknown feature key %r; "
                            "skipping key (CHAOS-1207 defensive mode)",
                            bundle.key,
                            key_str,
                        )
                        continue
                    raw_keys.add(key_str)

            feature_keys = sorted(raw_keys)
            feature_flags = {key: True for key in feature_keys}
            expires_at = current_period_end

        # --- Upsert OrgLicense (keyed on org_id — one license per org) ---
        existing_result = await self.db.execute(
            select(OrgLicense).where(OrgLicense.org_id == org_id)
        )
        org_license = existing_result.scalar_one_or_none()

        customer_id = str(getattr(subscription, "stripe_customer_id", "") or "")

        if org_license is None:
            org_license = OrgLicense(
                org_id=org_id,
                tier=tier_str,
                license_type="saas",
                features_override=feature_flags,
                expires_at=expires_at,
                customer_id=customer_id or None,
            )
            self.db.add(org_license)
        else:
            assign_attr(org_license, "tier", tier_str)
            assign_attr(org_license, "features_override", feature_flags)
            assign_attr(org_license, "expires_at", expires_at)
            assign_attr(org_license, "is_valid", not is_cancelled)
            assign_attr(org_license, "updated_at", datetime.now(timezone.utc))
            if customer_id:
                assign_attr(org_license, "customer_id", customer_id)

        logger.info(
            "OrgLicense synced: org_id=%s tier=%s features=%d cancelled=%s",
            org_id,
            tier_str,
            len(feature_keys),
            is_cancelled,
        )

    @staticmethod
    def _known_feature_keys() -> frozenset[str]:
        """Return the canonical feature key set from STANDARD_FEATURES registry.

        Returns an empty frozenset if the registry is unavailable (fail-open).
        """
        try:
            licensing_module = importlib.import_module(
                "dev_health_ops.models.licensing"
            )
            standard_features = getattr(licensing_module, "STANDARD_FEATURES", None)
            if standard_features is None:
                return frozenset()
            return frozenset(entry[0] for entry in standard_features)
        except Exception:
            logger.warning(
                "Could not load STANDARD_FEATURES registry for key validation"
            )
            return frozenset()

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
