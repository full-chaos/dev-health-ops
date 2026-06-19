from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import TYPE_CHECKING, TypedDict

from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.db import resolve_db_uri

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dev_health_ops.api.services.configuration import SettingsService


class _PlanPrice(TypedDict):
    interval: str
    amount: int
    currency: str


class _StandardPlan(TypedDict):
    key: str
    name: str
    description: str
    tier: str
    display_order: int
    prices: list[_PlanPrice]


async def _get_session(ns: argparse.Namespace) -> AsyncSession:
    engine = create_async_engine(resolve_db_uri(ns), pool_pre_ping=True)
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    return async_session()


async def _create_user_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import UserService

    session = await _get_session(ns)
    try:
        svc = UserService(session)
        user = await svc.create(
            email=ns.email,
            password=ns.password,
            username=ns.username,
            full_name=ns.full_name,
            is_superuser=ns.superuser,
            is_verified=True,
        )
        await session.commit()
        print(f"Created user: {user.email} (id: {user.id})")
        if ns.superuser:
            print("  [superuser]")
        return 0
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def create_user(ns: argparse.Namespace) -> int:
    return asyncio.run(_create_user_async(ns))


async def _create_org_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import OrganizationService, UserService

    session = await _get_session(ns)
    try:
        org_svc = OrganizationService(session)

        owner_user_id = None
        if ns.owner_email:
            user_svc = UserService(session)
            owner = await user_svc.get_by_email(ns.owner_email)
            if not owner:
                print(f"Error: User with email {ns.owner_email} not found")
                return 1
            owner_user_id = str(owner.id)

        org = await org_svc.create(
            name=ns.name,
            slug=ns.slug,
            description=ns.description,
            tier=ns.tier,
            owner_user_id=owner_user_id,
        )
        await session.commit()
        print(f"Created organization: {org.name} (slug: {org.slug}, id: {org.id})")
        if owner_user_id:
            print(f"  Owner: {ns.owner_email}")
        return 0
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def create_org(ns: argparse.Namespace) -> int:
    return asyncio.run(_create_org_async(ns))


async def _delete_org_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.org_deletion import OrganizationDeletionService
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    session = await _get_session(ns)
    clickhouse_sink = None
    try:
        analytics_db = getattr(ns, "analytics_db", None)
        if analytics_db:
            clickhouse_sink = ClickHouseMetricsSink(dsn=analytics_db)
        service = OrganizationDeletionService(
            session,
            clickhouse_client=clickhouse_sink,
        )
        result = await service.delete(ns.org_id, dry_run=ns.dry_run)
        if ns.dry_run:
            await session.rollback()
        else:
            await session.commit()
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return 0
    except ValueError as e:
        await session.rollback()
        print(f"Error: {e}")
        return 1
    except Exception as e:
        await session.rollback()
        print(f"Error: {e}")
        return 1
    finally:
        if clickhouse_sink is not None:
            clickhouse_sink.close()
        await session.close()


def delete_org(ns: argparse.Namespace) -> int:
    return asyncio.run(_delete_org_async(ns))


def _llm_settings_payload_json(response: BaseModel) -> str:
    return json.dumps(response.model_dump(), indent=2, sort_keys=True)


def _get_llm_settings_service(
    ns: argparse.Namespace, session: AsyncSession
) -> SettingsService:
    from dev_health_ops.api.services.configuration import SettingsService

    return SettingsService(session, ns.org)


async def _require_cli_byo_llm_access(
    ns: argparse.Namespace, session: AsyncSession, *, for_cleanup: bool = False
) -> None:
    from dev_health_ops.api.admin.llm_settings import require_byo_llm_access

    await require_byo_llm_access(session, ns.org, for_cleanup=for_cleanup)


def _normalize_llm_provider(provider: str | None) -> str:
    if provider is None:
        raise ValueError("provider is required")
    normalized = provider.strip()
    if not normalized:
        raise ValueError("provider must not be empty")
    return normalized


async def _llm_settings_get_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.admin.llm_settings import (
        LLMSettingsAccessError,
        get_llm_settings_response,
    )

    session = await _get_session(ns)
    try:
        await _require_cli_byo_llm_access(ns, session)
        svc = _get_llm_settings_service(ns, session)
        print(_llm_settings_payload_json(await get_llm_settings_response(svc)))
        return 0
    except LLMSettingsAccessError as exc:
        await session.rollback()
        print(f"Error: {exc.message}")
        return 1
    finally:
        await session.close()


def llm_settings_get(ns: argparse.Namespace) -> int:
    return asyncio.run(_llm_settings_get_async(ns))


async def _llm_settings_set_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.admin.llm_settings import (
        LLMSettingsAccessError,
        upsert_llm_settings,
    )
    from dev_health_ops.api.admin.schemas import LLMSettingsUpsert

    try:
        provider = _normalize_llm_provider(ns.provider)
        payload = LLMSettingsUpsert(
            provider=provider,
            model=ns.model,
            api_key=ns.api_key,
            base_url=ns.base_url,
        )
    except (ValidationError, ValueError) as exc:
        detail = (
            exc.errors()[0]["msg"] if isinstance(exc, ValidationError) else str(exc)
        )
        print(f"Error: invalid LLM settings input: {detail}")
        return 1

    session = await _get_session(ns)
    try:
        await _require_cli_byo_llm_access(ns, session)
        svc = _get_llm_settings_service(ns, session)
        response = await upsert_llm_settings(svc, payload)
        await session.commit()
        print(_llm_settings_payload_json(response))
        return 0
    except LLMSettingsAccessError as exc:
        await session.rollback()
        print(f"Error: {exc.message}")
        return 1
    except Exception:
        await session.rollback()
        print("Error: failed to update LLM settings")
        return 1
    finally:
        await session.close()


def llm_settings_set(ns: argparse.Namespace) -> int:
    return asyncio.run(_llm_settings_set_async(ns))


async def _llm_settings_delete_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.admin.llm_settings import (
        LLMSettingsAccessError,
        delete_llm_settings,
    )

    session = await _get_session(ns)
    try:
        # Cleanup must work even when the flag is disabled or the org has been
        # downgraded below the BYO tier (CHAOS-2551 review).
        await _require_cli_byo_llm_access(ns, session, for_cleanup=True)
        svc = _get_llm_settings_service(ns, session)
        deleted = await delete_llm_settings(svc)
        if not deleted:
            await session.rollback()
            print("Error: LLM settings not found")
            return 1
        await session.commit()
        print(json.dumps({"deleted": True}, indent=2, sort_keys=True))
        return 0
    except LLMSettingsAccessError as exc:
        await session.rollback()
        print(f"Error: {exc.message}")
        return 1
    except Exception:
        await session.rollback()
        print("Error: failed to delete LLM settings")
        return 1
    finally:
        await session.close()


def llm_settings_delete(ns: argparse.Namespace) -> int:
    return asyncio.run(_llm_settings_delete_async(ns))


async def _list_users_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import UserService

    session = await _get_session(ns)
    try:
        svc = UserService(session)
        users = await svc.list_all(limit=ns.limit, active_only=not ns.include_inactive)
        if not users:
            print("No users found.")
            return 0
        print(
            f"{'ID':<40} {'Email':<30} {'Username':<20} {'Superuser':<10} {'Active':<8}"
        )
        print("-" * 108)
        for u in users:
            is_superuser = bool(getattr(u, "is_superuser"))
            is_active = bool(getattr(u, "is_active"))
            print(
                f"{str(u.id):<40} {u.email:<30} {(u.username or ''):<20} "
                f"{'Yes' if is_superuser else 'No':<10} {'Yes' if is_active else 'No':<8}"
            )
        return 0
    finally:
        await session.close()


def list_users(ns: argparse.Namespace) -> int:
    return asyncio.run(_list_users_async(ns))


async def _update_user_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import (
        MembershipService,
        OrganizationService,
        UserService,
    )
    from dev_health_ops.models.users import MemberRole

    has_profile_change = any(
        value is not None
        for value in (
            ns.new_email,
            ns.new_username,
            ns.full_name,
            ns.active,
            ns.verified,
            ns.superuser,
        )
    )
    has_action = (
        has_profile_change
        or ns.password is not None
        or ns.membership_org is not None
        or ns.remove_from_org is not None
    )
    if not has_action:
        print("Error: No updates specified. Provide at least one field to update.")
        return 1
    if ns.role is not None and ns.membership_org is None:
        print("Error: --role requires --org.")
        return 1

    session = await _get_session(ns)
    try:
        user_svc = UserService(session)

        user = None
        if ns.id:
            user = await user_svc.get_by_id(ns.id)
        elif ns.email:
            user = await user_svc.get_by_email(ns.email)
        elif ns.username:
            user = await user_svc.get_by_username(ns.username)
        else:
            print("Error: Identify the user with --id, --email, or --username.")
            return 1

        if not user:
            print("Error: User not found.")
            return 1

        user_id = str(user.id)
        changes: list[str] = []

        if has_profile_change:
            await user_svc.update(
                user_id,
                email=ns.new_email,
                username=ns.new_username,
                full_name=ns.full_name,
                is_active=ns.active,
                is_verified=ns.verified,
                is_superuser=ns.superuser,
            )
            if ns.new_email is not None:
                changes.append(f"email -> {ns.new_email.lower().strip()}")
            if ns.new_username is not None:
                changes.append(f"username -> {ns.new_username or '(cleared)'}")
            if ns.full_name is not None:
                changes.append("full_name updated")
            if ns.active is not None:
                changes.append(f"active -> {ns.active}")
            if ns.verified is not None:
                changes.append(f"verified -> {ns.verified}")
            if ns.superuser is not None:
                changes.append(f"superuser -> {ns.superuser}")

        if ns.password is not None:
            await user_svc.set_password(user_id, ns.password)
            changes.append("password updated (existing sessions revoked)")

        org_svc = OrganizationService(session)
        membership_svc = MembershipService(session)

        if ns.membership_org is not None:
            org = await _resolve_org(org_svc, ns.membership_org)
            if org is None:
                print(f"Error: Organization '{ns.membership_org}' not found.")
                await session.rollback()
                return 1
            org_id = str(org.id)
            existing = await membership_svc.get_membership(org_id, user_id)
            if existing is not None:
                role = ns.role or str(existing.role)
                await membership_svc.update_role(org_id, user_id, role)
                changes.append(f"org '{org.slug}' role -> {role}")
            else:
                role = ns.role or MemberRole.MEMBER.value
                await membership_svc.add_member(
                    org_id=org_id, user_id=user_id, role=role
                )
                changes.append(f"added to org '{org.slug}' as {role}")

        if ns.remove_from_org is not None:
            org = await _resolve_org(org_svc, ns.remove_from_org)
            if org is None:
                print(f"Error: Organization '{ns.remove_from_org}' not found.")
                await session.rollback()
                return 1
            removed = await membership_svc.remove_member(str(org.id), user_id)
            if removed:
                changes.append(f"removed from org '{org.slug}'")
            else:
                changes.append(f"not a member of org '{org.slug}' (no change)")

        await session.commit()
        print(f"Updated user: {user.email} (id: {user_id})")
        for change in changes:
            print(f"  {change}")
        return 0
    except ValueError as e:
        await session.rollback()
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


async def _resolve_org(org_svc, identifier: str):
    org = await org_svc.get_by_slug(identifier)
    if org is not None:
        return org
    try:
        return await org_svc.get_by_id(identifier)
    except ValueError:
        return None


def update_user(ns: argparse.Namespace) -> int:
    return asyncio.run(_update_user_async(ns))


async def _list_orgs_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import OrganizationService

    session = await _get_session(ns)
    try:
        svc = OrganizationService(session)
        orgs = await svc.list_all(limit=ns.limit, active_only=not ns.include_inactive)
        if not orgs:
            print("No organizations found.")
            return 0
        print(f"{'ID':<40} {'Slug':<20} {'Name':<30} {'Tier':<10} {'Active':<8}")
        print("-" * 108)
        for o in orgs:
            is_active = bool(getattr(o, "is_active"))
            print(
                f"{str(o.id):<40} {o.slug:<20} {o.name:<30} "
                f"{o.tier:<10} {'Yes' if is_active else 'No':<8}"
            )
        return 0
    finally:
        await session.close()


def list_orgs(ns: argparse.Namespace) -> int:
    return asyncio.run(_list_orgs_async(ns))


async def _seed_features_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.licensing import seed_feature_flags_async

    session = await _get_session(ns)
    try:
        created = await seed_feature_flags_async(session)
        if created > 0:
            print(f"Seeded {created} feature flags.")
        else:
            print("All feature flags already exist.")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def seed_features(ns: argparse.Namespace) -> int:
    return asyncio.run(_seed_features_async(ns))


async def _seed_billing_plans_async(ns: argparse.Namespace) -> int:
    from sqlalchemy import select

    from dev_health_ops.models.billing import BillingPlan, BillingPrice

    STANDARD_PLANS: list[_StandardPlan] = [
        {
            "key": "community",
            "name": "Community",
            "description": "For individuals and small teams getting started with engineering analytics.",
            "tier": "community",
            "display_order": 0,
            "prices": [
                {"interval": "monthly", "amount": 0, "currency": "usd"},
                {"interval": "yearly", "amount": 0, "currency": "usd"},
            ],
        },
        {
            "key": "team",
            "name": "Team",
            "description": "For growing teams that need full visibility into delivery health and investment patterns.",
            "tier": "team",
            "display_order": 1,
            "prices": [
                {"interval": "monthly", "amount": 1200, "currency": "usd"},
                {"interval": "yearly", "amount": 11500, "currency": "usd"},
            ],
        },
        {
            "key": "enterprise",
            "name": "Enterprise",
            "description": "For organizations that need enterprise-grade security, compliance, and dedicated support.",
            "tier": "enterprise",
            "display_order": 2,
            "prices": [
                {"interval": "monthly", "amount": 12900, "currency": "usd"},
                {"interval": "yearly", "amount": 124000, "currency": "usd"},
            ],
        },
    ]

    session = await _get_session(ns)
    try:
        result = await session.execute(select(BillingPlan.key))
        existing = {row[0] for row in result.all()}
        created = 0

        for plan_data in STANDARD_PLANS:
            if plan_data["key"] in existing:
                continue

            plan = BillingPlan(
                key=plan_data["key"],
                name=plan_data["name"],
                description=plan_data["description"],
                tier=plan_data["tier"],
                display_order=plan_data["display_order"],
            )
            session.add(plan)
            await session.flush()  # get plan.id

            for price_data in plan_data["prices"]:
                price = BillingPrice(
                    plan_id=plan.id,
                    interval=price_data["interval"],
                    amount=price_data["amount"],
                    currency=price_data["currency"],
                )
                session.add(price)

            created += 1

        if created:
            await session.commit()
            print(f"Seeded {created} billing plan(s) with prices.")
        else:
            print("All standard billing plans already exist.")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def seed_billing_plans(ns: argparse.Namespace) -> int:
    return asyncio.run(_seed_billing_plans_async(ns))


async def _billing_list_async(ns: argparse.Namespace) -> int:
    from sqlalchemy import select

    from dev_health_ops.models.billing import BillingPlan, BillingPrice

    session = await _get_session(ns)
    try:
        result = await session.execute(
            select(BillingPlan).order_by(BillingPlan.display_order)
        )
        plans = list(result.scalars().all())
        if not plans:
            print("No billing plans found.")
            return 0
        print(
            f"{'Key':<15} {'Name':<15} {'Tier':<12} {'Active':<8} {'Stripe Product ID':<30} {'Prices'}"
        )
        print("-" * 110)
        for plan in plans:
            prices_result = await session.execute(
                select(BillingPrice).where(BillingPrice.plan_id == plan.id)
            )
            prices = list(prices_result.scalars().all())
            prices_summary = (
                ", ".join(f"{p.interval} ${p.amount / 100:.2f}" for p in prices)
                if prices
                else "none"
            )
            stripe_id = plan.stripe_product_id or "-"
            active = "Yes" if bool(getattr(plan, "is_active")) else "No"
            print(
                f"{plan.key:<15} {plan.name:<15} {plan.tier:<12} {active:<8} {stripe_id:<30} {prices_summary}"
            )
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def billing_list(ns: argparse.Namespace) -> int:
    return asyncio.run(_billing_list_async(ns))


async def _billing_pull_stripe_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.billing.plan_sync_service import pull_from_stripe

    session = await _get_session(ns)
    try:
        report = await pull_from_stripe(session, dry_run=ns.dry_run)
        if not ns.dry_run:
            await session.commit()
        if ns.dry_run:
            print("[dry-run] No changes written.")
        print(f"Created:  {report.created}")
        print(f"Updated:  {report.updated}")
        print(f"Skipped:  {report.skipped}")
        if report.errors:
            print(f"Errors:   {report.errors}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def billing_pull_stripe(ns: argparse.Namespace) -> int:
    return asyncio.run(_billing_pull_stripe_async(ns))


async def _billing_sync_stripe_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.billing.plan_sync_service import sync_all_to_stripe

    session = await _get_session(ns)
    try:
        report = await sync_all_to_stripe(session)
        await session.commit()
        print(f"Created:  {report.created}")
        print(f"Updated:  {report.updated}")
        print(f"Skipped:  {report.skipped}")
        if report.errors:
            print(f"Errors:   {report.errors}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def billing_sync_stripe(ns: argparse.Namespace) -> int:
    return asyncio.run(_billing_sync_stripe_async(ns))


async def _bundles_create_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.billing.bundle_validation import (
        validate_bundle_feature_keys,
    )
    from dev_health_ops.models.billing import FeatureBundle

    features = [f.strip() for f in ns.features.split(",") if f.strip()]
    try:
        validate_bundle_feature_keys(features)
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    session = await _get_session(ns)
    try:
        bundle = FeatureBundle(
            key=ns.key,
            name=ns.name,
            description=ns.description,
            features=features,
        )
        session.add(bundle)
        await session.commit()
        print(f"Created bundle: {bundle.key} ({len(features)} features)")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def bundles_create(ns: argparse.Namespace) -> int:
    return asyncio.run(_bundles_create_async(ns))


async def _bundles_list_async(ns: argparse.Namespace) -> int:
    from sqlalchemy import select

    from dev_health_ops.models.billing import (
        BillingPlan,
        FeatureBundle,
        PlanFeatureBundle,
    )

    session = await _get_session(ns)
    try:
        result = await session.execute(
            select(FeatureBundle).order_by(FeatureBundle.key)
        )
        bundles = list(result.scalars().all())
        if not bundles:
            print("No feature bundles found.")
            return 0
        for bundle in bundles:
            pfb_result = await session.execute(
                select(BillingPlan.key)
                .join(PlanFeatureBundle, PlanFeatureBundle.plan_id == BillingPlan.id)
                .where(PlanFeatureBundle.bundle_id == bundle.id)
            )
            plan_keys = [row[0] for row in pfb_result.all()]
            bundle_features = list(getattr(bundle, "features") or [])
            features_str = ", ".join(bundle_features) if bundle_features else "none"
            plans_str = ", ".join(plan_keys) if plan_keys else "none"
            print(f"{bundle.key} ({bundle.name})")
            print(f"  Features: {features_str}")
            print(f"  Plans:    {plans_str}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def bundles_list(ns: argparse.Namespace) -> int:
    return asyncio.run(_bundles_list_async(ns))


async def _bundles_assign_plan_async(ns: argparse.Namespace) -> int:
    from sqlalchemy import select

    from dev_health_ops.models.billing import (
        BillingPlan,
        FeatureBundle,
        PlanFeatureBundle,
    )

    session = await _get_session(ns)
    try:
        bundle_result = await session.execute(
            select(FeatureBundle).where(FeatureBundle.key == ns.bundle_key)
        )
        bundle = bundle_result.scalar_one_or_none()
        if not bundle:
            print(f"Error: Bundle '{ns.bundle_key}' not found")
            return 1

        plan_result = await session.execute(
            select(BillingPlan).where(BillingPlan.key == ns.plan_key)
        )
        plan = plan_result.scalar_one_or_none()
        if not plan:
            print(f"Error: Plan '{ns.plan_key}' not found")
            return 1

        link = PlanFeatureBundle(plan_id=plan.id, bundle_id=bundle.id)
        session.add(link)
        await session.commit()
        print(f"Assigned bundle '{ns.bundle_key}' to plan '{ns.plan_key}'")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def bundles_assign_plan(ns: argparse.Namespace) -> int:
    return asyncio.run(_bundles_assign_plan_async(ns))


async def _bundles_assign_org_async(ns: argparse.Namespace) -> int:
    from datetime import datetime, timedelta, timezone

    from sqlalchemy import select

    from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride

    session = await _get_session(ns)
    try:
        flag_result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.key == ns.feature_key)
        )
        flag = flag_result.scalar_one_or_none()
        if not flag:
            print(f"Error: Feature flag '{ns.feature_key}' not found")
            return 1

        expires_at = None
        if ns.expires_days:
            expires_at = datetime.now(timezone.utc) + timedelta(days=ns.expires_days)

        override = OrgFeatureOverride(
            org_id=ns.org_id,
            feature_id=getattr(flag, "id"),
            reason=ns.reason,
            expires_at=expires_at,
        )
        session.add(override)
        await session.commit()
        print(f"Assigned feature '{ns.feature_key}' override to org '{ns.org_id}'")
        if expires_at:
            print(f"  Expires: {expires_at.isoformat()}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def bundles_assign_org(ns: argparse.Namespace) -> int:
    return asyncio.run(_bundles_assign_org_async(ns))


def licenses_keygen_cmd(ns: argparse.Namespace) -> int:
    from dev_health_ops.licensing.generator import generate_keypair

    kp = generate_keypair()
    print(f"PUBLIC_KEY={kp.public_key}")
    print(f"LICENSE_PRIVATE_KEY={kp.private_key}")
    return 0


def licenses_create_cmd(ns: argparse.Namespace) -> int:
    import os

    from dev_health_ops.licensing.generator import sign_license

    private_key = os.environ.get("LICENSE_PRIVATE_KEY", "")
    if not private_key:
        print("Error: LICENSE_PRIVATE_KEY environment variable is required")
        return 1

    try:
        license_str = sign_license(
            private_key,
            org_id=ns.org_id,
            tier=ns.tier,
            duration_days=ns.duration_days,
            org_name=ns.org_name,
            contact_email=ns.contact_email,
        )
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    print(license_str)
    return 0


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    admin_parser = subparsers.add_parser(
        "admin", help="User and organization management."
    )
    admin_sub = admin_parser.add_subparsers(dest="admin_command", required=True)

    users_parser = admin_sub.add_parser("users", help="User management.")
    users_sub = users_parser.add_subparsers(dest="users_command", required=True)

    users_create = users_sub.add_parser("create", help="Create a new user.")
    users_create.add_argument("--email", required=True, help="User email address.")
    users_create.add_argument(
        "--password", required=True, help="User password (min 8 chars)."
    )
    users_create.add_argument("--username", help="Optional username.")
    users_create.add_argument("--full-name", dest="full_name", help="User's full name.")
    users_create.add_argument(
        "--superuser", action="store_true", help="Grant superuser privileges."
    )
    users_create.set_defaults(func=create_user)

    users_list = users_sub.add_parser("list", help="List all users.")
    users_list.add_argument("--limit", type=int, default=100, help="Max users to list.")
    users_list.add_argument(
        "--include-inactive", action="store_true", help="Include inactive users."
    )
    users_list.set_defaults(func=list_users)

    users_update = users_sub.add_parser("update", help="Update an existing user.")
    users_update.add_argument("--id", help="User ID to update (identifier).")
    users_update.add_argument(
        "--email", help="Email of the user to update (identifier)."
    )
    users_update.add_argument(
        "--username", help="Username of the user to update (identifier)."
    )
    users_update.add_argument(
        "--new-email", dest="new_email", help="Set a new email address."
    )
    users_update.add_argument(
        "--new-username",
        dest="new_username",
        help="Set a new username (pass an empty string to clear it).",
    )
    users_update.add_argument(
        "--full-name", dest="full_name", help="Set the user's full name."
    )
    users_update.add_argument(
        "--password",
        help="Set a new password (min 8 chars; revokes existing sessions).",
    )
    users_update.add_argument(
        "--verified",
        dest="verified",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Set validated/verified status (--verified / --no-verified).",
    )
    users_update.add_argument(
        "--superuser",
        dest="superuser",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Set superuser (user type) status (--superuser / --no-superuser).",
    )
    users_update.add_argument(
        "--active",
        dest="active",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Set active status (--active / --no-active).",
    )
    users_update.add_argument(
        "--org",
        dest="membership_org",
        help="Org slug or ID: add the user to this org, or update their role.",
    )
    users_update.add_argument(
        "--role",
        choices=["owner", "admin", "member", "viewer"],
        help="Membership role to set with --org (default: member when adding).",
    )
    users_update.add_argument(
        "--remove-from-org",
        dest="remove_from_org",
        help="Org slug or ID: remove the user's membership from this org.",
    )
    users_update.set_defaults(func=update_user)

    orgs_parser = admin_sub.add_parser("orgs", help="Organization management.")
    orgs_sub = orgs_parser.add_subparsers(dest="orgs_command", required=True)

    orgs_create = orgs_sub.add_parser("create", help="Create a new organization.")
    orgs_create.add_argument("--name", required=True, help="Organization name.")
    orgs_create.add_argument(
        "--slug", help="URL-safe slug (auto-generated if omitted)."
    )
    orgs_create.add_argument("--description", help="Organization description.")
    orgs_create.add_argument(
        "--tier", default="community", help="Subscription tier (default: community)."
    )
    orgs_create.add_argument(
        "--owner-email", dest="owner_email", help="Email of the initial owner."
    )
    orgs_create.set_defaults(func=create_org)

    orgs_delete = orgs_sub.add_parser(
        "delete", help="Delete an organization and all scoped data."
    )
    orgs_delete.add_argument(
        "--org-id", dest="org_id", required=True, help="Organization ID."
    )
    orgs_delete.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Return the deletion plan without deleting data.",
    )
    orgs_delete.set_defaults(func=delete_org)

    orgs_list = orgs_sub.add_parser("list", help="List all organizations.")
    orgs_list.add_argument("--limit", type=int, default=100, help="Max orgs to list.")
    orgs_list.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive organizations.",
    )
    orgs_list.set_defaults(func=list_orgs)

    llm_settings_parser = admin_sub.add_parser(
        "llm-settings", help="Manage organization BYO LLM settings."
    )
    llm_settings_sub = llm_settings_parser.add_subparsers(
        dest="llm_settings_command", required=True
    )

    llm_settings_get_parser = llm_settings_sub.add_parser(
        "get", help="Get organization BYO LLM settings."
    )
    llm_settings_get_parser.set_defaults(func=llm_settings_get)

    llm_settings_set_parser = llm_settings_sub.add_parser(
        "set", help="Set organization BYO LLM settings."
    )
    llm_settings_set_parser.add_argument(
        "--provider", required=True, help="BYO LLM provider."
    )
    llm_settings_set_parser.add_argument("--model", help="BYO LLM model.")
    llm_settings_set_parser.add_argument(
        "--api-key", dest="api_key", help="BYO LLM API key (encrypted at rest)."
    )
    llm_settings_set_parser.add_argument(
        "--base-url", dest="base_url", help="Base URL."
    )
    llm_settings_set_parser.set_defaults(func=llm_settings_set)

    llm_settings_delete_parser = llm_settings_sub.add_parser(
        "delete", help="Delete organization BYO LLM settings."
    )
    llm_settings_delete_parser.set_defaults(func=llm_settings_delete)

    licenses_parser = admin_sub.add_parser("licenses", help="License key management.")
    licenses_sub = licenses_parser.add_subparsers(
        dest="licenses_command", required=True
    )

    licenses_keygen = licenses_sub.add_parser(
        "keygen", help="Generate an Ed25519 key pair for license signing."
    )
    licenses_keygen.set_defaults(func=licenses_keygen_cmd)

    licenses_create = licenses_sub.add_parser(
        "create", help="Create a signed license key."
    )
    licenses_create.add_argument(
        "--org-id", dest="org_id", required=True, help="Organization ID."
    )
    licenses_create.add_argument(
        "--tier",
        required=True,
        choices=["community", "team", "enterprise"],
        help="License tier.",
    )
    licenses_create.add_argument(
        "--duration-days",
        dest="duration_days",
        type=int,
        default=365,
        help="Days until expiry (default: 365).",
    )
    licenses_create.add_argument(
        "--org-name", dest="org_name", help="Organization name."
    )
    licenses_create.add_argument(
        "--contact-email", dest="contact_email", help="Billing contact email."
    )
    licenses_create.set_defaults(func=licenses_create_cmd)

    features_parser = admin_sub.add_parser("features", help="Feature flag management.")
    features_sub = features_parser.add_subparsers(
        dest="features_command", required=True
    )

    features_seed = features_sub.add_parser(
        "seed", help="Seed standard feature flags into the database."
    )
    features_seed.set_defaults(func=seed_features)

    billing_parser = admin_sub.add_parser("billing", help="Billing plan management.")
    billing_sub = billing_parser.add_subparsers(dest="billing_command", required=True)

    billing_seed = billing_sub.add_parser(
        "seed",
        help="Seed standard billing plans (Community, Team, Enterprise) with prices.",
    )
    billing_seed.set_defaults(func=seed_billing_plans)

    billing_list_parser = billing_sub.add_parser(
        "list", help="List all billing plans with prices and Stripe sync status."
    )
    billing_list_parser.set_defaults(func=billing_list)

    billing_pull = billing_sub.add_parser(
        "pull-stripe", help="Pull billing plans from Stripe into the database."
    )
    billing_pull.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Preview changes without writing to the database.",
    )
    billing_pull.set_defaults(func=billing_pull_stripe)

    billing_sync = billing_sub.add_parser(
        "sync-stripe", help="Push unsynced billing plans to Stripe."
    )
    billing_sync.set_defaults(func=billing_sync_stripe)

    bundles_parser = admin_sub.add_parser("bundles", help="Feature bundle management.")
    bundles_sub = bundles_parser.add_subparsers(dest="bundles_command", required=True)

    bundles_create_parser = bundles_sub.add_parser(
        "create", help="Create a new feature bundle."
    )
    bundles_create_parser.add_argument(
        "--key", required=True, help="Unique bundle key."
    )
    bundles_create_parser.add_argument(
        "--name", required=True, help="Bundle display name."
    )
    bundles_create_parser.add_argument(
        "--features",
        required=True,
        help="Comma-separated list of feature keys.",
    )
    bundles_create_parser.add_argument("--description", help="Bundle description.")
    bundles_create_parser.set_defaults(func=bundles_create)

    bundles_list_parser = bundles_sub.add_parser(
        "list",
        help="List all feature bundles with their features and plan assignments.",
    )
    bundles_list_parser.set_defaults(func=bundles_list)

    bundles_assign_plan_parser = bundles_sub.add_parser(
        "assign-plan", help="Assign a feature bundle to a billing plan."
    )
    bundles_assign_plan_parser.add_argument(
        "--bundle-key", dest="bundle_key", required=True, help="Feature bundle key."
    )
    bundles_assign_plan_parser.add_argument(
        "--plan-key", dest="plan_key", required=True, help="Billing plan key."
    )
    bundles_assign_plan_parser.set_defaults(func=bundles_assign_plan)

    bundles_assign_org_parser = bundles_sub.add_parser(
        "assign-org", help="Grant an organization a feature override."
    )
    bundles_assign_org_parser.add_argument(
        "--org-id", dest="org_id", required=True, help="Organization ID (UUID)."
    )
    bundles_assign_org_parser.add_argument(
        "--feature-key", dest="feature_key", required=True, help="Feature flag key."
    )
    bundles_assign_org_parser.add_argument(
        "--reason", help="Reason for the override (e.g., support ticket, promotion)."
    )
    bundles_assign_org_parser.add_argument(
        "--expires-days",
        dest="expires_days",
        type=int,
        help="Days until the override expires.",
    )
    bundles_assign_org_parser.set_defaults(func=bundles_assign_org)
