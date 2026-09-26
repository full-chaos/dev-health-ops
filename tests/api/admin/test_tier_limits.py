"""Integration tests for tier-based RBAC limit enforcement.

Validates that the API correctly enforces limits at tier boundaries:
- Repo limits (max_repos)
- Sync interval minimums (min_sync_interval_hours)
- Initial sync depth (backfill_days)
- Scheduled jobs feature gating (Community cannot schedule)
- Work items count (max_work_items) — tested at route level

Each test creates an org with a specific tier (via OrgLicense) and verifies
that the enforcement returns 403 when limits are exceeded and 201/200 when
within bounds.
"""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgLicense, TierLimit
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.subscriptions import Subscription
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    FeatureFlag,
    TierLimit,
    IntegrationCredential,
    SyncConfiguration,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    ScheduledJob,
    JobRun,
    Subscription,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "tier-limits.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(
    session_maker,
    tier: str,
    *,
    with_license: bool = True,
    license_tier: str | None = None,
) -> dict[str, str]:
    """Create an org + user (+ optionally an OrgLicense) for a given tier.

    ``with_license=False`` models the common SaaS state where the org's tier
    lives only on ``Organization.tier`` (set at creation or by an admin) and
    no OrgLicense row exists yet — e.g. a trial before a Stripe plan resolves
    (CHAOS-2256). ``license_tier`` seeds an OrgLicense row whose tier diverges
    from ``Organization.tier`` (legacy stale-row state).
    """
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    org = Organization(
        id=org_id, slug=f"{tier}-org", name=f"{tier.title()} Org", tier=tier
    )
    user = User(id=user_id, email=f"{tier}@example.com", is_active=True)
    rows: list[object] = [org, user]
    if with_license:
        rows.append(OrgLicense(org_id=org_id, tier=license_tier or tier))

    async with session_maker() as session:
        session.add_all(rows)
        await session.commit()

    return {"org_id": str(org_id), "user_id": str(user_id)}


async def _seed_tier_limits(session_maker, tier: str, overrides: dict[str, str | None]):
    """Seed TierLimit rows for a specific tier (DB-driven limits)."""
    async with session_maker() as session:
        for key, val in overrides.items():
            session.add(TierLimit(tier=tier, limit_key=key, limit_value=val))
        await session.commit()


def _make_client(session_maker, state):
    """Build an HTTPX test client wired to the admin router."""
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=state["user_id"],
        email="admin@example.com",
        org_id=state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    return app


async def _create_config(ac, name: str, **extra):
    # Use a non-git provider so each config materializes exactly one planner
    # IntegrationSource (the unit the repo-limit counts). A github config with
    # no repos/all_repos creates zero sources and would not consume a slot.
    payload = {"name": name, "provider": "linear", "sync_targets": [], **extra}
    return await ac.post("/api/v1/admin/sync-configs", json=payload)


# ---------------------------------------------------------------------------
# Repo limit tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# DB-driven limit override tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Schedule gating tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Initial sync depth tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Enterprise tier resolution (CHAOS-2256)
# ---------------------------------------------------------------------------


async def _get_min_sync_interval(session_maker, org_id: str):
    from dev_health_ops.api.services.licensing import TierLimitService

    async with session_maker() as session:

        def _get(sync_session):
            tier_svc = TierLimitService(sync_session)
            return tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")

        return await session.run_sync(_get)


@pytest.mark.asyncio
async def test_enterprise_min_sync_interval_is_quarter_hour(session_maker):
    """Enterprise org (with OrgLicense): min_sync_interval_hours == 0.25."""
    state = await _seed_org(session_maker, "enterprise")
    assert await _get_min_sync_interval(session_maker, state["org_id"]) == 0.25


@pytest.mark.asyncio
async def test_enterprise_tier_resolves_via_organization_fallback(session_maker):
    """CHAOS-2256: enterprise org WITHOUT an OrgLicense row must resolve via
    Organization.tier, not silently fall back to community's 24h floor."""
    state = await _seed_org(session_maker, "enterprise", with_license=False)
    assert await _get_min_sync_interval(session_maker, state["org_id"]) == 0.25


@pytest.mark.asyncio
async def test_stale_org_license_takes_precedence_and_paths_agree(session_maker):
    """Documented precedence for a stale/diverged OrgLicense row: the license
    row wins over Organization.tier, and the limits path and entitlements path
    agree on it (CHAOS-2256 acceptance: limits resolution == entitlements).

    Divergence is prevented at the write paths (OrganizationService.update and
    SubscriptionService._sync_org_license keep both in lockstep); this pins the
    read behavior for pre-existing legacy rows.
    """
    from dev_health_ops.licensing.gating import get_org_entitlements_from_db

    state = await _seed_org(session_maker, "enterprise", license_tier="community")

    # Limits path: community license row wins → 24h floor.
    assert await _get_min_sync_interval(session_maker, state["org_id"]) == 24

    # Entitlements path agrees: tier resolves to community.
    async with session_maker() as session:
        entitlements = await get_org_entitlements_from_db(
            uuid.UUID(state["org_id"]), session
        )
    assert entitlements["tier"] == "community"


@pytest.mark.asyncio
async def test_admin_tier_update_heals_stale_org_license(session_maker):
    """CHAOS-2256 review regression: an admin tier change must update a
    pre-existing OrgLicense row, otherwise the org keeps being enforced at the
    stale license tier."""
    from dev_health_ops.api.services.users import OrganizationService

    state = await _seed_org(session_maker, "community")

    async with session_maker() as session:
        svc = OrganizationService(session)
        org = await svc.update(org_id=state["org_id"], tier="enterprise")
        assert org is not None
        await session.commit()

    async with session_maker() as session:
        result = await session.execute(
            select(OrgLicense).filter_by(org_id=uuid.UUID(state["org_id"]))
        )
        license_row = result.scalar_one()
        assert license_row.tier == "enterprise"

    assert await _get_min_sync_interval(session_maker, state["org_id"]) == 0.25
