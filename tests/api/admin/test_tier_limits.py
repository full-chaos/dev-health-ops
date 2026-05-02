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
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense, TierLimit
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    TierLimit,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
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


async def _seed_org(session_maker, tier: str) -> dict[str, str]:
    """Create an org + user + OrgLicense for a given tier."""
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    org = Organization(
        id=org_id, slug=f"{tier}-org", name=f"{tier.title()} Org", tier=tier
    )
    user = User(id=user_id, email=f"{tier}@example.com", is_active=True)
    license_row = OrgLicense(org_id=org_id, tier=tier)

    async with session_maker() as session:
        session.add_all([org, user, license_row])
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
    payload = {"name": name, "provider": "github", "sync_targets": [], **extra}
    return await ac.post("/api/v1/admin/sync-configs", json=payload)


# ---------------------------------------------------------------------------
# Repo limit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_community_repo_limit_blocks_at_3(session_maker):
    """Community tier: 4th sync config should be rejected (max_repos=3)."""
    state = await _seed_org(session_maker, "community")
    # Seed DB-driven limit so we don't rely on hardcoded fallback
    await _seed_tier_limits(session_maker, "community", {"max_repos": "3"})

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Create 3 configs — should all succeed
        for i in range(3):
            resp = await _create_config(ac, f"repo-{i}")
            assert resp.status_code == 201, f"Config {i} failed: {resp.text}"

        # 4th should be rejected
        resp = await _create_config(ac, "repo-blocked")
        assert resp.status_code == 403
        assert (
            "limit" in resp.json()["detail"].lower()
            or "exceeded" in resp.json()["detail"].lower()
        )


@pytest.mark.asyncio
async def test_team_repo_limit_allows_10(session_maker):
    """Team tier: 10 configs should be allowed (max_repos=10)."""
    state = await _seed_org(session_maker, "team")
    await _seed_tier_limits(session_maker, "team", {"max_repos": "10"})

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for i in range(10):
            resp = await _create_config(ac, f"repo-{i}")
            assert resp.status_code == 201, f"Config {i} failed: {resp.text}"

        # 11th should be rejected
        resp = await _create_config(ac, "repo-blocked")
        assert resp.status_code == 403


@pytest.mark.asyncio
async def test_enterprise_repo_limit_unlimited(session_maker):
    """Enterprise tier: no repo limit (max_repos=null)."""
    state = await _seed_org(session_maker, "enterprise")
    await _seed_tier_limits(session_maker, "enterprise", {"max_repos": None})

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Create 20 configs — should all succeed
        for i in range(20):
            resp = await _create_config(ac, f"repo-{i}")
            assert resp.status_code == 201, f"Config {i} failed: {resp.text}"


# ---------------------------------------------------------------------------
# DB-driven limit override tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_db_limit_override_changes_boundary(session_maker):
    """Changing max_repos from 3 to 5 in the DB should allow 5 configs."""
    state = await _seed_org(session_maker, "community")
    # Override the default (3) to 5 via the DB
    await _seed_tier_limits(session_maker, "community", {"max_repos": "5"})

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for i in range(5):
            resp = await _create_config(ac, f"repo-{i}")
            assert resp.status_code == 201, f"Config {i} failed: {resp.text}"

        # 6th should be rejected
        resp = await _create_config(ac, "repo-blocked")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Schedule gating tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_community_cannot_set_schedule(session_maker):
    """Community tier: setting schedule_cron should be rejected."""
    state = await _seed_org(session_maker, "community")

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await _create_config(
            ac,
            "scheduled-sync",
            sync_options={"schedule_cron": "0 0 * * *"},
        )
        assert resp.status_code == 403
        assert (
            "scheduled_jobs" in resp.json()["detail"].lower()
            or "tier" in resp.json()["detail"].lower()
        )


@pytest.mark.asyncio
async def test_team_can_set_daily_schedule(session_maker):
    """Team tier: daily schedule (24h interval) should be allowed (min=6h)."""
    state = await _seed_org(session_maker, "team")
    await _seed_tier_limits(
        session_maker,
        "team",
        {
            "max_repos": "10",
            "min_sync_interval_hours": "6",
        },
    )

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await _create_config(
            ac,
            "daily-sync",
            sync_options={"schedule_cron": "0 0 * * *"},
        )
        assert resp.status_code == 201, f"Expected 201: {resp.json()}"


@pytest.mark.asyncio
async def test_team_cannot_set_hourly_schedule(session_maker):
    """Team tier: hourly schedule (1h) should be rejected (min=6h)."""
    state = await _seed_org(session_maker, "team")
    await _seed_tier_limits(
        session_maker,
        "team",
        {
            "max_repos": "10",
            "min_sync_interval_hours": "6",
        },
    )

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await _create_config(
            ac,
            "hourly-sync",
            sync_options={"schedule_cron": "0 * * * *"},
        )
        assert resp.status_code == 403
        assert "interval" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Initial sync depth tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_community_initial_sync_depth_blocked_at_90(session_maker):
    """Community tier: 90-day depth should be rejected (max=30)."""
    state = await _seed_org(session_maker, "community")
    await _seed_tier_limits(session_maker, "community", {"backfill_days": "30"})

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await _create_config(
            ac,
            "deep-sync",
            sync_options={"initial_sync_depth": 90},
        )
        assert resp.status_code == 403
        assert (
            "backfill" in resp.json()["detail"].lower()
            or "limit" in resp.json()["detail"].lower()
        )


@pytest.mark.asyncio
async def test_community_initial_sync_depth_allowed_at_30(session_maker):
    """Community tier: 30-day depth should be allowed (max=30)."""
    state = await _seed_org(session_maker, "community")
    await _seed_tier_limits(
        session_maker, "community", {"backfill_days": "30", "max_repos": "3"}
    )

    app = _make_client(session_maker, state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await _create_config(
            ac,
            "shallow-sync",
            sync_options={"initial_sync_depth": 30},
        )
        assert resp.status_code == 201
