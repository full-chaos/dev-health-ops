from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.middleware import (
    get_admin_org_id,
    require_admin,
    require_superuser,
)
from dev_health_ops.api.admin.router import get_session
from dev_health_ops.api.main import app
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IntegrationCredential, SyncConfiguration
from dev_health_ops.models.users import Membership, Organization, User


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "platform-stats.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    Membership.__table__,
                    IntegrationCredential.__table__,
                    SyncConfiguration.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(session_maker):
    async def _override_get_session():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_session] = _override_get_session
    app.dependency_overrides[get_admin_org_id] = lambda: "test-org"
    app.dependency_overrides[require_admin] = lambda: AuthenticatedUser(
        user_id="test-user",
        email="test@example.com",
        org_id="test-org",
        role="owner",
    )
    app.dependency_overrides[require_superuser] = lambda: AuthenticatedUser(
        user_id="test-superuser",
        email="super@example.com",
        org_id="test-org",
        role="owner",
        is_superuser=True,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_platform_stats_returns_expected_schema(client: AsyncClient):
    response = await client.get("/api/v1/admin/platform/stats")

    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {
        "total_organizations",
        "active_organizations",
        "total_users",
        "active_users",
        "superuser_count",
        "total_memberships",
        "tier_distribution",
        "total_sync_configs",
        "active_sync_configs",
        "recent_syncs_success",
        "recent_syncs_failed",
    }
    assert body["tier_distribution"] == {}


@pytest.mark.asyncio
async def test_platform_stats_returns_seeded_aggregations(
    client: AsyncClient,
    session_maker,
):
    now = datetime.now(timezone.utc)

    org_one = Organization(
        id=uuid.uuid4(),
        slug="org-one",
        name="Org One",
        tier="community",
        is_active=True,
    )
    org_two = Organization(
        id=uuid.uuid4(),
        slug="org-two",
        name="Org Two",
        tier="enterprise",
        is_active=False,
    )

    user_one = User(
        id=uuid.uuid4(),
        email="one@example.com",
        is_active=True,
        is_superuser=False,
    )
    user_two = User(
        id=uuid.uuid4(),
        email="two@example.com",
        is_active=True,
        is_superuser=True,
    )
    user_three = User(
        id=uuid.uuid4(),
        email="three@example.com",
        is_active=False,
        is_superuser=False,
    )

    sync_recent_ok_one = SyncConfiguration(
        org_id=str(org_one.id),
        name="sync-recent-ok-1",
        provider="github",
        is_active=True,
        sync_targets=[],
        sync_options={},
    )
    sync_recent_ok_one.last_sync_at = now - timedelta(hours=1)
    sync_recent_ok_one.last_sync_success = True

    sync_recent_fail = SyncConfiguration(
        org_id=str(org_one.id),
        name="sync-recent-fail",
        provider="gitlab",
        is_active=True,
        sync_targets=[],
        sync_options={},
    )
    sync_recent_fail.last_sync_at = now - timedelta(hours=2)
    sync_recent_fail.last_sync_success = False

    sync_recent_ok_two = SyncConfiguration(
        org_id=str(org_two.id),
        name="sync-recent-ok-2",
        provider="jira",
        is_active=False,
        sync_targets=[],
        sync_options={},
    )
    sync_recent_ok_two.last_sync_at = now - timedelta(hours=3)
    sync_recent_ok_two.last_sync_success = True

    sync_old_ok = SyncConfiguration(
        org_id=str(org_two.id),
        name="sync-old-ok",
        provider="github",
        is_active=True,
        sync_targets=[],
        sync_options={},
    )
    sync_old_ok.last_sync_at = now - timedelta(days=2)
    sync_old_ok.last_sync_success = True

    async with session_maker() as session:
        session.add_all([org_one, org_two, user_one, user_two, user_three])

        session.add_all(
            [
                Membership(org_id=org_one.id, user_id=user_one.id, role="owner"),
                Membership(org_id=org_one.id, user_id=user_two.id, role="admin"),
                Membership(org_id=org_two.id, user_id=user_three.id, role="member"),
            ]
        )

        session.add_all(
            [
                sync_recent_ok_one,
                sync_recent_fail,
                sync_recent_ok_two,
                sync_old_ok,
            ]
        )

        await session.commit()

    response = await client.get("/api/v1/admin/platform/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["total_organizations"] == 2
    assert body["active_organizations"] == 1
    assert body["total_users"] == 3
    assert body["active_users"] == 2
    assert body["superuser_count"] == 1
    assert body["total_memberships"] == 3
    assert body["tier_distribution"] == {"community": 1, "enterprise": 1}
    assert body["total_sync_configs"] == 4
    assert body["active_sync_configs"] == 3
    assert body["recent_syncs_success"] == 2
    assert body["recent_syncs_failed"] == 1
