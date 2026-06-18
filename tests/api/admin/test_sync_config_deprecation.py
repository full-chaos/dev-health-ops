"""Tests for CHAOS-2520: deprecate child sync configs in UX and API defaults.

Covers:
- HIDE_MIGRATED_CHILD_CONFIGS flag hides migrated children from default list
- ?include_migrated=true bypasses the filter (support/rollback)
- Flag OFF → legacy list unchanged
- Planner flag ON → batch endpoint creates zero child SyncConfiguration rows
- Planner flag OFF → batch endpoint creates children (legacy path)
- Legacy single-config endpoints (get/create/update/delete) still work
"""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    Setting,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.models.users import Organization, User
from dev_health_ops.sync.config_migration import MIGRATED_TRIGGER_ROUTING_SETTING_KEY
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
    Setting,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "deprecation-test.db"
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


@pytest_asyncio.fixture
async def seeded_state(session_maker):
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="pro")
    user = User(id=user_id, email="admin@example.com", is_active=True)

    async with session_maker() as session:
        session.add_all([org, user])
        await session.commit()

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
    }


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_configs(session_maker, org_id: str):
    """Seed a parent, a legacy child (parent_id set), and a migrated child."""
    async with session_maker() as session:
        parent = SyncConfiguration(
            name="parent-config",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
        )
        session.add(parent)
        await session.flush()

        child_legacy = SyncConfiguration(
            name="child-legacy",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            parent_id=parent.id,
        )
        child_migrated_integration = SyncConfiguration(
            name="child-migrated-integration",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            migrated_integration_id=uuid.uuid4(),
        )
        child_migrated_source = SyncConfiguration(
            name="child-migrated-source",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            migrated_source_id=uuid.uuid4(),
        )
        session.add_all(
            [child_legacy, child_migrated_integration, child_migrated_source]
        )
        await session.commit()

        return {
            "parent_id": str(parent.id),
            "child_legacy_id": str(child_legacy.id),
            "child_migrated_integration_id": str(child_migrated_integration.id),
            "child_migrated_source_id": str(child_migrated_source.id),
        }


async def _seed_planner_flag(session_maker, org_id: str, value: str = "true"):
    """Write the migrated_trigger_routing_enabled Setting row."""
    async with session_maker() as session:
        setting = Setting(
            key=MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
            category=SettingCategory.SYNC.value,
            value=value,
            org_id=org_id,
        )
        session.add(setting)
        await session.commit()


# ---------------------------------------------------------------------------
# HIDE_MIGRATED_CHILD_CONFIGS flag tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_hides_migrated_children_when_flag_on(client, session_maker):
    """With HIDE_MIGRATED_CHILD_CONFIGS=true, migrated children are hidden."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    await _seed_configs(session_maker, org_id)

    with patch.dict("os.environ", {"HIDE_MIGRATED_CHILD_CONFIGS": "true"}):
        resp = await ac.get("/api/v1/admin/sync-configs")

    assert resp.status_code == 200
    names = {c["name"] for c in resp.json()}
    assert "parent-config" in names
    assert "child-legacy" not in names
    assert "child-migrated-integration" not in names
    assert "child-migrated-source" not in names


@pytest.mark.asyncio
async def test_list_returns_migrated_children_with_include_migrated(
    client, session_maker
):
    """?include_migrated=true bypasses the filter even when flag is on."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    await _seed_configs(session_maker, org_id)

    with patch.dict("os.environ", {"HIDE_MIGRATED_CHILD_CONFIGS": "true"}):
        resp = await ac.get("/api/v1/admin/sync-configs?include_migrated=true")

    assert resp.status_code == 200
    names = {c["name"] for c in resp.json()}
    assert "parent-config" in names
    assert "child-legacy" in names
    assert "child-migrated-integration" in names
    assert "child-migrated-source" in names


@pytest.mark.asyncio
async def test_list_unchanged_when_flag_off(client, session_maker):
    """With HIDE_MIGRATED_CHILD_CONFIGS unset, all configs are returned."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    await _seed_configs(session_maker, org_id)

    with patch.dict("os.environ", {}, clear=False):
        # Ensure the flag is absent
        import os

        os.environ.pop("HIDE_MIGRATED_CHILD_CONFIGS", None)
        resp = await ac.get("/api/v1/admin/sync-configs")

    assert resp.status_code == 200
    names = {c["name"] for c in resp.json()}
    assert "parent-config" in names
    assert "child-legacy" in names
    assert "child-migrated-integration" in names
    assert "child-migrated-source" in names


@pytest.mark.asyncio
async def test_list_include_migrated_false_with_flag_off_returns_all(
    client, session_maker
):
    """?include_migrated=false with flag OFF still returns all (flag governs)."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    await _seed_configs(session_maker, org_id)

    import os

    os.environ.pop("HIDE_MIGRATED_CHILD_CONFIGS", None)
    resp = await ac.get("/api/v1/admin/sync-configs?include_migrated=false")

    assert resp.status_code == 200
    names = {c["name"] for c in resp.json()}
    assert "child-legacy" in names


# ---------------------------------------------------------------------------
# Planner flag gates new child-config creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_creates_zero_children_when_planner_active(client, session_maker):
    """When planner flag is ON, batch endpoint creates parent only (zero children)."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    await _seed_planner_flag(session_maker, org_id, value="true")

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "planner-active-batch",
            "provider": "github",
            "sync_targets": ["git"],
            "repos": ["repo-a", "repo-b"],
            "sync_options": {"owner": "myorg"},
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []
    assert data["parent"]["name"] == "planner-active-batch"

    # Verify no child rows in DB
    async with session_maker() as session:
        result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.org_id == org_id,
                SyncConfiguration.parent_id.isnot(None),
            )
        )
        children = result.scalars().all()
    assert len(children) == 0


@pytest.mark.asyncio
async def test_batch_creates_children_when_planner_inactive(client, session_maker):
    """When planner flag is OFF (no Setting row), batch creates children normally."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    # No planner flag seeded → legacy path

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "legacy-batch",
            "provider": "github",
            "sync_targets": ["git"],
            "repos": ["repo-x", "repo-y"],
            "sync_options": {"owner": "myorg"},
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["total_created"] == 2
    assert len(data["children"]) == 2

    # Verify child rows in DB
    async with session_maker() as session:
        result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.org_id == org_id,
                SyncConfiguration.parent_id.isnot(None),
            )
        )
        children = result.scalars().all()
    assert len(children) == 2


# ---------------------------------------------------------------------------
# Legacy endpoints still work (rollback path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sync_config_still_works(client, session_maker):
    """GET /sync-configs/{id} works regardless of flags."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    ids = await _seed_configs(session_maker, org_id)

    with patch.dict("os.environ", {"HIDE_MIGRATED_CHILD_CONFIGS": "true"}):
        resp = await ac.get(f"/api/v1/admin/sync-configs/{ids['child_legacy_id']}")

    assert resp.status_code == 200
    assert resp.json()["name"] == "child-legacy"


@pytest.mark.asyncio
async def test_create_sync_config_still_works(client):
    """POST /sync-configs still creates a config regardless of flags."""
    ac, _ = client

    with patch.dict("os.environ", {"HIDE_MIGRATED_CHILD_CONFIGS": "true"}):
        resp = await ac.post(
            "/api/v1/admin/sync-configs",
            json={"name": "new-config", "provider": "github", "sync_targets": []},
        )

    assert resp.status_code == 201
    assert resp.json()["name"] == "new-config"


@pytest.mark.asyncio
async def test_update_sync_config_still_works(client, session_maker):
    """PATCH /sync-configs/{id} still updates a config regardless of flags."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    ids = await _seed_configs(session_maker, org_id)

    with patch.dict("os.environ", {"HIDE_MIGRATED_CHILD_CONFIGS": "true"}):
        resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{ids['parent_id']}",
            json={"is_active": False},
        )

    assert resp.status_code == 200
    assert resp.json()["is_active"] is False


@pytest.mark.asyncio
async def test_delete_sync_config_still_works(client, session_maker):
    """DELETE /sync-configs/{id} still deletes a config regardless of flags."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    ids = await _seed_configs(session_maker, org_id)

    with patch.dict("os.environ", {"HIDE_MIGRATED_CHILD_CONFIGS": "true"}):
        resp = await ac.delete(f"/api/v1/admin/sync-configs/{ids['child_legacy_id']}")

    assert resp.status_code == 204
