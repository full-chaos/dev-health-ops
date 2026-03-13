from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.users import Organization, User

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = [
    User.__table__,
    Organization.__table__,
    IntegrationCredential.__table__,
    SyncConfiguration.__table__,
    ScheduledJob.__table__,
    JobRun.__table__,
]


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "sync-configs.db"
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
# Helper
# ---------------------------------------------------------------------------


async def _create_sync_config(ac, name: str = "my-sync", provider: str = "github"):
    return await ac.post(
        "/api/v1/admin/sync-configs",
        json={"name": name, "provider": provider, "sync_targets": []},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_sync_configs_empty(client):
    ac, _ = client

    resp = await ac.get("/api/v1/admin/sync-configs")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_sync_config_returns_201_shape(client):
    ac, _ = client

    resp = await _create_sync_config(ac, name="test-sync", provider="github")

    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "test-sync"
    assert data["provider"] == "github"
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data


@pytest.mark.asyncio
async def test_create_sync_config_persists_to_db(client, session_maker):
    ac, seeded_state = client

    resp = await _create_sync_config(ac, name="persist-test", provider="gitlab")

    assert resp.status_code == 201
    config_id = resp.json()["id"]

    async with session_maker() as session:
        result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.id == uuid.UUID(config_id)
            )
        )
        config = result.scalar_one_or_none()

    assert config is not None
    assert config.name == "persist-test"
    assert config.provider == "gitlab"
    assert config.org_id == seeded_state["org_id"]


@pytest.mark.asyncio
async def test_get_sync_config_by_id(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="get-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}")

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == config_id
    assert data["name"] == "get-test"


@pytest.mark.asyncio
async def test_get_sync_config_nonexistent_returns_404(client):
    ac, _ = client

    resp = await ac.get(f"/api/v1/admin/sync-configs/{uuid.uuid4()}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_sync_config_changes_is_active(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="update-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{config_id}",
        json={"is_active": False},
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["is_active"] is False
    assert data["id"] == config_id


@pytest.mark.asyncio
async def test_delete_sync_config(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="delete-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    del_resp = await ac.delete(f"/api/v1/admin/sync-configs/{config_id}")

    assert del_resp.status_code == 204

    get_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_nonexistent_sync_config_returns_404(client):
    ac, _ = client

    resp = await ac.delete(f"/api/v1/admin/sync-configs/{uuid.uuid4()}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_sync_config_returns_202_with_task_id(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="trigger-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="fake-task-id")
    mock_run = MagicMock()
    mock_run.delay.return_value = mock_task

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "triggered"
    assert data["task_id"] == "fake-task-id"
    assert data["config_id"] == config_id


@pytest.mark.asyncio
async def test_trigger_sync_config_nonexistent_returns_404(client):
    ac, _ = client

    resp = await ac.post(f"/api/v1/admin/sync-configs/{uuid.uuid4()}/trigger")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_sync_config_celery_unavailable_returns_503(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="celery-fail-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_run = MagicMock()
    mock_run.delay.side_effect = Exception("Celery broker connection refused")

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 503
    assert "unavailable" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_sync_config_jobs_empty(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="jobs-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/jobs")

    assert resp.status_code == 200
    assert resp.json() == []
