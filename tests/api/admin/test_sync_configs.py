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
from dev_health_ops.models.licensing import OrgLicense
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
sync_router_module = importlib.import_module("dev_health_ops.api.admin.routers.sync")

_TABLES = tables_of(
    User,
    Organization,
    OrgLicense,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
)


class _CroniterStub:
    def __init__(self, *_args, **_kwargs):
        self._next = 0.0

    def get_next(self, _type):
        self._next += 86400.0
        return self._next


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
async def test_create_sync_config_creates_scheduled_job(client, session_maker):
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "scheduled-linear",
            "provider": "linear",
            "sync_targets": ["work-items"],
            "sync_options": {},
        },
    )

    assert resp.status_code == 201
    config_id = uuid.UUID(resp.json()["id"])
    async with session_maker() as session:
        result = await session.execute(
            select(ScheduledJob).where(ScheduledJob.sync_config_id == config_id)
        )
        job = result.scalar_one_or_none()

    assert job is not None
    assert job.schedule_cron == "0 * * * *"


@pytest.mark.asyncio
async def test_create_sync_config_folds_top_level_schedule_fields(
    client, session_maker
):
    ac, _ = client

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs",
            json={
                "name": "top-level-schedule",
                "provider": "linear",
                "sync_targets": ["work-items"],
                "sync_options": {"backfill_days": 1},
                "schedule_cron": "30 2 * * *",
                "timezone": "America/Los_Angeles",
                "initial_sync_depth": 14,
            },
        )

    assert resp.status_code == 201, resp.text
    config_id = uuid.UUID(resp.json()["id"])
    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(SyncConfiguration.id == config_id)
            )
        ).scalar_one()
        job = (
            await session.execute(
                select(ScheduledJob).where(ScheduledJob.sync_config_id == config_id)
            )
        ).scalar_one()

    assert config.sync_options["schedule_cron"] == "30 2 * * *"
    assert config.sync_options["timezone"] == "America/Los_Angeles"
    assert config.sync_options["initial_sync_depth"] == 14
    assert job.provider == "linear"
    assert job.timezone == "America/Los_Angeles"
    assert job.job_config == {"provider": "linear", "sync_config_id": str(config_id)}


@pytest.mark.asyncio
async def test_batch_create_linear_without_repos_creates_active_config_and_job(
    client, session_maker
):
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "linear-work-items",
            "provider": "linear",
            "sync_targets": ["work-items"],
            "sync_options": {},
            "repos": [],
            "schedule_cron": "15 * * * *",
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["children"] == []
    assert data["parent"]["is_active"] is True
    config_id = uuid.UUID(data["parent"]["id"])
    async with session_maker() as session:
        config_result = await session.execute(
            select(SyncConfiguration).where(SyncConfiguration.id == config_id)
        )
        config = config_result.scalar_one_or_none()
        job_result = await session.execute(
            select(ScheduledJob).where(ScheduledJob.sync_config_id == config_id)
        )
        job = job_result.scalar_one_or_none()

    assert config is not None
    assert config.is_active is True
    assert config.sync_options["schedule_cron"] == "15 * * * *"
    assert job is not None
    assert job.schedule_cron == "15 * * * *"


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
async def test_update_sync_config_merges_top_level_schedule_fields(
    client, session_maker
):
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "merge-schedule",
            "provider": "linear",
            "sync_targets": ["work-items"],
            "sync_options": {"backfill_days": 7},
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={
                "schedule_cron": "45 3 * * *",
                "timezone": "Europe/London",
                "initial_sync_depth": 21,
            },
        )

    assert resp.status_code == 200, resp.text
    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    assert config.sync_options["backfill_days"] == 7
    assert config.sync_options["schedule_cron"] == "45 3 * * *"
    assert config.sync_options["timezone"] == "Europe/London"
    assert config.sync_options["initial_sync_depth"] == 21
    assert job.schedule_cron == "45 3 * * *"
    assert job.provider == "linear"
    assert job.timezone == "Europe/London"


@pytest.mark.asyncio
async def test_schedule_job_upsert_propagates_schedule_cron(
    client, session_maker, seeded_state
):
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="schedule-update", provider="linear"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        config_result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.id == uuid.UUID(config_id)
            )
        )
        config = config_result.scalar_one()
        config.sync_options = {"schedule_cron": "5 * * * *"}
        await sync_router_module._upsert_scheduled_job(
            session, config, seeded_state["org_id"]
        )
        await session.flush()
        result = await session.execute(
            select(ScheduledJob).where(
                ScheduledJob.sync_config_id == uuid.UUID(config_id)
            )
        )
        job = result.scalar_one_or_none()

    assert job is not None
    assert job.schedule_cron == "5 * * * *"


@pytest.mark.asyncio
async def test_schedule_job_upsert_sets_provider_timezone_and_job_config(
    client, session_maker, seeded_state
):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="schedule-meta", provider="linear")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        config.sync_options = {"schedule_cron": "10 * * * *", "timezone": "Asia/Tokyo"}
        await sync_router_module._upsert_scheduled_job(
            session, config, seeded_state["org_id"]
        )
        await session.flush()
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    assert job.provider == "linear"
    assert job.timezone == "Asia/Tokyo"
    assert job.job_config == {"provider": "linear", "sync_config_id": config_id}


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
async def test_trigger_sync_config_routes_batch_eligible_to_batch_task(client):
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "batch-trigger",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"search": "full-chaos"},
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="batch-task-id")
    mock_batch = MagicMock()
    mock_batch.delay.return_value = mock_task
    mock_run = MagicMock()

    with (
        patch("dev_health_ops.workers.sync_tasks.dispatch_batch_sync", mock_batch),
        patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    assert resp.json()["task_id"] == "batch-task-id"
    mock_batch.delay.assert_called_once()
    mock_run.delay.assert_not_called()


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


# ---------------------------------------------------------------------------
# Provider-scoped uniqueness tests (CHAOS-2243)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_name_different_providers_can_coexist(client):
    """Two configs with the same name but different providers must both be created."""
    ac, _ = client

    resp_gh = await _create_sync_config(ac, name="chaos", provider="github")
    assert resp_gh.status_code == 201, resp_gh.text

    resp_lin = await _create_sync_config(ac, name="chaos", provider="linear")
    assert resp_lin.status_code == 201, resp_lin.text

    assert resp_gh.json()["id"] != resp_lin.json()["id"]

    list_resp = await ac.get("/api/v1/admin/sync-configs")
    assert list_resp.status_code == 200
    names = [c["name"] for c in list_resp.json()]
    assert names.count("chaos") == 2


@pytest.mark.asyncio
async def test_same_name_same_provider_is_rejected(client):
    """Two configs with the same (name, provider) must fail with a DB-level error."""
    from sqlalchemy.exc import IntegrityError

    ac, _ = client

    resp1 = await _create_sync_config(ac, name="dup-config", provider="github")
    assert resp1.status_code == 201, resp1.text

    # Second create with identical (name, provider) must raise a DB integrity error.
    # The ASGI test transport propagates the unhandled IntegrityError as an exception
    # rather than returning an HTTP response, so we catch it here.
    with pytest.raises((IntegrityError, Exception)) as exc_info:
        await _create_sync_config(ac, name="dup-config", provider="github")
    assert "UNIQUE" in str(exc_info.value) or "unique" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_delete_targets_correct_provider(client, session_maker):
    """Deleting a config by ID removes only the matching provider row."""
    ac, _ = client

    resp_gh = await _create_sync_config(ac, name="shared-name", provider="github")
    resp_lin = await _create_sync_config(ac, name="shared-name", provider="linear")
    assert resp_gh.status_code == 201
    assert resp_lin.status_code == 201

    gh_id = resp_gh.json()["id"]
    lin_id = resp_lin.json()["id"]

    del_resp = await ac.delete(f"/api/v1/admin/sync-configs/{gh_id}")
    assert del_resp.status_code == 204

    # GitHub config gone, Linear config still present.
    assert (await ac.get(f"/api/v1/admin/sync-configs/{gh_id}")).status_code == 404
    assert (await ac.get(f"/api/v1/admin/sync-configs/{lin_id}")).status_code == 200


@pytest.mark.asyncio
async def test_update_targets_correct_provider(client):
    """Updating a config by ID only modifies the matching provider row."""
    ac, _ = client

    resp_gh = await _create_sync_config(ac, name="upd-name", provider="github")
    resp_lin = await _create_sync_config(ac, name="upd-name", provider="linear")
    assert resp_gh.status_code == 201
    assert resp_lin.status_code == 201

    gh_id = resp_gh.json()["id"]
    lin_id = resp_lin.json()["id"]

    patch_resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{gh_id}",
        json={"is_active": False},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["is_active"] is False

    # Linear config must remain active.
    lin_resp = await ac.get(f"/api/v1/admin/sync-configs/{lin_id}")
    assert lin_resp.status_code == 200
    assert lin_resp.json()["is_active"] is True
