from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobStatus,
    ScheduledJob,
    Setting,
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
    Setting,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncRun,
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
async def test_batch_create_counts_cross_provider_planner_sources_for_repo_limit(
    client, monkeypatch
):
    ac, _ = client
    requested_counts = []

    class TierLimitStub:
        def __init__(self, _session):
            pass

        def check_repo_limit(self, _org_id, requested_count):
            requested_counts.append(requested_count)
            if requested_count > 3:
                return False, "Repo limit exceeded"
            return True, None

    monkeypatch.setattr(sync_router_module, "TierLimitService", TierLimitStub)

    first = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "repo-cap-one",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos"},
            "repos": ["one", "two"],
        },
    )
    second = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "repo-cap-gitlab",
            "provider": "gitlab",
            "sync_targets": ["git"],
            "sync_options": {"group": "full-chaos"},
            "repos": ["3", "4"],
        },
    )

    assert first.status_code == 201, first.text
    assert second.status_code == 403, second.text
    assert requested_counts == [2, 4]


@pytest.mark.asyncio
async def test_single_create_counts_existing_planner_sources_for_repo_limit(
    client, monkeypatch
):
    ac, _ = client
    requested_counts = []

    class TierLimitStub:
        def __init__(self, _session):
            pass

        def check_repo_limit(self, _org_id, requested_count):
            requested_counts.append(requested_count)
            if requested_count > 2:
                return False, "Repo limit exceeded"
            return True, None

    monkeypatch.setattr(sync_router_module, "TierLimitService", TierLimitStub)

    batch = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "repo-cap-batch",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos"},
            "repos": ["one", "two"],
        },
    )
    single = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "repo-cap-single",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos", "repo": "three"},
        },
    )

    assert batch.status_code == 201, batch.text
    assert single.status_code == 403, single.text
    assert requested_counts == [2, 3]


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
    # An explicit schedule on an active config keeps the job ACTIVE.
    assert job.status == JobStatus.ACTIVE.value


@pytest.mark.asyncio
async def test_update_sync_config_explicit_null_clears_schedule(client, session_maker):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="clear-schedule")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        set_resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={"schedule_cron": "0 0 * * *", "timezone": "America/Los_Angeles"},
        )
    assert set_resp.status_code == 200, set_resp.text

    # Clear the schedule. The payload also carries a stale nested copy of the
    # old cron (what a stale client sends); it must NOT resurrect the schedule.
    resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{config_id}",
        json={
            "schedule_cron": None,
            "timezone": "America/Los_Angeles",
            "sync_options": {
                "owner": "full-chaos",
                "schedule_cron": "0 0 * * *",
            },
        },
    )

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "schedule_cron" not in data["sync_options"]
    assert data["sync_options"]["owner"] == "full-chaos"
    assert data["sync_options"]["timezone"] == "America/Los_Angeles"
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
    assert "schedule_cron" not in config.sync_options
    assert config.sync_options["owner"] == "full-chaos"
    # Clearing the schedule parks the ScheduledJob so the scheduler never
    # auto-dispatches a manual-only config (CHAOS-2297).
    assert job.status == JobStatus.PAUSED.value


@pytest.mark.asyncio
async def test_update_sync_config_omitted_schedule_fields_left_unchanged(
    client, session_maker
):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="omit-schedule")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        set_resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={"schedule_cron": "30 2 * * *", "timezone": "Europe/Berlin"},
        )
    assert set_resp.status_code == 200, set_resp.text

    # Omitting schedule fields entirely must leave the stored values untouched.
    resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{config_id}",
        json={"is_active": False},
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
    assert config.is_active is False
    assert config.sync_options["schedule_cron"] == "30 2 * * *"
    assert config.sync_options["timezone"] == "Europe/Berlin"


@pytest.mark.asyncio
async def test_create_sync_config_without_schedule_creates_paused_job(
    client, session_maker
):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="manual-only")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    # Manual-only configs (no schedule_cron) keep a job row for manual-trigger
    # JobRun anchoring but must stay PAUSED (CHAOS-2297).
    assert job.status == JobStatus.PAUSED.value


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
async def test_trigger_sync_config_returns_202_with_task_id(client, monkeypatch):
    ac, _ = client
    monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")

    create_resp = await _create_sync_config(ac, name="trigger-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="fake-task-id")
    mock_run = MagicMock()
    mock_run.apply_async.return_value = mock_task

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "triggered"
    assert data["task_id"] == "fake-task-id"
    assert data["config_id"] == config_id
    # CHAOS-2299: manual triggers route to the provider's dedicated queue.
    assert mock_run.apply_async.call_args.kwargs["queue"] == "sync.github"


@pytest.mark.asyncio
async def test_trigger_sync_config_routes_batch_eligible_to_batch_task(
    client, monkeypatch
):
    ac, _ = client
    monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")

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
    mock_batch.apply_async.return_value = mock_task
    mock_run = MagicMock()

    with (
        patch("dev_health_ops.workers.sync_tasks.dispatch_batch_sync", mock_batch),
        patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    assert resp.json()["task_id"] == "batch-task-id"
    mock_batch.apply_async.assert_called_once()
    assert mock_batch.apply_async.call_args.kwargs["queue"] == "sync.github"
    mock_run.apply_async.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "expected_queue"),
    [
        ("github", "sync.github"),
        ("gitlab", "sync.gitlab"),
        ("linear", "sync.linear"),
        ("jira", "sync.jira"),
        ("launchdarkly", "sync.launchdarkly"),
        ("someday-provider", "sync"),
    ],
)
async def test_trigger_routes_to_per_provider_queue(
    client, provider, expected_queue, monkeypatch
):
    """CHAOS-2299: with PROVIDER_SYNC_QUEUES_ENABLED, manual triggers land on
    sync.<provider>; unknown providers fall back to the shared sync queue."""
    ac, _ = client
    monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")

    create_resp = await _create_sync_config(
        ac, name=f"queue-route-{provider}", provider=provider
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="queue-task-id")
    mock_run = MagicMock()
    mock_run.apply_async.return_value = mock_task

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    mock_run.apply_async.assert_called_once()
    assert mock_run.apply_async.call_args.kwargs["queue"] == expected_queue


@pytest.mark.asyncio
async def test_trigger_routes_to_shared_queue_when_flag_off(client, monkeypatch):
    """CHAOS-2299 rollout safety: with the flag unset (the default), even
    known providers stay on the legacy shared `sync` queue so workers that
    have not expanded their -Q lists still consume every dispatch."""
    ac, _ = client
    monkeypatch.delenv("PROVIDER_SYNC_QUEUES_ENABLED", raising=False)

    create_resp = await _create_sync_config(ac, name="queue-route-flag-off")
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="queue-task-id")
    mock_run = MagicMock()
    mock_run.apply_async.return_value = mock_task

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    mock_run.apply_async.assert_called_once()
    assert mock_run.apply_async.call_args.kwargs["queue"] == "sync"


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
    mock_run.apply_async.side_effect = Exception("Celery broker connection refused")

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


# ---------------------------------------------------------------------------
# CHAOS-2255: PENDING JobRun created at trigger time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_creates_pending_job_run(client, session_maker):
    """Trigger must persist a PENDING JobRun before dispatching the Celery task."""
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="pending-run-test", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="pending-task-id")
    mock_run = MagicMock()
    mock_run.apply_async.return_value = mock_task

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "triggered"
    assert "run_id" in data
    run_id = data["run_id"]

    # Verify a PENDING JobRun row was persisted.
    async with session_maker() as session:
        result = await session.execute(
            select(JobRun).where(JobRun.id == uuid.UUID(run_id))
        )
        run = result.scalar_one_or_none()

    assert run is not None
    assert run.status == JobRunStatus.PENDING.value
    assert run.triggered_by == "manual"


@pytest.mark.asyncio
async def test_trigger_passes_pending_run_id_to_task(client):
    """Trigger must pass pending_run_id kwarg to the dispatched Celery task."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="run-id-thread-test", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="threaded-task-id")
    mock_run = MagicMock()
    mock_run.apply_async.return_value = mock_task

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    run_id = resp.json()["run_id"]

    # The task must have been called with pending_run_id matching the returned run_id.
    call_kwargs = mock_run.apply_async.call_args.kwargs["kwargs"]
    assert call_kwargs.get("pending_run_id") == run_id


@pytest.mark.asyncio
async def test_trigger_commits_pending_job_run_before_enqueue(client, session_maker):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="pre-enqueue-commit-test", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]
    sync_url = str(session_maker.kw["bind"].url).replace("sqlite+aiosqlite", "sqlite")

    def assert_pending_run_is_visible(**_kwargs):
        engine = create_engine(sync_url)
        try:
            with Session(engine) as session:
                assert session.query(JobRun).count() == 1
        finally:
            engine.dispose()
        return MagicMock(id="visible-task-id")

    mock_run = MagicMock()
    mock_run.apply_async.side_effect = assert_pending_run_is_visible

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_trigger_marks_pending_job_run_failed_when_enqueue_fails(
    client, session_maker
):
    from dev_health_ops.models.settings import JobRunStatus

    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="enqueue-failure-test", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_run = MagicMock()
    mock_run.apply_async.side_effect = RuntimeError("broker down")

    with patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 503
    assert "broker down" in resp.json()["detail"]

    async with session_maker() as session:
        result = await session.execute(select(JobRun))
        runs = list(result.scalars().all())

    assert len(runs) == 1
    run = runs[0]
    assert run.status == JobRunStatus.FAILED.value
    assert run.completed_at is not None
    assert run.error == "dispatch enqueue failed: broker down"


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger_child", [False, True])
async def test_trigger_inactive_config_returns_409_without_execution(
    client, session_maker, trigger_child
):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    async with session_maker() as session:
        parent = SyncConfiguration(
            name="paused-parent",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            is_active=False,
        )
        session.add(parent)
        await session.flush()
        target_id = parent.id
        if trigger_child:
            child = SyncConfiguration(
                name="paused-parent/repo",
                provider="github",
                org_id=org_id,
                sync_targets=["git"],
                sync_options={"owner": "paused-parent", "repo": "repo"},
                is_active=False,
                parent_id=parent.id,
            )
            session.add(child)
            await session.flush()
            target_id = child.id
        await session.commit()

    mock_run = MagicMock()
    mock_batch = MagicMock()
    mock_dispatch = MagicMock()
    with (
        patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run),
        patch("dev_health_ops.workers.sync_tasks.dispatch_batch_sync", mock_batch),
        patch("dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{target_id}/trigger")

    assert resp.status_code == 409
    assert "paused" in resp.json()["detail"]
    mock_run.apply_async.assert_not_called()
    mock_batch.apply_async.assert_not_called()
    mock_dispatch.apply_async.assert_not_called()

    async with session_maker() as session:
        job_runs = (await session.execute(select(JobRun))).scalars().all()
        sync_runs = (await session.execute(select(SyncRun))).scalars().all()
    assert job_runs == []
    assert sync_runs == []


def test_sync_tasks_accept_pending_run_id_kwarg():
    """The dispatched Celery tasks must accept every kwarg the trigger endpoint
    passes. Celery validates kwargs against the task signature at dispatch time,
    so a missing parameter fails the API request itself (regression: PR #846
    clobbered the pending_run_id parameter that PR #844 added to run_sync_config).
    """
    import inspect

    from dev_health_ops.workers.sync_tasks import (
        dispatch_batch_sync,
        run_sync_config,
    )

    for task in (run_sync_config, dispatch_batch_sync):
        params = inspect.signature(task.run).parameters
        for kwarg in ("config_id", "org_id", "triggered_by", "pending_run_id"):
            assert kwarg in params, f"{task.name} is missing kwarg {kwarg!r}"


@pytest.mark.asyncio
async def test_trigger_batch_creates_pending_job_run(client, session_maker):
    """Batch-eligible trigger must also persist a PENDING JobRun."""
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "batch-pending-run",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"search": "full-chaos"},
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="batch-pending-task-id")
    mock_batch = MagicMock()
    mock_batch.apply_async.return_value = mock_task
    mock_run = MagicMock()

    with (
        patch("dev_health_ops.workers.sync_tasks.dispatch_batch_sync", mock_batch),
        patch("dev_health_ops.workers.sync_tasks.run_sync_config", mock_run),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202
    data = resp.json()
    assert "run_id" in data
    run_id = data["run_id"]

    # Verify a PENDING JobRun row was persisted for the batch parent.
    async with session_maker() as session:
        result = await session.execute(
            select(JobRun).where(JobRun.id == uuid.UUID(run_id))
        )
        run = result.scalar_one_or_none()

    assert run is not None
    assert run.status == JobRunStatus.PENDING.value

    # Batch task must have been called with pending_run_id.
    call_kwargs = mock_batch.apply_async.call_args.kwargs["kwargs"]
    assert call_kwargs.get("pending_run_id") == run_id
    mock_run.apply_async.assert_not_called()


# ---------------------------------------------------------------------------
# Batch create — provider-shaped child sync_options (CHAOS-2283)
# ---------------------------------------------------------------------------


def _gitlab_project(project_id: int, name: str, full_name: str):
    from dev_health_ops.connectors.models import Repository

    return Repository(
        id=project_id,
        name=name,
        full_name=full_name,
        default_branch="main",
    )


@pytest.mark.asyncio
async def test_batch_create_github_creates_planner_config_without_children(
    client, session_maker
):
    ac, seeded_state = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gh-batch",
            "provider": "github",
            "sync_targets": ["git", "prs"],
            "sync_options": {"owner": "acme"},
            "repos": ["alpha", "beta"],
            "schedule_cron": "0 3 * * *",
        },
    )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []
    assert data["parent"]["is_active"] is True

    async with session_maker() as session:
        config_id = uuid.UUID(data["parent"]["id"])
        children = (
            (
                await session.execute(
                    select(SyncConfiguration).where(
                        SyncConfiguration.parent_id == config_id
                    )
                )
            )
            .scalars()
            .all()
        )
        integrations = (
            (
                await session.execute(
                    select(Integration).where(
                        Integration.org_id == seeded_state["org_id"]
                    )
                )
            )
            .scalars()
            .all()
        )
        sources = (await session.execute(select(IntegrationSource))).scalars().all()
        datasets = (await session.execute(select(IntegrationDataset))).scalars().all()
        jobs = (await session.execute(select(ScheduledJob))).scalars().all()

    assert children == []
    assert len(integrations) == 1
    assert str(data["parent"]["id"])
    assert len(sources) == 2
    assert {source.external_id for source in sources} == {"acme/alpha", "acme/beta"}
    assert {dataset.dataset_key for dataset in datasets} >= {
        "repo-metadata",
        "commits",
        "prs",
    }
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.ACTIVE.value


@pytest.mark.asyncio
async def test_batch_create_gitlab_children_get_project_id_and_group(
    client, session_maker
):
    """GitLab children carry int project_id + group (+ gitlab_url), not repo."""
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
        _gitlab_project(202, "beta", "acme-group/sub/beta"),
    ]
    mock_connector_cls = MagicMock(return_value=mock_connector)

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            mock_connector_cls,
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git", "prs"],
                "sync_options": {
                    "owner": "acme-group",
                    "gitlab_url": "https://gitlab.example.com",
                },
                "repos": ["alpha", "beta"],
                "schedule_cron": "0 3 * * *",
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []

    mock_connector_cls.assert_called_once_with(
        url="https://gitlab.example.com", private_token="glpat-test"
    )
    mock_connector.list_repositories.assert_called_once_with(org_name="acme-group")

    async with session_maker() as session:
        sources = (await session.execute(select(IntegrationSource))).scalars().all()
    assert {source.external_id for source in sources} == {"101", "202"}
    assert {source.full_name for source in sources} == {
        "acme-group/alpha",
        "acme-group/sub/beta",
    }


@pytest.mark.asyncio
async def test_batch_create_gitlab_numeric_ids_skip_resolution(client):
    """Numeric repos entries are used as project ids without a credential."""
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gl-batch-ids",
            "provider": "gitlab",
            "sync_targets": ["git"],
            "sync_options": {"group": "acme-group"},
            "repos": ["123", "456"],
        },
    )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []


@pytest.mark.asyncio
async def test_batch_create_gitlab_unknown_project_name_returns_400(client):
    ac, _ = client

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            MagicMock(return_value=mock_connector),
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-missing",
                "provider": "gitlab",
                "credential_id": str(uuid.uuid4()),
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["ghost"],
            },
        )

    assert resp.status_code == 400
    assert "ghost" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_batch_create_gitlab_names_without_credential_returns_400(client):
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gl-batch-nocred",
            "provider": "gitlab",
            "sync_targets": ["git"],
            "sync_options": {"owner": "acme-group"},
            "repos": ["alpha"],
        },
    )

    assert resp.status_code == 400
    assert "credential_id" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_batch_create_gitlab_credential_url_persisted_into_children(
    client, session_maker
):
    """Self-hosted URL from the credential lands in parent + child options.

    Regression: name resolution used the credential's self-hosted URL but
    children only inherited a gitlab_url already present in parent options,
    so they later synced against the gitlab.com runtime default.
    """
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=(
            {"token": "glpat-test", "url": "https://gitlab.internal.example.com"},
            MagicMock(config={}),
        )
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]
    mock_connector_cls = MagicMock(return_value=mock_connector)

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            mock_connector_cls,
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-selfhosted",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["alpha"],
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()

    # Resolution used the credential's self-hosted URL...
    mock_connector_cls.assert_called_once_with(
        url="https://gitlab.internal.example.com", private_token="glpat-test"
    )
    # ...and that URL is persisted into both parent and child options.
    assert (
        data["parent"]["sync_options"]["gitlab_url"]
        == "https://gitlab.internal.example.com"
    )
    assert data["children"] == []
    async with session_maker() as session:
        source = (await session.execute(select(IntegrationSource))).scalar_one()
    assert source.external_id == "101"
    assert source.full_name == "acme-group/alpha"


@pytest.mark.asyncio
async def test_batch_create_gitlab_numeric_entry_matching_name_resolves_as_name(
    client, session_maker
):
    """A numeric entry that matches a listed project NAME is a name, not an id."""
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(7007, "007", "acme-group/007"),
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            MagicMock(return_value=mock_connector),
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-numeric-name",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["007"],
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    mock_connector.list_repositories.assert_called_once_with(org_name="acme-group")

    assert data["children"] == []
    async with session_maker() as session:
        source = (await session.execute(select(IntegrationSource))).scalar_one()
    assert source.full_name == "acme-group/007"
    assert source.external_id == "7007"


@pytest.mark.asyncio
async def test_batch_create_gitlab_numeric_entry_not_in_listing_used_as_id(
    client, session_maker
):
    """A numeric entry matching no listed name keeps project-id semantics."""
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            MagicMock(return_value=mock_connector),
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-numeric-id",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["12345"],
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    # The listing WAS consulted (credential + group present)...
    mock_connector.list_repositories.assert_called_once_with(org_name="acme-group")

    assert data["children"] == []
    async with session_maker() as session:
        source = (await session.execute(select(IntegrationSource))).scalar_one()
    assert source.external_id == "12345"
    assert source.full_name == "acme-group/12345"
