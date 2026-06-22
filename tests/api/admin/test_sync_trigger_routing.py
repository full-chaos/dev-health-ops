"""Tests for CHAOS-2516: trigger routing via migrated-trigger-routing flag.

Covers:
- flag ON + migrated config => planner path (plan_sync_run called,
  dispatch_sync_run.apply_async called, legacy run_sync_config NOT called)
- flag OFF => legacy path (run_sync_config called, planner NOT called)
- flag ON + un-migrated config => legacy path (no migrated_integration_id)
"""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone
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
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunStatus,
    SyncRunUnit,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    Setting,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.models.users import Organization, User
from dev_health_ops.sync.trigger_routing import MIGRATED_TRIGGER_ROUTING_SETTING_KEY
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
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunUnit,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "trigger-routing-test.db"
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


async def _seed_config(
    session_maker,
    org_id: str,
    *,
    migrated_integration_id: uuid.UUID | None = None,
    planner_managed: bool = False,
    credential_id: uuid.UUID | None = None,
) -> str:
    """Seed a SyncConfiguration and return its id."""
    async with session_maker() as session:
        config = SyncConfiguration(
            name="test-config",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            credential_id=credential_id,
            migrated_integration_id=migrated_integration_id,
            planner_managed=planner_managed,
        )
        session.add(config)
        await session.commit()
        return str(config.id)


async def _seed_child(session_maker, org_id: str, parent_id: str) -> None:
    async with session_maker() as session:
        child = SyncConfiguration(
            name="test-config/child",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            parent_id=uuid.UUID(parent_id),
        )
        session.add(child)
        await session.commit()


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


async def _seed_source(session_maker, org_id: str, integration_id: uuid.UUID) -> None:
    async with session_maker() as session:
        session.add(
            Integration(
                id=integration_id,
                org_id=org_id,
                provider="github",
                name="github-integration",
                config={},
            )
        )
        session.add(
            IntegrationSource(
                org_id=org_id,
                integration_id=integration_id,
                provider="github",
                source_type="repository",
                external_id="full-chaos/dev-health",
                name="dev-health",
                full_name="full-chaos/dev-health",
                is_enabled=True,
            )
        )
        await session.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flag_on_migrated_config_uses_planner_path(client, session_maker):
    """Flag ON + migrated config => plan_sync_run + dispatch_sync_run called,
    legacy run_sync_config NOT called."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    integration_id = uuid.uuid4()
    config_id = await _seed_config(
        session_maker,
        org_id,
        migrated_integration_id=integration_id,
    )
    await _seed_source(session_maker, org_id, integration_id)
    await _seed_planner_flag(session_maker, org_id, value="true")

    fake_plan = MagicMock()
    fake_plan.sync_run_id = str(uuid.uuid4())
    fake_plan.total_units = 3

    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(return_value=MagicMock(id="task-abc"))

    fake_run_sync_config = MagicMock()
    fake_dispatch_batch_sync = MagicMock()

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.plan_sync_run",
            return_value=fake_plan,
        ) as mock_plan,
        patch(
            "dev_health_ops.api.admin.routers.sync.dispatch_sync_run",
            fake_dispatch,
        ),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "triggered"
    assert body["config_id"] == config_id
    assert body["sync_run_id"] == fake_plan.sync_run_id
    assert body["total_units"] == fake_plan.total_units

    # Planner was called
    mock_plan.assert_called_once()
    fake_dispatch.apply_async.assert_called_once_with(
        args=(fake_plan.sync_run_id,), queue="sync"
    )

    # Legacy tasks were NOT called
    fake_run_sync_config.apply_async.assert_not_called()
    fake_dispatch_batch_sync.apply_async.assert_not_called()


@pytest.mark.asyncio
async def test_planner_trigger_rejects_known_bad_credential_before_planning(
    client, session_maker
):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]
    integration_id = uuid.uuid4()
    credential_id = uuid.uuid4()
    async with session_maker() as session:
        credential = IntegrationCredential(
            provider="github",
            name="bad-github",
            org_id=org_id,
            is_active=True,
        )
        credential.id = credential_id
        credential.last_test_success = False
        credential.last_test_error = "GitHub authentication failed"
        session.add(credential)
        await session.commit()

    config_id = await _seed_config(
        session_maker,
        org_id,
        migrated_integration_id=integration_id,
        credential_id=credential_id,
    )
    await _seed_source(session_maker, org_id, integration_id)
    await _seed_planner_flag(session_maker, org_id, value="true")

    with patch("dev_health_ops.api.admin.routers.sync.plan_sync_run") as plan_mock:
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 409
    assert resp.json()["detail"] == "GitHub authentication failed"
    plan_mock.assert_not_called()
    async with session_maker() as session:
        sync_runs = (await session.execute(select(SyncRun))).scalars().all()
        assert sync_runs == []


@pytest.mark.asyncio
async def test_flag_off_uses_legacy_path(client, session_maker):
    """Flag OFF => legacy path (run_sync_config called, planner NOT called)."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    integration_id = uuid.uuid4()
    config_id = await _seed_config(
        session_maker,
        org_id,
        migrated_integration_id=integration_id,
    )
    await _seed_child(session_maker, org_id, config_id)
    # No flag seeded => flag is off

    fake_plan = MagicMock()
    fake_plan.sync_run_id = str(uuid.uuid4())
    fake_plan.total_units = 2

    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(return_value=MagicMock(id="task-xyz"))

    fake_task_result = MagicMock()
    fake_task_result.id = "legacy-task-id"
    fake_run_sync_config = MagicMock()
    fake_run_sync_config.apply_async = MagicMock(return_value=fake_task_result)
    fake_dispatch_batch_sync = MagicMock()
    fake_dispatch_batch_sync.apply_async = MagicMock(return_value=fake_task_result)

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.plan_sync_run",
            return_value=fake_plan,
        ) as mock_plan,
        patch(
            "dev_health_ops.api.admin.routers.sync.dispatch_sync_run",
            fake_dispatch,
        ),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "triggered"
    assert body["config_id"] == config_id
    # Legacy path returns task_id + run_id, not sync_run_id
    assert "task_id" in body or "run_id" in body

    # Planner was NOT called
    mock_plan.assert_not_called()
    fake_dispatch.apply_async.assert_not_called()


@pytest.mark.asyncio
async def test_planner_managed_config_routes_without_flag(client, session_maker):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    integration_id = uuid.uuid4()
    config_id = await _seed_config(
        session_maker,
        org_id,
        migrated_integration_id=integration_id,
        planner_managed=True,
    )
    await _seed_source(session_maker, org_id, integration_id)

    fake_plan = MagicMock()
    fake_plan.sync_run_id = str(uuid.uuid4())
    fake_plan.total_units = 2
    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(return_value=MagicMock(id="task-planner"))
    fake_run_sync_config = MagicMock()
    fake_dispatch_batch_sync = MagicMock()

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.plan_sync_run",
            return_value=fake_plan,
        ) as mock_plan,
        patch("dev_health_ops.api.admin.routers.sync.dispatch_sync_run", fake_dispatch),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    assert resp.json()["sync_run_id"] == fake_plan.sync_run_id
    mock_plan.assert_called_once()
    fake_dispatch.apply_async.assert_called_once_with(
        args=(fake_plan.sync_run_id,), queue="sync"
    )
    fake_run_sync_config.apply_async.assert_not_called()
    fake_dispatch_batch_sync.apply_async.assert_not_called()


@pytest.mark.asyncio
async def test_sourceless_migrated_config_without_flag_uses_legacy_path(
    client, session_maker
):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    config_id = await _seed_config(
        session_maker, org_id, migrated_integration_id=uuid.uuid4()
    )

    fake_plan = MagicMock()
    fake_plan.sync_run_id = str(uuid.uuid4())
    fake_plan.total_units = 2
    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(return_value=MagicMock(id="task-planner"))
    fake_task_result = MagicMock()
    fake_task_result.id = "legacy-task-id"
    fake_run_sync_config = MagicMock()
    fake_run_sync_config.apply_async = MagicMock(return_value=fake_task_result)
    fake_dispatch_batch_sync = MagicMock()
    fake_dispatch_batch_sync.apply_async = MagicMock(return_value=fake_task_result)

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.plan_sync_run",
            return_value=fake_plan,
        ) as mock_plan,
        patch("dev_health_ops.api.admin.routers.sync.dispatch_sync_run", fake_dispatch),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    assert "sync_run_id" not in resp.json()
    mock_plan.assert_not_called()
    fake_dispatch.apply_async.assert_not_called()
    fake_run_sync_config.apply_async.assert_called_once()


@pytest.mark.asyncio
async def test_flag_on_unmigrated_config_uses_legacy_path(client, session_maker):
    """Flag ON + un-migrated config (no migrated_integration_id) => legacy path."""
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    # No migrated_integration_id => un-migrated config
    config_id = await _seed_config(session_maker, org_id, migrated_integration_id=None)
    await _seed_planner_flag(session_maker, org_id, value="true")

    fake_plan = MagicMock()
    fake_plan.sync_run_id = str(uuid.uuid4())
    fake_plan.total_units = 1

    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(return_value=MagicMock(id="task-nope"))

    fake_task_result = MagicMock()
    fake_task_result.id = "legacy-task-id-2"
    fake_run_sync_config = MagicMock()
    fake_run_sync_config.apply_async = MagicMock(return_value=fake_task_result)
    fake_dispatch_batch_sync = MagicMock()
    fake_dispatch_batch_sync.apply_async = MagicMock(return_value=fake_task_result)

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.plan_sync_run",
            return_value=fake_plan,
        ) as mock_plan,
        patch(
            "dev_health_ops.api.admin.routers.sync.dispatch_sync_run",
            fake_dispatch,
        ),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "triggered"
    assert body["config_id"] == config_id
    # Legacy path returns task_id + run_id
    assert "task_id" in body or "run_id" in body

    # Planner was NOT called (plan_request_for_config returns None for un-migrated)
    mock_plan.assert_not_called()
    fake_dispatch.apply_async.assert_not_called()


@pytest.mark.asyncio
async def test_planner_enqueue_failure_returns_202_for_durable_dispatch(
    client, session_maker
):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    integration_id = uuid.uuid4()
    config_id = await _seed_config(
        session_maker, org_id, migrated_integration_id=integration_id
    )
    await _seed_planner_flag(session_maker, org_id, value="true")

    fake_plan = MagicMock()
    fake_plan.sync_run_id = str(uuid.uuid4())
    fake_plan.total_units = 2

    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(side_effect=RuntimeError("broker down"))

    fake_run_sync_config = MagicMock()
    fake_dispatch_batch_sync = MagicMock()

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.plan_sync_run",
            return_value=fake_plan,
        ),
        patch(
            "dev_health_ops.api.admin.routers.sync.dispatch_sync_run",
            fake_dispatch,
        ),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 503, resp.text
    fake_dispatch.apply_async.assert_called_once()
    # Committed run marked FAILED so it is not stranded PLANNED with no dispatcher.
    async with session_maker() as session:
        job_run = (await session.execute(select(JobRun))).scalar_one()
        assert job_run.status == JobRunStatus.FAILED.value
        assert job_run.error == "dispatch enqueue failed: broker down"
        assert job_run.completed_at is not None
    # Legacy path NOT used -- we surfaced the queue outage instead.
    fake_run_sync_config.apply_async.assert_not_called()
    fake_dispatch_batch_sync.apply_async.assert_not_called()


# ---------------------------------------------------------------------------
# WS-A CHAOS-2579: migrated config full_resync intent mapping
# ---------------------------------------------------------------------------


def test_plan_request_for_config_promotes_full_resync_from_sync_options():
    """Migrated config with sync_options.full_resync=True -> mode=full_resync.

    Mirrors legacy worker semantics (sync_runtime.py:656 / sync_batch.py:657).
    """
    from dev_health_ops.models import SyncRunMode
    from dev_health_ops.sync.trigger_routing import plan_request_for_config

    config = SyncConfiguration(
        name="full-resync-config",
        provider="github",
        org_id=str(uuid.uuid4()),
        sync_targets=["git"],
        sync_options={"full_resync": True},
        migrated_integration_id=uuid.uuid4(),
    )

    request = plan_request_for_config(config, triggered_by="test")

    assert request is not None
    assert request.mode == SyncRunMode.FULL_RESYNC.value, (
        "sync_options.full_resync=True must promote mode to full_resync"
    )


def test_plan_request_for_config_does_not_override_explicit_backfill():
    """Explicit mode=backfill is NOT overridden even if sync_options.full_resync=True."""
    from dev_health_ops.models import SyncRunMode
    from dev_health_ops.sync.trigger_routing import plan_request_for_config

    config = SyncConfiguration(
        name="full-resync-config",
        provider="github",
        org_id=str(uuid.uuid4()),
        sync_targets=["git"],
        sync_options={"full_resync": True},
        migrated_integration_id=uuid.uuid4(),
    )

    request = plan_request_for_config(
        config, triggered_by="test", mode=SyncRunMode.BACKFILL.value
    )

    assert request is not None
    assert request.mode == SyncRunMode.BACKFILL.value, (
        "Explicit backfill mode must not be overridden by sync_options.full_resync"
    )


def test_plan_request_for_config_incremental_without_full_resync_flag():
    """Migrated config without sync_options.full_resync stays incremental."""
    from dev_health_ops.models import SyncRunMode
    from dev_health_ops.sync.trigger_routing import plan_request_for_config

    config = SyncConfiguration(
        name="incremental-config",
        provider="github",
        org_id=str(uuid.uuid4()),
        sync_targets=["git"],
        sync_options={},
        migrated_integration_id=uuid.uuid4(),
    )

    request = plan_request_for_config(config, triggered_by="test")

    assert request is not None
    assert request.mode == SyncRunMode.INCREMENTAL.value


@pytest.mark.asyncio
async def test_planner_trigger_creates_jobrun_anchor_visible_in_jobs(
    client, session_maker
):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    integration_id = uuid.uuid4()
    config_id = await _seed_config(
        session_maker,
        org_id,
        migrated_integration_id=integration_id,
        planner_managed=True,
    )
    await _seed_source(session_maker, org_id, integration_id)

    fake_dispatch = MagicMock()
    fake_dispatch.apply_async = MagicMock(return_value=MagicMock(id="task-real"))
    fake_run_sync_config = MagicMock()
    fake_dispatch_batch_sync = MagicMock()

    with (
        patch(
            "dev_health_ops.api.admin.routers.sync.dispatch_sync_run",
            fake_dispatch,
        ),
        patch.dict(
            "sys.modules",
            {
                "dev_health_ops.workers.sync_tasks": MagicMock(
                    run_sync_config=fake_run_sync_config,
                    dispatch_batch_sync=fake_dispatch_batch_sync,
                )
            },
        ),
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    body = resp.json()
    sync_run_id = body.get("sync_run_id")
    job_run_id = body.get("run_id")
    assert sync_run_id, f"planner trigger must return a sync_run_id: {body}"
    assert job_run_id, f"planner trigger must return a JobRun run_id: {body}"
    fake_dispatch.apply_async.assert_called_once()

    async with session_maker() as session:
        run = await session.get(SyncRun, uuid.UUID(sync_run_id))
        assert run is not None, "trigger must create a SyncRun row"
        assert str(run.integration_id) == str(integration_id)
        assert run.org_id == org_id
        job_run = await session.get(JobRun, uuid.UUID(job_run_id))
        assert job_run is not None, "trigger must create a JobRun anchor"
        assert job_run.triggered_by == "manual"
        assert job_run.status == JobRunStatus.PENDING.value
        assert job_run.completed_at is None
        assert job_run.result == {
            "sync_run_id": sync_run_id,
            "dispatch_task_id": "task-real",
        }

    jobs_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/jobs")
    assert jobs_resp.status_code == 200, jobs_resp.text
    jobs = jobs_resp.json()
    ids = {job["id"] for job in jobs}
    assert job_run_id in ids, f"planner JobRun anchor must appear in jobs: {ids}"
    surfaced = next(job for job in jobs if job["id"] == job_run_id)
    assert surfaced["triggered_by"] == "manual"
    assert surfaced["status"] == "pending"
    assert surfaced["started_at"] is None
    assert surfaced["completed_at"] is None
    assert surfaced["duration_seconds"] is None
    assert surfaced["items_synced"] == 0
    assert surfaced["result"]["sync_run_id"] == sync_run_id
    assert surfaced["result"]["dispatch_task_id"] == "task-real"
    assert surfaced["result"]["sync_run_status"] == SyncRunStatus.PLANNED.value
    assert surfaced["result"]["total_units"] == run.total_units
    assert surfaced["result"]["completed_units"] == 0
    assert surfaced["result"]["failed_units"] == 0

    started_at = datetime.now(timezone.utc) - timedelta(seconds=42)
    async with session_maker() as session:
        dispatching_run = await session.get(SyncRun, uuid.UUID(sync_run_id))
        assert dispatching_run is not None
        dispatching_run.status = SyncRunStatus.DISPATCHING.value
        dispatching_run.started_at = started_at
        await session.commit()

    jobs_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/jobs")
    assert jobs_resp.status_code == 200, jobs_resp.text
    surfaced = next(job for job in jobs_resp.json() if job["id"] == job_run_id)
    assert surfaced["status"] == "running"
    assert surfaced["started_at"] is not None
    assert surfaced["completed_at"] is None
    assert surfaced["duration_seconds"] is None
    assert surfaced["result"]["sync_run_status"] == SyncRunStatus.DISPATCHING.value

    completed_at = datetime.now(timezone.utc)
    async with session_maker() as session:
        completed_run = await session.get(SyncRun, uuid.UUID(sync_run_id))
        assert completed_run is not None
        completed_run.status = SyncRunStatus.SUCCESS.value
        completed_run.started_at = started_at
        completed_run.completed_at = completed_at
        completed_run.completed_units = 3
        completed_run.failed_units = 0
        completed_run.result = {"completed_units": 3, "failed_units": 0}
        await session.commit()

    jobs_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/jobs")
    assert jobs_resp.status_code == 200, jobs_resp.text
    surfaced = next(job for job in jobs_resp.json() if job["id"] == job_run_id)
    assert surfaced["status"] == "success"
    assert surfaced["started_at"] is not None
    assert surfaced["completed_at"] is not None
    assert surfaced["duration_seconds"] == 42
    assert surfaced["items_synced"] == 3
    assert surfaced["result"]["sync_run_status"] == SyncRunStatus.SUCCESS.value
    assert surfaced["result"]["completed_units"] == 3
    assert surfaced["result"]["failed_units"] == 0

    async with session_maker() as session:
        partial_run = await session.get(SyncRun, uuid.UUID(sync_run_id))
        assert partial_run is not None
        partial_run.status = SyncRunStatus.PARTIAL_FAILED.value
        partial_run.completed_units = 1
        partial_run.failed_units = 2
        partial_run.error = "unit failure"
        partial_run.result = {"completed_units": 999, "failed_units": 999}
        await session.commit()

    jobs_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/jobs")
    assert jobs_resp.status_code == 200, jobs_resp.text
    surfaced = next(job for job in jobs_resp.json() if job["id"] == job_run_id)
    assert surfaced["status"] == "failed"
    assert surfaced["error"] == "unit failure"
    assert surfaced["items_synced"] == 1
    assert surfaced["result"]["sync_run_status"] == SyncRunStatus.PARTIAL_FAILED.value
    assert surfaced["result"]["completed_units"] == 1
    assert surfaced["result"]["failed_units"] == 2
