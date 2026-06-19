"""Tests for CHAOS-2536: legacy backfill creates a visible PENDING JobRun.

Covers:
- Endpoint (legacy path): creates PENDING JobRun anchored to sync ScheduledJob.
- Endpoint (legacy path): passes pending_run_id to run_backfill.delay().
- Endpoint (fanout path): does NOT create a JobRun (passes pending_run_id=None).
- Worker: run_backfill transitions JobRun RUNNING→SUCCESS on success.
- Worker: run_backfill transitions JobRun RUNNING→FAILED on exception.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
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
    BackfillJob,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "backfill-jobrun.db"
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


async def _create_sync_config(ac, name: str = "my-sync", provider: str = "github"):
    return await ac.post(
        "/api/v1/admin/sync-configs",
        json={"name": name, "provider": provider, "sync_targets": []},
    )


# ---------------------------------------------------------------------------
# Endpoint tests — legacy path (fanout disabled)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_legacy_creates_pending_job_run(client, session_maker):
    """Legacy backfill must persist a PENDING JobRun before dispatching."""
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="bf-legacy-run", provider="github")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="bf-task-id")
    mock_run_backfill = MagicMock()
    mock_run_backfill.delay.return_value = mock_task

    # Ensure legacy path: migrated_integration_id is None (default), fanout env off.
    with (
        patch("dev_health_ops.workers.sync_tasks.run_backfill", mock_run_backfill),
        patch.dict("os.environ", {"SYNC_FANOUT_BACKFILL": ""}),
    ):
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["mode"] == "legacy"

    # A PENDING JobRun must exist anchored to the config's ScheduledJob.
    async with session_maker() as session:
        # Find the ScheduledJob for this config.
        sj_result = await session.execute(
            select(ScheduledJob).where(
                ScheduledJob.sync_config_id == uuid.UUID(config_id),
                ScheduledJob.job_type == "sync",
            )
        )
        sched_job = sj_result.scalar_one_or_none()
        assert sched_job is not None, "ScheduledJob must be created"

        # Find the JobRun anchored to that ScheduledJob.
        jr_result = await session.execute(
            select(JobRun).where(JobRun.job_id == sched_job.id)
        )
        runs = list(jr_result.scalars().all())

    assert len(runs) == 1
    run = runs[0]
    assert run.status == JobRunStatus.PENDING.value
    assert run.triggered_by == "backfill"


@pytest.mark.asyncio
async def test_backfill_legacy_passes_pending_run_id_to_task(client):
    """Legacy backfill must thread pending_run_id into run_backfill.delay()."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-run-id-thread", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    mock_task = MagicMock(id="bf-thread-task-id")
    mock_run_backfill = MagicMock()
    mock_run_backfill.delay.return_value = mock_task

    with (
        patch("dev_health_ops.workers.sync_tasks.run_backfill", mock_run_backfill),
        patch.dict("os.environ", {"SYNC_FANOUT_BACKFILL": ""}),
    ):
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text

    # pending_run_id must be a non-None UUID string passed to .delay().
    call_kwargs = mock_run_backfill.delay.call_args.kwargs
    pending_run_id = call_kwargs.get("pending_run_id")
    assert pending_run_id is not None
    # Must be a valid UUID string.
    uuid.UUID(pending_run_id)


@pytest.mark.asyncio
async def test_backfill_fanout_does_not_create_job_run(client, session_maker):
    """Fan-out backfill must NOT create a JobRun (passes pending_run_id=None)."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-fanout-no-run", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    # Patch the config to look migrated so fanout path is taken.
    async with session_maker() as session:
        result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.id == uuid.UUID(config_id)
            )
        )
        cfg = result.scalar_one()
        setattr(cfg, "migrated_integration_id", uuid.uuid4())
        await session.commit()

    mock_task = MagicMock(id="bf-fanout-task-id")
    mock_run_backfill = MagicMock()
    mock_run_backfill.delay.return_value = mock_task

    with (
        patch("dev_health_ops.workers.sync_tasks.run_backfill", mock_run_backfill),
        patch.dict("os.environ", {"SYNC_FANOUT_BACKFILL": "true"}),
    ):
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["mode"] == "fanout"

    # pending_run_id must be None for the fanout path.
    call_kwargs = mock_run_backfill.delay.call_args.kwargs
    assert call_kwargs.get("pending_run_id") is None

    # No JobRun rows should exist.
    async with session_maker() as session:
        jr_result = await session.execute(select(JobRun))
        runs = list(jr_result.scalars().all())
    assert runs == []


# ---------------------------------------------------------------------------
# Worker-level tests — JobRun state transitions
# ---------------------------------------------------------------------------


def _make_sqlite_session_factory(db_path: str):
    """Return a sync SQLAlchemy session factory backed by SQLite."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine, tables=_TABLES)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False), engine


@pytest.mark.asyncio
async def test_run_backfill_worker_transitions_job_run_running_then_success(
    tmp_path,
):
    """run_backfill (legacy) must transition JobRun PENDING→RUNNING→SUCCESS."""
    from dev_health_ops.workers.sync_backfill import (
        _mark_sync_job_run_running,
        _mark_sync_job_run_success,
    )

    db_path = str(tmp_path / "worker-success.db")
    SessionFactory, engine = _make_sqlite_session_factory(db_path)

    # Seed: ScheduledJob + PENDING JobRun.
    with SessionFactory() as session:
        org_id = str(uuid.uuid4())
        config_id = uuid.uuid4()
        sj = ScheduledJob(
            name=f"sync-config-{config_id}",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id=org_id,
            provider="github",
            job_config={},
            sync_config_id=config_id,
            tz="UTC",
            status=1,
        )
        session.add(sj)
        session.flush()
        run = JobRun(
            job_id=sj.id,
            triggered_by="backfill",
            status=JobRunStatus.PENDING.value,
        )
        session.add(run)
        session.commit()
        pending_run_id = str(run.id)

    started_at = datetime.now(timezone.utc)
    completed_at = datetime.now(timezone.utc)

    # Patch get_postgres_session_sync to use our SQLite session.
    from contextlib import contextmanager

    @contextmanager
    def _fake_pg_session():
        with SessionFactory() as s:
            yield s
            s.commit()

    with patch(
        "dev_health_ops.db.get_postgres_session_sync",
        _fake_pg_session,
    ):
        _mark_sync_job_run_running(pending_run_id, started_at)

    with SessionFactory() as session:
        run_row = (
            session.query(JobRun).filter(JobRun.id == uuid.UUID(pending_run_id)).one()
        )
        assert run_row.status == JobRunStatus.RUNNING.value
        assert run_row.started_at is not None

    with patch(
        "dev_health_ops.db.get_postgres_session_sync",
        _fake_pg_session,
    ):
        _mark_sync_job_run_success(pending_run_id, completed_at)

    with SessionFactory() as session:
        run_row = (
            session.query(JobRun).filter(JobRun.id == uuid.UUID(pending_run_id)).one()
        )
        assert run_row.status == JobRunStatus.SUCCESS.value
        assert run_row.completed_at is not None

    engine.dispose()


@pytest.mark.asyncio
async def test_run_backfill_worker_transitions_job_run_running_then_failed(
    tmp_path,
):
    """run_backfill (legacy) must transition JobRun PENDING→RUNNING→FAILED on error."""
    from dev_health_ops.workers.sync_backfill import (
        _mark_sync_job_run_failed,
        _mark_sync_job_run_running,
    )

    db_path = str(tmp_path / "worker-failed.db")
    SessionFactory, engine = _make_sqlite_session_factory(db_path)

    with SessionFactory() as session:
        org_id = str(uuid.uuid4())
        config_id = uuid.uuid4()
        sj = ScheduledJob(
            name=f"sync-config-{config_id}",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id=org_id,
            provider="github",
            job_config={},
            sync_config_id=config_id,
            tz="UTC",
            status=1,
        )
        session.add(sj)
        session.flush()
        run = JobRun(
            job_id=sj.id,
            triggered_by="backfill",
            status=JobRunStatus.PENDING.value,
        )
        session.add(run)
        session.commit()
        pending_run_id = str(run.id)

    started_at = datetime.now(timezone.utc)
    completed_at = datetime.now(timezone.utc)

    from contextlib import contextmanager

    @contextmanager
    def _fake_pg_session():
        with SessionFactory() as s:
            yield s
            s.commit()

    with patch(
        "dev_health_ops.db.get_postgres_session_sync",
        _fake_pg_session,
    ):
        _mark_sync_job_run_running(pending_run_id, started_at)
        _mark_sync_job_run_failed(pending_run_id, "something exploded", completed_at)

    with SessionFactory() as session:
        run_row = (
            session.query(JobRun).filter(JobRun.id == uuid.UUID(pending_run_id)).one()
        )
        assert run_row.status == JobRunStatus.FAILED.value
        assert run_row.error == "something exploded"
        assert run_row.completed_at is not None

    engine.dispose()


def test_run_backfill_helpers_noop_on_none():
    """All three JobRun helpers must silently no-op when pending_run_id is None."""
    from dev_health_ops.workers.sync_backfill import (
        _mark_sync_job_run_failed,
        _mark_sync_job_run_running,
        _mark_sync_job_run_success,
    )

    now = datetime.now(timezone.utc)
    # Must not raise, must not call get_postgres_session_sync.
    with patch("dev_health_ops.db.get_postgres_session_sync") as mock_pg:
        _mark_sync_job_run_running(None, now)
        _mark_sync_job_run_success(None, now)
        _mark_sync_job_run_failed(None, "err", now)
    mock_pg.assert_not_called()


def test_run_backfill_signature_has_pending_run_id():
    """run_backfill task must declare pending_run_id parameter."""
    import inspect

    from dev_health_ops.workers.sync_backfill import run_backfill

    params = inspect.signature(run_backfill.run).parameters
    assert "pending_run_id" in params, (
        "run_backfill is missing pending_run_id parameter"
    )
