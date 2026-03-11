from __future__ import annotations

import importlib
import json
import os
import uuid
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobStatus,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.users import Organization, User

admin_router_module = importlib.import_module("dev_health_ops.api.admin.router")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = [
    User.__table__,
    Organization.__table__,
    SyncConfiguration.__table__,
    ScheduledJob.__table__,
    JobRun.__table__,
    BackfillJob.__table__,
]


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "backfill-integration.db"
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
    sync_config_id = uuid.uuid4()
    sync_config = SyncConfiguration(
        org_id=str(org_id),
        name="sync-integration",
        provider="github",
        sync_targets=[],
        sync_options={},
        is_active=True,
    )
    sync_config.id = sync_config_id

    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="test-org", name="Test Org", tier="pro"),
                User(id=user_id, email="admin@example.com", is_active=True),
                sync_config,
            ]
        )
        await session.commit()

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
        "sync_config_id": str(sync_config_id),
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


@pytest.mark.asyncio
async def test_trigger_backfill_creates_backfill_job_and_returns_id(
    client, session_maker, monkeypatch: pytest.MonkeyPatch
):
    ac, seeded_state = client

    monkeypatch.setattr(
        "dev_health_ops.api.services.licensing.TierLimitService.check_backfill_limit",
        lambda self, org_uuid, requested_days: (True, None),
    )

    mock_task = MagicMock(id="celery-backfill-task")
    mock_run_backfill = MagicMock()
    mock_run_backfill.delay.return_value = mock_task
    monkeypatch.setattr("dev_health_ops.workers.tasks.run_backfill", mock_run_backfill)

    response = await ac.post(
        f"/api/v1/admin/sync-configs/{seeded_state['sync_config_id']}/backfill",
        json={"since": "2026-01-01", "before": "2026-01-14"},
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["task_id"] == "celery-backfill-task"
    assert payload["config_id"] == seeded_state["sync_config_id"]
    assert payload["backfill_job_id"]

    mock_run_backfill.delay.assert_called_once()
    assert (
        mock_run_backfill.delay.call_args.kwargs["backfill_job_id"]
        == payload["backfill_job_id"]
    )

    async with session_maker() as session:
        job = await session.get(BackfillJob, uuid.UUID(payload["backfill_job_id"]))
        assert job is not None
        assert job.org_id == seeded_state["org_id"]
        assert job.sync_config_id == uuid.UUID(seeded_state["sync_config_id"])
        assert job.status == "pending"
        assert job.since_date == date(2026, 1, 1)
        assert job.before_date == date(2026, 1, 14)
        assert job.total_chunks == 2
        assert job.completed_chunks == 0
        assert job.celery_task_id == "celery-backfill-task"


def test_run_backfill_progress_callback_updates_backfill_job_completed_chunks(
    monkeypatch: pytest.MonkeyPatch,
):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            SyncConfiguration.__table__,
            ScheduledJob.__table__,
            JobRun.__table__,
            BackfillJob.__table__,
        ],
    )

    org_id = str(uuid.uuid4())
    sync_config_id = uuid.uuid4()

    with Session(engine) as session:
        config = SyncConfiguration(
            org_id=org_id,
            name="sync-integration",
            provider="github",
            sync_targets=[],
            sync_options={},
            is_active=True,
        )
        config.id = sync_config_id
        backfill_job = BackfillJob(
            org_id=org_id,
            sync_config_id=sync_config_id,
            status="pending",
            since_date=date(2026, 1, 1),
            before_date=date(2026, 1, 14),
            total_chunks=2,
        )
        session.add_all([config, backfill_job])
        session.commit()
        backfill_job_id = str(backfill_job.id)

    @contextmanager
    def _session_ctx():
        with Session(engine) as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _session_ctx(),
    )

    def _fake_run_backfill_for_config(**kwargs):
        progress_cb = kwargs["progress_cb"]
        progress_cb(1, 2, date(2026, 1, 1), date(2026, 1, 7))
        progress_cb(2, 2, date(2026, 1, 8), date(2026, 1, 14))
        return {"status": "success", "window_count": 2}

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_backfill_for_config",
        _fake_run_backfill_for_config,
    )

    from dev_health_ops.workers.tasks import run_backfill

    task: Any = run_backfill
    task.push_request(id="backfill-integration")
    try:
        result = task(
            sync_config_id=str(sync_config_id),
            since="2026-01-01",
            before="2026-01-14",
            org_id=org_id,
            backfill_job_id=backfill_job_id,
        )
    finally:
        task.pop_request()

    assert result["status"] == "success"

    with Session(engine) as session:
        tracked_job = cast(Any, session.get(BackfillJob, uuid.UUID(backfill_job_id)))
        assert tracked_job is not None
        assert tracked_job.status == "completed"
        assert tracked_job.completed_chunks == 2
        assert tracked_job.started_at is not None
        assert tracked_job.completed_at is not None

    engine.dispose()


def test_run_backfill_does_not_create_scheduled_job(
    monkeypatch: pytest.MonkeyPatch,
):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            SyncConfiguration.__table__,
            ScheduledJob.__table__,
            JobRun.__table__,
            BackfillJob.__table__,
        ],
    )

    org_id = str(uuid.uuid4())
    sync_config_id = uuid.uuid4()

    with Session(engine) as session:
        config = SyncConfiguration(
            org_id=org_id,
            name="sync-integration",
            provider="github",
            sync_targets=[],
            sync_options={},
            is_active=True,
        )
        config.id = sync_config_id
        backfill_job = BackfillJob(
            org_id=org_id,
            sync_config_id=sync_config_id,
            status="pending",
            since_date=date(2026, 1, 1),
            before_date=date(2026, 1, 14),
            total_chunks=2,
        )
        session.add_all([config, backfill_job])
        session.commit()
        backfill_job_id = str(backfill_job.id)

    @contextmanager
    def _session_ctx():
        with Session(engine) as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _session_ctx(),
    )

    def _fake_run_backfill_for_config(**kwargs):
        progress_cb = kwargs["progress_cb"]
        progress_cb(1, 2, date(2026, 1, 1), date(2026, 1, 7))
        progress_cb(2, 2, date(2026, 1, 8), date(2026, 1, 14))
        return {"status": "success", "window_count": 2}

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_backfill_for_config",
        _fake_run_backfill_for_config,
    )

    from dev_health_ops.workers.tasks import run_backfill

    task: Any = run_backfill
    task.push_request(id="backfill-no-scheduled-job")
    try:
        result = task(
            sync_config_id=str(sync_config_id),
            since="2026-01-01",
            before="2026-01-14",
            org_id=org_id,
            backfill_job_id=backfill_job_id,
        )
    finally:
        task.pop_request()

    assert result["status"] == "success"

    with Session(engine) as session:
        tracked_job = cast(Any, session.get(BackfillJob, uuid.UUID(backfill_job_id)))
        assert tracked_job is not None
        assert tracked_job.status == "completed"
        assert cast(Any, session.query(ScheduledJob).count()) == 0
        assert cast(Any, session.query(JobRun).count()) == 0

    engine.dispose()


def test_dispatch_scheduled_syncs_ignores_backfill_jobs(
    monkeypatch: pytest.MonkeyPatch,
):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            SyncConfiguration.__table__,
            ScheduledJob.__table__,
            JobRun.__table__,
        ],
    )

    org_id = str(uuid.uuid4())
    sync_config_id = uuid.uuid4()

    with Session(engine) as session:
        config = SyncConfiguration(
            org_id=org_id,
            name="sync-integration",
            provider="github",
            sync_targets=[],
            sync_options={},
            is_active=True,
        )
        config.id = sync_config_id
        config.last_sync_at = date(2026, 1, 1)

        sync_job = ScheduledJob(
            name="sync-job",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id=org_id,
            sync_config_id=sync_config_id,
            status=JobStatus.ACTIVE.value,
        )
        backfill_job = ScheduledJob(
            name="backfill-job",
            job_type="backfill",
            schedule_cron="0 * * * *",
            org_id=org_id,
            sync_config_id=sync_config_id,
            status=JobStatus.ACTIVE.value,
        )
        session.add_all([config, sync_job, backfill_job])
        session.commit()

    @contextmanager
    def _session_ctx():
        with Session(engine) as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    class _AlwaysDue:
        def __le__(self, _other):
            return True

        def __lt__(self, _other):
            return True

        def __ge__(self, _other):
            return True

        def __gt__(self, _other):
            return True

    class _DueCron:
        def get_next(self, _kind):
            return _AlwaysDue()

    class _CroniterModule:
        @staticmethod
        def croniter(*_args):
            return _DueCron()

    dispatched: list[dict[str, Any]] = []

    def _fake_run_sync_apply_async(*, kwargs, queue):
        dispatched.append({"kwargs": kwargs, "queue": queue})

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _session_ctx(),
    )
    monkeypatch.setitem(
        cast(Any, __import__("sys").modules), "croniter", _CroniterModule()
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.tasks._is_batch_eligible", lambda _cfg: False
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.tasks.run_sync_config.apply_async",
        _fake_run_sync_apply_async,
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.tasks.dispatch_batch_sync.apply_async",
        lambda **_kwargs: None,
    )

    from dev_health_ops.workers.tasks import dispatch_scheduled_syncs

    task: Any = dispatch_scheduled_syncs
    task.push_request(id="dispatch-ignore-backfill-jobs")
    try:
        result = task()
    finally:
        task.pop_request()

    assert str(sync_config_id) in result["dispatched"]
    assert len(dispatched) == 1
    assert dispatched[0]["kwargs"]["config_id"] == str(sync_config_id)

    engine.dispose()


def test_run_backfill_resolves_credentials_from_db(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            SyncConfiguration.__table__,
            ScheduledJob.__table__,
            JobRun.__table__,
            BackfillJob.__table__,
            IntegrationCredential.__table__,
        ],
    )

    org_id = str(uuid.uuid4())
    sync_config_id = uuid.uuid4()
    credential_id = uuid.uuid4()

    with Session(engine) as session:
        credential = IntegrationCredential(
            provider="linear",
            name="linear-credential",
            org_id=org_id,
            credentials_encrypted="encrypted-payload",
        )
        credential.id = credential_id
        config = SyncConfiguration(
            org_id=org_id,
            name="sync-linear-integration",
            provider="linear",
            credential_id=credential_id,
            sync_targets=[],
            sync_options={},
            is_active=True,
        )
        config.id = sync_config_id
        session.add_all([credential, config])
        session.commit()

    @contextmanager
    def _session_ctx():
        with Session(engine) as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _session_ctx(),
    )
    monkeypatch.setattr(
        "dev_health_ops.core.encryption.decrypt_value",
        lambda _value: json.dumps({"api_key": "lin_test_cred_from_db"}),
    )

    captured: dict[str, object] = {}

    def _fake_run_backfill_for_config(**kwargs):
        captured.update(kwargs)
        return {"status": "success", "window_count": 1}

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_backfill_for_config",
        _fake_run_backfill_for_config,
    )

    from dev_health_ops.workers.tasks import run_backfill

    task: Any = run_backfill
    task.push_request(id="backfill-credential-integration")
    try:
        result = task(
            sync_config_id=str(sync_config_id),
            since="2026-01-01",
            before="2026-01-14",
            org_id=org_id,
        )
    finally:
        task.pop_request()

    assert result["status"] == "success"
    assert captured["sync_config_id"] == str(sync_config_id)
    assert os.environ.get("LINEAR_API_KEY") == "lin_test_cred_from_db"

    engine.dispose()
