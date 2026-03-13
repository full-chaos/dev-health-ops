from __future__ import annotations

import importlib
import uuid
from datetime import date
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.backfill import BackfillJobService
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.users import Organization, User

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = [
    User.__table__,
    Organization.__table__,
    SyncConfiguration.__table__,
    BackfillJob.__table__,
]


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "backfill-observability.db"
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
        name="sync-default",
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
async def test_backfill_job_service_create_get_list(session_maker, seeded_state):
    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        first = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 1, 1),
            before=date(2026, 1, 7),
            total_chunks=4,
        )
        second = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 1, 8),
            before=date(2026, 1, 15),
            total_chunks=2,
        )
        await session.commit()

    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        found = await svc.get_job(str(first.id))
        assert found is not None
        assert found.status == "pending"
        assert found.completed_chunks == 0
        assert found.failed_chunks == 0
        assert found.sync_config_id == uuid.UUID(seeded_state["sync_config_id"])

        jobs, total = await svc.list_jobs(limit=10, offset=0)
        assert total == 2
        assert len(jobs) == 2
        assert {str(item.id) for item in jobs} == {str(first.id), str(second.id)}


@pytest.mark.asyncio
async def test_backfill_job_service_progress_calculation(session_maker, seeded_state):
    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        job = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 2, 1),
            before=date(2026, 2, 7),
            total_chunks=5,
        )
        await svc.update_progress(
            str(job.id),
            completed_chunks=2,
            failed_chunks=1,
            status="running",
        )
        await session.commit()

    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        found = await svc.get_job(str(job.id))
        assert found is not None
        progress_pct = (
            found.completed_chunks / found.total_chunks * 100
            if found.total_chunks > 0
            else 0.0
        )
        assert progress_pct == 40.0


@pytest.mark.asyncio
async def test_backfill_job_service_status_transitions(session_maker, seeded_state):
    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        completed_job = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 2, 1),
            before=date(2026, 2, 7),
            total_chunks=2,
        )
        failed_job = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 2, 8),
            before=date(2026, 2, 14),
            total_chunks=2,
        )
        await svc.mark_running(str(completed_job.id))
        await svc.update_progress(
            str(completed_job.id),
            completed_chunks=2,
            failed_chunks=0,
        )
        await svc.mark_completed(str(completed_job.id))
        await svc.mark_running(str(failed_job.id))
        await svc.update_progress(
            str(failed_job.id),
            completed_chunks=1,
            failed_chunks=1,
        )
        await svc.mark_failed(str(failed_job.id), "chunk timeout")
        await session.commit()

    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        completed = await svc.get_job(str(completed_job.id))
        failed = await svc.get_job(str(failed_job.id))

        assert completed is not None
        assert completed.status == "completed"
        assert completed.started_at is not None
        assert completed.completed_at is not None

        assert failed is not None
        assert failed.status == "failed"
        assert failed.started_at is not None
        assert failed.error_message == "chunk timeout"
        assert failed.completed_at is not None


@pytest.mark.asyncio
async def test_backfill_job_endpoints_return_expected_schema(client, session_maker):
    ac, seeded_state = client

    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        job = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 3, 1),
            before=date(2026, 3, 5),
            total_chunks=5,
        )
        await svc.mark_running(str(job.id))
        await svc.update_progress(
            str(job.id),
            completed_chunks=2,
            failed_chunks=1,
            status="running",
        )
        await session.commit()
        job_id = str(job.id)

    list_resp = await ac.get("/api/v1/admin/backfill-jobs?limit=50&offset=0")
    assert list_resp.status_code == 200
    list_data = list_resp.json()
    assert list_data["total"] >= 1
    assert list_data["limit"] == 50
    assert list_data["offset"] == 0
    assert any(item["id"] == job_id for item in list_data["items"])

    detail_resp = await ac.get(f"/api/v1/admin/backfill-jobs/{job_id}")
    assert detail_resp.status_code == 200
    detail_data = detail_resp.json()
    assert detail_data["id"] == job_id
    assert detail_data["status"] == "running"
    assert detail_data["total_chunks"] == 5
    assert detail_data["completed_chunks"] == 2
    assert detail_data["failed_chunks"] == 1
    assert detail_data["progress_pct"] == 40.0


@pytest.mark.asyncio
async def test_backfill_job_detail_not_found_returns_404(client):
    ac, _ = client
    resp = await ac.get(f"/api/v1/admin/backfill-jobs/{uuid.uuid4()}")
    assert resp.status_code == 404
