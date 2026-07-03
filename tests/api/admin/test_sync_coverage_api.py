from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobStatus,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.users import Organization, User
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
    SyncRun,
    SyncRunUnit,
    SyncWatermark,
    BackfillJob,
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "sync-coverage.db"
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
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="test-org", name="Test Org", tier="pro"),
                User(id=user_id, email="admin@example.com", is_active=True),
            ]
        )
        await session.commit()
    return {"org_id": str(org_id), "user_id": str(user_id)}


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


async def _seed_scope(session_maker, org_id: str, *, other_org: bool = False) -> dict:
    row_org_id = str(uuid.uuid4()) if other_org else org_id
    async with session_maker() as session:
        integration = Integration(
            org_id=row_org_id,
            provider="github",
            name="GitHub",
            config={},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        config = SyncConfiguration(
            org_id=row_org_id,
            name="Coverage",
            provider="github",
            sync_targets=["git"],
            sync_options={"schedule_cron": "0 * * * *"},
            integration_id=integration.id,
            planner_managed=True,
        )
        session.add(config)
        await session.flush()
        source = IntegrationSource(
            org_id=row_org_id,
            integration_id=integration.id,
            provider="github",
            source_type="repository",
            external_id="acme/repo",
            name="repo",
            full_name="acme/repo",
            metadata_={"planner_managed_sync_config_id": str(config.id)},
            is_enabled=True,
        )
        dataset = IntegrationDataset(
            org_id=row_org_id,
            integration_id=integration.id,
            dataset_key="commits",
            is_enabled=True,
            options={},
        )
        job = ScheduledJob(
            org_id=row_org_id,
            name="sync-config-coverage",
            job_type="sync",
            provider="github",
            schedule_cron="0 * * * *",
            sync_config_id=config.id,
            status=JobStatus.ACTIVE.value,
        )
        job.next_run_at = datetime(2026, 1, 5, 1, tzinfo=timezone.utc)
        session.add_all([source, dataset, job])
        await session.flush()
        await session.commit()
        return {
            "org_id": row_org_id,
            "config_id": str(config.id),
            "integration_id": str(integration.id),
            "source_id": str(source.id),
        }


async def _seed_legacy_config(session_maker, org_id: str) -> str:
    async with session_maker() as session:
        config = SyncConfiguration(
            org_id=org_id,
            name="Legacy Coverage",
            provider="github",
            sync_targets=["git"],
            sync_options={},
            integration_id=None,
            planner_managed=False,
        )
        session.add(config)
        await session.commit()
        return str(config.id)


async def _seed_unit(
    session_maker,
    scope: dict,
    *,
    since: datetime,
    before: datetime,
    status: str = "success",
    updated_at: datetime | None = None,
    source_id: str | None = None,
) -> str:
    async with session_maker() as session:
        run_status = "success"
        if status == "failed":
            run_status = "failed"
        elif status in {"planned", "dispatching", "running", "retrying"}:
            run_status = "running"
        run = SyncRun(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            triggered_by="manual",
            mode="incremental",
            status=run_status,
            total_units=1,
            completed_units=1 if status == "success" else 0,
            failed_units=1 if status == "failed" else 0,
            started_at=since,
            completed_at=before,
        )
        session.add(run)
        await session.flush()
        unit = SyncRunUnit(
            org_id=scope["org_id"],
            sync_run_id=run.id,
            integration_id=uuid.UUID(scope["integration_id"]),
            source_id=uuid.UUID(source_id or scope["source_id"]),
            provider="github",
            dataset_key="commits",
            cost_class="standard",
            mode="incremental",
            since_at=since,
            before_at=before,
            status=status,
            attempts=1,
        )
        if updated_at is not None:
            unit.updated_at = updated_at
        session.add(unit)
        await session.commit()
        return str(run.id)


@pytest.mark.asyncio
async def test_sync_coverage_api_returns_complete_summary(client, session_maker):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    before = datetime.now(timezone.utc) - timedelta(minutes=30)
    await _seed_unit(
        session_maker,
        scope,
        since=before - timedelta(hours=1),
        before=before,
    )

    resp = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["overall"]["health"] == "healthy"
    assert data["overall"]["gap_count"] == 0
    assert data["datasets"][0]["covered_ranges"][0]["source_ids"] == [
        scope["source_id"]
    ]
    assert data["overall"]["next_scheduled_run_at"] is not None


@pytest.mark.asyncio
async def test_sync_coverage_api_includes_backfill_gap(client, session_maker):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    await _seed_unit(
        session_maker,
        scope,
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        before=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="pending",
                since_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                before_date=datetime(2026, 1, 3, tzinfo=timezone.utc).date(),
                total_chunks=0,
            )
        )
        await session.commit()

    resp = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["overall"]["health"] == "gaps"
    assert data["overall"]["gap_count"] == 1
    assert data["datasets"][0]["gaps"]
    assert data["sources"][0]["gap_count"] == 1


@pytest.mark.asyncio
async def test_sync_coverage_api_keeps_source_gaps_separate(client, session_maker):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    async with session_maker() as session:
        extra_source = IntegrationSource(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            provider="github",
            source_type="repository",
            external_id="acme/other",
            name="other",
            full_name="acme/other",
            metadata_={"planner_managed_sync_config_id": scope["config_id"]},
            is_enabled=True,
        )
        session.add(extra_source)
        await session.commit()
        extra_source_id = str(extra_source.id)
    since = datetime(2026, 1, 1, tzinfo=timezone.utc)
    before = datetime(2026, 1, 2, tzinfo=timezone.utc)
    await _seed_unit(session_maker, scope, since=since, before=before, status="success")
    await _seed_unit(
        session_maker,
        scope,
        since=since,
        before=before,
        status="planned",
        source_id=extra_source_id,
    )

    resp = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["overall"]["health"] == "gaps"
    assert data["datasets"][0]["gaps"][0]["source_ids"] == [extra_source_id]
    sources = {source["source_id"]: source for source in data["sources"]}
    assert sources[scope["source_id"]]["gap_count"] == 0
    assert sources[extra_source_id]["gap_count"] == 1


@pytest.mark.asyncio
async def test_sync_coverage_api_planned_units_create_requested_gap(
    client, session_maker
):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    await _seed_unit(
        session_maker,
        scope,
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        before=datetime(2026, 1, 2, tzinfo=timezone.utc),
        status="planned",
    )

    resp = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["datasets"][0]["requested_ranges"]
    assert data["datasets"][0]["covered_ranges"] == []
    assert data["overall"]["health"] == "gaps"


@pytest.mark.asyncio
async def test_sync_coverage_fetches_latest_success_outside_lookback(
    client, session_maker
):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    await _seed_unit(
        session_maker,
        scope,
        since=datetime(2025, 1, 1, tzinfo=timezone.utc),
        before=datetime(2025, 1, 2, tzinfo=timezone.utc),
        updated_at=datetime.now(timezone.utc) - timedelta(days=400),
    )

    resp = await ac.get(
        f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage?history_lookback_days=30"
    )

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["history_lookback_days"] == 30
    assert data["overall"]["latest_covered_through"] is not None


@pytest.mark.asyncio
async def test_sync_coverage_zero_run_planner_config_reports_planner_basis(
    client, session_maker
):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])

    resp = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["data_basis"] == "planner"
    assert data["overall"]["health"] == "insufficient_data"


@pytest.mark.asyncio
async def test_sync_coverage_legacy_config_reports_legacy_basis(client, session_maker):
    ac, seeded_state = client
    config_id = await _seed_legacy_config(session_maker, seeded_state["org_id"])

    resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/coverage")

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["data_basis"] == "legacy"
    assert data["overall"]["health"] == "insufficient_data"


@pytest.mark.asyncio
async def test_sync_coverage_cross_org_config_returns_404(client, session_maker):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"], other_org=True)

    resp = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert resp.status_code == 404
