from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.schemas_flat import (
    BackfillSelectorRequest,
    SyncCoverageBackfillWindow,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.sync_coverage import (
    invalidate_sync_coverage_projection,
    rebuild_sync_coverage_projection,
)
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
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
sync_coverage_module = importlib.import_module(
    "dev_health_ops.api.services.sync_coverage"
)

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
    SyncCoverageProjection,
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


async def _seed_scope(
    session_maker,
    org_id: str,
    *,
    other_org: bool = False,
    provider: str = "github",
    source_external_id: str = "acme/repo",
) -> dict:
    row_org_id = str(uuid.uuid4()) if other_org else org_id
    async with session_maker() as session:
        integration = Integration(
            org_id=row_org_id,
            provider=provider,
            name=f"{provider.title()} Integration",
            config={},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        config = SyncConfiguration(
            org_id=row_org_id,
            name="Coverage",
            provider=provider,
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
            provider=provider,
            source_type="repository",
            external_id=source_external_id,
            name="repo",
            full_name=source_external_id,
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
            provider=provider,
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
            "provider": provider,
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
    dataset_key: str = "commits",
    processor_flags: dict | None = None,
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
            provider=scope.get("provider", "github"),
            dataset_key=dataset_key,
            cost_class="standard",
            mode="incremental",
            since_at=since,
            before_at=before,
            status=status,
            attempts=1,
            processor_flags=processor_flags,
        )
        if updated_at is not None:
            unit.updated_at = updated_at
        session.add(unit)
        await session.commit()
        return str(run.id)


async def _seed_run(session_maker, scope: dict, *, status: str = "success") -> str:
    async with session_maker() as session:
        run = SyncRun(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            triggered_by="backfill",
            mode="backfill",
            status=status,
            total_units=1,
            completed_units=1 if status == "success" else 0,
        )
        session.add(run)
        await session.commit()
        return str(run.id)


async def _seed_run_unit(
    session_maker,
    scope: dict,
    run_id: str,
    *,
    since: datetime,
    before: datetime,
    status: str = "success",
    source_id: str | None = None,
    dataset_key: str = "commits",
    updated_at: datetime | None = None,
    processor_flags: dict | None = None,
) -> None:
    async with session_maker() as session:
        unit = SyncRunUnit(
            org_id=scope["org_id"],
            sync_run_id=uuid.UUID(run_id),
            integration_id=uuid.UUID(scope["integration_id"]),
            source_id=uuid.UUID(source_id or scope["source_id"]),
            provider=scope.get("provider", "github"),
            dataset_key=dataset_key,
            cost_class="standard",
            mode="incremental",
            since_at=since,
            before_at=before,
            status=status,
            attempts=1,
            processor_flags=processor_flags,
        )
        if updated_at is not None:
            unit.updated_at = updated_at
        session.add(unit)
        await session.commit()


async def _coverage_summary_at(
    session_maker,
    scope: dict,
    *,
    org_id: str,
    generated_at: datetime,
) -> dict:
    async with session_maker() as session:
        config = await session.get(SyncConfiguration, uuid.UUID(scope["config_id"]))
        assert config is not None
        payload = await rebuild_sync_coverage_projection(
            session,
            org_id,
            config,
            generated_at=generated_at,
        )
        await session.commit()
        return payload


async def _warm_projection(
    session_maker,
    scope: dict,
    *,
    org_id: str | None = None,
    generated_at: datetime | None = None,
    lookback_days: int = sync_coverage_module.HISTORY_LOOKBACK_DAYS,
) -> dict:
    async with session_maker() as session:
        config = await session.get(SyncConfiguration, uuid.UUID(scope["config_id"]))
        assert config is not None
        payload = await rebuild_sync_coverage_projection(
            session,
            org_id or scope["org_id"],
            config,
            generated_at=generated_at,
            lookback_days=lookback_days,
        )
        await session.commit()
        return payload


async def _get_warm_coverage(ac, session_maker, scope: dict):
    await _warm_projection(session_maker, scope)
    return await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")


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

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["overall"]["health"] == "healthy"
    assert data["overall"]["gap_count"] == 0
    assert data["datasets"][0]["covered_ranges"][0]["source_ids"] == [
        scope["source_id"]
    ]
    assert data["overall"]["next_scheduled_run_at"] is not None


@pytest.mark.asyncio
async def test_sync_coverage_api_reads_only_projection(client, session_maker, caplog):
    """The request path cannot scan or hydrate sync history."""

    caplog.set_level("INFO", logger="dev_health_ops.api.services.sync_coverage")
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    before = datetime.now(timezone.utc) - timedelta(minutes=30)
    await _seed_unit(
        session_maker,
        scope,
        since=before - timedelta(hours=1),
        before=before,
    )
    await _warm_projection(session_maker, scope)

    statements: list[str] = []

    def capture_statement(_conn, _cursor, statement, _parameters, _context, _many):
        statements.append(statement)

    engine = session_maker.kw["bind"]
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        response = await ac.get(
            f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage"
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    assert response.status_code == 200, response.text
    unit_queries = [
        statement.lower()
        for statement in statements
        if "sync_run_units" in statement.lower() and "select" in statement.lower()
    ]
    assert unit_queries == []
    assert any("sync_coverage_projections" in statement for statement in statements)

    coverage_logs = {
        record.message: record
        for record in caplog.records
        if record.name == "dev_health_ops.api.services.sync_coverage"
    }
    assert (
        getattr(coverage_logs["sync_coverage_projection_rebuilt"], "raw_unit_row_count")
        == 1
    )
    assert "sync_coverage_summary_completed" in coverage_logs


@pytest.mark.asyncio
async def test_sync_coverage_streams_history_outside_request_query_budget(
    client, session_maker, monkeypatch, caplog
):
    """Raw history is streamed exactly; the request row budget cannot truncate it."""

    caplog.set_level("WARNING", logger="dev_health_ops.api.services.sync_coverage")
    original_budget = sync_coverage_module._CoverageQueryBudget
    monkeypatch.setattr(
        sync_coverage_module,
        "_CoverageQueryBudget",
        lambda: original_budget(limit=4),
    )
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    before = datetime.now(timezone.utc) - timedelta(minutes=30)
    for offset in range(3):
        await _seed_unit(
            session_maker,
            scope,
            since=before - timedelta(hours=offset + 1),
            before=before - timedelta(hours=offset),
        )

    statements: list[str] = []

    def capture_statement(_conn, _cursor, statement, _parameters, _context, _many):
        statements.append(statement)

    engine = session_maker.kw["bind"]
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        await _warm_projection(session_maker, scope)
        response = await ac.get(
            f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage"
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["history_lookback_days"] == 3650
    assert data["projection_complete"] is True
    assert data["datasets"][0]["covered_ranges"]
    unit_queries = [
        statement.lower()
        for statement in statements
        if "sync_run_units" in statement.lower() and "select" in statement.lower()
    ]
    assert unit_queries, "overflow path did not execute a measured unit query"
    assert not any(
        record.message == "sync_coverage_summary_complexity_rejected"
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_sync_coverage_never_reduces_requested_history_window(
    client, session_maker, monkeypatch, caplog
):
    """Complex raw history cannot silently shrink an exact coverage response."""

    caplog.set_level("WARNING", logger="dev_health_ops.api.services.sync_coverage")

    async def adaptive_selection_must_not_run(*_args, **_kwargs):
        raise AssertionError("adaptive lookback selection was called")

    monkeypatch.setattr(
        sync_coverage_module,
        "_select_coverage_lookback",
        adaptive_selection_must_not_run,
    )
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    reference = datetime.now(timezone.utc)
    for age_days in (120, 60, 10):
        before = reference - timedelta(days=age_days)
        await _seed_unit(
            session_maker,
            scope,
            since=before - timedelta(hours=1),
            before=before,
            status="failed",
            updated_at=before,
        )
    old_success = reference - timedelta(days=150)
    await _seed_unit(
        session_maker,
        scope,
        since=old_success - timedelta(hours=1),
        before=old_success,
        updated_at=old_success,
    )
    backfill_unit = reference - timedelta(days=61)
    backfill_run_id = await _seed_unit(
        session_maker,
        scope,
        since=backfill_unit - timedelta(hours=1),
        before=backfill_unit,
        status="failed",
        updated_at=backfill_unit,
    )
    backfill_created_at = reference - timedelta(days=58)
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="failed",
                since_date=(backfill_unit - timedelta(hours=1)).date(),
                before_date=backfill_unit.date(),
                total_chunks=1,
                failed_chunks=1,
                celery_task_id=f"sync_run:{backfill_run_id}",
                created_at=backfill_created_at,
                updated_at=backfill_created_at,
            )
        )
        await session.commit()

    response = await _get_warm_coverage(ac, session_maker, scope)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["history_lookback_days"] == 3650
    generated_at = datetime.fromisoformat(data["generated_at"])
    truncated_before = datetime.fromisoformat(data["truncated_before"])
    assert generated_at - truncated_before == timedelta(
        days=data["history_lookback_days"]
    )
    assert not any(
        record.message == "sync_coverage_history_window_reduced"
        for record in caplog.records
    )
    assert len(data["datasets"][0]["failed_ranges"]) == 4


@pytest.mark.asyncio
async def test_sync_coverage_projects_more_than_40000_raw_units_exactly(
    client, session_maker, caplog
):
    """The production failure threshold is crossed without truncation or a 422."""

    caplog.set_level("INFO", logger="dev_health_ops.api.services.sync_coverage")
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    now = datetime.now(timezone.utc)
    common_since = now - timedelta(days=2)
    common_before = common_since + timedelta(hours=1)
    sentinel_since = now - timedelta(days=1)
    sentinel_before = sentinel_since + timedelta(hours=2)
    async with session_maker() as session:
        run = SyncRun(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            triggered_by="manual",
            mode="incremental",
            status="failed",
            total_units=40_001,
            completed_units=40_000,
            failed_units=1,
            started_at=common_since,
            completed_at=sentinel_before,
        )
        session.add(run)
        await session.flush()
        base = {
            "org_id": scope["org_id"],
            "sync_run_id": run.id,
            "integration_id": uuid.UUID(scope["integration_id"]),
            "source_id": uuid.UUID(scope["source_id"]),
            "provider": "github",
            "dataset_key": "commits",
            "cost_class": "standard",
            "mode": "incremental",
            "attempts": 1,
            "created_at": now,
            "updated_at": now,
        }
        for _batch in range(8):
            rows = [
                {
                    **base,
                    "id": uuid.uuid4(),
                    "since_at": common_since,
                    "before_at": common_before,
                    "status": "success",
                }
                for _ in range(5_000)
            ]
            await session.execute(insert(SyncRunUnit), rows)
        await session.execute(
            insert(SyncRunUnit),
            [
                {
                    **base,
                    "id": uuid.uuid4(),
                    "since_at": sentinel_since,
                    "before_at": sentinel_before,
                    "status": "failed",
                }
            ],
        )
        await session.commit()

    response = await _get_warm_coverage(ac, session_maker, scope)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["history_lookback_days"] == 3650
    assert data["projection_complete"] is True
    assert data["datasets"][0]["failed_ranges"] == [
        {
            "since": sentinel_since.isoformat().replace("+00:00", "Z"),
            "before": sentinel_before.isoformat().replace("+00:00", "Z"),
            "source_ids": [scope["source_id"]],
            "run_ids": [],
        }
    ]
    rebuilt = next(
        record
        for record in caplog.records
        if record.message == "sync_coverage_projection_rebuilt"
    )
    assert getattr(rebuilt, "raw_unit_row_count") == 40_001
    assert not any(
        record.message == "sync_coverage_history_window_reduced"
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_sync_coverage_projection_is_reused_when_source_is_unchanged(
    client, session_maker, monkeypatch
):
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

    before = datetime.now(timezone.utc) - timedelta(minutes=30)
    for source_id in (scope["source_id"], extra_source_id):
        await _seed_unit(
            session_maker,
            scope,
            since=before - timedelta(hours=1),
            before=before,
            updated_at=before,
            source_id=source_id,
        )

    await _warm_projection(session_maker, scope)
    first = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")
    second = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert second.json() == first.json()
    async with session_maker() as session:
        rows = list((await session.execute(select(SyncCoverageProjection))).scalars())
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_sync_coverage_missing_projection_is_pending_without_building(
    client, session_maker, monkeypatch
):
    """A cold projection is explicit and cannot start an API-process rebuild."""

    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])

    async def request_time_build_must_not_run(*_args, **_kwargs):
        raise AssertionError("request attempted a raw-history projection rebuild")

    monkeypatch.setattr(
        sync_coverage_module,
        "rebuild_sync_coverage_projection",
        request_time_build_must_not_run,
    )
    url = f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage"
    response = await ac.get(url)
    unrelated = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}")

    assert unrelated.status_code == 200, unrelated.text
    assert response.status_code == 503
    assert response.headers["retry-after"] == "30"
    assert response.json()["detail"] == {
        "code": "sync_coverage_projection_pending",
        "message": "Coverage is being prepared. Retry shortly.",
    }


@pytest.mark.asyncio
async def test_sync_coverage_invalidated_projection_stays_visible_until_rebuilt(
    client, session_maker
):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    await _warm_projection(session_maker, scope)
    url = f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage"
    current = await ac.get(url)
    assert current.status_code == 200, current.text
    assert current.json()["projection_refreshing"] is False

    async with session_maker() as session:
        await invalidate_sync_coverage_projection(
            session,
            scope["org_id"],
            sync_config_id=uuid.UUID(scope["config_id"]),
        )
        await session.commit()

    refreshing = await ac.get(url)
    assert refreshing.status_code == 200, refreshing.text
    assert refreshing.json() == {
        **current.json(),
        "projection_refreshing": True,
    }

    await _warm_projection(session_maker, scope)
    rebuilt = await ac.get(url)
    assert rebuilt.status_code == 200, rebuilt.text
    assert rebuilt.json()["projection_refreshing"] is False


@pytest.mark.asyncio
async def test_sync_config_update_invalidates_coverage_projection(
    client, session_maker
):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    await _warm_projection(session_maker, scope)

    updated = await ac.patch(
        f"/api/v1/admin/sync-configs/{scope['config_id']}",
        json={"is_active": False},
    )
    assert updated.status_code == 200, updated.text

    coverage = await ac.get(f"/api/v1/admin/sync-configs/{scope['config_id']}/coverage")
    assert coverage.status_code == 200, coverage.text
    assert coverage.json()["projection_refreshing"] is True


def test_canonical_backfill_windows_preserve_pair_scope_and_half_open_bounds():
    """Coverage actions must not join adjacent gaps from different sources.

    The focused-backfill dialog sends source and dataset scope back to the
    planner. Joining these pair windows would quietly schedule a source over a
    time period that coverage never marked as missing.
    """
    source_one = "11111111-1111-1111-1111-111111111111"
    source_two = "22222222-2222-2222-2222-222222222222"
    pair_coverages = [
        sync_coverage_module._PairCoverage(
            source_id=source_one,
            dataset_key="commits",
            gaps=[
                sync_coverage_module.CoverageInterval(
                    since=datetime(2026, 1, 2, tzinfo=timezone.utc),
                    before=datetime(2026, 1, 3, tzinfo=timezone.utc),
                    source_ids=(source_one,),
                )
            ],
        ),
        sync_coverage_module._PairCoverage(
            source_id=source_two,
            dataset_key="commits",
            failed_ranges=[
                sync_coverage_module.CoverageInterval(
                    since=datetime(2026, 1, 3, tzinfo=timezone.utc),
                    before=datetime(2026, 1, 4, tzinfo=timezone.utc),
                    source_ids=(source_two,),
                )
            ],
        ),
        sync_coverage_module._PairCoverage(
            source_id=source_one,
            dataset_key="deployments",
            gaps=[
                sync_coverage_module.CoverageInterval(
                    since=datetime(2026, 1, 2, 10, tzinfo=timezone.utc),
                    before=datetime(2026, 1, 2, 11, tzinfo=timezone.utc),
                    source_ids=(source_one,),
                )
            ],
        ),
    ]

    assert sync_coverage_module._canonical_backfill_windows(pair_coverages) == [
        {
            "since": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "before": datetime(2026, 1, 3, tzinfo=timezone.utc),
            "source_ids": [source_one],
            "dataset_keys": ["commits"],
            "reasons": ["gap"],
        },
        # An intra-day window is advertised verbatim rather than skipped: the
        # planner keeps the requested instants at the window edges, so this is
        # submittable exactly as shown.
        {
            "since": datetime(2026, 1, 2, 10, tzinfo=timezone.utc),
            "before": datetime(2026, 1, 2, 11, tzinfo=timezone.utc),
            "source_ids": [source_one],
            "dataset_keys": ["deployments"],
            "reasons": ["gap"],
        },
        {
            "since": datetime(2026, 1, 3, tzinfo=timezone.utc),
            "before": datetime(2026, 1, 4, tzinfo=timezone.utc),
            "source_ids": [source_two],
            "dataset_keys": ["commits"],
            "reasons": ["failed"],
        },
    ]


def test_canonical_backfill_windows_advertise_off_midnight_boundaries():
    """Real coverage gaps almost never start at midnight.

    Intervals derive from sync run unit windows, which begin whenever a sync
    ran. Gating suggestions on exact UTC-midnight boundaries matched 0 of 138
    real intervals in a populated org, so the focused-backfill dialog could
    never offer a window (CHAOS-3915). These boundaries are taken verbatim
    from a live projection.
    """
    source_id = "4addf46f-c4d2-4226-b0b0-2e7c51cb91fe"
    since = datetime(2026, 8, 8, 2, 46, 6, 501450, tzinfo=timezone.utc)
    before = datetime(2026, 8, 18, 22, 28, 46, 890654, tzinfo=timezone.utc)
    pair_coverages = [
        sync_coverage_module._PairCoverage(
            source_id=source_id,
            dataset_key="cicd",
            gaps=[
                sync_coverage_module.CoverageInterval(
                    since=since, before=before, source_ids=(source_id,)
                )
            ],
        ),
    ]

    assert sync_coverage_module._canonical_backfill_windows(pair_coverages) == [
        {
            "since": since,
            "before": before,
            "source_ids": [source_id],
            "dataset_keys": ["cicd"],
            "reasons": ["gap"],
        },
    ]


def test_canonical_backfill_windows_drop_empty_intervals():
    """An empty interval is not a submittable selector.

    BackfillSelectorRequest requires ``since < before``. Suggesting a window
    that fails that validator hands the operator a button that always 422s.
    """
    source_id = "11111111-1111-1111-1111-111111111111"
    instant = datetime(2026, 3, 12, tzinfo=timezone.utc)
    pair_coverages = [
        sync_coverage_module._PairCoverage(
            source_id=source_id,
            dataset_key="commits",
            gaps=[
                sync_coverage_module.CoverageInterval(
                    since=instant,
                    before=instant,
                    source_ids=(source_id,),
                )
            ],
            failed_ranges=[
                sync_coverage_module.CoverageInterval(
                    since=instant,
                    before=instant,
                    source_ids=(source_id,),
                )
            ],
        ),
    ]

    assert sync_coverage_module._canonical_backfill_windows(pair_coverages) == []


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param({"since": "2026-08-08", "before": "2026-08-13"}, id="v1-dates"),
        pytest.param(
            {"since": "2026-08-08T00:00:00", "before": "2026-08-13T00:00:00"},
            id="naive-datetimes",
        ),
    ],
)
def test_advertised_backfill_window_is_accepted_by_the_backfill_selector(stored):
    """A suggested window must survive being echoed back as a selector.

    Web sends ``backfill_windows`` entries to POST /backfill verbatim. The
    response model used to be naive-tolerant while the request model is
    ``AwareDatetime``, so a version-1 projection produced
    ``2026-08-08T00:00:00`` and the server rejected its own suggestion with a
    ``timezone_aware`` 422.
    """
    window = SyncCoverageBackfillWindow.model_validate(
        {**stored, "reasons": ["failed", "gap"]}
    )
    assert window.since.tzinfo is not None
    assert window.before.tzinfo is not None

    emitted = window.model_dump(mode="json")
    selector = BackfillSelectorRequest.model_validate(
        {"since": emitted["since"], "before": emitted["before"]}
    )

    assert selector.since == datetime(2026, 8, 8, tzinfo=timezone.utc)
    assert selector.before == datetime(2026, 8, 13, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_sync_coverage_capacity_is_released_after_builder_error():
    controller = sync_coverage_module._CoverageAdmissionController(capacity=1)

    with pytest.raises(RuntimeError, match="builder failed"):
        async with controller.slot():
            raise RuntimeError("builder failed")

    async with controller.slot():
        assert controller._active == 1
    assert controller._active == 0


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

    resp = await _get_warm_coverage(ac, session_maker, scope)

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

    resp = await _get_warm_coverage(ac, session_maker, scope)

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

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["datasets"][0]["requested_ranges"]
    assert data["datasets"][0]["covered_ranges"] == []
    assert data["coverage_since"] is None
    assert data["coverage_through"] is None
    assert data["backfill_windows"] == [
        {
            "since": "2026-01-01T00:00:00Z",
            "before": "2026-01-02T00:00:00Z",
            "source_ids": [scope["source_id"]],
            "dataset_keys": ["commits"],
            "reasons": ["gap"],
        }
    ]
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
        since=datetime(2010, 1, 1, tzinfo=timezone.utc),
        before=datetime(2010, 1, 2, tzinfo=timezone.utc),
        updated_at=datetime(2010, 1, 2, tzinfo=timezone.utc),
    )

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["history_lookback_days"] == 3650
    assert data["overall"]["latest_covered_through"] is None
    assert data["datasets"][0]["covered_ranges"] == []
    assert data["is_truncated"] is True


@pytest.mark.asyncio
async def test_sync_coverage_zero_run_planner_config_reports_planner_basis(
    client, session_maker
):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["data_basis"] == "planner"
    assert data["overall"]["health"] == "insufficient_data"


@pytest.mark.asyncio
async def test_sync_coverage_legacy_config_reports_legacy_basis(client, session_maker):
    ac, seeded_state = client
    config_id = await _seed_legacy_config(session_maker, seeded_state["org_id"])
    scope = {"config_id": config_id, "org_id": seeded_state["org_id"]}

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["data_basis"] == "legacy"
    assert data["overall"]["health"] == "insufficient_data"


@pytest.mark.asyncio
async def test_sync_coverage_cross_org_config_returns_404(client, session_maker):
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"], other_org=True)

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_sync_coverage_api_backfill_pair_scoped_to_planned_run_units(
    client, session_maker
):
    """CHAOS-2869 core repro: a backfill's requested range only applies to the
    (source, dataset) pairs its linked SyncRun actually planned units for, and
    is clipped to the ACTUAL unit windows -- not the job's full date range.
    A job spanning wider than what its run actually planned must not create a
    phantom gap beyond the units themselves, and untouched pairs/datasets
    must not inherit a permanent requested gap at all.
    """
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
        pr_dataset = IntegrationDataset(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            dataset_key="pull_requests",
            is_enabled=True,
            options={},
        )
        session.add_all([extra_source, pr_dataset])
        await session.commit()
        extra_source_id = str(extra_source.id)

    now = datetime.now(timezone.utc)
    # Unit window is recent (avoids stale classification); the backfill job's
    # day-quantized range is deliberately WIDER than the unit's actual window
    # to prove the wider job range does not inflate the requested range.
    since = now - timedelta(days=2)
    before = now - timedelta(minutes=5)
    run_id = await _seed_run(session_maker, scope, status="success")
    await _seed_run_unit(
        session_maker, scope, run_id, since=since, before=before, status="success"
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="success",
                since_date=(now - timedelta(days=3)).date(),
                before_date=(now + timedelta(days=1)).date(),
                total_chunks=1,
                completed_chunks=1,
                celery_task_id=f"sync_run:{run_id}",
            )
        )
        await session.commit()

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    sources = {source["source_id"]: source for source in data["sources"]}
    # Pair (source A, commits) is what the run actually planned units for,
    # but only for [since, before) -- the backfill job's much wider
    # [since-1d, before+1d] window must NOT inflate the requested range
    # beyond that actual unit window, so there is no phantom gap.
    assert sources[scope["source_id"]]["gap_count"] == 0
    assert sources[scope["source_id"]]["status"] == "healthy"
    # Pair (source B, commits) and (either source, pull_requests) were never
    # planned by this run: they must not inherit a permanent requested gap.
    assert sources[extra_source_id]["gap_count"] == 0
    assert sources[extra_source_id]["status"] == "insufficient_data"
    datasets = {dataset["dataset_key"]: dataset for dataset in data["datasets"]}
    assert datasets["commits"]["gaps"] == []
    assert datasets["pull_requests"]["status"] == "insufficient_data"
    assert datasets["pull_requests"]["gaps"] == []
    assert data["overall"]["health"] == "healthy"


@pytest.mark.asyncio
async def test_sync_coverage_api_success_matching_backfill_range_clears_gap(
    client, session_maker
):
    """A pair-scoped backfill whose linked run's SUCCESS unit fully covers
    the job's requested range must resolve to a clean (non-gapped) pair."""
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    run_id = await _seed_run(session_maker, scope, status="success")
    now = datetime.now(timezone.utc)
    # Covered window is recent (avoids stale classification); the backfill's
    # day-quantized range sits fully inside it so it resolves clean.
    covered_since = now - timedelta(days=5)
    covered_before = now - timedelta(minutes=5)
    await _seed_run_unit(
        session_maker,
        scope,
        run_id,
        since=covered_since,
        before=covered_before,
        status="success",
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="success",
                since_date=(now - timedelta(days=4)).date(),
                before_date=(now - timedelta(days=3)).date(),
                total_chunks=1,
                completed_chunks=1,
                celery_task_id=f"sync_run:{run_id}",
            )
        )
        await session.commit()

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["overall"]["health"] == "healthy"
    assert data["datasets"][0]["gaps"] == []


@pytest.mark.asyncio
async def test_sync_coverage_recent_backfill_success_stays_cleared_after_unit_ages_out(
    session_maker, seeded_state
):
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    generated_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    old_unit_updated_at = generated_at - timedelta(days=181)
    backfill_created_at = generated_at - timedelta(days=179)

    backfill_run_id = await _seed_run(session_maker, scope, status="success")
    await _seed_run_unit(
        session_maker,
        scope,
        backfill_run_id,
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        before=datetime(2026, 1, 2, tzinfo=timezone.utc),
        status="success",
        updated_at=old_unit_updated_at,
    )
    latest_run_id = await _seed_run(session_maker, scope, status="success")
    await _seed_run_unit(
        session_maker,
        scope,
        latest_run_id,
        since=generated_at - timedelta(hours=2),
        before=generated_at - timedelta(hours=1),
        status="success",
        updated_at=generated_at - timedelta(hours=1),
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="success",
                since_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                before_date=datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
                total_chunks=1,
                completed_chunks=1,
                celery_task_id=f"sync_run:{backfill_run_id}",
                created_at=backfill_created_at,
                updated_at=backfill_created_at,
            )
        )
        await session.commit()

    data = await _coverage_summary_at(
        session_maker,
        scope,
        org_id=seeded_state["org_id"],
        generated_at=generated_at,
    )

    assert data["overall"]["health"] == "healthy"
    assert data["datasets"][0]["gaps"] == []
    assert data["sources"][0]["gap_count"] == 0


@pytest.mark.asyncio
async def test_sync_coverage_recent_backfill_uncovered_range_still_reports_gap(
    session_maker, seeded_state
):
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    generated_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    old_unit_updated_at = generated_at - timedelta(days=181)
    backfill_created_at = generated_at - timedelta(days=179)

    backfill_run_id = await _seed_run(session_maker, scope, status="running")
    await _seed_run_unit(
        session_maker,
        scope,
        backfill_run_id,
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        before=datetime(2026, 1, 2, tzinfo=timezone.utc),
        status="planned",
        updated_at=old_unit_updated_at,
    )
    latest_run_id = await _seed_run(session_maker, scope, status="success")
    await _seed_run_unit(
        session_maker,
        scope,
        latest_run_id,
        since=generated_at - timedelta(hours=2),
        before=generated_at - timedelta(hours=1),
        status="success",
        updated_at=generated_at - timedelta(hours=1),
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="running",
                since_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                before_date=datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
                total_chunks=1,
                completed_chunks=0,
                celery_task_id=f"sync_run:{backfill_run_id}",
                created_at=backfill_created_at,
                updated_at=backfill_created_at,
            )
        )
        await session.commit()

    data = await _coverage_summary_at(
        session_maker,
        scope,
        org_id=seeded_state["org_id"],
        generated_at=generated_at,
    )

    assert data["overall"]["health"] == "gaps"
    assert [(gap["since"], gap["before"]) for gap in data["datasets"][0]["gaps"]] == [
        (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
    ]
    assert data["sources"][0]["gap_count"] == 1


@pytest.mark.asyncio
async def test_sync_coverage_recent_backfill_clock_alignment_remains_pair_scoped(
    session_maker, seeded_state
):
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    async with session_maker() as session:
        extra_source = IntegrationSource(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            provider="github",
            source_type="repository",
            external_id="acme/clock-pair-scope",
            name="clock-pair-scope",
            full_name="acme/clock-pair-scope",
            metadata_={"planner_managed_sync_config_id": scope["config_id"]},
            is_enabled=True,
        )
        session.add(extra_source)
        await session.commit()
        extra_source_id = str(extra_source.id)
    generated_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    backfill_run_id = await _seed_run(session_maker, scope, status="success")
    await _seed_run_unit(
        session_maker,
        scope,
        backfill_run_id,
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        before=datetime(2026, 1, 2, tzinfo=timezone.utc),
        status="success",
        updated_at=generated_at - timedelta(days=181),
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="success",
                since_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                before_date=datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
                total_chunks=1,
                completed_chunks=1,
                celery_task_id=f"sync_run:{backfill_run_id}",
                created_at=generated_at - timedelta(days=179),
                updated_at=generated_at - timedelta(days=179),
            )
        )
        await session.commit()

    data = await _coverage_summary_at(
        session_maker,
        scope,
        org_id=seeded_state["org_id"],
        generated_at=generated_at,
    )

    sources = {source["source_id"]: source for source in data["sources"]}
    assert sources[scope["source_id"]]["gap_count"] == 0
    assert sources[extra_source_id]["gap_count"] == 0
    assert sources[extra_source_id]["status"] == "insufficient_data"


@pytest.mark.asyncio
async def test_sync_coverage_api_unresolvable_backfill_marker_falls_back_to_all_pairs(
    client, session_maker
):
    """An unparseable sync_run marker (or one whose run/units are gone) falls
    back to the legacy all-pairs-in-scope requested range, same as a
    backfill job with no marker at all."""
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    async with session_maker() as session:
        extra_source = IntegrationSource(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            provider="github",
            source_type="repository",
            external_id="acme/other-2",
            name="other-2",
            full_name="acme/other-2",
            metadata_={"planner_managed_sync_config_id": scope["config_id"]},
            is_enabled=True,
        )
        session.add(extra_source)
        await session.commit()
        extra_source_id = str(extra_source.id)

    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="pending",
                since_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                before_date=datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
                total_chunks=0,
                celery_task_id="sync_run:not-a-uuid",
            )
        )
        await session.commit()

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    sources = {source["source_id"]: source for source in data["sources"]}
    assert sources[scope["source_id"]]["gap_count"] == 1
    assert sources[extra_source_id]["gap_count"] == 1


@pytest.mark.asyncio
async def test_sync_coverage_api_backfill_marker_resolved_zero_units_contributes_nothing(
    client, session_maker
):
    """A sync_run marker that resolves to an existing run with zero planned
    units must contribute NOTHING -- it must NOT fall back to the legacy
    all-pairs-in-scope behavior. Conflating "zero units" with "unresolvable"
    would reintroduce the exact phantom-gap class CHAOS-2869 exists to kill:
    a run that legitimately planned no work would otherwise "request"
    coverage on every in-scope pair."""
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"])
    async with session_maker() as session:
        extra_source = IntegrationSource(
            org_id=scope["org_id"],
            integration_id=uuid.UUID(scope["integration_id"]),
            provider="github",
            source_type="repository",
            external_id="acme/zero-units",
            name="zero-units",
            full_name="acme/zero-units",
            metadata_={"planner_managed_sync_config_id": scope["config_id"]},
            is_enabled=True,
        )
        session.add(extra_source)
        await session.commit()
        extra_source_id = str(extra_source.id)

    # A SyncRun that exists but planned zero SyncRunUnit rows.
    run_id = await _seed_run(session_maker, scope, status="success")
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="success",
                since_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                before_date=datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
                total_chunks=0,
                celery_task_id=f"sync_run:{run_id}",
            )
        )
        await session.commit()

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    sources = {source["source_id"]: source for source in data["sources"]}
    assert sources[scope["source_id"]]["gap_count"] == 0
    assert sources[extra_source_id]["gap_count"] == 0
    assert data["overall"]["health"] == "insufficient_data"


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["jira", "gitlab", "github", "linear"])
async def test_sync_coverage_api_backfill_requested_range_expands_by_family_flag(
    client, session_maker, provider
):
    """CHAOS-2721/coverage-fix core repro: a pair-scoped backfill's linked
    collapsed composite work-items run unit must only request coverage for
    the work-item-family child dataset(s) whose ``family_dataset_*`` flag was
    true on that unit. A disabled child dataset in the same scope must NOT
    inherit a requested range from the same composite unit -- provider
    agnostic across jira/gitlab/github/linear."""
    ac, seeded_state = client
    scope = await _seed_scope(session_maker, seeded_state["org_id"], provider=provider)
    async with session_maker() as session:
        for dataset_key in ("work-item-comments", "work-item-labels"):
            session.add(
                IntegrationDataset(
                    org_id=scope["org_id"],
                    integration_id=uuid.UUID(scope["integration_id"]),
                    dataset_key=dataset_key,
                    is_enabled=True,
                    options={},
                )
            )
        await session.commit()

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=2)
    before = now - timedelta(minutes=5)
    run_id = await _seed_run(session_maker, scope, status="success")
    await _seed_run_unit(
        session_maker,
        scope,
        run_id,
        since=since,
        before=before,
        status="success",
        dataset_key="work-items",
        processor_flags={"family_dataset_work_item_comments": True},
    )
    async with session_maker() as session:
        session.add(
            BackfillJob(
                org_id=seeded_state["org_id"],
                sync_config_id=uuid.UUID(scope["config_id"]),
                status="success",
                since_date=(now - timedelta(days=3)).date(),
                before_date=(now + timedelta(days=1)).date(),
                total_chunks=1,
                completed_chunks=1,
                celery_task_id=f"sync_run:{run_id}",
            )
        )
        await session.commit()

    resp = await _get_warm_coverage(ac, session_maker, scope)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    datasets = {dataset["dataset_key"]: dataset for dataset in data["datasets"]}
    # Flagged child: the composite run's actual window resolves the pair
    # clean, with no gap and no phantom failed range.
    assert datasets["work-item-comments"]["status"] == "healthy"
    assert datasets["work-item-comments"]["gaps"] == []
    # Unflagged child in the same scope must not inherit any requested range
    # from the composite unit -- it stays at insufficient_data, not gapped.
    assert datasets["work-item-labels"]["requested_ranges"] == []
    assert datasets["work-item-labels"]["gaps"] == []
    assert datasets["work-item-labels"]["status"] == "insufficient_data"
