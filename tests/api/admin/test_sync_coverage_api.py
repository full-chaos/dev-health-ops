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

from dev_health_ops.api.admin.schemas_flat import (
    BackfillSelectorRequest,
    SyncCoverageBackfillWindow,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.sync_coverage import (
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


def test_canonical_backfill_windows_drop_day_boundary_seams():
    """A one-microsecond seam is not a gap.

    Where one day-bounded window meets the next, coverage leaves a residue of
    exactly INTERVAL_ADJACENCY_TOLERANCE -- the same span the merge step treats
    as adjacent. These boundaries are taken verbatim from a live projection; 66
    of 114 candidate windows in a populated org had this exact shape, and
    advertising them would offer a backfill covering no time at all.
    """
    source_id = "11111111-1111-1111-1111-111111111111"
    pair_coverages = [
        sync_coverage_module._PairCoverage(
            source_id=source_id,
            dataset_key="commit-stats",
            gaps=[
                sync_coverage_module.CoverageInterval(
                    since=datetime(
                        2026, 3, 12, 23, 59, 59, 999999, tzinfo=timezone.utc
                    ),
                    before=datetime(2026, 3, 13, tzinfo=timezone.utc),
                    source_ids=(source_id,),
                )
            ],
        ),
    ]

    assert sync_coverage_module._canonical_backfill_windows(pair_coverages) == []


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
