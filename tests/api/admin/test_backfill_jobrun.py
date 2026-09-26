"""Tests for CHAOS-2536 / CHAOS-2647: planner (fanout) backfill JobRun anchoring.

Covers:
- Endpoint: an unmigrated config (no ``integration_id``) is planner-only
  and returns HTTP 400 without creating any records or dispatching.
- Endpoint (fanout path): creates a visible PENDING JobRun anchored to the sync
  ScheduledJob and threads the planner ``sync_run_id`` into it.
- Endpoint (fanout path): commits the BackfillJob with a stable
  ``sync_run:<id>`` marker and a durable reference-discovery wakeup.
- Endpoint (fanout path): does not publish a parallel Celery task; the
  BackfillJob, JobRun, and SyncRun remain pending/planned for the reconciler.
- Endpoint: a ``planner_managed`` config routes to fanout.
- Endpoint: a paused config returns 409 without dispatching.

The legacy worker path (``run_backfill``/``sync_backfill``/``sync_tasks``) was
removed in CHAOS-2647; all backfills now go through the planner + unitized
fan-out (``plan_sync_run`` + durable ``reference_discovery`` outbox wakeup).
"""

from __future__ import annotations

import importlib
import types
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.routers.integrations import _unit_to_response
from dev_health_ops.api.admin.routers.sync import _job_run_response, _SyncRunUnitRollup
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
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
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
    SyncDispatchOutbox,
    BackfillJob,
    SyncCoverageProjection,
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
    # github/gitlab plain creates are token-wide (all_repos); non-git providers
    # ignore the flag and materialize a single source. Either way the config is
    # integration-native and triggerable.
    return await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": name,
            "provider": provider,
            "sync_targets": [],
            "sync_options": {"all_repos": True},
        },
    )


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


async def _link_migrated_integration(
    session_maker,
    org_id: str,
    config_id: str,
    *,
    planner_managed: bool = False,
) -> uuid.UUID:
    """Link the config to a freshly seeded migrated integration.

    This is what makes the config eligible for the planner/fan-out backfill
    path (``planner_request_for_config_if_routed`` returns a request only when
    ``integration_id`` is set).
    """
    integration_id = uuid.uuid4()
    async with session_maker() as session:
        cfg = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        setattr(cfg, "integration_id", integration_id)
        if planner_managed:
            cfg.planner_managed = True
        await session.commit()
    await _seed_source(session_maker, org_id, integration_id)
    return integration_id


@contextmanager
def _patch_dispatch(
    *,
    side_effect: BaseException | None = None,
    task_id: str = "bf-task-id",
) -> Iterator[MagicMock]:
    """Patch the retired Celery fastpath to prove the endpoint does not use it.

    The real planner (``plan_sync_run``) still runs against the test DB so the
    PENDING JobRun anchor, SyncRun, and durable outbox wakeup are created for real.
    """
    mock_dispatch = MagicMock()
    if side_effect is not None:
        mock_dispatch.apply_async.side_effect = side_effect
    else:
        mock_dispatch.apply_async.return_value = MagicMock(id=task_id)
    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        yield mock_dispatch


# ---------------------------------------------------------------------------
# Endpoint tests — planner-only routing guard
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Endpoint tests — fanout (planner) path
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# CHAOS-2710: operator-visible retry/timeout projection (Phase 6)
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class _BackfillFreshnessTimes:
    job_updated_at: datetime
    unit_updated_at: datetime
    unit_heartbeat_at: datetime | None


async def _backfill_response_for_unit_activity(
    session_maker,
    seeded_state,
    times: _BackfillFreshnessTimes,
):
    config_id = uuid.uuid4()
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    sync_run_id = uuid.uuid4()

    async with session_maker() as session:
        config = SyncConfiguration(
            name=f"bf-freshness-{config_id}",
            provider="github",
            org_id=seeded_state["org_id"],
            integration_id=integration_id,
        )
        config.id = config_id
        session.add_all(
            [
                config,
                Integration(
                    id=integration_id,
                    org_id=seeded_state["org_id"],
                    provider="github",
                    name="github-integration",
                    config={},
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=seeded_state["org_id"],
                    integration_id=integration_id,
                    provider="github",
                    source_type="repository",
                    external_id="full-chaos/dev-health",
                    name="dev-health",
                    full_name="full-chaos/dev-health",
                    is_enabled=True,
                ),
                SyncRun(
                    id=sync_run_id,
                    org_id=seeded_state["org_id"],
                    integration_id=integration_id,
                    triggered_by="backfill",
                    mode="backfill",
                    status=SyncRunStatus.RUNNING.value,
                    total_units=2,
                    completed_units=1,
                    failed_units=0,
                ),
                SyncRunUnit(
                    org_id=seeded_state["org_id"],
                    sync_run_id=sync_run_id,
                    integration_id=integration_id,
                    source_id=source_id,
                    provider="github",
                    dataset_key="commits",
                    cost_class="rest",
                    mode="backfill",
                    status=SyncRunUnitStatus.SUCCESS.value,
                    updated_at=times.unit_updated_at,
                    last_heartbeat_at=times.unit_heartbeat_at,
                ),
                BackfillJob(
                    id=uuid.uuid4(),
                    org_id=seeded_state["org_id"],
                    sync_config_id=config_id,
                    celery_task_id=f"sync_run:{sync_run_id}",
                    status="running",
                    since_date=datetime(2025, 12, 1, tzinfo=timezone.utc).date(),
                    before_date=datetime(2025, 12, 8, tzinfo=timezone.utc).date(),
                    total_chunks=2,
                    completed_chunks=0,
                    failed_chunks=0,
                    started_at=times.job_updated_at,
                    created_at=times.job_updated_at,
                    updated_at=times.job_updated_at,
                ),
            ]
        )
        await session.commit()

        job = (await session.execute(select(BackfillJob))).scalar_one()
        run_counts = await sync_router_module._backfill_job_run_counts(session, job)
        return sync_router_module._backfill_job_response(job, run_counts)


def _fake_unit(**overrides):
    base = dict(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        sync_run_id=uuid.uuid4(),
        integration_id=uuid.uuid4(),
        source_id=uuid.uuid4(),
        source=None,
        provider="linear",
        dataset_key="work_items",
        cost_class="graphql_cost",
        mode="backfill",
        since_at=None,
        before_at=None,
        status="running",
        attempts=1,
        available_at=None,
        rate_limit_deferrals=0,
        duration_seconds=None,
        error=None,
        result=None,
        last_heartbeat_at=None,
        created_at=_T0,
        updated_at=_T0,
    )
    base.update(overrides)
    return types.SimpleNamespace(**base)


def _unit_rollup(units) -> _SyncRunUnitRollup:
    status_counts: dict[str, int] = {}
    for unit in units:
        status = str(unit.status)
        status_counts[status] = status_counts.get(status, 0) + 1
    return _SyncRunUnitRollup(
        status_counts=status_counts,
        requested_range=None,
        covered_range=None,
    )


def test_unit_to_response_projects_all_retry_result_keys():
    """All 8 worker-emitted retry/timeout result keys project onto the response."""
    resp = _unit_to_response(
        _fake_unit(
            status="retrying",
            result={
                "retry_count": 1,
                "retry_reason": "lease_expired",
                "last_lease_expired_at": "2026-01-01T00:05:00+00:00",
                "next_retry_at": "2026-01-01T00:06:00+00:00",
                "retry_exhausted": False,
                "retry_surfaces": ["work_items", "transitions"],
                "linear_page_count": 12,
                "linear_batch_count": 3,
                "error_category": "soft_timeout",
            },
        )
    )
    assert resp.retry_count == 1
    assert resp.retry_reason == "lease_expired"
    assert resp.last_lease_expired_at == datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)
    assert resp.next_retry_at == datetime(2026, 1, 1, 0, 6, tzinfo=timezone.utc)
    assert resp.retry_exhausted is False
    assert resp.retry_surfaces == ["work_items", "transitions"]
    assert resp.linear_page_count == 12
    assert resp.linear_batch_count == 3
    assert resp.error_category == "soft_timeout"


def test_backfill_job_response_includes_updated_at_freshness_timestamp():
    """Given a persisted backfill job, the response exposes updated_at freshness."""
    updated_at = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)
    job = types.SimpleNamespace(
        id=uuid.uuid4(),
        sync_config_id=uuid.uuid4(),
        status="running",
        since_date=datetime(2025, 12, 1, tzinfo=timezone.utc).date(),
        before_date=datetime(2025, 12, 8, tzinfo=timezone.utc).date(),
        total_chunks=10,
        completed_chunks=4,
        failed_chunks=0,
        error_message=None,
        started_at=_T0,
        completed_at=None,
        created_at=_T0,
        updated_at=updated_at,
    )

    response = sync_router_module._backfill_job_response(job)

    assert response.updated_at == updated_at


@pytest.mark.asyncio
async def test_backfill_job_response_uses_linked_unit_activity_for_freshness(
    session_maker, seeded_state
):
    stale_at = _T0 - timedelta(days=2)
    recent_at = _T0 - timedelta(hours=1)
    response = await _backfill_response_for_unit_activity(
        session_maker,
        seeded_state,
        _BackfillFreshnessTimes(
            job_updated_at=stale_at,
            unit_updated_at=stale_at,
            unit_heartbeat_at=recent_at,
        ),
    )

    assert response.updated_at == recent_at
    assert response.completed_chunks == 1


@pytest.mark.asyncio
async def test_backfill_job_response_remains_stale_when_linked_units_are_stale(
    session_maker, seeded_state
):
    stale_at = _T0 - timedelta(days=2)
    response = await _backfill_response_for_unit_activity(
        session_maker,
        seeded_state,
        _BackfillFreshnessTimes(
            job_updated_at=stale_at,
            unit_updated_at=stale_at,
            unit_heartbeat_at=None,
        ),
    )

    assert response.updated_at == stale_at
    assert response.completed_chunks == 1


@pytest.mark.asyncio
async def test_backfill_job_response_counts_actual_linked_unit_statuses(
    session_maker, seeded_state
):
    config_id = uuid.uuid4()
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    sync_run_id = uuid.uuid4()

    async with session_maker() as session:
        config = SyncConfiguration(
            name="bf-live-unit-statuses",
            provider="github",
            org_id=seeded_state["org_id"],
            integration_id=integration_id,
        )
        config.id = config_id
        session.add_all(
            [
                config,
                Integration(
                    id=integration_id,
                    org_id=seeded_state["org_id"],
                    provider="github",
                    name="github-integration",
                    config={},
                ),
                IntegrationSource(
                    id=source_id,
                    org_id=seeded_state["org_id"],
                    integration_id=integration_id,
                    provider="github",
                    source_type="repository",
                    external_id="full-chaos/dev-health",
                    name="dev-health",
                    full_name="full-chaos/dev-health",
                    is_enabled=True,
                ),
                SyncRun(
                    id=sync_run_id,
                    org_id=seeded_state["org_id"],
                    integration_id=integration_id,
                    triggered_by="backfill",
                    mode="backfill",
                    status=SyncRunStatus.DISPATCHING.value,
                    total_units=3,
                    completed_units=0,
                    failed_units=0,
                ),
                BackfillJob(
                    id=uuid.uuid4(),
                    org_id=seeded_state["org_id"],
                    sync_config_id=config_id,
                    celery_task_id=f"sync_run:{sync_run_id}",
                    status="dispatching",
                    since_date=datetime(2025, 12, 1, tzinfo=timezone.utc).date(),
                    before_date=datetime(2025, 12, 8, tzinfo=timezone.utc).date(),
                    total_chunks=3,
                    completed_chunks=0,
                    failed_chunks=0,
                    started_at=_T0,
                    created_at=_T0,
                    updated_at=_T0,
                ),
            ]
        )
        session.add_all(
            SyncRunUnit(
                org_id=seeded_state["org_id"],
                sync_run_id=sync_run_id,
                integration_id=integration_id,
                source_id=source_id,
                provider="github",
                dataset_key=f"dataset-{status.value}",
                cost_class="rest",
                mode="backfill",
                status=status.value,
                updated_at=_T0,
            )
            for status in (
                SyncRunUnitStatus.SUCCESS,
                SyncRunUnitStatus.FAILED,
            )
        )
        await session.commit()

        job = (await session.execute(select(BackfillJob))).scalar_one()
        run_counts = await sync_router_module._backfill_job_run_counts(session, job)
        response = sync_router_module._backfill_job_response(job, run_counts)

    assert response.status == SyncRunStatus.RUNNING.value
    assert response.completed_chunks == 1
    assert response.failed_chunks == 1
    assert response.progress_pct == pytest.approx(1 / 3 * 100)


def test_unit_to_response_next_retry_at_derived_from_available_at_when_absent():
    """With no result next_retry_at, a retrying unit derives it from available_at."""
    avail = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    resp = _unit_to_response(
        _fake_unit(status="retrying", available_at=avail, result={"retry_count": 1})
    )
    assert resp.next_retry_at == avail


def test_unit_to_response_next_retry_at_result_value_is_authoritative():
    """A result next_retry_at overrides the available_at derivation."""
    avail = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    resp = _unit_to_response(
        _fake_unit(
            status="retrying",
            available_at=avail,
            result={"next_retry_at": "2026-01-01T00:06:00+00:00"},
        )
    )
    assert resp.next_retry_at == datetime(2026, 1, 1, 0, 6, tzinfo=timezone.utc)


def test_unit_to_response_no_retry_projection_for_non_retrying_unit():
    """A non-retrying unit with no result projects null retry fields (no derivation)."""
    avail = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    resp = _unit_to_response(
        _fake_unit(status="running", available_at=avail, result=None)
    )
    assert resp.next_retry_at is None
    assert resp.retry_count is None
    assert resp.retry_reason is None
    assert resp.retry_exhausted is None
    assert resp.retry_surfaces is None
    assert resp.linear_page_count is None
    assert resp.linear_batch_count is None


@pytest.mark.parametrize(
    "category",
    ["worker_lost", "worker_lost_retry_exhausted"],
)
def test_unit_to_response_terminal_worker_lost_categories(category):
    """Terminal expired-lease/exhausted categories are surfaced via error_category."""
    resp = _unit_to_response(
        _fake_unit(
            status="failed",
            result={
                "error_category": category,
                "retry_exhausted": category == "worker_lost_retry_exhausted",
            },
        )
    )
    assert resp.status == "failed"
    assert resp.error_category == category
    assert resp.retry_exhausted is (category == "worker_lost_retry_exhausted")


def test_job_run_response_distinguishes_partial_failed_and_forwards_retry_aggregates():
    """PARTIAL_FAILED is distinguishable via result.sync_run_status (enum unchanged),
    and worker-emitted retry aggregates in the SyncRun result flow through."""
    run = types.SimpleNamespace(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        status=JobRunStatus.RUNNING.value,
        started_at=None,
        completed_at=None,
        duration_seconds=None,
        error=None,
        result={"sync_run_id": str(uuid.uuid4()), "planner_managed": True},
        triggered_by="backfill",
        created_at=_T0,
    )
    planner = types.SimpleNamespace(
        status=SyncRunStatus.PARTIAL_FAILED.value,
        result={"retry_exhausted_units": 1, "retrying_units": 0},
        total_units=3,
        completed_units=0,
        failed_units=0,
        started_at=_T0,
        completed_at=datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc),
        error=None,
    )
    units = [
        _fake_unit(status=SyncRunUnitStatus.SUCCESS.value),
        _fake_unit(status=SyncRunUnitStatus.SUCCESS.value),
        _fake_unit(status=SyncRunUnitStatus.FAILED.value),
    ]

    resp = _job_run_response(run, cast(SyncRun, planner), _unit_rollup(units))

    # JobRunStatus enum/labels are unchanged: PARTIAL_FAILED collapses to the
    # 'failed' label, but the literal run state stays distinguishable in result.
    assert resp.status == "failed"
    assert resp.result is not None
    assert resp.result["sync_run_status"] == "partial_failed"
    assert resp.result["retry_exhausted_units"] == 1
    assert resp.result["retrying_units"] == 0
    assert resp.result["failed_units"] == 1
    assert resp.result["completed_units"] == 2
    assert resp.items_synced == 2


def test_job_run_response_uses_unit_statuses_when_sync_run_counters_are_stale():
    run = types.SimpleNamespace(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        status=JobRunStatus.RUNNING.value,
        started_at=None,
        completed_at=None,
        duration_seconds=None,
        error=None,
        result={"sync_run_id": str(uuid.uuid4()), "planner_managed": True},
        triggered_by="sync",
        created_at=_T0,
    )
    planner = types.SimpleNamespace(
        id=uuid.uuid4(),
        status=SyncRunStatus.RUNNING.value,
        result={},
        mode="incremental",
        triggered_by="sync",
        total_units=4,
        completed_units=0,
        failed_units=0,
        started_at=_T0,
        completed_at=None,
        error=None,
    )
    units = [
        _fake_unit(status=SyncRunUnitStatus.SUCCESS.value),
        _fake_unit(status=SyncRunUnitStatus.SUCCESS.value),
        _fake_unit(status=SyncRunUnitStatus.SUCCESS.value),
        _fake_unit(status=SyncRunUnitStatus.FAILED.value),
    ]

    resp = _job_run_response(run, cast(SyncRun, planner), _unit_rollup(units))

    assert resp.status == "failed"
    assert resp.items_synced == 3
    assert resp.result is not None
    assert resp.result["sync_run_status"] == SyncRunStatus.PARTIAL_FAILED.value
    assert resp.result["completed_units"] == 3
    assert resp.result["failed_units"] == 1
    assert resp.sync_run is not None
    assert resp.sync_run.completed_units == 3
    assert resp.sync_run.failed_units == 1


def test_job_run_response_uses_unit_statuses_when_run_status_is_stale_terminal():
    run = types.SimpleNamespace(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        status=JobRunStatus.SUCCESS.value,
        started_at=None,
        completed_at=None,
        duration_seconds=None,
        error=None,
        result={"sync_run_id": str(uuid.uuid4()), "planner_managed": True},
        triggered_by="sync",
        created_at=_T0,
    )
    planner = types.SimpleNamespace(
        id=uuid.uuid4(),
        status=SyncRunStatus.SUCCESS.value,
        result={},
        mode="incremental",
        triggered_by="sync",
        total_units=4,
        completed_units=4,
        failed_units=0,
        started_at=_T0,
        completed_at=None,
        error=None,
    )
    units = [
        _fake_unit(status=SyncRunUnitStatus.SUCCESS.value),
        _fake_unit(status=SyncRunUnitStatus.RUNNING.value),
        _fake_unit(status=SyncRunUnitStatus.RUNNING.value),
        _fake_unit(status=SyncRunUnitStatus.RUNNING.value),
    ]

    resp = _job_run_response(run, cast(SyncRun, planner), _unit_rollup(units))

    assert resp.status == "running"
    assert resp.items_synced == 1
    assert resp.result is not None
    assert resp.result["sync_run_status"] == SyncRunStatus.RUNNING.value
    assert resp.result["completed_units"] == 1
