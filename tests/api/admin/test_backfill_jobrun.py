"""Tests for CHAOS-2536 / CHAOS-2647: planner (fanout) backfill JobRun anchoring.

Covers:
- Endpoint: an unmigrated config (no ``integration_id``) is planner-only
  and returns HTTP 400 without creating any records or dispatching.
- Endpoint (fanout path): creates a visible PENDING JobRun anchored to the sync
  ScheduledJob and threads the planner ``sync_run_id`` into it.
- Endpoint (fanout path): commits the BackfillJob before dispatching.
- Endpoint (fanout path): a dispatch enqueue failure rolls the BackfillJob,
  JobRun and SyncRun to FAILED and returns 503.
- Endpoint: a ``planner_managed`` config routes to fanout.
- Endpoint: a paused config returns 409 without dispatching.

The legacy worker path (``run_backfill``/``sync_backfill``/``sync_tasks``) was
removed in CHAOS-2647; all backfills now go through the planner + unitized
fan-out (``plan_sync_run`` + ``dispatch_sync_run``).
"""

from __future__ import annotations

import importlib
import sqlite3
import types
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
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
from dev_health_ops.api.admin.routers.sync import _job_run_response
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
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
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
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
    SyncDispatchOutbox,
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
    """Patch the unitized dispatcher used by the backfill endpoint.

    The real planner (``plan_sync_run``) still runs against the test DB so the
    PENDING JobRun anchor and SyncRun are created for real; only the Celery
    dispatch is intercepted.
    """
    mock_dispatch = MagicMock()
    if side_effect is not None:
        mock_dispatch.apply_async.side_effect = side_effect
    else:
        mock_dispatch.apply_async.return_value = MagicMock(id=task_id)
    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        yield mock_dispatch


# ---------------------------------------------------------------------------
# Endpoint tests — planner-only routing guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_config_created_via_plain_endpoint_succeeds(
    client, session_maker
):
    """A config created via POST /sync-configs is integration-native, so backfill
    routes through the fan-out planner and is accepted.

    Regression guard: non-git providers fall through to the plain create endpoint
    and must be backfillable, not rejected with the old 'no linked integration'
    400 that only git providers (routed via /batch) avoided.
    """
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="bf-native", provider="linear")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    with _patch_dispatch() as mock_dispatch:
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text
    mock_dispatch.apply_async.assert_called_once()

    async with session_maker() as session:
        backfill_jobs = (await session.execute(select(BackfillJob))).scalars().all()
    assert len(backfill_jobs) == 1


# ---------------------------------------------------------------------------
# Endpoint tests — fanout (planner) path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_fanout_creates_job_run_anchor(
    client, session_maker, seeded_state
):
    """Fan-out backfill creates a PENDING JobRun anchor + threads sync_run_id."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-fanout-anchor", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]
    await _link_migrated_integration(session_maker, seeded_state["org_id"], config_id)

    with _patch_dispatch(task_id="bf-fanout-task-id") as mock_dispatch:
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["mode"] == "fanout"
    sync_run_id = data["sync_run_id"]
    assert sync_run_id is not None
    uuid.UUID(sync_run_id)
    mock_dispatch.apply_async.assert_called_once_with(args=(sync_run_id,), queue="sync")

    # A PENDING JobRun must exist anchored to the config's sync ScheduledJob.
    async with session_maker() as session:
        sched_job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id),
                    ScheduledJob.job_type == "sync",
                )
            )
        ).scalar_one()
        runs = list(
            (await session.execute(select(JobRun).where(JobRun.job_id == sched_job.id)))
            .scalars()
            .all()
        )

    assert len(runs) == 1
    run = runs[0]
    assert run.status == JobRunStatus.PENDING.value
    assert run.triggered_by == "backfill"
    assert run.result["planner_managed"] is True
    assert run.result["sync_run_id"] == sync_run_id


@pytest.mark.asyncio
async def test_backfill_fanout_commits_backfill_job_before_dispatch(
    client, session_maker, seeded_state
):
    """The BackfillJob must be committed (visible) before the dispatch fires."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-fanout-committed-before-dispatch", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]
    await _link_migrated_integration(
        session_maker, seeded_state["org_id"], config_id, planner_managed=True
    )

    db_path = session_maker.kw["bind"].url.database
    visible_at_dispatch: list[tuple[str, int, str | None]] = []

    def _delay_side_effect(*args, **kwargs):
        assert db_path is not None
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT status, total_chunks, celery_task_id
                FROM backfill_jobs
                """
            ).fetchone()
        assert row is not None, "BackfillJob must be committed before dispatch"
        visible_at_dispatch.append(row)
        return MagicMock(id="bf-fanout-visible-task-id")

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.side_effect = _delay_side_effect
    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text
    assert resp.json()["mode"] == "fanout"
    # Durability (CHAOS-2647): the sync_run:<id> marker is committed BEFORE
    # dispatch, so a crash between enqueue and the post-dispatch commit still lets
    # finalize_sync_run link this BackfillJob. At dispatch time the committed row
    # is pending/0-chunks and already carries the marker.
    assert len(visible_at_dispatch) == 1
    status_at_dispatch, chunks_at_dispatch, marker_at_dispatch = visible_at_dispatch[0]
    assert (status_at_dispatch, chunks_at_dispatch) == ("pending", 0)
    assert marker_at_dispatch is not None
    assert marker_at_dispatch.startswith("sync_run:")

    async with session_maker() as session:
        backfill_job = (await session.execute(select(BackfillJob))).scalar_one()
    # The celery_task_id must carry the ``sync_run:<id>`` marker so finalize_sync_run
    # (which looks up BackfillJob by celery_task_id.contains("sync_run:<id>")) can link
    # the job to its run and update status/chunk counts (CHAOS-2647 regression).
    assert (
        backfill_job.celery_task_id
        == f"bf-fanout-visible-task-id|sync_run:{resp.json()['sync_run_id']}"
    )


@pytest.mark.asyncio
async def test_backfill_fanout_enqueue_failure_marks_records_failed(
    client, session_maker, seeded_state
):
    """A dispatch enqueue failure flips BackfillJob, JobRun and SyncRun to FAILED.

    Unlike the deleted legacy path, the planner commits the SyncRun + units
    before dispatching, so on enqueue failure the committed SyncRun is rolled to
    FAILED (rather than left non-existent).
    """
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-fanout-enqueue-fails", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]
    await _link_migrated_integration(
        session_maker, seeded_state["org_id"], config_id, planner_managed=True
    )

    with _patch_dispatch(side_effect=RuntimeError("broker down")):
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 503
    assert "Task queue unavailable: RuntimeError: broker down" in resp.json()["detail"]

    async with session_maker() as session:
        backfill_job = (await session.execute(select(BackfillJob))).scalar_one()
        job_runs = list((await session.execute(select(JobRun))).scalars().all())
        sync_runs = list((await session.execute(select(SyncRun))).scalars().all())

    assert backfill_job.status == "failed"
    assert backfill_job.error_message == "RuntimeError: broker down"
    assert backfill_job.completed_at is not None
    assert backfill_job.celery_task_id is None
    assert len(job_runs) == 1
    assert job_runs[0].status == JobRunStatus.FAILED.value
    assert job_runs[0].error == "RuntimeError: broker down"
    assert job_runs[0].completed_at is not None
    assert len(sync_runs) == 1
    assert sync_runs[0].status == SyncRunStatus.FAILED.value


@pytest.mark.asyncio
async def test_backfill_fanout_enqueue_failure_sanitizes_broker_url_credential(
    client, session_maker, seeded_state
):
    """Codex review finding (CHAOS-2766 PR #1123): a Celery/broker
    enqueue-failure exception can embed the configured broker/result-backend
    URL, credentials included -- e.g. Celery/kombu wrapping a connection
    error with the DSN it tried. That credential must not survive into
    BackfillJob.error_message, JobRun.error, or the API-returned detail; all
    three are populated from the same exception."""
    from dev_health_ops.sync.error_sanitize import REDACTION_MARKER

    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-fanout-broker-credential-leak", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]
    await _link_migrated_integration(
        session_maker, seeded_state["org_id"], config_id, planner_managed=True
    )

    # Built via concatenation, not a single literal (Gitleaks; see
    # tests/test_error_sanitize.py's module docstring for why).
    fixture_value = "brokerXzqmno" + "9876543210"
    broker_dsn_error = RuntimeError(
        "Error 111 connecting to redis://:"
        + fixture_value
        + "@redis-broker.internal:6379/0."
    )

    with _patch_dispatch(side_effect=broker_dsn_error):
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert fixture_value not in detail
    assert REDACTION_MARKER in detail

    async with session_maker() as session:
        backfill_job = (await session.execute(select(BackfillJob))).scalar_one()
        job_runs = list((await session.execute(select(JobRun))).scalars().all())

    assert backfill_job.status == "failed"
    assert backfill_job.error_message is not None
    assert fixture_value not in backfill_job.error_message
    assert REDACTION_MARKER in backfill_job.error_message

    assert len(job_runs) == 1
    assert job_runs[0].error is not None
    assert fixture_value not in job_runs[0].error
    assert REDACTION_MARKER in job_runs[0].error


@pytest.mark.asyncio
async def test_backfill_planner_managed_config_routes_to_fanout(
    client, session_maker, seeded_state
):
    """A planner_managed migrated config routes to the fan-out path."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-planner-managed", provider="github"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]
    await _link_migrated_integration(
        session_maker, seeded_state["org_id"], config_id, planner_managed=True
    )

    with _patch_dispatch(task_id="bf-planner-managed-task-id") as mock_dispatch:
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["mode"] == "fanout"
    sync_run_id = data["sync_run_id"]
    mock_dispatch.apply_async.assert_called_once_with(args=(sync_run_id,), queue="sync")

    async with session_maker() as session:
        sched_job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id),
                    ScheduledJob.job_type == "sync",
                )
            )
        ).scalar_one()
        run = (
            await session.execute(select(JobRun).where(JobRun.job_id == sched_job.id))
        ).scalar_one()
    assert run.status == JobRunStatus.PENDING.value
    assert run.triggered_by == "backfill"
    assert run.result["planner_managed"] is True
    assert run.result["sync_run_id"] == sync_run_id


@pytest.mark.asyncio
async def test_backfill_paused_config_returns_409_without_dispatch(
    client, session_maker, seeded_state
):
    """A paused config is rejected with 409 before any planning/dispatch."""
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="bf-paused-rejected", provider="github"
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]
    await _link_migrated_integration(session_maker, seeded_state["org_id"], config_id)

    async with session_maker() as session:
        cfg = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        cfg.is_active = False
        await session.commit()

    with _patch_dispatch() as mock_dispatch:
        resp = await ac.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-01-01", "before": "2026-01-08"},
        )

    assert resp.status_code == 409
    assert "paused" in resp.json()["detail"]
    mock_dispatch.apply_async.assert_not_called()

    async with session_maker() as session:
        job_runs = (await session.execute(select(JobRun))).scalars().all()
        backfill_jobs = (await session.execute(select(BackfillJob))).scalars().all()
    assert job_runs == []
    assert backfill_jobs == []


# ---------------------------------------------------------------------------
# CHAOS-2710: operator-visible retry/timeout projection (Phase 6)
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


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
        completed_units=2,
        failed_units=1,
        started_at=_T0,
        completed_at=datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc),
        error=None,
    )
    resp = _job_run_response(run, cast(SyncRun, planner))
    # JobRunStatus enum/labels are unchanged: PARTIAL_FAILED collapses to the
    # 'failed' label, but the literal run state stays distinguishable in result.
    assert resp.status == "failed"
    assert resp.result is not None
    assert resp.result["sync_run_status"] == "partial_failed"
    assert resp.result["retry_exhausted_units"] == 1
    assert resp.result["retrying_units"] == 0
    assert resp.result["failed_units"] == 1
