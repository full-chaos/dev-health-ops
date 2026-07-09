from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.backfill import BackfillJobService
from dev_health_ops.api.services.backfill_diagnostics import (
    build_backfill_metrics_diagnostics,
)
from dev_health_ops.metrics.compounding_risk import (
    REASON_MISSING_COMPLEXITY_DELTA,
    REASON_MISSING_OWNERSHIP_SIGNAL,
    REASON_MISSING_REVIEW_LATENCY,
    REASON_MISSING_REWORK_CHURN,
)
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
sync_router_module = importlib.import_module("dev_health_ops.api.admin.routers.sync")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")


class _FakeMetricsSink:
    """Fake ClickHouse metrics sink for backfill diagnostics tests.

    Rows are dispatched by matching a table-name substring in the query
    text -- mirrors the real diagnostics builder, which issues one query
    per table (``repo_metrics_daily``, ``repo_complexity_daily``,
    ``compounding_risk_daily``).
    """

    def __init__(self) -> None:
        self.repo_metrics_rows: list[dict] = []
        self.repo_complexity_rows: list[dict] = []
        self.compounding_risk_rows: list[dict] = []
        self.calls: list[dict] = []
        self.queries: list[str] = []
        self.open_calls: int = 0

    def query_dicts(self, query: str, parameters: dict) -> list[dict]:
        self.calls.append(parameters)
        self.queries.append(query)
        if "repo_metrics_daily" in query:
            return self.repo_metrics_rows
        if "repo_complexity_daily" in query:
            return self.repo_complexity_rows
        if "compounding_risk_daily" in query:
            return self.compounding_risk_rows
        # Fail loudly rather than silently returning no rows: an unrecognized
        # query means the diagnostics builder changed and this fake is stale.
        raise ValueError(f"_FakeMetricsSink received unexpected query: {query!r}")


_TABLES = tables_of(User, Organization, SyncConfiguration, BackfillJob)


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

    metrics_sink = _FakeMetricsSink()

    @asynccontextmanager
    async def _metrics_sink_cm():
        # Counts how many times the lazy sink factory is actually entered,
        # so tests can prove a 404 detail response never opens the sink
        # (CHAOS-2888 Workstream C review fix).
        metrics_sink.open_calls += 1
        yield metrics_sink

    def _metrics_sink_factory_override():
        return _metrics_sink_cm

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    app.dependency_overrides[sync_router_module.get_backfill_metrics_sink] = (
        _metrics_sink_factory_override
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state, metrics_sink

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
        assert found.completed_chunks is not None
        assert found.total_chunks is not None
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
    ac, seeded_state, _metrics_sink = client

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
    assert detail_data["metrics_diagnostics"] is not None
    assert detail_data["metrics_diagnostics"]["range_start"] == "2026-03-01"
    assert detail_data["metrics_diagnostics"]["range_end"] == "2026-03-05"

    list_item = next(item for item in list_data["items"] if item["id"] == job_id)
    assert list_item["metrics_diagnostics"] is None


@pytest.mark.asyncio
async def test_backfill_job_detail_not_found_returns_404(client):
    ac, _seeded_state, metrics_sink = client
    resp = await ac.get(f"/api/v1/admin/backfill-jobs/{uuid.uuid4()}")
    assert resp.status_code == 404
    # A missing job must never open the ClickHouse diagnostics sink
    # (CHAOS-2888 Workstream C review fix): the factory is only entered
    # after the job-existence check passes.
    assert metrics_sink.open_calls == 0
    assert metrics_sink.calls == []


# ---------------------------------------------------------------------------
# Metrics diagnostics builder (CHAOS-2888 Workstream C, pure aggregation)
# ---------------------------------------------------------------------------


def test_build_backfill_metrics_diagnostics_aggregates_row_counts_and_reasons() -> None:
    sink = _FakeMetricsSink()
    sink.repo_metrics_rows = [
        {"day": date(2026, 4, 1), "row_count": 2},
        {"day": date(2026, 4, 2), "row_count": 1},
    ]
    sink.repo_complexity_rows = [
        {"day": date(2026, 4, 1), "row_count": 2},
    ]
    sink.compounding_risk_rows = [
        {
            "day": date(2026, 4, 1),
            "total_rows": 2,
            "non_null_rows": 1,
            "unknown_rows": 1,
            "missing_rework_churn": 0,
            "missing_complexity_delta": 1,
            "missing_review_latency": 0,
            "missing_ownership_signal": 0,
        },
        {
            "day": date(2026, 4, 2),
            "total_rows": 1,
            "non_null_rows": 1,
            "unknown_rows": 0,
            "missing_rework_churn": 0,
            "missing_complexity_delta": 0,
            "missing_review_latency": 0,
            "missing_ownership_signal": 0,
        },
    ]

    diagnostics = build_backfill_metrics_diagnostics(
        sink,
        org_id="acme",
        range_start=date(2026, 4, 1),
        range_end=date(2026, 4, 2),
    )

    assert diagnostics.range_start == date(2026, 4, 1)
    assert diagnostics.range_end == date(2026, 4, 2)
    assert diagnostics.aggregate.repo_metrics_rows == 3
    assert diagnostics.aggregate.repo_complexity_rows == 2
    assert diagnostics.aggregate.compounding_risk_rows == 3
    assert diagnostics.aggregate.compounding_risk_non_null_rows == 2
    assert diagnostics.aggregate.compounding_risk_unknown_rows == 1
    assert diagnostics.aggregate.reason_counts[REASON_MISSING_COMPLEXITY_DELTA] == 1
    assert diagnostics.aggregate.reason_counts[REASON_MISSING_REWORK_CHURN] == 0
    assert len(diagnostics.per_day) == 2
    day1, day2 = diagnostics.per_day
    assert day1.day == date(2026, 4, 1)
    assert day1.repo_complexity_rows == 2
    assert day1.compounding_risk_unknown_rows == 1
    assert day2.day == date(2026, 4, 2)
    assert day2.repo_complexity_rows == 0
    assert day2.compounding_risk_unknown_rows == 0


def test_build_backfill_metrics_diagnostics_reports_unsupported_historical_complexity() -> (
    None
):
    sink = _FakeMetricsSink()
    sink.repo_metrics_rows = [
        {"day": date(2026, 5, 1), "row_count": 3},
        {"day": date(2026, 5, 2), "row_count": 3},
    ]
    # No repo_complexity_daily rows at all: historical complexity is
    # unsupported for this window (CHAOS-2888 behavior contract item 3-5).
    sink.compounding_risk_rows = [
        {
            "day": day,
            "total_rows": 3,
            "non_null_rows": 0,
            "unknown_rows": 3,
            "missing_rework_churn": 0,
            "missing_complexity_delta": 3,
            "missing_review_latency": 0,
            "missing_ownership_signal": 0,
        }
        for day in (date(2026, 5, 1), date(2026, 5, 2))
    ]

    diagnostics = build_backfill_metrics_diagnostics(
        sink,
        org_id="acme",
        range_start=date(2026, 5, 1),
        range_end=date(2026, 5, 2),
    )

    assert diagnostics.aggregate.repo_complexity_rows == 0
    assert diagnostics.aggregate.compounding_risk_non_null_rows == 0
    assert diagnostics.aggregate.compounding_risk_unknown_rows == 6
    assert diagnostics.aggregate.reason_counts[REASON_MISSING_COMPLEXITY_DELTA] == 6
    for day in diagnostics.per_day:
        assert day.repo_complexity_rows == 0
        assert day.compounding_risk_non_null_rows == 0
        assert (
            day.reason_counts[REASON_MISSING_COMPLEXITY_DELTA]
            == day.compounding_risk_rows
        )


def test_build_backfill_metrics_diagnostics_zero_fills_days_without_data() -> None:
    sink = _FakeMetricsSink()  # no data anywhere
    diagnostics = build_backfill_metrics_diagnostics(
        sink, org_id="acme", range_start=date(2026, 6, 1), range_end=date(2026, 6, 3)
    )
    assert len(diagnostics.per_day) == 3
    fixed_reasons = {
        REASON_MISSING_REWORK_CHURN,
        REASON_MISSING_COMPLEXITY_DELTA,
        REASON_MISSING_REVIEW_LATENCY,
        REASON_MISSING_OWNERSHIP_SIGNAL,
    }
    for day in diagnostics.per_day:
        assert day.repo_metrics_rows == 0
        assert day.repo_complexity_rows == 0
        assert day.compounding_risk_rows == 0
        assert set(day.reason_counts) == fixed_reasons
        assert all(count == 0 for count in day.reason_counts.values())
    assert diagnostics.aggregate.repo_metrics_rows == 0


def test_build_backfill_metrics_diagnostics_scopes_compounding_risk_to_repo() -> None:
    """The compounding_risk_daily read must filter to scope='repo'.

    Rows are persisted per (day, scope, scope_id): a 'repo' row and a
    'team' row both cover the same org/day. Without a scope filter, the
    aggregate query's ``GROUP BY day`` sums across both scopes and
    double-counts row/reason counts for orgs with team mappings
    (CHAOS-2888 Workstream C review fix).
    """
    sink = _FakeMetricsSink()
    build_backfill_metrics_diagnostics(
        sink,
        org_id="acme",
        range_start=date(2026, 4, 1),
        range_end=date(2026, 4, 2),
    )
    risk_queries = [q for q in sink.queries if "compounding_risk_daily" in q]
    assert risk_queries, "compounding_risk_daily was never queried"
    assert all("scope = 'repo'" in q for q in risk_queries)


# ---------------------------------------------------------------------------
# Detail endpoint wiring (CHAOS-2888 Workstream C, API surface)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_job_detail_reports_missing_complexity_delta_for_unsupported_historical_window(
    client, session_maker
):
    ac, seeded_state, metrics_sink = client

    async with session_maker() as session:
        svc = BackfillJobService(session, seeded_state["org_id"])
        job = await svc.create_job(
            sync_config_id=seeded_state["sync_config_id"],
            since=date(2026, 7, 1),
            before=date(2026, 7, 2),
            total_chunks=2,
        )
        await session.commit()
        job_id = str(job.id)

    metrics_sink.repo_metrics_rows = [
        {"day": date(2026, 7, 1), "row_count": 2},
        {"day": date(2026, 7, 2), "row_count": 2},
    ]
    metrics_sink.compounding_risk_rows = [
        {
            "day": day,
            "total_rows": 2,
            "non_null_rows": 0,
            "unknown_rows": 2,
            "missing_rework_churn": 0,
            "missing_complexity_delta": 2,
            "missing_review_latency": 0,
            "missing_ownership_signal": 0,
        }
        for day in (date(2026, 7, 1), date(2026, 7, 2))
    ]

    resp = await ac.get(f"/api/v1/admin/backfill-jobs/{job_id}")
    assert resp.status_code == 200
    diagnostics = resp.json()["metrics_diagnostics"]
    assert diagnostics["aggregate"]["repo_complexity_rows"] == 0
    assert diagnostics["aggregate"]["compounding_risk_non_null_rows"] == 0
    assert diagnostics["aggregate"]["compounding_risk_unknown_rows"] == 4
    assert diagnostics["aggregate"]["reason_counts"]["missing_complexity_delta"] == 4
    assert len(diagnostics["per_day"]) == 2
    for day in diagnostics["per_day"]:
        assert day["repo_complexity_rows"] == 0
        assert day["reason_counts"]["missing_complexity_delta"] == 2
