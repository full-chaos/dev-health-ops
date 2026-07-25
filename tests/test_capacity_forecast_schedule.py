"""CHAOS-2878: capacity forecast beat must fan out per active org."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from celery.schedules import crontab

from dev_health_ops.metrics.compute_capacity import (
    ForecastResult,
    ThroughputHistory,
    ThroughputSample,
)
from dev_health_ops.metrics.schemas import CapacityForecastRecord


class FakeClickHouseSink:
    backend_type = "clickhouse"
    org_id: str

    def __init__(self) -> None:
        self.client = MagicMock()

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        result = self.client.query(query, parameters=parameters)
        rows = result.result_rows or []
        if "SELECT DISTINCT team_id, work_scope_id" in query:
            return [{"team_id": row[0], "work_scope_id": row[1]} for row in rows]
        if "wip_count_end_of_day" in query:
            return [{"wip_count_end_of_day": row[0]} for row in rows]
        return [{"day": row[0], "items_completed": row[1]} for row in rows]


class FakeSqlSink:
    backend_type = "postgres"
    org_id: str

    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, str]]] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.queries.append((query, parameters))
        return []


class FakeCapacitySink:
    backend_type = "clickhouse"
    org_id: str

    def __init__(self) -> None:
        self.closed = False
        self.written: list[CapacityForecastRecord] = []

    def write_capacity_forecasts(self, rows: Sequence[CapacityForecastRecord]) -> None:
        self.written.extend(rows)

    def close(self) -> None:
        self.closed = True


class TestCapacityForecastTaskRegistered:
    def test_tasks_exported_from_tasks_module(self) -> None:
        """Given the worker task module, When imported, Then both capacity tasks exist."""
        from dev_health_ops.workers import tasks

        assert "run_capacity_forecast_job" in tasks.__all__
        assert hasattr(tasks, "run_capacity_forecast_job")
        assert "dispatch_capacity_forecast" in tasks.__all__
        assert hasattr(tasks, "dispatch_capacity_forecast")

    def test_dispatcher_is_celery_task(self) -> None:
        """Given the dispatcher, When inspected, Then it is registered on default."""
        from dev_health_ops.workers.celery_app import celery_app

        dispatch_capacity_forecast = celery_app.tasks[
            "dev_health_ops.workers.tasks.dispatch_capacity_forecast"
        ]
        assert (
            getattr(dispatch_capacity_forecast, "name")
            == "dev_health_ops.workers.tasks.dispatch_capacity_forecast"
        )
        assert getattr(dispatch_capacity_forecast, "queue") == "default"


class TestCapacityForecastBeatSchedule:
    def test_beat_schedule_dispatches_per_org(self) -> None:
        """Given beat config, When due, Then it targets the per-org dispatcher."""
        from dev_health_ops.workers.config import beat_schedule

        entry = beat_schedule["run-capacity-forecast"]
        assert (
            entry["task"] == "dev_health_ops.workers.tasks.dispatch_capacity_forecast"
        )
        assert entry["options"]["queue"] == "default"
        assert "kwargs" not in entry

    def test_beat_schedule_uses_weekly_crontab(self) -> None:
        """Given beat config, When inspected, Then cadence remains weekly."""
        from dev_health_ops.workers.config import beat_schedule

        schedule = beat_schedule["run-capacity-forecast"]["schedule"]
        assert isinstance(schedule, crontab)

    def test_beat_schedule_call_shape_is_signature_valid(self) -> None:
        """Given beat config, When Celery validates args, Then no org_id is missing."""
        from dev_health_ops.workers.celery_app import celery_app
        from dev_health_ops.workers.config import beat_schedule

        entry = beat_schedule["run-capacity-forecast"]
        dispatch_capacity_forecast = celery_app.tasks[entry["task"]]
        kwargs = entry.get("kwargs", {})
        getattr(dispatch_capacity_forecast, "__header__")(**kwargs)


class TestCapacityForecastDispatcherFansOutPerOrg:
    def test_dispatcher_enqueues_one_task_per_active_org(self) -> None:
        """Given active orgs, When dispatcher runs, Then each task has org_id."""
        from dev_health_ops.workers import product_tasks

        org_a = str(uuid4())
        org_b = str(uuid4())

        with (
            patch(
                "dev_health_ops.workers.recommendations_tasks._discover_active_org_ids",
                return_value=[org_a, org_b],
            ),
            patch.object(product_tasks.celery_app, "send_task") as mock_send_task,
        ):
            result = getattr(product_tasks.dispatch_capacity_forecast, "run")(
                db_url="clickhouse://fake"
            )

        assert mock_send_task.call_count == 2
        dispatched_orgs = {
            call.kwargs["kwargs"]["org_id"] for call in mock_send_task.call_args_list
        }
        assert dispatched_orgs == {org_a, org_b}
        worker_task = product_tasks.celery_app.tasks[
            "dev_health_ops.workers.tasks.run_capacity_forecast_job"
        ]
        for call in mock_send_task.call_args_list:
            assert (
                call.args[0] == "dev_health_ops.workers.tasks.run_capacity_forecast_job"
            )
            assert call.kwargs["queue"] == "metrics"
            assert call.kwargs["kwargs"]["db_url"] == "clickhouse://fake"
            assert call.kwargs["kwargs"]["all_teams"] is True
            getattr(worker_task, "__header__")(**call.kwargs["kwargs"])
        assert set(result["dispatched"]) == {org_a, org_b}


@pytest.mark.asyncio
async def test_run_capacity_forecast_persists_rows_with_requested_org_id() -> None:
    from dev_health_ops.metrics import job_capacity

    sink = FakeCapacitySink()
    forecast = ForecastResult(
        forecast_id="forecast-1",
        computed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        team_id="team-a",
        work_scope_id="scope-a",
        backlog_size=7,
        target_items=7,
        target_date=None,
        history_days=30,
        simulation_count=10,
        p50_days=1,
        p85_days=2,
        p95_days=3,
        p50_date=date(2026, 1, 2),
        p85_date=date(2026, 1, 3),
        p95_date=date(2026, 1, 4),
        p50_items=None,
        p85_items=None,
        p95_items=None,
        throughput_mean=1.0,
        throughput_stddev=0.0,
    )
    history = ThroughputHistory(
        [ThroughputSample(day=date(2025, 12, 31), items_completed=1)]
    )

    with (
        patch("dev_health_ops.metrics.job_capacity.create_sink", return_value=sink),
        patch(
            "dev_health_ops.metrics.job_capacity.load_throughput_from_sink",
            return_value=history,
        ),
        patch(
            "dev_health_ops.metrics.job_capacity.get_backlog_from_sink", return_value=7
        ),
        patch(
            "dev_health_ops.metrics.job_capacity.forecast_capacity",
            return_value=forecast,
        ) as mock_forecast,
    ):
        results = await job_capacity.run_capacity_forecast(
            db_url="clickhouse://fake",
            org_id="org-1",
            team_id="team-a",
            work_scope_id="scope-a",
            target_items=7,
            simulations=10,
            persist=True,
            seed=1234,
        )

    assert results == [forecast]
    assert [row.org_id for row in sink.written] == ["org-1"]
    assert sink.closed is True
    assert mock_forecast.call_args.kwargs["seed"] == 1234


@pytest.mark.asyncio
async def test_run_capacity_forecast_rejects_empty_org_id_before_querying() -> None:
    from dev_health_ops.metrics import job_capacity

    with patch("dev_health_ops.metrics.job_capacity.create_sink") as mock_create_sink:
        with pytest.raises(ValueError, match="org_id is required"):
            await job_capacity.run_capacity_forecast(
                db_url="clickhouse://fake",
                org_id="",
                all_teams=True,
            )

    mock_create_sink.assert_not_called()


@pytest.mark.asyncio
async def test_discover_team_scopes_filters_clickhouse_by_org_id() -> None:
    """Given an org-scoped sink, When discovering scopes, Then query filters org."""
    from dev_health_ops.metrics.job_capacity import discover_team_scopes

    sink = FakeClickHouseSink()
    sink.org_id = "org-1"
    sink.client.query.return_value = SimpleNamespace(
        result_rows=[("team-a", "scope-a")]
    )

    result = await discover_team_scopes(sink)

    assert result == [("team-a", "scope-a")]
    query = sink.client.query.call_args.args[0]
    params = sink.client.query.call_args.kwargs["parameters"]
    assert "org_id = {org_id:String}" in query
    assert params == {"org_id": "org-1"}


@pytest.mark.asyncio
async def test_load_throughput_filters_sql_sink_by_org_id() -> None:
    """Given an org-scoped SQL sink, When loading history, Then query filters org."""
    from dev_health_ops.metrics.job_capacity import load_throughput_from_sink

    sink = FakeSqlSink()
    sink.org_id = "org-1"

    history = await load_throughput_from_sink(
        sink,
        team_id="team-a",
        work_scope_id="scope-a",
        history_days=30,
    )

    assert history.daily_throughputs == []
    query, params = sink.queries[0]
    assert "org_id = :org_id" in query
    assert params == {
        "org_id": "org-1",
        "team_id": "team-a",
        "work_scope_id": "scope-a",
    }


@pytest.mark.asyncio
async def test_get_backlog_filters_clickhouse_by_org_id() -> None:
    """Given an org-scoped ClickHouse sink, When loading backlog, Then filters org."""
    from dev_health_ops.metrics.job_capacity import get_backlog_from_sink

    sink = FakeClickHouseSink()
    sink.org_id = "org-1"
    sink.client.query.return_value = SimpleNamespace(result_rows=[(7,)])

    backlog = await get_backlog_from_sink(sink, team_id="team-a")

    assert backlog == 7
    query = sink.client.query.call_args.args[0]
    params = sink.client.query.call_args.kwargs["parameters"]
    assert "org_id = {org_id:String}" in query
    assert params == {"org_id": "org-1", "team_id": "team-a"}


@pytest.mark.asyncio
async def test_capacity_forecasts_resolver_filters_persisted_rows_by_org_id() -> None:
    """Given persisted forecasts, When resolving, Then query is tenant-scoped."""
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecasts

    context = GraphQLContext(
        org_id="org-1", db_url="clickhouse://fake", client=MagicMock()
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        return_value=[],
    ) as mock_query:
        result = await resolve_capacity_forecasts(context, filters=None)

    assert result.total_count == 0
    query = mock_query.call_args.args[1]
    params = mock_query.call_args.args[2]
    assert "org_id = %(org_id)s" in query
    assert params["org_id"] == "org-1"
