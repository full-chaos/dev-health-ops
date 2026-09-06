"""CHAOS-2878: capacity forecast beat must fan out per active org."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


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


# CHAOS-4026 (2026-08-21): TestCapacityForecastTaskRegistered,
# TestCapacityForecastBeatSchedule, and TestCapacityForecastDispatcherFansOutPerOrg
# tested product_tasks.dispatch_capacity_forecast/run_capacity_forecast_job
# and the run-capacity-forecast beat entry, all deleted with this cleanup
# (Go's capacity_forecast_weekly_fanout fixed schedule now owns the
# periodic cadence). See tests/workers/test_celery_dead_code_contract.py.
#
# CHAOS-5336: job_capacity.py itself is now deleted outright -- its only
# caller (api/internal/worker_metrics.py's HTTP-compat-bridge handler) was
# already dead (the Go worker's native CapacityExecutor is the only live
# batch path, confirmed via cmd/dev-health-worker/daily.go's unconditional
# executor construction). run_capacity_forecast had no other live caller and
# is deleted with it; discover_team_scopes/load_throughput_from_sink/
# get_backlog_from_sink are re-exported from capacity_queries.py (still live:
# the GraphQL capacity resolver calls compute_capacity.forecast_capacity
# directly, a separate epic per team-lead's CHAOS-5336 ruling) -- the tests
# below import from there now instead of through job_capacity's re-export.


@pytest.mark.asyncio
async def test_discover_team_scopes_filters_clickhouse_by_org_id() -> None:
    """Given an org-scoped sink, When discovering scopes, Then query filters org."""
    from dev_health_ops.metrics.capacity_queries import discover_team_scopes

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
    from dev_health_ops.metrics.capacity_queries import load_throughput_from_sink

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
    from dev_health_ops.metrics.capacity_queries import get_backlog_from_sink

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
