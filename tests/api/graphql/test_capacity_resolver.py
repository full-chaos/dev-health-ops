from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.metrics.compute_capacity import ForecastResult, ThroughputHistory

strawberry = pytest.importorskip("strawberry")


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.org_id = "test-org"
    ctx.db_url = "clickhouse://localhost:8123/default"
    ctx.client = MagicMock()
    return ctx


@pytest.fixture
def sample_forecast_result():
    return ForecastResult(
        forecast_id="test-123",
        computed_at=datetime(2026, 1, 29, 12, 0, 0, tzinfo=timezone.utc),
        team_id="team-a",
        work_scope_id="project-1",
        backlog_size=50,
        target_items=50,
        target_date=None,
        history_days=90,
        simulation_count=10000,
        p50_days=5,
        p85_days=7,
        p95_days=10,
        p50_date=date(2026, 2, 3),
        p85_date=date(2026, 2, 5),
        p95_date=date(2026, 2, 8),
        p50_items=None,
        p85_items=None,
        p95_items=None,
        throughput_mean=10.5,
        throughput_stddev=3.2,
        insufficient_history=False,
        high_variance=False,
    )


@pytest.mark.asyncio
async def test_resolve_capacity_forecast_returns_result(
    mock_context, sample_forecast_result
):
    from dev_health_ops.api.graphql.models.inputs import CapacityForecastInput
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecast

    mock_history = MagicMock(spec=ThroughputHistory)
    mock_history.daily_throughputs = [5, 10, 8, 12, 9]

    mock_sink = MagicMock()

    with (
        patch(
            "dev_health_ops.metrics.job_capacity.load_throughput_from_sink",
            new_callable=AsyncMock,
            return_value=mock_history,
        ),
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=mock_sink,
        ),
        patch(
            "dev_health_ops.metrics.job_capacity.get_backlog_from_sink",
            new_callable=AsyncMock,
            return_value=50,
        ),
        patch(
            "dev_health_ops.metrics.compute_capacity.forecast_capacity",
            return_value=sample_forecast_result,
        ),
    ):
        input_data = CapacityForecastInput(
            team_id="team-a",
            work_scope_id="project-1",
            target_items=50,
            history_days=90,
        )

        result = await resolve_capacity_forecast(mock_context, input_data)

        assert result is not None
        assert result.forecast_id == "test-123"
        assert result.team_id == "team-a"
        assert result.p50_days == 5
        assert result.throughput_mean == 10.5
        mock_sink.close.assert_called_once()


@pytest.mark.asyncio
async def test_resolve_capacity_forecast_no_history_returns_none(mock_context):
    from dev_health_ops.api.graphql.models.inputs import CapacityForecastInput
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecast

    mock_history = MagicMock(spec=ThroughputHistory)
    mock_history.daily_throughputs = []
    mock_sink = MagicMock()

    with (
        patch(
            "dev_health_ops.metrics.job_capacity.load_throughput_from_sink",
            new_callable=AsyncMock,
            return_value=mock_history,
        ),
        patch(
            "dev_health_ops.metrics.sinks.factory.create_sink",
            return_value=mock_sink,
        ),
    ):
        input_data = CapacityForecastInput(team_id="team-a")
        result = await resolve_capacity_forecast(mock_context, input_data)
        assert result is None
        mock_sink.close.assert_called_once()


@pytest.mark.asyncio
async def test_resolve_capacity_forecasts_returns_connection(mock_context):
    from dev_health_ops.api.graphql.models.inputs import CapacityForecastFilterInput
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecasts

    mock_rows = [
        {
            "forecast_id": "f1",
            "computed_at": "2026-01-29 12:00:00",
            "team_id": "team-a",
            "work_scope_id": "project-1",
            "backlog_size": 50,
            "target_items": 50,
            "target_date": None,
            "p50_date": date(2026, 2, 3),
            "p85_date": date(2026, 2, 5),
            "p95_date": date(2026, 2, 8),
            "p50_days": 5,
            "p85_days": 7,
            "p95_days": 10,
            "p50_items": None,
            "p85_items": None,
            "p95_items": None,
            "throughput_mean": 10.5,
            "throughput_stddev": 3.2,
            "history_days": 90,
            "insufficient_history": False,
            "high_variance": False,
        }
    ]

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=mock_rows,
    ):
        filters = CapacityForecastFilterInput(team_id="team-a", limit=10)
        result = await resolve_capacity_forecasts(mock_context, filters)

        assert result.total_count == 1
        assert len(result.edges) == 1
        assert result.edges[0].node.forecast_id == "f1"
        assert result.edges[0].node.team_id == "team-a"


@pytest.mark.asyncio
async def test_resolve_capacity_forecasts_empty_returns_empty_connection(mock_context):
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecasts

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await resolve_capacity_forecasts(mock_context, None)

        assert result.total_count == 0
        assert len(result.edges) == 0
        assert result.page_info.has_next_page is False


@pytest.mark.asyncio
async def test_resolve_capacity_forecast_requires_client(mock_context):
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecast

    mock_context.client = None

    with pytest.raises(RuntimeError, match="Database client not available"):
        await resolve_capacity_forecast(mock_context, None)


@pytest.mark.asyncio
async def test_resolve_capacity_forecasts_requires_client(mock_context):
    from dev_health_ops.api.graphql.resolvers.capacity import resolve_capacity_forecasts

    mock_context.client = None

    with pytest.raises(RuntimeError, match="Database client not available"):
        await resolve_capacity_forecasts(mock_context, None)
