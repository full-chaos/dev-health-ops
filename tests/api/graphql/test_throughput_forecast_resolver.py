"""Tests for the throughput-forecast GraphQL resolver (CHAOS-1783).

Covers two behaviours added by the capacity-planning UX fix:

* When ``ThroughputForecastInput.team_id`` is ``None`` the resolver
  aggregates org-wide instead of falling back to sample data.
* When ``ThroughputForecastInput.backlog_size`` is ``None`` the resolver
  derives the backlog from the latest ``work_item_metrics_daily`` rows
  matching the same scope.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.metrics.compute_capacity import ThroughputHistory, ThroughputSample
from dev_health_ops.metrics.forecast import (
    RiskKind,
    RiskOverlay,
    RollingWindowThroughput,
    ThroughputForecastResult,
)

strawberry = pytest.importorskip("strawberry")


def _risk(kind: RiskKind) -> RiskOverlay:
    return RiskOverlay(
        kind=kind,
        score=0.0,
        label=kind.value,
        value=0.0,
        threshold=1.0,
        active=False,
    )


def _result(team_id: str | None, backlog_size: int) -> ThroughputForecastResult:
    return ThroughputForecastResult(
        forecast_id="forecast-123",
        computed_at=datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc),
        team_id=team_id,
        work_scope_id=None,
        backlog_size=backlog_size,
        history_weeks=12,
        p50_weeks=4,
        p75_weeks=6,
        p90_weeks=8,
        rolling_windows=(
            RollingWindowThroughput(
                window_weeks=4,
                mean_weekly_throughput=8.0,
                samples=(8.0,),
                insufficient_history=False,
            ),
        ),
        primary_risk=_risk(RiskKind.REVIEW),
        wip_congestion=_risk(RiskKind.WIP),
        review_bottleneck=_risk(RiskKind.REVIEW),
        incident_load=_risk(RiskKind.INCIDENT),
        insufficient_history=False,
    )


@pytest.fixture
def ctx():
    c = MagicMock()
    c.org_id = "test-org"
    c.client = MagicMock()
    return c


@pytest.mark.asyncio
async def test_resolver_aggregates_org_wide_when_team_id_is_none(ctx):
    """``team_id=None`` must NOT short-circuit to None or sample data."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    history = ThroughputHistory(
        [
            ThroughputSample(
                day=date(2026, 5, i + 1),
                items_completed=5,
                team_id=None,
                work_scope_id=None,
            )
            for i in range(14)
        ]
    )

    fake_result = _result(team_id=None, backlog_size=42)

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=history,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_work_item_overlay",
            new_callable=AsyncMock,
            return_value=(0.0, 0.0),
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_review_overlay",
            new_callable=AsyncMock,
            return_value=0.0,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_incident_overlay",
            new_callable=AsyncMock,
            return_value=0.0,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_backlog",
            new_callable=AsyncMock,
            return_value=42,
        ) as load_backlog,
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast.forecast_throughput_capacity",
            return_value=fake_result,
        ),
    ):
        result = await resolve_throughput_forecast(
            ctx,
            ThroughputForecastInput(),  # no team_id, no backlog_size
        )

    assert result is not None
    assert result.team_id is None
    assert result.backlog_size == 42
    load_backlog.assert_awaited_once_with(ctx, team_id=None, work_scope_id=None)


@pytest.mark.asyncio
async def test_resolver_skips_backlog_query_when_caller_provides_size(ctx):
    """Explicit ``backlog_size`` must bypass ``_load_backlog``."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    history = ThroughputHistory(
        [
            ThroughputSample(
                day=date(2026, 5, i + 1),
                items_completed=3,
                team_id="team-a",
                work_scope_id=None,
            )
            for i in range(14)
        ]
    )

    fake_result = _result(team_id="team-a", backlog_size=99)

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=history,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_work_item_overlay",
            new_callable=AsyncMock,
            return_value=(0.0, 0.0),
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_review_overlay",
            new_callable=AsyncMock,
            return_value=0.0,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_incident_overlay",
            new_callable=AsyncMock,
            return_value=0.0,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_backlog",
            new_callable=AsyncMock,
        ) as load_backlog,
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast.forecast_throughput_capacity",
            return_value=fake_result,
        ),
    ):
        result = await resolve_throughput_forecast(
            ctx, ThroughputForecastInput(team_id="team-a", backlog_size=99)
        )

    assert result is not None
    assert result.team_id == "team-a"
    assert result.backlog_size == 99
    load_backlog.assert_not_awaited()


@pytest.mark.asyncio
async def test_load_backlog_sums_across_partitions_when_team_id_is_none(ctx):
    """``_load_backlog`` must aggregate the latest day across all teams."""
    from dev_health_ops.api.graphql.resolvers.forecast import _load_backlog

    with patch(
        "dev_health_ops.api.graphql.resolvers.forecast.query_dicts",
        new_callable=AsyncMock,
        return_value=[{"backlog": 137}],
    ) as query:
        backlog = await _load_backlog(ctx, team_id=None, work_scope_id=None)

    assert backlog == 137
    # No team_id / work_scope_id should appear in params when both are None.
    call_args = query.await_args
    assert call_args is not None
    params = call_args.args[2]
    assert "team_id" not in params
    assert "work_scope_id" not in params
