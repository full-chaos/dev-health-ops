"""Tests for the throughput-forecast GraphQL resolver (CHAOS-1783).

Covers behaviours added by the capacity-planning UX fix:

* When ``ThroughputForecastInput.team_ids`` is ``None`` or empty the
  resolver aggregates org-wide instead of falling back to sample data.
* When ``team_ids`` contains multiple ids the resolver aggregates across
  the selected teams (single team scopes are a special case of the
  multi-team path).
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


def _history(team_id: str | None = None) -> ThroughputHistory:
    return ThroughputHistory(
        [
            ThroughputSample(
                day=date(2026, 5, i + 1),
                items_completed=5,
                team_id=team_id,
                work_scope_id=None,
            )
            for i in range(14)
        ]
    )


@pytest.fixture
def ctx():
    c = MagicMock()
    c.org_id = "test-org"
    c.client = MagicMock()
    return c


@pytest.mark.asyncio
async def test_resolver_aggregates_org_wide_when_team_ids_is_none(ctx):
    """``team_ids=None`` must NOT short-circuit to None or sample data."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=_history(),
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
            return_value=_result(team_id=None, backlog_size=42),
        ),
    ):
        result = await resolve_throughput_forecast(ctx, ThroughputForecastInput())

    assert result is not None
    assert result.team_id is None
    assert result.backlog_size == 42
    load_backlog.assert_awaited_once_with(ctx, team_ids=None, work_scope_id=None)


@pytest.mark.asyncio
async def test_resolver_aggregates_multi_team_selection(ctx):
    """Multi-team ``team_ids`` must NOT collapse to one team's data."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=_history(),
        ) as load_history,
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_work_item_overlay",
            new_callable=AsyncMock,
            return_value=(0.0, 0.0),
        ) as load_wip,
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
            return_value=84,
        ) as load_backlog,
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast.forecast_throughput_capacity",
            return_value=_result(team_id=None, backlog_size=84),
        ),
    ):
        result = await resolve_throughput_forecast(
            ctx,
            ThroughputForecastInput(team_ids=["team-1", "team-2"]),
        )

    assert result is not None
    # Multi-team scope: result's team_id stays None (aggregated across teams).
    assert result.team_id is None
    assert result.backlog_size == 84
    # Every loader receives the FULL list, not just the first id.
    load_history.assert_awaited_once_with(
        ctx, team_ids=["team-1", "team-2"], work_scope_id=None, history_weeks=12
    )
    load_wip.assert_awaited_once_with(
        ctx, team_ids=["team-1", "team-2"], work_scope_id=None, history_weeks=12
    )
    load_backlog.assert_awaited_once_with(
        ctx, team_ids=["team-1", "team-2"], work_scope_id=None
    )


@pytest.mark.asyncio
async def test_resolver_single_team_sets_result_team_id(ctx):
    """Single-team scope echoes the team id back on the result."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=_history(team_id="team-a"),
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
            return_value=40,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast.forecast_throughput_capacity",
            return_value=_result(team_id="team-a", backlog_size=40),
        ),
    ):
        result = await resolve_throughput_forecast(
            ctx, ThroughputForecastInput(team_ids=["team-a"])
        )

    assert result is not None
    assert result.team_id == "team-a"


@pytest.mark.asyncio
async def test_resolver_skips_backlog_query_when_caller_provides_size(ctx):
    """Explicit ``backlog_size`` must bypass ``_load_backlog``."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=_history(team_id="team-a"),
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
            return_value=_result(team_id="team-a", backlog_size=99),
        ),
    ):
        result = await resolve_throughput_forecast(
            ctx, ThroughputForecastInput(team_ids=["team-a"], backlog_size=99)
        )

    assert result is not None
    assert result.team_id == "team-a"
    assert result.backlog_size == 99
    load_backlog.assert_not_awaited()


@pytest.mark.asyncio
async def test_load_backlog_omits_team_clause_when_team_ids_empty(ctx):
    """``_load_backlog`` must aggregate the latest day across all teams."""
    from dev_health_ops.api.graphql.resolvers.forecast import _load_backlog

    with patch(
        "dev_health_ops.api.graphql.resolvers.forecast.query_dicts",
        new_callable=AsyncMock,
        return_value=[{"backlog": 137}],
    ) as query:
        backlog = await _load_backlog(ctx, team_ids=None, work_scope_id=None)

    assert backlog == 137
    call_args = query.await_args
    assert call_args is not None
    params = call_args.args[2]
    assert "team_id" not in params
    assert "team_ids" not in params
    assert "work_scope_id" not in params


@pytest.mark.asyncio
async def test_load_backlog_uses_in_clause_for_multiple_teams(ctx):
    """Multi-team scope must produce an ``IN`` clause with an Array param."""
    from dev_health_ops.api.graphql.resolvers.forecast import _load_backlog

    with patch(
        "dev_health_ops.api.graphql.resolvers.forecast.query_dicts",
        new_callable=AsyncMock,
        return_value=[{"backlog": 84}],
    ) as query:
        backlog = await _load_backlog(
            ctx, team_ids=["team-1", "team-2"], work_scope_id=None
        )

    assert backlog == 84
    call_args = query.await_args
    assert call_args is not None
    sql = call_args.args[1]
    params = call_args.args[2]
    assert "team_id IN {team_ids:Array(String)}" in sql
    assert params["team_ids"] == ["team-1", "team-2"]
    assert "team_id" not in params


@pytest.mark.asyncio
async def test_load_backlog_uses_equality_for_single_team(ctx):
    """Single-team scope must still use ``team_id =``, not ``IN``."""
    from dev_health_ops.api.graphql.resolvers.forecast import _load_backlog

    with patch(
        "dev_health_ops.api.graphql.resolvers.forecast.query_dicts",
        new_callable=AsyncMock,
        return_value=[{"backlog": 40}],
    ) as query:
        backlog = await _load_backlog(ctx, team_ids=["team-a"], work_scope_id=None)

    assert backlog == 40
    call_args = query.await_args
    assert call_args is not None
    sql = call_args.args[1]
    params = call_args.args[2]
    assert "team_id = {team_id:String}" in sql
    assert params["team_id"] == "team-a"
    assert "team_ids" not in params


def _insufficient_result() -> ThroughputForecastResult:
    """Short-history forecast: no estimate, every window flagged insufficient."""
    return ThroughputForecastResult(
        forecast_id="forecast-short",
        computed_at=datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc),
        team_id=None,
        work_scope_id=None,
        backlog_size=20,
        history_weeks=12,
        p50_weeks=None,
        p75_weeks=None,
        p90_weeks=None,
        rolling_windows=tuple(
            RollingWindowThroughput(
                window_weeks=weeks,
                mean_weekly_throughput=0.0,
                samples=(),
                insufficient_history=True,
            )
            for weeks in (4, 8, 12)
        ),
        primary_risk=_risk(RiskKind.NONE),
        wip_congestion=_risk(RiskKind.WIP),
        review_bottleneck=_risk(RiskKind.REVIEW),
        incident_load=_risk(RiskKind.INCIDENT),
        insufficient_history=True,
    )


@pytest.mark.asyncio
async def test_resolver_surfaces_insufficient_history_and_sample_count(ctx):
    """Resolver output must populate insufficientHistory + per-window sampleCount."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=_history(),
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
            return_value=20,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast.forecast_throughput_capacity",
            return_value=_insufficient_result(),
        ),
    ):
        result = await resolve_throughput_forecast(ctx, ThroughputForecastInput())

    assert result is not None
    # Top-level honesty flag surfaced for the UI warning/disabled state.
    assert result.insufficient_history is True
    # No point estimate emitted under short history.
    assert result.p50_weeks is None
    assert result.p75_weeks is None
    assert result.p90_weeks is None
    # Per-window provenance: every window carries sample_count + insufficient flag.
    assert [w.window_weeks for w in result.rolling_windows] == [4, 8, 12]
    assert all(w.sample_count == 0 for w in result.rolling_windows)
    assert all(w.insufficient_history for w in result.rolling_windows)


# ---------------------------------------------------------------------------
# Finding #1 — empty-history GraphQL must not return null
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolver_returns_no_estimate_payload_for_zero_row_history(ctx):
    """Zero-row history must return a structured no-estimate payload, NOT null.

    An empty scope / new team previously short-circuited to ``return None``
    which made the GraphQL field resolve to null. The contract requires a
    structured ThroughputForecast with insufficientHistory=True and per-window
    sampleCount=0 so the UI can render a "no data yet" state instead of
    crashing on a null forecast.
    """
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )
    from dev_health_ops.metrics.compute_capacity import ThroughputHistory

    empty_history = ThroughputHistory([])

    with patch(
        "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
        new_callable=AsyncMock,
        return_value=empty_history,
    ):
        result = await resolve_throughput_forecast(ctx, ThroughputForecastInput())

    # Must NOT be null.
    assert result is not None
    # Top-level insufficient flag must be set.
    assert result.insufficient_history is True
    # No point estimates.
    assert result.p50_weeks is None
    assert result.p75_weeks is None
    assert result.p90_weeks is None
    # All three rolling windows present with sampleCount=0 and insufficient flag.
    assert [w.window_weeks for w in result.rolling_windows] == [4, 8, 12]
    assert all(w.sample_count == 0 for w in result.rolling_windows)
    assert all(w.insufficient_history for w in result.rolling_windows)


# ---------------------------------------------------------------------------
# Finding #2 — empty-history payload must use resolved backlog, not hardcoded 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_history_omitted_backlog_uses_load_backlog(ctx):
    """Empty history + omitted backlog_size must call _load_backlog, not hardcode 0."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )
    from dev_health_ops.metrics.compute_capacity import ThroughputHistory

    empty_history = ThroughputHistory([])

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=empty_history,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_backlog",
            new_callable=AsyncMock,
            return_value=77,
        ) as load_backlog,
    ):
        result = await resolve_throughput_forecast(ctx, ThroughputForecastInput())

    assert result is not None
    # backlog_size must reflect _load_backlog, NOT the hardcoded 0.
    assert result.backlog_size == 77
    assert result.insufficient_history is True
    load_backlog.assert_awaited_once_with(ctx, team_ids=None, work_scope_id=None)


@pytest.mark.asyncio
async def test_empty_history_negative_backlog_rejected(ctx):
    """Empty history + negative explicit backlog_size must raise ValueError."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )
    from dev_health_ops.metrics.compute_capacity import ThroughputHistory

    empty_history = ThroughputHistory([])

    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.forecast._load_throughput_history",
            new_callable=AsyncMock,
            return_value=empty_history,
        ),
        pytest.raises(ValueError, match="backlog_size"),
    ):
        await resolve_throughput_forecast(ctx, ThroughputForecastInput(backlog_size=-1))


# ---------------------------------------------------------------------------
# Finding #3 — history_weeks validation must fire before empty-history branch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_weeks", [0, -1])
@pytest.mark.asyncio
async def test_empty_history_invalid_history_weeks_rejected(ctx, bad_weeks):
    """history_weeks <= 0 must raise ValueError regardless of whether rows exist."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with pytest.raises(ValueError, match="history_weeks must be positive"):
        await resolve_throughput_forecast(
            ctx, ThroughputForecastInput(history_weeks=bad_weeks)
        )


@pytest.mark.asyncio
async def test_rows_exist_invalid_history_weeks_same_error(ctx):
    """history_weeks <= 0 raises the same ValueError when rows exist (parity check)."""
    from dev_health_ops.api.graphql.models.inputs import ThroughputForecastInput
    from dev_health_ops.api.graphql.resolvers.forecast import (
        resolve_throughput_forecast,
    )

    with pytest.raises(ValueError, match="history_weeks must be positive"):
        await resolve_throughput_forecast(ctx, ThroughputForecastInput(history_weeks=0))
