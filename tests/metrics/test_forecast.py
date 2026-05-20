from datetime import date, timedelta

import pytest

from dev_health_ops.metrics.compute_capacity import ThroughputHistory, ThroughputSample
from dev_health_ops.metrics.forecast import (
    RiskKind,
    compute_risk_overlays,
    forecast_throughput_capacity,
    rolling_weekly_throughput,
)


def _history(daily: list[int]) -> ThroughputHistory:
    start = date(2025, 1, 1)
    return ThroughputHistory(
        [
            ThroughputSample(day=start + timedelta(days=index), items_completed=value)
            for index, value in enumerate(daily)
        ]
    )


def test_rolling_forecast_is_deterministic_for_constant_throughput() -> None:
    history = _history([2] * 84)

    result = forecast_throughput_capacity(
        history=history,
        backlog_size=100,
        team_id="team-1",
        work_scope_id="scope-1",
        history_weeks=12,
    )

    assert result.team_id == "team-1"
    assert result.work_scope_id == "scope-1"
    assert result.p50_weeks == 8
    assert result.p75_weeks == 8
    assert result.p90_weeks == 8
    assert [window.window_weeks for window in result.rolling_windows] == [4, 8, 12]
    assert [window.mean_weekly_throughput for window in result.rolling_windows] == [
        14.0,
        14.0,
        14.0,
    ]
    assert not result.insufficient_history
    assert result.primary_risk.kind is RiskKind.NONE


def test_insufficient_history_uses_observed_weekly_rate() -> None:
    history = _history([3] * 14)

    window = rolling_weekly_throughput(history, 4)
    result = forecast_throughput_capacity(
        history=history,
        backlog_size=42,
        history_weeks=4,
    )

    assert window.insufficient_history
    assert window.mean_weekly_throughput == 21.0
    assert result.insufficient_history
    assert result.p50_weeks == 2
    assert result.p75_weeks == 2
    assert result.p90_weeks == 2


def test_zero_throughput_returns_unknown_completion_weeks() -> None:
    result = forecast_throughput_capacity(history=_history([0] * 84), backlog_size=10)

    assert result.p50_weeks is None
    assert result.p75_weeks is None
    assert result.p90_weeks is None


@pytest.mark.parametrize("backlog_size", [-1])
def test_negative_backlog_rejected(backlog_size: int) -> None:
    with pytest.raises(ValueError, match="backlog_size"):
        forecast_throughput_capacity(
            history=_history([1] * 28), backlog_size=backlog_size
        )


def test_primary_risk_prefers_highest_active_normalized_score() -> None:
    primary, wip, review, incident = compute_risk_overlays(
        current_wip=30,
        average_wip=20,
        review_latency_hours=120,
        incident_count=1,
    )

    assert wip.active
    assert review.active
    assert incident.active
    assert primary.kind is RiskKind.REVIEW
    assert primary.label == "Review bottleneck"


def test_primary_risk_reports_no_elevated_risk_when_below_thresholds() -> None:
    primary, wip, review, incident = compute_risk_overlays(
        current_wip=10,
        average_wip=20,
        review_latency_hours=8,
        incident_count=0,
    )

    assert not wip.active
    assert not review.active
    assert not incident.active
    assert primary.kind is RiskKind.NONE
    assert primary.label == "No elevated risk"
