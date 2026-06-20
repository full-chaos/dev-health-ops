from datetime import date, timedelta

import pytest

from dev_health_ops.metrics.compute_capacity import ThroughputHistory, ThroughputSample
from dev_health_ops.metrics.forecast import (
    INCIDENT_LOAD_THRESHOLD,
    REVIEW_BOTTLENECK_THRESHOLD_HOURS,
    WIP_CONGESTION_THRESHOLD,
    RiskKind,
    _assert_monotonic_weeks,
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


def test_insufficient_history_returns_no_estimate() -> None:
    # 14 days is shorter than a single 4-week window. The old math floored the
    # partial week to 1.0 and reported a confident 21.0/week estimate; the
    # honest contract now flags the window as insufficient and emits no
    # samples, so the forecast yields no point estimate (CHAOS-2574).
    history = _history([3] * 14)

    window = rolling_weekly_throughput(history, 4)
    result = forecast_throughput_capacity(
        history=history,
        backlog_size=42,
        history_weeks=4,
    )

    assert window.insufficient_history
    assert window.mean_weekly_throughput == 0.0
    assert window.samples == ()
    assert result.insufficient_history
    assert result.p50_weeks is None
    assert result.p75_weeks is None
    assert result.p90_weeks is None


def test_short_history_windows_not_identical() -> None:
    # One day of five completed items cannot support a 4/8/12 week rolling
    # mean. The old math collapsed every window to one fabricated value
    # (CHAOS-2574); the honest contract flags each window insufficient and
    # emits no point estimate.
    history = _history([5])

    windows = [rolling_weekly_throughput(history, weeks) for weeks in (4, 8, 12)]

    assert all(window.insufficient_history for window in windows)
    assert all(window.samples == () for window in windows)
    assert all(window.mean_weekly_throughput == 0.0 for window in windows)

    result = forecast_throughput_capacity(history=history, backlog_size=20)
    assert result.insufficient_history
    assert result.p50_weeks is None
    assert result.p75_weeks is None
    assert result.p90_weeks is None


def test_forecast_percentiles_use_distribution_when_selected_window_collapses() -> None:
    history = _history([6] * 28 + [2] * 28 + [1] * 28)

    result = forecast_throughput_capacity(
        history=history,
        backlog_size=100,
        history_weeks=12,
    )

    assert result.p50_weeks is not None
    assert result.p75_weeks is not None
    assert result.p90_weeks is not None
    assert result.p50_weeks <= result.p75_weeks <= result.p90_weeks
    assert (result.p50_weeks, result.p75_weeks, result.p90_weeks) == (6, 7, 9)


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
        incident_count=12,
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


def test_risk_threshold_defaults_are_in_planning_units() -> None:
    assert WIP_CONGESTION_THRESHOLD == pytest.approx(1.25)
    assert REVIEW_BOTTLENECK_THRESHOLD_HOURS == pytest.approx(48.0)
    assert INCIDENT_LOAD_THRESHOLD == pytest.approx(10.0)

    _, _, _, incident = compute_risk_overlays(incident_count=9.9)
    assert incident.threshold == pytest.approx(10.0)
    assert not incident.active

    _, _, _, incident = compute_risk_overlays(incident_count=10.0)
    assert incident.active


def test_assert_monotonic_weeks_raises_on_non_monotonic() -> None:
    with pytest.raises(ValueError, match="monotonic"):
        _assert_monotonic_weeks(9, 7, 6)


def test_assert_monotonic_weeks_allows_monotonic() -> None:
    _assert_monotonic_weeks(6, 7, 9)


@pytest.mark.parametrize(
    ("p50", "p75", "p90"),
    [(None, 7, 9), (6, None, 9), (6, 7, None), (None, None, None)],
)
def test_assert_monotonic_weeks_is_null_safe(
    p50: int | None, p75: int | None, p90: int | None
) -> None:
    _assert_monotonic_weeks(p50, p75, p90)


# ---------------------------------------------------------------------------
# Finding #2 — percentile estimate provenance at exact window boundaries
# ---------------------------------------------------------------------------


def test_exactly_4_weeks_history_emits_no_estimate() -> None:
    # 4 weeks = 28 days. rolling_weekly_throughput needs len(throughputs) >= window_days
    # to produce samples. At exactly 28 days the 4w window produces exactly 1 rolling
    # sample (range(0, 28-28+1) = range(0,1)). With MIN_SAMPLES_FOR_ESTIMATE=2 that
    # single sample is insufficient; no shorter window exists, so the forecast must
    # return no estimate.
    history = _history([3] * 28)

    result = forecast_throughput_capacity(
        history=history,
        backlog_size=20,
        history_weeks=4,
    )

    # The 4w window has exactly 1 rolling sample — below the minimum.
    four_week = next(w for w in result.rolling_windows if w.window_weeks == 4)
    assert len(four_week.samples) == 1
    # No shorter window can provide a fallback, so estimates must be None.
    assert result.p50_weeks is None
    assert result.p75_weeks is None
    assert result.p90_weeks is None
    assert result.insufficient_history


def test_exactly_8_weeks_history_emits_no_estimate_for_8w_window() -> None:
    # 8 weeks = 56 days. The 8w window produces exactly 1 rolling sample.
    # The 4w window (28 days) produces 56-28+1 = 29 samples — above the minimum.
    # _select_percentile_distribution with history_weeks=8 selects the 8w window
    # (1 sample < MIN_SAMPLES_FOR_ESTIMATE), then falls back to the 4w window.
    # With history_weeks=12 (default), the 12w window is selected (0 samples,
    # insufficient), falls back to 8w (1 sample, insufficient), then 4w (29 samples).
    history = _history([3] * 56)

    result_8w = forecast_throughput_capacity(
        history=history,
        backlog_size=20,
        history_weeks=8,
    )
    result_12w = forecast_throughput_capacity(
        history=history,
        backlog_size=20,
        history_weeks=12,
    )

    eight_week = next(w for w in result_8w.rolling_windows if w.window_weeks == 8)
    assert len(eight_week.samples) == 1
    # history_weeks=8: 8w window has 1 sample (below min); falls back to 4w (29 samples).
    # The 4w fallback has enough samples so estimates are produced.
    assert result_8w.p50_weeks is not None
    assert result_8w.p75_weeks is not None
    assert result_8w.p90_weeks is not None
    # history_weeks=12: 12w window has 0 samples; 8w has 1 (below min); 4w has 29.
    # Falls back to 4w — estimates produced.
    assert result_12w.p50_weeks is not None


def test_exactly_12_weeks_history_emits_estimate_via_fallback() -> None:
    # 12 weeks = 84 days. The 12w window produces exactly 1 rolling sample.
    # The 8w window (56 days) produces 84-56+1 = 29 samples.
    # The 4w window (28 days) produces 84-28+1 = 57 samples.
    # _select_percentile_distribution with history_weeks=12 selects the 12w window
    # (1 sample < MIN_SAMPLES_FOR_ESTIMATE), then falls back to the longest shorter
    # window that meets the minimum — the 8w window (29 samples).
    history = _history([3] * 84)

    result = forecast_throughput_capacity(
        history=history,
        backlog_size=20,
        history_weeks=12,
    )

    twelve_week = next(w for w in result.rolling_windows if w.window_weeks == 12)
    assert len(twelve_week.samples) == 1
    # 12w has 1 sample (below min); falls back to 8w (29 samples) — estimates produced.
    assert result.p50_weeks is not None
    assert result.p75_weeks is not None
    assert result.p90_weeks is not None
    # The 12w window itself is flagged insufficient (only 1 sample).
    assert twelve_week.insufficient_history is False  # 84 days >= 84 days threshold
    # But the overall result is NOT flagged insufficient because 8w/4w have data.
    # (insufficient_history = any(window.insufficient_history for window in windows))
    # 4w and 8w windows have sufficient history at 84 days.
    eight_week = next(w for w in result.rolling_windows if w.window_weeks == 8)
    four_week = next(w for w in result.rolling_windows if w.window_weeks == 4)
    assert eight_week.insufficient_history is False
    assert four_week.insufficient_history is False
