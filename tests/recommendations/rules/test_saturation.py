"""Golden tests for the saturation rule.

Covers:
1. Positive trigger   -- WIP rising + throughput flat -> Recommendation returned.
2. Negative (WIP just-below threshold) -> None.
3. Negative (throughput rising despite high WIP) -> None.
4. Evidence integrity -- correct tables, fields, team_id, window.
5. Rationale presence -- mentions threshold name and observed value.
6. Helper unit tests  -- _linear_slope edge cases.
"""

from __future__ import annotations

import pytest

from dev_health_ops.recommendations.rules.saturation import (
    RULE_ID,
    SUCCESS_CRITERION,
    _linear_slope,
    evaluate_saturation,
)
from dev_health_ops.recommendations.thresholds import (
    THROUGHPUT_FLAT_DELTA_THRESHOLD,
    WIP_RISING_SLOPE_THRESHOLD,
)

from .conftest import NOW, make_snapshot


def _rising_wip(slope: float, n: int = 7) -> list[float]:
    return [5.0 + slope * i for i in range(n)]


# ---------------------------------------------------------------------------
# 1. Positive trigger
# ---------------------------------------------------------------------------


def test_saturation_fires_when_wip_rising_and_throughput_flat() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 2),
        throughput_by_cycle=[10.0, 10.0],
    )
    result = evaluate_saturation(snap, NOW)

    assert result is not None
    assert result.rule_id == RULE_ID
    assert result.team_id == snap.team_id
    assert result.org_id == snap.org_id
    assert result.window_start == snap.window_start
    assert result.window_end == snap.window_end
    assert result.severity == "warning"
    assert result.success_criterion == SUCCESS_CRITERION


def test_saturation_fires_when_throughput_declining() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 2),
        throughput_by_cycle=[10.0, 8.0],  # delta = -2 <= threshold
    )
    assert evaluate_saturation(snap, NOW) is not None


# ---------------------------------------------------------------------------
# 2. Negative -- WIP slope just below threshold
# ---------------------------------------------------------------------------


def test_saturation_does_not_fire_when_wip_slope_just_below() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 0.99),
        throughput_by_cycle=[10.0, 8.0],
    )
    assert evaluate_saturation(snap, NOW) is None


# ---------------------------------------------------------------------------
# 3. Negative -- throughput rising (no saturation)
# ---------------------------------------------------------------------------


def test_saturation_does_not_fire_when_throughput_rising() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 3),
        throughput_by_cycle=[8.0, 14.0],  # delta = +6 > threshold
    )
    assert evaluate_saturation(snap, NOW) is None


# ---------------------------------------------------------------------------
# 4. Evidence integrity
# ---------------------------------------------------------------------------


def test_saturation_evidence_tables_and_fields() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 2),
        throughput_by_cycle=[10.0, 10.0],
    )
    result = evaluate_saturation(snap, NOW)
    assert result is not None

    tables = {e.metric_table for e in result.evidence}
    fields = {e.field for e in result.evidence}

    assert "work_item_metrics_daily" in tables
    assert "wip_count_end_of_day" in fields
    assert "items_completed_delta" in fields

    for ev in result.evidence:
        assert ev.team_id == snap.team_id
        assert ev.window_start == snap.window_start
        assert ev.window_end == snap.window_end


# ---------------------------------------------------------------------------
# 5. Rationale presence
# ---------------------------------------------------------------------------


def test_saturation_rationale_mentions_threshold_and_observed() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 2),
        throughput_by_cycle=[10.0, 10.0],
    )
    result = evaluate_saturation(snap, NOW)
    assert result is not None
    assert str(WIP_RISING_SLOPE_THRESHOLD) in result.rationale
    assert str(THROUGHPUT_FLAT_DELTA_THRESHOLD) in result.rationale


# ---------------------------------------------------------------------------
# 6. Not enough data -> None
# ---------------------------------------------------------------------------


def test_saturation_returns_none_with_one_wip_point() -> None:
    snap = make_snapshot(
        wip_by_day=[10.0],
        throughput_by_cycle=[5.0, 3.0],
    )
    assert evaluate_saturation(snap, NOW) is None


def test_saturation_returns_none_with_one_throughput_point() -> None:
    snap = make_snapshot(
        wip_by_day=_rising_wip(WIP_RISING_SLOPE_THRESHOLD * 2),
        throughput_by_cycle=[10.0],
    )
    assert evaluate_saturation(snap, NOW) is None


# ---------------------------------------------------------------------------
# Helper unit tests -- _linear_slope
# ---------------------------------------------------------------------------


def test_linear_slope_flat_is_zero() -> None:
    assert _linear_slope([5.0, 5.0, 5.0]) == pytest.approx(0.0)


def test_linear_slope_single_value_is_zero() -> None:
    assert _linear_slope([7.0]) == pytest.approx(0.0)


def test_linear_slope_ascending() -> None:
    assert _linear_slope([0.0, 1.0, 2.0]) == pytest.approx(1.0)


def test_linear_slope_descending() -> None:
    assert _linear_slope([2.0, 1.0, 0.0]) == pytest.approx(-1.0)
