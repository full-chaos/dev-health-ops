"""Golden tests for the sustainability-risk rule.

Covers:
1. Positive trigger   -- high after-hours + rising cycle time -> Recommendation.
2. Negative (after-hours just-below threshold) -> None.
3. Negative (cycle time flat despite high after-hours) -> None.
4. None inputs        -- after_hours_ratio=None returns None.
5. Not enough data    -- single cycle-time point returns None.
6. Evidence integrity -- correct tables, fields, team_id, window.
7. Rationale presence -- mentions both threshold values.
"""

from __future__ import annotations

import pytest

from dev_health_ops.recommendations.rules.sustainability_risk import (
    RULE_ID,
    SUCCESS_CRITERION,
    _linear_slope,
    evaluate_sustainability_risk,
)
from dev_health_ops.recommendations.thresholds import (
    AFTER_HOURS_RATIO_THRESHOLD,
    CYCLE_TIME_RISING_SLOPE_THRESHOLD,
)

from .conftest import NOW, make_snapshot


def _rising_cycle_times(slope: float, n: int = 7, base: float = 24.0) -> list[float]:
    return [base + slope * i for i in range(n)]


# ---------------------------------------------------------------------------
# 1. Positive trigger
# ---------------------------------------------------------------------------


def test_sustainability_risk_fires_when_both_conditions_met() -> None:
    snap = make_snapshot(
        after_hours_ratio=AFTER_HOURS_RATIO_THRESHOLD + 0.05,
        cycle_time_by_day=_rising_cycle_times(CYCLE_TIME_RISING_SLOPE_THRESHOLD * 2),
    )
    result = evaluate_sustainability_risk(snap, NOW)

    assert result is not None
    assert result.rule_id == RULE_ID
    assert result.severity == "warning"
    assert result.success_criterion == SUCCESS_CRITERION
    assert result.team_id == snap.team_id


# ---------------------------------------------------------------------------
# 2. Negative -- after-hours just below
# ---------------------------------------------------------------------------


def test_sustainability_risk_does_not_fire_when_after_hours_just_below() -> None:
    snap = make_snapshot(
        after_hours_ratio=AFTER_HOURS_RATIO_THRESHOLD * 0.99,
        cycle_time_by_day=_rising_cycle_times(CYCLE_TIME_RISING_SLOPE_THRESHOLD * 3),
    )
    assert evaluate_sustainability_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 3. Negative -- cycle time flat
# ---------------------------------------------------------------------------


def test_sustainability_risk_does_not_fire_when_cycle_time_flat() -> None:
    snap = make_snapshot(
        after_hours_ratio=AFTER_HOURS_RATIO_THRESHOLD + 0.1,
        cycle_time_by_day=[24.0] * 7,
    )
    assert evaluate_sustainability_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 4. None input
# ---------------------------------------------------------------------------


def test_sustainability_risk_returns_none_when_after_hours_none() -> None:
    snap = make_snapshot(
        after_hours_ratio=None,
        cycle_time_by_day=_rising_cycle_times(CYCLE_TIME_RISING_SLOPE_THRESHOLD * 2),
    )
    assert evaluate_sustainability_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 5. Not enough cycle-time data
# ---------------------------------------------------------------------------


def test_sustainability_risk_returns_none_with_single_day() -> None:
    snap = make_snapshot(
        after_hours_ratio=AFTER_HOURS_RATIO_THRESHOLD + 0.1,
        cycle_time_by_day=[48.0],
    )
    assert evaluate_sustainability_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 6. Evidence integrity
# ---------------------------------------------------------------------------


def test_sustainability_risk_evidence_tables_and_fields() -> None:
    snap = make_snapshot(
        after_hours_ratio=AFTER_HOURS_RATIO_THRESHOLD + 0.05,
        cycle_time_by_day=_rising_cycle_times(CYCLE_TIME_RISING_SLOPE_THRESHOLD * 2),
    )
    result = evaluate_sustainability_risk(snap, NOW)
    assert result is not None

    tables = {e.metric_table for e in result.evidence}
    fields = {e.field for e in result.evidence}

    assert "team_metrics_daily" in tables
    assert "work_item_metrics_daily" in tables
    assert "after_hours_commit_ratio" in fields
    assert "cycle_time_p50_hours_slope" in fields

    for ev in result.evidence:
        assert ev.team_id == snap.team_id
        assert ev.window_start == snap.window_start
        assert ev.window_end == snap.window_end


# ---------------------------------------------------------------------------
# 7. Rationale presence
# ---------------------------------------------------------------------------


def test_sustainability_risk_rationale_mentions_both_thresholds() -> None:
    snap = make_snapshot(
        after_hours_ratio=AFTER_HOURS_RATIO_THRESHOLD + 0.05,
        cycle_time_by_day=_rising_cycle_times(CYCLE_TIME_RISING_SLOPE_THRESHOLD * 2),
    )
    result = evaluate_sustainability_risk(snap, NOW)
    assert result is not None
    assert str(AFTER_HOURS_RATIO_THRESHOLD) in result.rationale
    assert str(CYCLE_TIME_RISING_SLOPE_THRESHOLD) in result.rationale
