"""Golden tests for the compounding-risk rule.

Covers:
1. Positive trigger   -- complexity delta + churn overlap both above -> Recommendation.
2. Negative (complexity just-below threshold) -> None.
3. Negative (churn overlap just-below threshold) -> None.
4. None inputs        -- either field None returns None.
5. Both zero          -- returns None.
6. Evidence integrity -- correct tables, fields, team_id, window.
7. Rationale presence -- mentions both threshold values.
"""

from __future__ import annotations

from dev_health_ops.recommendations.rules.compounding_risk import (
    RULE_ID,
    SUCCESS_CRITERION,
    evaluate_compounding_risk,
)
from dev_health_ops.recommendations.thresholds import (
    COMPLEXITY_DELTA_THRESHOLD,
    HOTSPOT_CHURN_OVERLAP_THRESHOLD,
)

from .conftest import NOW, make_snapshot

# ---------------------------------------------------------------------------
# 1. Positive trigger
# ---------------------------------------------------------------------------


def test_compounding_risk_fires_when_both_above_threshold() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD + 0.1,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD + 0.1,
    )
    result = evaluate_compounding_risk(snap, NOW)

    assert result is not None
    assert result.rule_id == RULE_ID
    assert result.severity == "warning"
    assert result.success_criterion == SUCCESS_CRITERION
    assert result.team_id == snap.team_id


# ---------------------------------------------------------------------------
# 2. Negative -- complexity just below
# ---------------------------------------------------------------------------


def test_compounding_risk_does_not_fire_complexity_just_below() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD * 0.99,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD + 0.2,
    )
    assert evaluate_compounding_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 3. Negative -- churn overlap just below
# ---------------------------------------------------------------------------


def test_compounding_risk_does_not_fire_overlap_just_below() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD + 0.2,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD * 0.99,
    )
    assert evaluate_compounding_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 4. None inputs
# ---------------------------------------------------------------------------


def test_compounding_risk_returns_none_when_complexity_delta_none() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=None,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD + 0.1,
    )
    assert evaluate_compounding_risk(snap, NOW) is None


def test_compounding_risk_returns_none_when_churn_overlap_none() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD + 0.1,
        hotspot_churn_overlap=None,
    )
    assert evaluate_compounding_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 5. Both zero
# ---------------------------------------------------------------------------


def test_compounding_risk_does_not_fire_when_both_zero() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=0.0,
        hotspot_churn_overlap=0.0,
    )
    assert evaluate_compounding_risk(snap, NOW) is None


# ---------------------------------------------------------------------------
# 6. Evidence integrity
# ---------------------------------------------------------------------------


def test_compounding_risk_evidence_tables_and_fields() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD + 0.1,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD + 0.1,
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is not None

    tables = {e.metric_table for e in result.evidence}
    fields = {e.field for e in result.evidence}

    assert "file_complexity_snapshots" in tables
    assert "file_metrics_daily" in tables
    assert "hotspot_complexity_delta" in fields
    assert "hotspot_churn_overlap" in fields

    for ev in result.evidence:
        assert ev.team_id == snap.team_id
        assert ev.window_start == snap.window_start
        assert ev.window_end == snap.window_end


# ---------------------------------------------------------------------------
# 7. Rationale presence
# ---------------------------------------------------------------------------


def test_compounding_risk_rationale_mentions_both_thresholds() -> None:
    snap = make_snapshot(
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD + 0.1,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD + 0.1,
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is not None
    assert str(COMPLEXITY_DELTA_THRESHOLD) in result.rationale
    assert str(HOTSPOT_CHURN_OVERLAP_THRESHOLD) in result.rationale
