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


# ---------------------------------------------------------------------------
# 8. Persisted Compounding Risk path (CHAOS-1641)
# ---------------------------------------------------------------------------


def test_persisted_high_severity_fires_with_critical_severity() -> None:
    """When the snapshot carries persisted Compounding Risk = high, the rule
    fires with severity='critical' and cites compounding_risk_daily."""
    snap = make_snapshot(
        compounding_risk_score=0.75,
        compounding_risk_severity="high",
        # Legacy proxy explicitly below threshold to prove preference.
        hotspot_complexity_delta=0.0,
        hotspot_churn_overlap=0.0,
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is not None
    assert result.severity == "critical"
    assert result.rule_id == RULE_ID
    assert result.success_criterion == SUCCESS_CRITERION
    # Evidence cites the new table.
    assert any(
        ev.metric_table == "compounding_risk_daily" for ev in result.evidence
    )


def test_persisted_elevated_severity_fires_with_warning_severity() -> None:
    snap = make_snapshot(
        compounding_risk_score=0.50,
        compounding_risk_severity="elevated",
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is not None
    assert result.severity == "warning"


def test_persisted_low_severity_does_not_fire_alone() -> None:
    snap = make_snapshot(
        compounding_risk_score=0.20,
        compounding_risk_severity="low",
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is None


def test_persisted_unknown_severity_falls_through_to_legacy_path() -> None:
    """When the persisted score is missing/unknown, the legacy hotspot proxy
    still drives the decision — ensures back-compat during backfill warmup."""
    snap = make_snapshot(
        compounding_risk_score=None,
        compounding_risk_severity=None,
        hotspot_complexity_delta=COMPLEXITY_DELTA_THRESHOLD + 0.05,
        hotspot_churn_overlap=HOTSPOT_CHURN_OVERLAP_THRESHOLD + 0.05,
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is not None
    # Legacy path uses the file_complexity_snapshots evidence trail.
    assert any(
        ev.metric_table == "file_complexity_snapshots" for ev in result.evidence
    )


def test_persisted_score_takes_precedence_over_legacy_proxy() -> None:
    """Even if the legacy proxy would NOT fire, a persisted high score fires."""
    snap = make_snapshot(
        compounding_risk_score=0.80,
        compounding_risk_severity="high",
        hotspot_complexity_delta=0.0,  # below threshold
        hotspot_churn_overlap=0.0,     # below threshold
    )
    result = evaluate_compounding_risk(snap, NOW)
    assert result is not None
    assert result.severity == "critical"
    assert "compounding_risk_daily" in {
        ev.metric_table for ev in result.evidence
    }
