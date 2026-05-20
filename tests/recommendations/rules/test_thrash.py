"""Golden tests for the thrash rule.

Covers:
1. Positive trigger   -- high churn + flat throughput -> Recommendation.
2. Positive trigger   -- high churn + declining throughput -> Recommendation.
3. Negative (churn just-below threshold) -> None.
4. Negative (churn high but throughput rising) -> None.
5. None inputs        -- rework_churn_ratio=None returns None.
6. Not enough data    -- single cycle point returns None.
7. Evidence integrity -- correct tables, fields, team_id, window.
8. Rationale presence -- mentions threshold and observed value.
"""

from __future__ import annotations

from dev_health_ops.recommendations.rules.thrash import (
    RULE_ID,
    SUCCESS_CRITERION,
    evaluate_thrash,
)
from dev_health_ops.recommendations.thresholds import (
    CHURN_RATIO_THRESHOLD,
    THROUGHPUT_LOW_DELTA_THRESHOLD,
)

from .conftest import NOW, make_snapshot

# ---------------------------------------------------------------------------
# 1 & 2. Positive triggers
# ---------------------------------------------------------------------------


def test_thrash_fires_when_churn_high_and_throughput_flat() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD + 0.1,
        throughput_by_cycle=[12.0, 12.0],
    )
    result = evaluate_thrash(snap, NOW)

    assert result is not None
    assert result.rule_id == RULE_ID
    assert result.severity == "warning"
    assert result.success_criterion == SUCCESS_CRITERION


def test_thrash_fires_when_churn_high_and_throughput_declining() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD + 0.05,
        throughput_by_cycle=[10.0, 8.0],
    )
    assert evaluate_thrash(snap, NOW) is not None


# ---------------------------------------------------------------------------
# 3. Negative -- churn just below
# ---------------------------------------------------------------------------


def test_thrash_does_not_fire_when_churn_just_below() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD * 0.99,
        throughput_by_cycle=[10.0, 9.0],
    )
    assert evaluate_thrash(snap, NOW) is None


# ---------------------------------------------------------------------------
# 4. Negative -- throughput rising
# ---------------------------------------------------------------------------


def test_thrash_does_not_fire_when_throughput_rising() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD + 0.2,
        throughput_by_cycle=[8.0, 14.0],
    )
    assert evaluate_thrash(snap, NOW) is None


# ---------------------------------------------------------------------------
# 5. None input
# ---------------------------------------------------------------------------


def test_thrash_returns_none_when_rework_ratio_none() -> None:
    snap = make_snapshot(
        rework_churn_ratio=None,
        throughput_by_cycle=[5.0, 4.0],
    )
    assert evaluate_thrash(snap, NOW) is None


# ---------------------------------------------------------------------------
# 6. Not enough throughput data
# ---------------------------------------------------------------------------


def test_thrash_returns_none_with_single_cycle_point() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD + 0.2,
        throughput_by_cycle=[10.0],
    )
    assert evaluate_thrash(snap, NOW) is None


# ---------------------------------------------------------------------------
# 7. Evidence integrity
# ---------------------------------------------------------------------------


def test_thrash_evidence_tables_and_fields() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD + 0.1,
        throughput_by_cycle=[12.0, 11.0],
    )
    result = evaluate_thrash(snap, NOW)
    assert result is not None

    tables = {e.metric_table for e in result.evidence}
    fields = {e.field for e in result.evidence}

    assert "repo_metrics_daily" in tables
    assert "work_item_metrics_daily" in tables
    assert "rework_churn_ratio_30d" in fields
    assert "items_completed_delta" in fields

    for ev in result.evidence:
        assert ev.team_id == snap.team_id
        assert ev.window_start == snap.window_start
        assert ev.window_end == snap.window_end


# ---------------------------------------------------------------------------
# 8. Rationale presence
# ---------------------------------------------------------------------------


def test_thrash_rationale_mentions_threshold_and_observed() -> None:
    snap = make_snapshot(
        rework_churn_ratio=CHURN_RATIO_THRESHOLD + 0.1,
        throughput_by_cycle=[12.0, 12.0],
    )
    result = evaluate_thrash(snap, NOW)
    assert result is not None
    assert str(CHURN_RATIO_THRESHOLD) in result.rationale
    assert str(THROUGHPUT_LOW_DELTA_THRESHOLD) in result.rationale
