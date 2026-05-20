"""Golden tests for the review-concentration rule.

Covers:
1. Positive trigger   -- high latency + high Gini -> Recommendation returned.
2. Negative (latency just-below threshold) -> None.
3. Negative (Gini just-below threshold) -> None.
4. None inputs        -- missing data returns None.
5. Evidence integrity -- correct tables, fields, team_id, window.
6. Rationale presence -- mentions both threshold values.
7. gini() helper unit tests.
"""

from __future__ import annotations

import pytest

from dev_health_ops.recommendations.rules.review_concentration import (
    RULE_ID,
    SUCCESS_CRITERION,
    evaluate_review_concentration,
    gini,
)
from dev_health_ops.recommendations.thresholds import (
    REVIEW_LATENCY_P75_HOURS,
    REVIEWER_GINI_THRESHOLD,
)

from .conftest import NOW, make_snapshot


# ---------------------------------------------------------------------------
# 1. Positive trigger
# ---------------------------------------------------------------------------


def test_review_concentration_fires_when_both_above_threshold() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=REVIEW_LATENCY_P75_HOURS + 10.0,
        reviewer_gini=REVIEWER_GINI_THRESHOLD + 0.1,
    )
    result = evaluate_review_concentration(snap, NOW)

    assert result is not None
    assert result.rule_id == RULE_ID
    assert result.severity == "warning"
    assert result.success_criterion == SUCCESS_CRITERION
    assert result.team_id == snap.team_id


# ---------------------------------------------------------------------------
# 2. Negative -- latency just below
# ---------------------------------------------------------------------------


def test_review_concentration_does_not_fire_latency_just_below() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=REVIEW_LATENCY_P75_HOURS * 0.99,
        reviewer_gini=REVIEWER_GINI_THRESHOLD + 0.2,
    )
    assert evaluate_review_concentration(snap, NOW) is None


# ---------------------------------------------------------------------------
# 3. Negative -- Gini just below
# ---------------------------------------------------------------------------


def test_review_concentration_does_not_fire_gini_just_below() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=REVIEW_LATENCY_P75_HOURS * 2.0,
        reviewer_gini=REVIEWER_GINI_THRESHOLD * 0.99,
    )
    assert evaluate_review_concentration(snap, NOW) is None


# ---------------------------------------------------------------------------
# 4. None inputs
# ---------------------------------------------------------------------------


def test_review_concentration_returns_none_when_latency_none() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=None,
        reviewer_gini=REVIEWER_GINI_THRESHOLD + 0.1,
    )
    assert evaluate_review_concentration(snap, NOW) is None


def test_review_concentration_returns_none_when_gini_none() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=REVIEW_LATENCY_P75_HOURS + 10.0,
        reviewer_gini=None,
    )
    assert evaluate_review_concentration(snap, NOW) is None


# ---------------------------------------------------------------------------
# 5. Evidence integrity
# ---------------------------------------------------------------------------


def test_review_concentration_evidence_tables_and_fields() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=REVIEW_LATENCY_P75_HOURS + 10.0,
        reviewer_gini=REVIEWER_GINI_THRESHOLD + 0.1,
    )
    result = evaluate_review_concentration(snap, NOW)
    assert result is not None

    tables = {e.metric_table for e in result.evidence}
    fields = {e.field for e in result.evidence}

    assert "repo_metrics_daily" in tables
    assert "review_edge_daily" in tables
    assert "review_latency_p75_hours" in fields
    assert "reviewer_gini" in fields

    for ev in result.evidence:
        assert ev.team_id == snap.team_id
        assert ev.window_start == snap.window_start
        assert ev.window_end == snap.window_end


# ---------------------------------------------------------------------------
# 6. Rationale presence
# ---------------------------------------------------------------------------


def test_review_concentration_rationale_mentions_both_thresholds() -> None:
    snap = make_snapshot(
        review_latency_p75_hours=REVIEW_LATENCY_P75_HOURS + 10.0,
        reviewer_gini=REVIEWER_GINI_THRESHOLD + 0.1,
    )
    result = evaluate_review_concentration(snap, NOW)
    assert result is not None
    assert str(REVIEW_LATENCY_P75_HOURS) in result.rationale
    assert str(REVIEWER_GINI_THRESHOLD) in result.rationale


# ---------------------------------------------------------------------------
# 7. gini() helper unit tests
# ---------------------------------------------------------------------------


def test_gini_perfectly_even_returns_zero() -> None:
    assert gini([2.0, 2.0, 2.0, 2.0]) == pytest.approx(0.0, abs=1e-9)


def test_gini_single_dominant_reviewer_is_high() -> None:
    assert gini([0.0, 0.0, 0.0, 10.0]) > 0.7


def test_gini_empty_returns_zero() -> None:
    assert gini([]) == 0.0


def test_gini_all_zero_returns_zero() -> None:
    assert gini([0.0, 0.0, 0.0]) == 0.0


def test_gini_two_equal_reviewers() -> None:
    assert gini([5.0, 5.0]) == pytest.approx(0.0, abs=1e-9)
