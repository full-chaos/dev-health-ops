"""Unit tests for dev_health_ops.metrics.opportunities.scoring.

Tests cover:
- clamp boundary and interior values
- score_ratio at/above/below threshold, monotonicity
- score_delta at/above/below threshold, monotonicity
- stable_opportunity_id determinism, length, uniqueness
"""

from __future__ import annotations

import pytest

from dev_health_ops.metrics.opportunities.scoring import (
    clamp,
    score_delta,
    score_ratio,
    stable_opportunity_id,
)


class TestClamp:
    def test_below_lo_returns_lo(self) -> None:
        assert clamp(-1.0) == 0.0

    def test_above_hi_returns_hi(self) -> None:
        assert clamp(2.0) == 1.0

    def test_at_lo_boundary(self) -> None:
        assert clamp(0.0) == 0.0

    def test_at_hi_boundary(self) -> None:
        assert clamp(1.0) == 1.0

    def test_interior_unchanged(self) -> None:
        assert clamp(0.5) == pytest.approx(0.5)

    def test_custom_bounds(self) -> None:
        assert clamp(5.0, lo=2.0, hi=4.0) == pytest.approx(4.0)
        assert clamp(1.0, lo=2.0, hi=4.0) == pytest.approx(2.0)
        assert clamp(3.0, lo=2.0, hi=4.0) == pytest.approx(3.0)


class TestScoreRatio:
    def test_at_threshold_returns_half(self) -> None:
        assert score_ratio(1.0, 1.0) == pytest.approx(0.5)

    def test_double_threshold_returns_one(self) -> None:
        assert score_ratio(2.0, 1.0) == pytest.approx(1.0)

    def test_below_threshold_returns_below_half(self) -> None:
        assert score_ratio(0.5, 1.0) < 0.5

    def test_zero_threshold_returns_zero(self) -> None:
        assert score_ratio(10.0, 0.0) == 0.0

    def test_negative_threshold_returns_zero(self) -> None:
        assert score_ratio(10.0, -1.0) == 0.0

    def test_result_clamped_to_unit_interval(self) -> None:
        result = score_ratio(1000.0, 1.0)
        assert 0.0 <= result <= 1.0

    def test_monotone_increasing(self) -> None:
        """Higher ratio → higher score."""
        threshold = 24.0
        values = [12.0, 24.0, 36.0, 48.0, 96.0]
        scores = [score_ratio(v, threshold) for v in values]
        assert scores == sorted(scores), "score_ratio must be monotone non-decreasing"


class TestScoreDelta:
    def test_at_threshold_returns_half(self) -> None:
        assert score_delta(0.10, 0.10) == pytest.approx(0.5)

    def test_double_threshold_returns_one(self) -> None:
        assert score_delta(0.20, 0.10) == pytest.approx(1.0)

    def test_below_threshold_returns_below_half(self) -> None:
        assert score_delta(0.05, 0.10) < 0.5

    def test_zero_threshold_returns_zero(self) -> None:
        assert score_delta(0.5, 0.0) == 0.0

    def test_negative_threshold_returns_zero(self) -> None:
        assert score_delta(0.5, -0.1) == 0.0

    def test_result_clamped_to_unit_interval(self) -> None:
        result = score_delta(999.0, 0.1)
        assert 0.0 <= result <= 1.0

    def test_monotone_increasing(self) -> None:
        threshold = 0.10
        values = [0.0, 0.05, 0.10, 0.15, 0.30]
        scores = [score_delta(v, threshold) for v in values]
        assert scores == sorted(scores), "score_delta must be monotone non-decreasing"


class TestStableOpportunityId:
    def test_deterministic_same_inputs(self) -> None:
        id1 = stable_opportunity_id("HIGH_REVIEW_LATENCY", "repo-abc", None)
        id2 = stable_opportunity_id("HIGH_REVIEW_LATENCY", "repo-abc", None)
        assert id1 == id2

    def test_length_is_24(self) -> None:
        result = stable_opportunity_id("HIGH_REWORK", "team-x", "secondary")
        assert len(result) == 24

    def test_hexadecimal_only(self) -> None:
        result = stable_opportunity_id("HIGH_WIP", "team-y", None)
        assert all(c in "0123456789abcdef" for c in result)

    def test_unique_for_different_kinds(self) -> None:
        id1 = stable_opportunity_id("HIGH_WIP", "repo-1", None)
        id2 = stable_opportunity_id("HIGH_REWORK", "repo-1", None)
        assert id1 != id2

    def test_unique_for_different_entities(self) -> None:
        id1 = stable_opportunity_id("SLOW_CYCLE_TIME", "repo-1", None)
        id2 = stable_opportunity_id("SLOW_CYCLE_TIME", "repo-2", None)
        assert id1 != id2

    def test_unique_for_different_secondary(self) -> None:
        id1 = stable_opportunity_id("HIGH_CHURN", "repo-1", "a")
        id2 = stable_opportunity_id("HIGH_CHURN", "repo-1", "b")
        assert id1 != id2

    def test_none_and_empty_secondary_equivalent(self) -> None:
        """None secondary_id is treated the same as empty string."""
        id1 = stable_opportunity_id("HIGH_CHURN", "repo-1", None)
        id2 = stable_opportunity_id("HIGH_CHURN", "repo-1", "")
        assert id1 == id2

    def test_uses_enum_value(self) -> None:
        """Objects with a .value attribute use that value, not repr()."""
        from dev_health_ops.metrics.opportunities.models import ImproveOpportunityKind

        id_via_enum = stable_opportunity_id(
            ImproveOpportunityKind.HIGH_REVIEW_LATENCY, "repo-1", None
        )
        id_via_string = stable_opportunity_id("high_review_latency", "repo-1", None)
        assert id_via_enum == id_via_string
