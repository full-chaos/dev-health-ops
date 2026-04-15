from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from dev_health_ops.metrics.confidence import (
    COVERAGE_SUPPRESS_THRESHOLD,
    COVERAGE_WARN_THRESHOLD,
    DEFAULT_MIN_SAMPLE,
    PROVENANCE_CONFIDENCE,
    DisplayGate,
    Provenance,
    classify_display_gate,
    compute_cohort_contamination,
    compute_impact_confidence,
    compute_link_attribution_confidence,
    provenance_base_confidence,
)


@pytest.mark.parametrize("provenance", ["native", "explicit_text", "heuristic"])
def test_confidence_fixture_covers_prd_bands(
    provenance: str, confidence_case_map: dict[str, dict[str, Any]]
) -> None:
    band = confidence_case_map[provenance]
    assert band["expected_min"] <= band["expected_max"]


@pytest.mark.parametrize(
    ("provenance", "expected_range"),
    [
        ("native", (1.0, 1.0)),
        ("explicit_text", (0.8, 0.9)),
        ("heuristic", (0.3, 0.3)),
    ],
)
def test_confidence_scoring_matches_prd_ranges(
    provenance: str,
    expected_range: tuple[float, float],
    confidence_case_map: dict[str, dict[str, Any]],
) -> None:
    case = confidence_case_map[provenance]
    assert (case["expected_min"], case["expected_max"]) == expected_range


class TestProvenanceBaseConfidence:
    def test_native_returns_1(self) -> None:
        assert provenance_base_confidence(Provenance.native) == 1.0

    def test_explicit_text_returns_085(self) -> None:
        assert provenance_base_confidence(Provenance.explicit_text) == 0.85

    def test_heuristic_returns_03(self) -> None:
        assert provenance_base_confidence(Provenance.heuristic) == 0.3

    def test_string_input_native(self) -> None:
        assert provenance_base_confidence("native") == 1.0

    def test_string_input_explicit_text(self) -> None:
        assert provenance_base_confidence("explicit_text") == 0.85

    def test_unknown_string_falls_back_to_heuristic(self) -> None:
        assert provenance_base_confidence("unknown_provenance") == 0.3


class TestComputeImpactConfidence:
    def test_perfect_inputs_native(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(1.0)

    def test_perfect_inputs_heuristic_base(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=0.3,
        )
        assert score == pytest.approx(0.3)

    def test_coverage_below_threshold_applies_quadratic_penalty(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=0.5,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        # 0.5 < 0.70 → coverage_factor = 0.5² = 0.25
        assert score == pytest.approx(0.25)

    def test_coverage_above_threshold_linear(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=0.8,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(0.8)

    def test_coverage_at_threshold_boundary(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=0.70,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(0.70)

    def test_sample_size_below_minimum(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=15,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
            min_sample=30,
        )
        assert score == pytest.approx(0.5)

    def test_sample_size_at_minimum(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=30,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
            min_sample=30,
        )
        assert score == pytest.approx(1.0)

    def test_sample_size_above_minimum_capped(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=1000,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
            min_sample=30,
        )
        assert score == pytest.approx(1.0)

    def test_one_concurrent_deploy(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=100,
            concurrent_deploy_count=1,
            release_ref_confidence=1.0,
        )
        # 1 / (1 + 1) = 0.5
        assert score == pytest.approx(0.5)

    def test_many_concurrent_deploys(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=100,
            concurrent_deploy_count=4,
            release_ref_confidence=1.0,
        )
        # 1 / (1 + 4) = 0.2
        assert score == pytest.approx(0.2)

    def test_all_factors_combined(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=0.8,
            sample_size=15,
            concurrent_deploy_count=1,
            release_ref_confidence=0.85,
            min_sample=30,
        )
        # base=0.85, cov=0.8 (above threshold), sample=15/30=0.5, confounder=0.5
        expected = 0.85 * 0.8 * 0.5 * 0.5
        assert score == pytest.approx(expected)

    def test_zero_coverage(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=0.0,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(0.0)

    def test_zero_samples(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=0,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(0.0)

    def test_negative_coverage_clamped(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=-0.5,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(0.0)

    def test_coverage_above_one_clamped(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.5,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=1.0,
        )
        assert score == pytest.approx(1.0)

    def test_release_ref_confidence_clamped(self) -> None:
        score = compute_impact_confidence(
            coverage_ratio=1.0,
            sample_size=100,
            concurrent_deploy_count=0,
            release_ref_confidence=2.0,
        )
        assert score == pytest.approx(1.0)


class TestComputeCohortContamination:
    _window_start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    _window_end = datetime(2026, 3, 8, tzinfo=timezone.utc)

    def test_no_concurrent_flags(self) -> None:
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=[],
        )
        assert result == 0.0

    def test_one_concurrent_flag(self) -> None:
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=["dark_mode"],
        )
        assert result == pytest.approx(0.5)

    def test_two_concurrent_flags(self) -> None:
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=["dark_mode", "new_pricing"],
        )
        # 1 - 1/(1+2) = 2/3
        assert result == pytest.approx(2.0 / 3.0)

    def test_three_concurrent_flags(self) -> None:
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=["a", "b", "c"],
        )
        assert result == pytest.approx(0.75)

    def test_self_reference_excluded(self) -> None:
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=["checkout_v2"],
        )
        assert result == 0.0

    def test_self_reference_with_others(self) -> None:
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=["checkout_v2", "dark_mode"],
        )
        assert result == pytest.approx(0.5)

    def test_many_concurrent_flags_approaches_one(self) -> None:
        flags = [f"flag_{i}" for i in range(100)]
        result = compute_cohort_contamination(
            flag_key="checkout_v2",
            environment="production",
            window_start=self._window_start,
            window_end=self._window_end,
            concurrent_flags=flags,
        )
        assert result > 0.99
        assert result < 1.0


class TestClassifyDisplayGate:
    def test_show_at_threshold(self) -> None:
        assert classify_display_gate(0.70, min_sample_met=True) == "show"

    def test_show_above_threshold(self) -> None:
        assert classify_display_gate(0.95, min_sample_met=True) == "show"

    def test_warn_at_lower_threshold(self) -> None:
        assert classify_display_gate(0.50, min_sample_met=True) == "warn"

    def test_warn_between_thresholds(self) -> None:
        assert classify_display_gate(0.60, min_sample_met=True) == "warn"

    def test_suppress_below_lower_threshold(self) -> None:
        assert classify_display_gate(0.49, min_sample_met=True) == "suppress"

    def test_suppress_zero_coverage(self) -> None:
        assert classify_display_gate(0.0, min_sample_met=True) == "suppress"

    def test_suppress_when_min_sample_not_met(self) -> None:
        assert classify_display_gate(0.90, min_sample_met=False) == "suppress"

    def test_suppress_low_coverage_and_no_sample(self) -> None:
        assert classify_display_gate(0.30, min_sample_met=False) == "suppress"

    @pytest.mark.parametrize(
        ("coverage", "min_sample_met", "expected"),
        [
            (0.70, True, "show"),
            (0.50, True, "warn"),
            (0.49, True, "suppress"),
            (0.70, False, "suppress"),
        ],
    )
    def test_boundary_matrix(
        self,
        coverage: float,
        min_sample_met: bool,
        expected: DisplayGate,
    ) -> None:
        assert classify_display_gate(coverage, min_sample_met) == expected


class TestComputeLinkAttributionConfidence:
    def test_native_always_1(self) -> None:
        assert compute_link_attribution_confidence(Provenance.native) == 1.0

    def test_native_ignores_text_match_count(self) -> None:
        assert (
            compute_link_attribution_confidence(Provenance.native, text_match_count=5)
            == 1.0
        )

    def test_heuristic_always_03(self) -> None:
        assert compute_link_attribution_confidence(
            Provenance.heuristic
        ) == pytest.approx(0.3)

    def test_explicit_text_one_match(self) -> None:
        score = compute_link_attribution_confidence(
            Provenance.explicit_text, text_match_count=1
        )
        assert 0.80 <= score <= 0.90

    def test_explicit_text_two_matches(self) -> None:
        score = compute_link_attribution_confidence(
            Provenance.explicit_text, text_match_count=2
        )
        assert 0.80 <= score <= 0.90

    def test_explicit_text_three_matches_at_cap(self) -> None:
        score = compute_link_attribution_confidence(
            Provenance.explicit_text, text_match_count=3
        )
        assert score == pytest.approx(0.90)

    def test_explicit_text_many_matches_capped(self) -> None:
        score = compute_link_attribution_confidence(
            Provenance.explicit_text, text_match_count=100
        )
        assert score == pytest.approx(0.90)

    def test_explicit_text_zero_matches(self) -> None:
        score = compute_link_attribution_confidence(
            Provenance.explicit_text, text_match_count=0
        )
        assert score == pytest.approx(0.80)

    def test_explicit_text_monotonically_increasing(self) -> None:
        scores = [
            compute_link_attribution_confidence(
                Provenance.explicit_text, text_match_count=i
            )
            for i in range(5)
        ]
        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1]

    def test_string_provenance_native(self) -> None:
        assert compute_link_attribution_confidence("native") == 1.0

    def test_string_provenance_unknown_falls_back(self) -> None:
        assert compute_link_attribution_confidence("bogus") == pytest.approx(0.3)


class TestConstants:
    def test_provenance_confidence_keys(self) -> None:
        assert set(PROVENANCE_CONFIDENCE.keys()) == {
            Provenance.native,
            Provenance.explicit_text,
            Provenance.heuristic,
        }

    def test_coverage_thresholds_ordered(self) -> None:
        assert COVERAGE_SUPPRESS_THRESHOLD < COVERAGE_WARN_THRESHOLD

    def test_default_min_sample(self) -> None:
        assert DEFAULT_MIN_SAMPLE == 30
