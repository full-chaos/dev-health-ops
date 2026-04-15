from __future__ import annotations

from typing import Any

import pytest

EXPECTED_METRIC_KEYS = [
    "release_user_friction_delta",
    "release_error_rate_delta",
    "time_to_first_user_issue_after_release",
    "release_impact_confidence_score",
    "release_impact_coverage_ratio",
    "flag_exposure_rate",
    "flag_activation_rate",
    "flag_reliability_guardrail",
    "flag_friction_delta",
    "flag_rollout_half_life",
    "flag_churn_rate",
    "issue_to_release_impact_link_rate",
    "rollback_or_disable_after_impact_spike",
]


def test_metric_formula_fixture_covers_full_prd_catalog(
    metric_formula_case_map: dict[str, dict[str, Any]],
) -> None:
    # PRD: lines 224-253, 389
    assert list(metric_formula_case_map) == EXPECTED_METRIC_KEYS


@pytest.mark.parametrize("metric_key", EXPECTED_METRIC_KEYS)
def test_metric_formula_fixture_contains_known_inputs_and_expected_outputs(
    metric_key: str, metric_formula_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: lines 224-253, 389
    case = metric_formula_case_map[metric_key]

    assert case["inputs"]
    assert case["expected"] is not None


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for release/flag metric computation"
)
@pytest.mark.parametrize("metric_key", EXPECTED_METRIC_KEYS)
def test_metric_formulas_match_known_inputs(
    metric_key: str, metric_formula_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: lines 224-253, 389
    case = metric_formula_case_map[metric_key]

    assert case["expected"] is not None
