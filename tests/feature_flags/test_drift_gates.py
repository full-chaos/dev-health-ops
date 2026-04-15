from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.parametrize("drift_case", ["schema_version_shift", "volume_shift"])
def test_drift_fixture_covers_schema_and_volume_shifts(
    drift_case: str, drift_gate_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: line 393
    case = drift_gate_case_map[drift_case]

    assert case["expected_flag"] is True


@pytest.mark.parametrize("visibility", ["show", "warn", "suppress"])
def test_coverage_fixture_covers_show_warn_suppress_thresholds(
    visibility: str, coverage_gate_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: lines 296-299, 394
    case = coverage_gate_case_map[visibility]

    assert case["expected_visibility"] == visibility


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for instrumentation drift gates"
)
@pytest.mark.parametrize("drift_case", ["schema_version_shift", "volume_shift"])
def test_instrumentation_change_flag_triggers_on_drift(
    drift_case: str, drift_gate_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: line 393
    case = drift_gate_case_map[drift_case]

    assert case["expected_flag"] is True


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for coverage suppression gating"
)
def test_metrics_are_suppressed_when_coverage_below_half(
    coverage_gate_case_map: dict[str, dict[str, Any]],
) -> None:
    # PRD: lines 296-299, 394
    suppress_case = coverage_gate_case_map["suppress"]

    assert suppress_case["coverage_ratio"] < 0.50
    assert suppress_case["expected_visibility"] == "suppress"
