from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.parametrize("provenance", ["native", "explicit_text", "heuristic"])
def test_confidence_fixture_covers_prd_bands(
    provenance: str, confidence_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: lines 182-185, 390
    band = confidence_case_map[provenance]

    assert band["expected_min"] <= band["expected_max"]


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for confidence band assignment"
)
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
    # PRD: lines 182-185, 390
    case = confidence_case_map[provenance]

    assert (case["expected_min"], case["expected_max"]) == expected_range
