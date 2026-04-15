from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest


def test_recomputation_fixture_targets_last_seven_days(
    recomputation_case: dict[str, Any],
) -> None:
    # PRD: lines 156-163, 395
    recomputed_days = recomputation_case["recomputed_days"]
    stable_days = recomputation_case["stable_days"]

    assert len(recomputed_days) == 7
    assert min(recomputed_days) > max(stable_days)


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for seven-day recomputation window"
)
def test_recomputation_updates_release_impact_daily_for_last_seven_days(
    recomputation_case: dict[str, Any],
) -> None:
    # PRD: lines 156-163, 395
    anchor_day = recomputation_case["anchor_day"]
    recomputed_days = recomputation_case["recomputed_days"]

    assert recomputed_days[-1] == anchor_day - timedelta(days=6)
