"""Shared fixtures for recommendation rule golden tests.

Uses the real MetricsSnapshot from CHAOS-1622 (rec-engine-core).
All fields required; use make_snapshot() helper to build variants.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dev_health_ops.recommendations.engine import MetricsSnapshot

TEAM_ID = "team-alpha"
ORG_ID = "org-1"
WINDOW_START = date(2026, 4, 1)
WINDOW_END = date(2026, 4, 14)
NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)


def make_snapshot(**overrides: object) -> MetricsSnapshot:
    """Build a MetricsSnapshot with safe below-threshold defaults.

    Pass keyword arguments to override any field.
    """
    defaults: dict[str, object] = {
        "team_id": TEAM_ID,
        "org_id": ORG_ID,
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        # saturation + thrash (flat WIP, positive throughput trend)
        "wip_by_day": [5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0],
        "throughput_by_cycle": [10.0, 11.0],
        # review-concentration (below both thresholds)
        "review_latency_p75_hours": 12.0,
        "reviewer_gini": 0.3,
        # thrash (low churn)
        "rework_churn_ratio": 0.1,
        # sustainability-risk (low after-hours, flat cycle time)
        "after_hours_ratio": 0.05,
        "cycle_time_by_day": [24.0, 24.0, 24.0, 24.0, 24.0, 24.0, 24.0],
        # compounding-risk (below both thresholds)
        "hotspot_complexity_delta": 0.05,
        "hotspot_churn_overlap": 0.1,
    }
    defaults.update(overrides)
    return MetricsSnapshot(**defaults)  # type: ignore[arg-type]


@pytest.fixture()
def base_snapshot() -> MetricsSnapshot:
    """Snapshot with all values below every threshold — no rule should fire."""
    return make_snapshot()
