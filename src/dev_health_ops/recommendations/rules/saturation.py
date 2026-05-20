"""Saturation rule — Rising WIP + flat throughput.

PRD trigger (§P1: Operational Recommendations):
    "Rising WIP + flat throughput → The team is saturating.
     Reduce active work before adding scope."

Logic (deterministic, no I/O):
    1. Compute linear slope of wip_by_day over the window.
    2. Compute throughput_delta = last point - first point of throughput_by_cycle.
    3. Trigger when:
       slope >= WIP_RISING_SLOPE_THRESHOLD AND
       throughput_delta <= THROUGHPUT_FLAT_DELTA_THRESHOLD

Metric loader fields consumed from MetricsSnapshot:
    - snapshot.wip_by_day           : list[float]  (wip_count_end_of_day per day)
    - snapshot.throughput_by_cycle  : list[float]  (items_completed per cycle)
    - snapshot.team_id              : str
    - snapshot.org_id               : str
    - snapshot.window_start         : date
    - snapshot.window_end           : date

Source tables referenced in evidence:
    - work_item_metrics_daily  (wip_count_end_of_day, items_completed)
"""

from __future__ import annotations

from datetime import datetime

from dev_health_ops.recommendations.engine import MetricsSnapshot
from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation
from dev_health_ops.recommendations.thresholds import (
    THROUGHPUT_FLAT_DELTA_THRESHOLD,
    WIP_RISING_SLOPE_THRESHOLD,
)

RULE_ID = "saturation"
RULE_VERSION = "1.0.0"
SUCCESS_CRITERION = (
    "WIP trend turns negative or throughput trend turns positive in 2 cycles"
)


def _linear_slope(values: list[float]) -> float:
    """Return OLS slope of *values* (x = index, y = value). Returns 0.0 for < 2 points."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def evaluate_saturation(
    snapshot: MetricsSnapshot,
    now: datetime,
) -> Recommendation | None:
    """Trigger when WIP is rising and throughput is flat.

    Args:
        snapshot: Pre-loaded metrics for the evaluation window.
        now: Evaluation instant (UTC). Passed explicitly for determinism.

    Returns:
        A Recommendation if the rule fires, else None.
    """
    wip = snapshot.wip_by_day
    throughput = snapshot.throughput_by_cycle

    if len(wip) < 2 or len(throughput) < 2:
        return None

    wip_slope = _linear_slope(list(wip))
    throughput_delta = throughput[-1] - throughput[0]

    if wip_slope < WIP_RISING_SLOPE_THRESHOLD:
        return None
    if throughput_delta > THROUGHPUT_FLAT_DELTA_THRESHOLD:
        return None

    evidence: tuple[EvidenceRef, ...] = (
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="work_item_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="wip_count_end_of_day",
            value=round(wip_slope, 4),
        ),
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="work_item_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="items_completed_delta",
            value=round(throughput_delta, 4),
        ),
    )

    return Recommendation(
        rule_id=RULE_ID,
        team_id=snapshot.team_id,
        org_id=snapshot.org_id,
        computed_at=now,
        window_start=snapshot.window_start,
        window_end=snapshot.window_end,
        severity="warning",
        title="Team is saturating. Reduce active work before adding scope.",
        rationale=(
            f"WIP slope is {wip_slope:.3f} items/day "
            f"(threshold: {WIP_RISING_SLOPE_THRESHOLD}) and throughput delta is "
            f"{throughput_delta:.1f} items/cycle "
            f"(threshold: \u2264{THROUGHPUT_FLAT_DELTA_THRESHOLD})."
        ),
        success_criterion=SUCCESS_CRITERION,
        evidence=evidence,
    )
