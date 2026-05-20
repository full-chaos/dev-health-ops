"""Sustainability-risk rule — High after-hours activity + rising cycle time.

PRD trigger (§P1: Operational Recommendations):
    "High after-hours + rising cycle time ->
     Sustainability risk. Delivery may be propped up by time debt."

Logic (deterministic, no I/O):
    1. Check after_hours_ratio >= AFTER_HOURS_RATIO_THRESHOLD.
    2. Compute linear slope of cycle_time_by_day.
    3. Trigger when BOTH conditions hold:
       after_hours_ratio >= AFTER_HOURS_RATIO_THRESHOLD AND
       cycle_time_slope >= CYCLE_TIME_RISING_SLOPE_THRESHOLD
    4. Return None if after_hours_ratio is None or cycle_time_by_day has < 2 points.

Metric loader fields consumed from MetricsSnapshot:
    - snapshot.after_hours_ratio     : float | None  (after_hours_commit_ratio)
    - snapshot.cycle_time_by_day     : list[float]   (cycle_time_p50_hours per day)
    - snapshot.team_id, org_id, window_start, window_end

Source tables referenced in evidence:
    - team_metrics_daily       (after_hours_commit_ratio)
    - work_item_metrics_daily  (cycle_time_p50_hours)
"""

from __future__ import annotations

from datetime import datetime

from dev_health_ops.recommendations.engine import MetricsSnapshot
from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation
from dev_health_ops.recommendations.thresholds import (
    AFTER_HOURS_RATIO_THRESHOLD,
    CYCLE_TIME_RISING_SLOPE_THRESHOLD,
)

RULE_ID = "sustainability-risk"
RULE_VERSION = "1.0.0"
SUCCESS_CRITERION = (
    f"After-hours ratio drops below {AFTER_HOURS_RATIO_THRESHOLD} AND "
    "cycle time trend stabilises in 2 cycles"
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


def evaluate_sustainability_risk(
    snapshot: MetricsSnapshot,
    now: datetime,
) -> Recommendation | None:
    """Trigger when after-hours activity is high AND cycle time is rising.

    Args:
        snapshot: Pre-loaded metrics for the evaluation window.
        now: Evaluation instant (UTC). Passed explicitly for determinism.

    Returns:
        A Recommendation if the rule fires, else None.
    """
    if snapshot.after_hours_ratio is None:
        return None

    after_hours: float = snapshot.after_hours_ratio
    cycle_times = snapshot.cycle_time_by_day

    if after_hours < AFTER_HOURS_RATIO_THRESHOLD:
        return None
    if len(cycle_times) < 2:
        return None

    ct_slope = _linear_slope(list(cycle_times))
    if ct_slope < CYCLE_TIME_RISING_SLOPE_THRESHOLD:
        return None

    evidence: tuple[EvidenceRef, ...] = (
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="team_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="after_hours_commit_ratio",
            value=round(after_hours, 4),
        ),
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="work_item_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="cycle_time_p50_hours_slope",
            value=round(ct_slope, 4),
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
        title="Sustainability risk. Delivery may be propped up by time debt.",
        rationale=(
            f"After-hours commit ratio is {after_hours:.3f} "
            f"(threshold: {AFTER_HOURS_RATIO_THRESHOLD}) and cycle time slope is "
            f"{ct_slope:.3f} hours/day "
            f"(threshold: {CYCLE_TIME_RISING_SLOPE_THRESHOLD}), "
            "suggesting extended hours are masking growing delivery pressure."
        ),
        success_criterion=SUCCESS_CRITERION,
        evidence=evidence,
    )
