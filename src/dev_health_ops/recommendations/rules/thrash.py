"""Thrash rule — High churn + low delivery.

PRD trigger (§P1: Operational Recommendations):
    "High churn + low delivery ->
     Thrash likely. Inspect hotspots and rework loops."

Logic (deterministic, no I/O):
    1. Check rework_churn_ratio >= CHURN_RATIO_THRESHOLD.
    2. Compute throughput_delta = last point - first point of throughput_by_cycle.
    3. Trigger when BOTH conditions hold:
       rework_churn_ratio >= CHURN_RATIO_THRESHOLD AND
       throughput_delta <= THROUGHPUT_LOW_DELTA_THRESHOLD
    4. Return None if rework_churn_ratio is None (no PR data).

Metric loader fields consumed from MetricsSnapshot:
    - snapshot.rework_churn_ratio    : float | None  (rework_churn_ratio_30d)
    - snapshot.throughput_by_cycle   : list[float]   (items_completed per day)
    - snapshot.team_id, org_id, window_start, window_end

Source tables referenced in evidence:
    - repo_metrics_daily       (rework_churn_ratio_30d)
    - work_item_metrics_daily  (items_completed)
"""

from __future__ import annotations

from datetime import datetime

from dev_health_ops.recommendations.engine import MetricsSnapshot
from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation
from dev_health_ops.recommendations.thresholds import (
    CHURN_RATIO_THRESHOLD,
    THROUGHPUT_LOW_DELTA_THRESHOLD,
)

RULE_ID = "thrash"
RULE_VERSION = "1.0.0"
SUCCESS_CRITERION = (
    f"Churn ratio drops below {CHURN_RATIO_THRESHOLD} OR throughput delta turns "
    "positive in 2 cycles"
)


def evaluate_thrash(
    snapshot: MetricsSnapshot,
    now: datetime,
) -> Recommendation | None:
    """Trigger when rework churn is high and throughput is flat or declining.

    Args:
        snapshot: Pre-loaded metrics for the evaluation window.
        now: Evaluation instant (UTC). Passed explicitly for determinism.

    Returns:
        A Recommendation if the rule fires, else None.
    """
    if snapshot.rework_churn_ratio is None:
        return None

    churn_ratio: float = snapshot.rework_churn_ratio
    throughput = snapshot.throughput_by_cycle

    if churn_ratio < CHURN_RATIO_THRESHOLD:
        return None
    if len(throughput) < 2:
        return None

    throughput_delta = throughput[-1] - throughput[0]
    if throughput_delta > THROUGHPUT_LOW_DELTA_THRESHOLD:
        return None

    evidence: tuple[EvidenceRef, ...] = (
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="repo_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="rework_churn_ratio_30d",
            value=round(churn_ratio, 4),
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
        title="Thrash likely. Inspect hotspots and rework loops.",
        rationale=(
            f"Rework churn ratio is {churn_ratio:.3f} "
            f"(threshold: {CHURN_RATIO_THRESHOLD}) and throughput delta is "
            f"{throughput_delta:.1f} items/cycle "
            f"(threshold: \u2264{THROUGHPUT_LOW_DELTA_THRESHOLD}), "
            "suggesting repeated rework is consuming capacity without advancing delivery."
        ),
        success_criterion=SUCCESS_CRITERION,
        evidence=evidence,
    )
