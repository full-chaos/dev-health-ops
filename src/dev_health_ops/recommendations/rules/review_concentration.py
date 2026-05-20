"""Review-concentration rule — High review latency + concentrated reviewers (Gini).

PRD trigger (§P1: Operational Recommendations):
    "High review latency + concentrated reviewers ->
     Review dependency risk. Add reviewers or rotate ownership."

Logic (deterministic, no I/O):
    1. Check review_latency_p75_hours >= REVIEW_LATENCY_P75_HOURS.
    2. Check reviewer_gini >= REVIEWER_GINI_THRESHOLD.
    3. Trigger when BOTH conditions hold.
    4. Return None if either field is None (insufficient data).

Metric loader fields consumed from MetricsSnapshot:
    - snapshot.review_latency_p75_hours  : float | None
    - snapshot.reviewer_gini             : float | None
    - snapshot.team_id, org_id, window_start, window_end

Source tables referenced in evidence:
    - repo_metrics_daily   (pr_cycle_p75_hours)
    - review_edge_daily    (reviewer distribution -> Gini)
"""

from __future__ import annotations

from datetime import datetime

from dev_health_ops.recommendations.engine import MetricsSnapshot
from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation
from dev_health_ops.recommendations.thresholds import (
    REVIEW_LATENCY_P75_HOURS,
    REVIEWER_GINI_THRESHOLD,
)

RULE_ID = "review-concentration"
RULE_VERSION = "1.0.0"
SUCCESS_CRITERION = (
    f"Reviewer Gini drops below {REVIEWER_GINI_THRESHOLD} OR review latency p75 drops "
    f"below {REVIEW_LATENCY_P75_HOURS}h in 2 cycles"
)


def gini(values: list[float]) -> float:
    """Compute Gini coefficient for a list of non-negative values.

    Returns 0.0 for empty input or when all values are zero.
    """
    if not values:
        return 0.0
    total = sum(values)
    if total == 0.0:
        return 0.0
    n = len(values)
    sorted_vals = sorted(values)
    cum_sum = sum((i + 1) * v for i, v in enumerate(sorted_vals))
    return (2.0 * cum_sum) / (n * total) - (n + 1.0) / n


def evaluate_review_concentration(
    snapshot: MetricsSnapshot,
    now: datetime,
) -> Recommendation | None:
    """Trigger when review latency is high AND reviewer load is concentrated.

    Args:
        snapshot: Pre-loaded metrics for the evaluation window.
        now: Evaluation instant (UTC). Passed explicitly for determinism.

    Returns:
        A Recommendation if the rule fires, else None.
    """
    if snapshot.review_latency_p75_hours is None or snapshot.reviewer_gini is None:
        return None

    latency: float = snapshot.review_latency_p75_hours
    gini_score: float = snapshot.reviewer_gini

    if latency < REVIEW_LATENCY_P75_HOURS:
        return None
    if gini_score < REVIEWER_GINI_THRESHOLD:
        return None

    evidence: tuple[EvidenceRef, ...] = (
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="repo_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="review_latency_p75_hours",
            value=round(latency, 2),
        ),
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="review_edge_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="reviewer_gini",
            value=round(gini_score, 4),
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
        title="Review dependency risk. Add reviewers or rotate ownership.",
        rationale=(
            f"Review latency p75 is {latency:.1f}h "
            f"(threshold: {REVIEW_LATENCY_P75_HOURS}h) and reviewer Gini is "
            f"{gini_score:.3f} (threshold: {REVIEWER_GINI_THRESHOLD}), "
            "indicating review load is concentrated in few individuals."
        ),
        success_criterion=SUCCESS_CRITERION,
        evidence=evidence,
    )
