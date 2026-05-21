"""Compounding-risk rule — Complexity rising in hotspots.

PRD trigger (§P1: Operational Recommendations):
    "Complexity rising in hotspots ->
     Code risk is compounding where change pressure is highest."

Logic (deterministic, no I/O):
    1. Check hotspot_complexity_delta >= COMPLEXITY_DELTA_THRESHOLD.
    2. Check hotspot_churn_overlap >= HOTSPOT_CHURN_OVERLAP_THRESHOLD.
    3. Trigger when BOTH conditions hold.
    4. Return None if either field is None (no hotspot data).

Field interpretations:
    hotspot_complexity_delta : normalised increase in cyclomatic complexity
                               across top-N hotspot files over the window.
    hotspot_churn_overlap    : fraction of hotspot files that also have rising
                               complexity (intersection of high-churn + high-complexity).

Metric loader fields consumed from MetricsSnapshot:
    - snapshot.hotspot_complexity_delta  : float | None
    - snapshot.hotspot_churn_overlap     : float | None
    - snapshot.team_id, org_id, window_start, window_end

Source tables referenced in evidence:
    - file_complexity_snapshots (cyclomatic_per_kloc delta)
    - file_metrics_daily        (hotspot_score, churn)
"""

from __future__ import annotations

from datetime import datetime

from dev_health_ops.recommendations.engine import MetricsSnapshot
from dev_health_ops.recommendations.schema import EvidenceRef, Recommendation
from dev_health_ops.recommendations.thresholds import (
    COMPLEXITY_DELTA_THRESHOLD,
    HOTSPOT_CHURN_OVERLAP_THRESHOLD,
)

RULE_ID = "compounding-risk"
RULE_VERSION = "1.0.0"
SUCCESS_CRITERION = (
    f"Hotspot complexity delta drops below {COMPLEXITY_DELTA_THRESHOLD} OR "
    f"churn-complexity overlap drops below {HOTSPOT_CHURN_OVERLAP_THRESHOLD} in 2 cycles"
)


def evaluate_compounding_risk(
    snapshot: MetricsSnapshot,
    now: datetime,
) -> Recommendation | None:
    """Trigger when code complexity is rising in the highest-churn hotspots.

    Args:
        snapshot: Pre-loaded metrics for the evaluation window.
        now: Evaluation instant (UTC). Passed explicitly for determinism.

    Returns:
        A Recommendation if the rule fires, else None.
    """
    # Preferred path (CHAOS-1641): use the persisted Compounding Risk
    # composite when available. ``severity`` is the canonical signal.
    if snapshot.compounding_risk_severity in ("elevated", "high"):
        score = snapshot.compounding_risk_score
        rationale = (
            (
                f"Compounding Risk score is {score:.3f} "
                f"(severity: {snapshot.compounding_risk_severity}). "
                "Churn, complexity trend, ownership concentration, and review "
                "latency are compounding above their tuned thresholds."
            )
            if score is not None
            else (
                f"Compounding Risk severity is "
                f"{snapshot.compounding_risk_severity}."
            )
        )
        evidence: tuple[EvidenceRef, ...] = (
            EvidenceRef(
                team_id=snapshot.team_id,
                metric_table="compounding_risk_daily",
                window_start=snapshot.window_start,
                window_end=snapshot.window_end,
                field="compounding_risk",
                value=round(score, 4) if score is not None else 0.0,
            ),
        )
        return Recommendation(
            rule_id=RULE_ID,
            team_id=snapshot.team_id,
            org_id=snapshot.org_id,
            computed_at=now,
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            severity=(
                "critical"
                if snapshot.compounding_risk_severity == "high"
                else "warning"
            ),
            title="Code risk is compounding where change pressure is highest.",
            rationale=rationale,
            success_criterion=SUCCESS_CRITERION,
            evidence=evidence,
        )

    # Fallback (pre-1641 / backfill warmup): legacy hotspot proxy.
    if (
        snapshot.hotspot_complexity_delta is None
        or snapshot.hotspot_churn_overlap is None
    ):
        return None

    complexity_delta: float = snapshot.hotspot_complexity_delta
    churn_overlap: float = snapshot.hotspot_churn_overlap

    if complexity_delta < COMPLEXITY_DELTA_THRESHOLD:
        return None
    if churn_overlap < HOTSPOT_CHURN_OVERLAP_THRESHOLD:
        return None

    evidence = (
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="file_complexity_snapshots",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="hotspot_complexity_delta",
            value=round(complexity_delta, 4),
        ),
        EvidenceRef(
            team_id=snapshot.team_id,
            metric_table="file_metrics_daily",
            window_start=snapshot.window_start,
            window_end=snapshot.window_end,
            field="hotspot_churn_overlap",
            value=round(churn_overlap, 4),
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
        title="Code risk is compounding where change pressure is highest.",
        rationale=(
            f"Hotspot complexity delta is {complexity_delta:.3f} "
            f"(threshold: {COMPLEXITY_DELTA_THRESHOLD}) and churn-complexity overlap "
            f"is {churn_overlap:.3f} "
            f"(threshold: {HOTSPOT_CHURN_OVERLAP_THRESHOLD}), "
            "indicating growing technical debt in the most actively-changed files."
        ),
        success_criterion=SUCCESS_CRITERION,
        evidence=evidence,
    )
