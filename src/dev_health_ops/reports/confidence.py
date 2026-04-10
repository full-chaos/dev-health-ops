from __future__ import annotations

from enum import Enum

from dev_health_ops.metrics.testops_schemas import InsightBlock

THRESHOLD_ORDER = ["direct_fact", "inferred", "hypothesis"]


class ConfidenceLevel(str, Enum):
    DIRECT_FACT = "direct_fact"
    INFERRED = "inferred"
    HYPOTHESIS = "hypothesis"


def assess_confidence(
    insight: InsightBlock,
    data_points_count: int,
) -> ConfidenceLevel:
    if insight.insight_type == "correlation":
        return ConfidenceLevel.HYPOTHESIS
    if data_points_count >= 3 and insight.insight_type in ("trend_delta", "regression"):
        return ConfidenceLevel.INFERRED
    return ConfidenceLevel.DIRECT_FACT


def filter_by_confidence(
    insights: list[InsightBlock],
    threshold: str,
    data_points_counts: dict[str, int] | None = None,
) -> list[InsightBlock]:
    counts = data_points_counts or {}
    threshold_idx = (
        THRESHOLD_ORDER.index(threshold) if threshold in THRESHOLD_ORDER else 0
    )
    result: list[InsightBlock] = []
    for insight in insights:
        primary_metric = (
            insight.supporting_metrics[0] if insight.supporting_metrics else ""
        )
        n = counts.get(primary_metric, 1)
        level = assess_confidence(insight, n)
        level_idx = THRESHOLD_ORDER.index(level.value)
        if level_idx <= threshold_idx:
            result.append(insight)
    return result
