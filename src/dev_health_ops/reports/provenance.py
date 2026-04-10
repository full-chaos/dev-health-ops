from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.metrics.testops_schemas import InsightBlock
from dev_health_ops.reports.charts import ChartResult
from dev_health_ops.reports.narrative import NarrativeSection


@dataclass(frozen=True)
class ProvenanceValidation:
    valid: bool
    violations: list[str]


def _available_metrics(chart_results: list[ChartResult]) -> set[str]:
    return {cr.spec.metric for cr in chart_results}


def validate_narrative_provenance(
    narrative_sections: list[NarrativeSection],
    chart_results: list[ChartResult],
) -> ProvenanceValidation:
    available = _available_metrics(chart_results)
    violations: list[str] = []
    for section in narrative_sections:
        for metric in section.supporting_metrics:
            if metric not in available:
                violations.append(
                    f"Section '{section.section_type}' references metric "
                    f"'{metric}' not present in chart results"
                )
    return ProvenanceValidation(valid=len(violations) == 0, violations=violations)


def validate_insight_provenance(
    insights: list[InsightBlock],
    chart_results: list[ChartResult],
) -> ProvenanceValidation:
    available = _available_metrics(chart_results)
    violations: list[str] = []
    for insight in insights:
        for metric in insight.supporting_metrics:
            if metric not in available:
                violations.append(
                    f"Insight '{insight.insight_type}' references metric "
                    f"'{metric}' not present in chart results"
                )
    return ProvenanceValidation(valid=len(violations) == 0, violations=violations)
