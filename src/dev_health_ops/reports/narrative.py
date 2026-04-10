"""Template-based grounded narrative generation."""

from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.metrics.testops_schemas import InsightBlock, ReportPlan
from dev_health_ops.reports.charts import ChartResult
from dev_health_ops.reports.metric_registry import get_metric_definition

SECTION_METRICS = {
    "summary": (),
    "delivery": ("items_completed", "cycle_time_p50_hours", "lead_time_p50_hours"),
    "quality": (
        "failure_rate",
        "pass_rate",
        "line_coverage_pct",
        "coverage_regression_count",
    ),
    "testops": (
        "success_rate",
        "flake_rate",
        "rerun_rate",
        "retry_dependency_rate",
        "avg_queue_seconds",
        "median_duration_seconds",
    ),
    "wellbeing": ("after_hours_commit_ratio", "weekend_commit_ratio"),
}

SECTION_TITLES = {
    "summary": "Summary",
    "delivery": "Delivery",
    "quality": "Quality",
    "testops": "TestOps",
    "wellbeing": "Wellbeing",
}


@dataclass(frozen=True)
class NarrativeSection:
    section_type: str
    title: str
    body: str
    supporting_metrics: list[str]


def _format_value(metric: str, value: float) -> str:
    definition = get_metric_definition(metric)
    unit = definition.unit if definition else "unitless"
    if unit == "ratio":
        return f"{value * 100:.1f}%"
    if unit == "percent":
        return f"{value:.1f}%"
    if unit == "seconds":
        return f"{value:.1f}s"
    if unit == "minutes":
        return f"{value:.1f}m"
    if unit == "hours":
        return f"{value:.1f}h"
    if unit == "count":
        return f"{value:.0f}"
    return f"{value:.2f}"


def _metric_display(metric: str) -> str:
    definition = get_metric_definition(metric)
    if definition is None:
        return metric.replace("_", " ").title()
    return definition.display_name


def _latest_value(chart_result: ChartResult) -> float | None:
    if not chart_result.data_points:
        return None
    value = chart_result.data_points[-1].get("y")
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


def _prior_value(chart_result: ChartResult) -> float | None:
    if len(chart_result.data_points) < 2:
        return None
    value = chart_result.data_points[0].get("y")
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


def _sentence_for_chart(chart_result: ChartResult) -> str | None:
    metric = chart_result.spec.metric
    current = _latest_value(chart_result)
    if current is None:
        return None
    prior = _prior_value(chart_result)
    display = _metric_display(metric)
    current_text = _format_value(metric, current)
    if prior is None:
        return f"{display} appears near {current_text} for the selected window."
    prior_text = _format_value(metric, prior)
    return f"{display} appears near {current_text}, compared with {prior_text} at the opening of the selected window."


def _relevant_metrics(section_type: str, chart_results: list[ChartResult]) -> list[str]:
    available_metrics = [
        chart.spec.metric for chart in chart_results if not chart.empty
    ]
    if section_type == "summary":
        return list(dict.fromkeys(available_metrics[:3]))
    desired = SECTION_METRICS.get(section_type, ())
    return [metric for metric in desired if metric in available_metrics]


def generate_narrative(
    plan: ReportPlan,
    chart_results: list[ChartResult],
    insights: list[InsightBlock],
) -> list[NarrativeSection]:
    chart_by_metric = {
        chart_result.spec.metric: chart_result
        for chart_result in chart_results
        if not chart_result.empty
    }
    sections: list[NarrativeSection] = []

    for section_type in plan.sections:
        metrics = _relevant_metrics(section_type, chart_results)
        sentences = []
        for metric in metrics:
            sentence = _sentence_for_chart(chart_by_metric[metric])
            if sentence:
                sentences.append(sentence)

        section_insights = [
            insight
            for insight in insights
            if set(insight.supporting_metrics).intersection(metrics)
        ]
        if section_insights:
            highlight = section_insights[0]
            sentences.append(
                f"Current evidence suggests: {highlight.summary[0].lower() + highlight.summary[1:]}"
            )

        body = "\n\n".join(sentences)
        if not body:
            body = "Available evidence appears limited for this section in the current report window."

        sections.append(
            NarrativeSection(
                section_type=section_type,
                title=SECTION_TITLES.get(
                    section_type, section_type.replace("_", " ").title()
                ),
                body=body,
                supporting_metrics=metrics,
            )
        )

    return sections
