"""Markdown report rendering."""

from __future__ import annotations

from datetime import UTC, datetime

from dev_health_ops.metrics.testops_schemas import (
    InsightBlock,
    ProvenanceRecord,
    ReportPlan,
)
from dev_health_ops.reports.charts import ChartResult
from dev_health_ops.reports.narrative import NarrativeSection


def _format_time_range(plan: ReportPlan) -> str:
    if plan.time_range_start and plan.time_range_end:
        return (
            f"{plan.time_range_start.isoformat()} → {plan.time_range_end.isoformat()}"
        )
    return "unspecified window"


def _format_scope(plan: ReportPlan) -> str:
    parts = []
    if plan.scope_teams:
        parts.append(f"teams={', '.join(plan.scope_teams)}")
    if plan.scope_repos:
        parts.append(f"repos={', '.join(plan.scope_repos)}")
    if plan.scope_services:
        parts.append(f"services={', '.join(plan.scope_services)}")
    return " | ".join(parts) if parts else "global"


def _render_insights(insights: list[InsightBlock]) -> str:
    if not insights:
        return "No grounded insights for this section."
    lines = []
    for insight in insights:
        lines.append(
            f"- **{insight.confidence} · {insight.severity}** — {insight.summary}"
        )
    return "\n".join(lines)


def _render_chart_table(chart_result: ChartResult) -> str:
    if chart_result.empty:
        return f"#### {chart_result.title}\n\n_No data returned for this chart._"
    lines = [
        f"#### {chart_result.title}",
        "",
        "| x | y | group |",
        "| --- | ---: | --- |",
    ]
    for point in chart_result.data_points:
        lines.append(
            f"| {point.get('x', '')} | {point.get('y', '')} | {point.get('group', '') or ''} |"
        )
    return "\n".join(lines)


def _render_provenance(provenance: list[ProvenanceRecord]) -> str:
    lines = ["## Provenance", ""]
    if not provenance:
        lines.append("No provenance records available.")
        return "\n".join(lines)
    for record in provenance:
        sources = ", ".join(record.data_sources) or "n/a"
        metrics = ", ".join(record.metrics_used) or "n/a"
        lines.append(
            f"- **{record.artifact_type}:{record.artifact_id}** — sources: {sources}; metrics: {metrics}"
        )
    return "\n".join(lines)


def render_report_markdown(
    plan: ReportPlan,
    chart_results: list[ChartResult],
    insights: list[InsightBlock],
    narrative_sections: list[NarrativeSection],
    provenance: list[ProvenanceRecord],
) -> str:
    summary_section = next(
        (
            section
            for section in narrative_sections
            if section.section_type == "summary"
        ),
        None,
    )
    lines = [
        f"# {plan.report_type.replace('_', ' ').title()} Report — {_format_time_range(plan)}",
        "",
        "## Summary",
        summary_section.body
        if summary_section
        else "Available evidence appears limited for this summary.",
        "",
    ]

    for section in narrative_sections:
        if section.section_type == "summary":
            continue
        section_insights = [
            insight
            for insight in insights
            if set(insight.supporting_metrics).intersection(section.supporting_metrics)
        ]
        section_charts = [
            chart
            for chart in chart_results
            if chart.spec.metric in section.supporting_metrics
        ]
        lines.extend(
            [
                f"## {section.title}",
                section.body,
                "",
                "### Insights",
                _render_insights(section_insights),
                "",
                "### Charts",
            ]
        )
        if section_charts:
            for chart in section_charts:
                lines.extend([_render_chart_table(chart), ""])
        else:
            lines.extend(["No charts linked to this section.", ""])

    lines.extend([_render_provenance(provenance), ""])

    generated_at = (plan.created_at or datetime.now(UTC)).isoformat()
    lines.extend(
        [
            "---",
            f"Generated at {generated_at} | Confidence: {plan.confidence_threshold} | Scope: {_format_scope(plan)}",
        ]
    )
    return "\n".join(lines).strip() + "\n"
