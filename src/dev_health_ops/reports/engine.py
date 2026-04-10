"""Execution engine for deterministic report rendering."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.testops_schemas import (
    ChartSpec,
    InsightBlock,
    ProvenanceRecord,
    ReportPlan,
)
from dev_health_ops.reports.charts import ChartResult, execute_chart
from dev_health_ops.reports.insights import generate_insights
from dev_health_ops.reports.metric_registry import get_metric_definition
from dev_health_ops.reports.narrative import NarrativeSection, generate_narrative
from dev_health_ops.reports.renderer import render_report_markdown


@dataclass(frozen=True)
class ReportResult:
    plan: ReportPlan
    chart_results: list[ChartResult]
    insights: list[InsightBlock]
    narrative_sections: list[NarrativeSection]
    provenance: list[ProvenanceRecord]
    rendered_markdown: str
    generated_at: datetime


def _provenance_id(plan_id: str, artifact_type: str, artifact_id: str) -> str:
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"{plan_id}:{artifact_type}:{artifact_id}")
    )


def _chart_provenance(plan: ReportPlan, chart_result: ChartResult) -> ProvenanceRecord:
    definition = get_metric_definition(chart_result.spec.metric)
    return ProvenanceRecord(
        provenance_id=_provenance_id(plan.plan_id, "chart", chart_result.spec.chart_id),
        artifact_type="chart",
        artifact_id=chart_result.spec.chart_id,
        plan_id=plan.plan_id,
        data_sources=[definition.source_table] if definition else [],
        metrics_used=[chart_result.spec.metric],
        time_range_start=chart_result.spec.time_range_start,
        time_range_end=chart_result.spec.time_range_end,
        filters_applied={
            "teams": ",".join(chart_result.spec.filter_teams),
            "repos": ",".join(chart_result.spec.filter_repos),
            "group_by": chart_result.spec.group_by or "total",
        },
        generated_at=datetime.now(UTC),
        generator_version="reports.v1",
        org_id=plan.org_id,
    )


def _narrative_provenance(
    plan: ReportPlan, section: NarrativeSection, generated_at: datetime
) -> ProvenanceRecord:
    metric_sources = []
    for metric in section.supporting_metrics:
        definition = get_metric_definition(metric)
        if definition and definition.source_table not in metric_sources:
            metric_sources.append(definition.source_table)
    return ProvenanceRecord(
        provenance_id=_provenance_id(plan.plan_id, "narrative", section.section_type),
        artifact_type="narrative",
        artifact_id=section.section_type,
        plan_id=plan.plan_id,
        data_sources=metric_sources,
        metrics_used=section.supporting_metrics,
        time_range_start=plan.time_range_start,
        time_range_end=plan.time_range_end,
        filters_applied={
            "teams": ",".join(plan.scope_teams),
            "repos": ",".join(plan.scope_repos),
            "services": ",".join(plan.scope_services),
        },
        generated_at=generated_at,
        generator_version="reports.v1",
        org_id=plan.org_id,
    )


def _report_provenance(plan: ReportPlan, generated_at: datetime) -> ProvenanceRecord:
    return ProvenanceRecord(
        provenance_id=_provenance_id(plan.plan_id, "report", plan.plan_id),
        artifact_type="report",
        artifact_id=plan.plan_id,
        plan_id=plan.plan_id,
        data_sources=[],
        metrics_used=plan.requested_metrics,
        time_range_start=plan.time_range_start,
        time_range_end=plan.time_range_end,
        filters_applied={
            "teams": ",".join(plan.scope_teams),
            "repos": ",".join(plan.scope_repos),
            "services": ",".join(plan.scope_services),
        },
        generated_at=generated_at,
        generator_version="reports.v1",
        org_id=plan.org_id,
    )


async def execute_report(
    plan: ReportPlan,
    chart_specs: list[ChartSpec],
    clickhouse_dsn: str,
) -> ReportResult:
    sink = ClickHouseMetricsSink(clickhouse_dsn)
    try:
        chart_results = await asyncio.gather(
            *(execute_chart(spec, sink) for spec in chart_specs)
        )
    finally:
        await asyncio.to_thread(sink.close)

    insights, insight_provenance = generate_insights(plan, chart_results)
    narrative_sections = generate_narrative(plan, chart_results, insights)
    generated_at = datetime.now(UTC)

    provenance = [
        *[_chart_provenance(plan, chart_result) for chart_result in chart_results],
        *insight_provenance,
        *[
            _narrative_provenance(plan, section, generated_at)
            for section in narrative_sections
        ],
        _report_provenance(plan, generated_at),
    ]
    rendered_markdown = render_report_markdown(
        plan=plan,
        chart_results=chart_results,
        insights=insights,
        narrative_sections=narrative_sections,
        provenance=provenance,
    )
    return ReportResult(
        plan=plan,
        chart_results=chart_results,
        insights=insights,
        narrative_sections=narrative_sections,
        provenance=provenance,
        rendered_markdown=rendered_markdown,
        generated_at=generated_at,
    )
