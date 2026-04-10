"""Deterministic report planner built on parser, resolver, and templates."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from dev_health_ops.metrics.testops_schemas import ChartSpec, ReportPlan
from dev_health_ops.reports.parser import ParsedPrompt, parse_prompt
from dev_health_ops.reports.resolver import (
    EntityCatalog,
    EntityResolution,
    MetricResolution,
    resolve_entities,
    resolve_metrics,
)
from dev_health_ops.reports.templates import default_time_range, get_template
from dev_health_ops.reports.validation import ValidationResult, validate_plan_request


@dataclass(frozen=True)
class PlanningResult:
    ok: bool
    parsed_prompt: ParsedPrompt
    metric_resolution: MetricResolution
    entity_resolution: EntityResolution
    validation: ValidationResult
    report_plan: ReportPlan | None = None
    chart_specs: list[ChartSpec] = field(default_factory=list)


def _build_plan_id(prompt: str, org_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{org_id}:{prompt.strip().lower()}"))


def _build_chart_id(plan_id: str, metric: str, title: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{plan_id}:{metric}:{title}"))


def build_report_plan(
    prompt: str,
    *,
    org_id: str,
    entity_catalog: EntityCatalog,
    today: date | None = None,
) -> PlanningResult:
    today = today or date.today()
    parsed_prompt = parse_prompt(prompt, today=today)
    metric_resolution = resolve_metrics(parsed_prompt.metric_terms)
    entity_resolution = resolve_entities(parsed_prompt, entity_catalog)

    report_type = parsed_prompt.report_type or "weekly_health"
    template = get_template(report_type)
    time_range_start = parsed_prompt.time_range_start
    time_range_end = parsed_prompt.time_range_end
    if time_range_start is None or time_range_end is None:
        time_range_start, time_range_end = default_time_range(report_type, today=today)

    validation = validate_plan_request(
        metric_resolution=metric_resolution,
        entity_resolution=entity_resolution,
        time_range_start=time_range_start,
        time_range_end=time_range_end,
        invalid_reasons=parsed_prompt.invalid_reasons,
    )
    if not validation.ok:
        return PlanningResult(
            ok=False,
            parsed_prompt=parsed_prompt,
            metric_resolution=metric_resolution,
            entity_resolution=entity_resolution,
            validation=validation,
        )

    template_metrics = list(template.default_metrics) if template else []
    requested_metrics = list(
        dict.fromkeys([*template_metrics, *metric_resolution.canonical_metrics])
    )
    plan_id = _build_plan_id(prompt, org_id)
    chart_specs: list[ChartSpec] = []
    requested_charts: list[str] = []

    if template:
        for chart in template.charts:
            chart_id = _build_chart_id(plan_id, chart.metric, chart.title)
            chart_specs.append(
                ChartSpec(
                    chart_id=chart_id,
                    plan_id=plan_id,
                    chart_type=chart.chart_type,
                    metric=chart.metric,
                    group_by=parsed_prompt.group_by or chart.group_by,
                    filter_teams=entity_resolution.team_ids,
                    filter_repos=entity_resolution.repo_ids,
                    time_range_start=time_range_start,
                    time_range_end=time_range_end,
                    title=chart.title,
                    org_id=org_id,
                )
            )
            requested_charts.append(chart_id)

    report_plan = ReportPlan(
        plan_id=plan_id,
        report_type=report_type,
        audience=parsed_prompt.audience,
        scope_teams=entity_resolution.team_ids,
        scope_repos=entity_resolution.repo_ids,
        scope_services=entity_resolution.service_ids,
        time_range_start=time_range_start,
        time_range_end=time_range_end,
        comparison_period=parsed_prompt.comparison_period
        or (template.default_comparison if template else None),
        sections=list(template.sections) if template else ["summary"],
        requested_metrics=requested_metrics,
        requested_charts=requested_charts,
        include_insights=True,
        include_anomalies=True,
        confidence_threshold="direct_fact",
        created_at=datetime.now(UTC),
        org_id=org_id,
    )
    return PlanningResult(
        ok=True,
        parsed_prompt=parsed_prompt,
        metric_resolution=metric_resolution,
        entity_resolution=entity_resolution,
        validation=validation,
        report_plan=report_plan,
        chart_specs=chart_specs,
    )
