"""Insight generation for grounded report artifacts."""

from __future__ import annotations

import math
import statistics
import uuid
from datetime import UTC, datetime

from dev_health_ops.metrics.testops_schemas import (
    InsightBlock,
    ProvenanceRecord,
    ReportPlan,
)
from dev_health_ops.reports.charts import ChartResult
from dev_health_ops.reports.metric_registry import get_metric_definition

REGRESSION_METRICS = {"line_coverage_pct", "success_rate", "pass_rate"}
NEGATIVE_DIRECTION_METRICS = {
    "failure_rate",
    "flake_rate",
    "rerun_rate",
    "retry_dependency_rate",
    "avg_queue_seconds",
    "median_duration_seconds",
    "p95_duration_seconds",
    "p95_queue_seconds",
}


def _artifact_id(plan_id: str, metric: str, insight_type: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{plan_id}:{metric}:{insight_type}"))


def _provenance_id(plan_id: str, artifact_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{plan_id}:provenance:{artifact_id}"))


def _numeric_series(chart_result: ChartResult) -> list[float]:
    series: list[float] = []
    for point in chart_result.data_points:
        value = point.get("y")
        if isinstance(value, int | float) and not isinstance(value, bool):
            series.append(float(value))
    return series


def _delta_ratio(prior: float, current: float) -> float | None:
    if math.isclose(prior, 0.0, abs_tol=1e-9):
        return None
    return (current - prior) / abs(prior)


def _format_delta(delta_ratio: float) -> str:
    return f"{delta_ratio * 100:.1f}%"


def _severity(metric: str, delta_ratio: float) -> str:
    magnitude = abs(delta_ratio)
    if metric in REGRESSION_METRICS and delta_ratio < -0.1:
        return "critical"
    if magnitude >= 0.2:
        return "warning"
    return "info"


def _build_provenance(
    plan: ReportPlan,
    chart_result: ChartResult,
    artifact_id: str,
) -> ProvenanceRecord:
    definition = get_metric_definition(chart_result.spec.metric)
    filters_applied = {
        "teams": ",".join(chart_result.spec.filter_teams),
        "repos": ",".join(chart_result.spec.filter_repos),
        "group_by": chart_result.spec.group_by or "total",
    }
    return ProvenanceRecord(
        provenance_id=_provenance_id(plan.plan_id, artifact_id),
        artifact_type="insight",
        artifact_id=artifact_id,
        plan_id=plan.plan_id,
        data_sources=[definition.source_table] if definition else [],
        metrics_used=[chart_result.spec.metric],
        time_range_start=chart_result.spec.time_range_start,
        time_range_end=chart_result.spec.time_range_end,
        filters_applied=filters_applied,
        generated_at=datetime.now(UTC),
        generator_version="reports.v1",
        org_id=plan.org_id,
    )


def generate_insights(
    plan: ReportPlan,
    chart_results: list[ChartResult],
) -> tuple[list[InsightBlock], list[ProvenanceRecord]]:
    insights: list[InsightBlock] = []
    provenance: list[ProvenanceRecord] = []

    for chart_result in chart_results:
        if chart_result.empty:
            continue
        series = _numeric_series(chart_result)
        if not series:
            continue

        metric = chart_result.spec.metric
        first_value = series[0]
        last_value = series[-1]
        delta_ratio = _delta_ratio(first_value, last_value)

        if delta_ratio is not None and abs(delta_ratio) > 0.1:
            insight_id = _artifact_id(plan.plan_id, metric, "trend_delta")
            insights.append(
                InsightBlock(
                    insight_id=insight_id,
                    plan_id=plan.plan_id,
                    insight_type="trend_delta",
                    confidence="direct_fact",
                    summary=(
                        f"{metric.replace('_', ' ').title()} appears {_format_delta(delta_ratio)} "
                        f"from the opening value to the latest value in this window."
                    ),
                    supporting_metrics=[metric],
                    supporting_values={metric: last_value},
                    severity=_severity(metric, delta_ratio),
                    org_id=plan.org_id,
                )
            )
            provenance.append(_build_provenance(plan, chart_result, insight_id))

        if (
            metric in REGRESSION_METRICS
            and delta_ratio is not None
            and delta_ratio < -0.02
        ):
            insight_id = _artifact_id(plan.plan_id, metric, "regression")
            insights.append(
                InsightBlock(
                    insight_id=insight_id,
                    plan_id=plan.plan_id,
                    insight_type="regression",
                    confidence="direct_fact",
                    summary=(
                        f"{metric.replace('_', ' ').title()} appears lower than the opening value, "
                        f"which suggests a regression over the selected window."
                    ),
                    supporting_metrics=[metric],
                    supporting_values={metric: last_value},
                    severity="critical" if delta_ratio < -0.1 else "warning",
                    org_id=plan.org_id,
                )
            )
            provenance.append(_build_provenance(plan, chart_result, insight_id))

        if len(series) >= 3:
            mean_value = statistics.fmean(series)
            stdev = statistics.pstdev(series)
            if stdev > 0:
                anomaly_value = None
                for value in series:
                    z_score = abs((value - mean_value) / stdev)
                    if z_score > 2:
                        anomaly_value = value
                        break
                if anomaly_value is not None:
                    insight_id = _artifact_id(plan.plan_id, metric, "anomaly")
                    insights.append(
                        InsightBlock(
                            insight_id=insight_id,
                            plan_id=plan.plan_id,
                            insight_type="anomaly",
                            confidence="inferred",
                            summary=(
                                f"{metric.replace('_', ' ').title()} includes an outlier value that appears "
                                f"materially outside the normal range for this window."
                            ),
                            supporting_metrics=[metric],
                            supporting_values={metric: anomaly_value},
                            severity="warning",
                            org_id=plan.org_id,
                        )
                    )
                    provenance.append(_build_provenance(plan, chart_result, insight_id))

    return insights, provenance
