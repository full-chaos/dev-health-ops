from __future__ import annotations

import json
import uuid
from datetime import date, datetime

from dev_health_ops.metrics.benchmarking.anomalies import (
    build_metric_anomalies_from_clickhouse,
    detect_metric_anomalies,
)
from dev_health_ops.metrics.benchmarking.baselines import (
    build_internal_baselines_from_clickhouse,
    compute_internal_baselines,
)
from dev_health_ops.metrics.benchmarking.correlations import (
    DEFAULT_CORRELATION_PAIRS,
    build_metric_correlation_from_clickhouse,
    compute_metric_correlation,
)
from dev_health_ops.metrics.benchmarking.maturity import classify_maturity_bands
from dev_health_ops.metrics.benchmarking.period_comparison import (
    build_period_comparison_from_clickhouse,
    compute_period_comparison,
)
from dev_health_ops.metrics.testops_schemas import (
    BenchmarkAnomalyRecord,
    BenchmarkInsightRecord,
    MetricCorrelationRecord,
    PeriodComparisonRecord,
)


def _insight_id(*parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, ":".join(parts)))


def generate_benchmark_insights(
    *,
    period_comparisons: list[PeriodComparisonRecord],
    anomalies: list[BenchmarkAnomalyRecord],
    correlations: list[MetricCorrelationRecord],
    computed_at: datetime,
) -> list[BenchmarkInsightRecord]:
    insights: list[BenchmarkInsightRecord] = []

    for comparison in period_comparisons:
        if comparison.trend_direction == "stable":
            continue
        if (
            comparison.percentage_change is not None
            and abs(comparison.percentage_change) < 5.0
        ):
            continue
        summary = (
            f"{comparison.metric_name} appears {comparison.trend_direction} versus the prior period, "
            f"which suggests a {comparison.absolute_delta:.2f} shift over the selected window."
        )
        insights.append(
            BenchmarkInsightRecord(
                insight_id=_insight_id(
                    "comparison",
                    comparison.metric_name,
                    comparison.scope_type,
                    comparison.scope_key,
                    comparison.current_period_end.isoformat(),
                ),
                insight_type="comparison",
                scope_type=comparison.scope_type,
                scope_key=comparison.scope_key,
                metric_name=comparison.metric_name,
                paired_metric_name=None,
                period_start=comparison.current_period_start,
                period_end=comparison.current_period_end,
                severity="warning"
                if comparison.trend_direction == "regressing"
                else "info",
                summary=summary,
                evidence_json=json.dumps(
                    {
                        "current_value": comparison.current_value,
                        "comparison_value": comparison.comparison_value,
                        "absolute_delta": comparison.absolute_delta,
                        "percentage_change": comparison.percentage_change,
                    }
                ),
                computed_at=computed_at,
                org_id=comparison.org_id,
            )
        )

    for anomaly in anomalies:
        summary = (
            f"{anomaly.metric_name} appears to lean {anomaly.anomaly_type} on {anomaly.day.isoformat()}, "
            f"which suggests the observed value moved away from its rolling baseline."
        )
        insights.append(
            BenchmarkInsightRecord(
                insight_id=_insight_id(
                    "anomaly",
                    anomaly.metric_name,
                    anomaly.scope_type,
                    anomaly.scope_key,
                    anomaly.day.isoformat(),
                    anomaly.anomaly_type,
                ),
                insight_type="anomaly",
                scope_type=anomaly.scope_type,
                scope_key=anomaly.scope_key,
                metric_name=anomaly.metric_name,
                paired_metric_name=None,
                period_start=anomaly.day,
                period_end=anomaly.day,
                severity=anomaly.severity,
                summary=summary,
                evidence_json=json.dumps(
                    {
                        "value": anomaly.value,
                        "baseline_value": anomaly.baseline_value,
                        "z_score": anomaly.z_score,
                        "volatility_score": anomaly.volatility_score,
                    }
                ),
                computed_at=computed_at,
                org_id=anomaly.org_id,
            )
        )

    for correlation in correlations:
        if not correlation.is_significant:
            continue
        insights.append(
            BenchmarkInsightRecord(
                insight_id=_insight_id(
                    "correlation",
                    correlation.metric_name,
                    correlation.paired_metric_name,
                    correlation.scope_type,
                    correlation.scope_key,
                    correlation.period_end.isoformat(),
                ),
                insight_type="correlation",
                scope_type=correlation.scope_type,
                scope_key=correlation.scope_key,
                metric_name=correlation.metric_name,
                paired_metric_name=correlation.paired_metric_name,
                period_start=correlation.period_start,
                period_end=correlation.period_end,
                severity="info",
                summary=correlation.interpretation,
                evidence_json=json.dumps(
                    {
                        "coefficient": correlation.coefficient,
                        "p_value": correlation.p_value,
                        "sample_size": correlation.sample_size,
                    }
                ),
                computed_at=computed_at,
                org_id=correlation.org_id,
            )
        )
    return insights


__all__ = [
    "DEFAULT_CORRELATION_PAIRS",
    "build_internal_baselines_from_clickhouse",
    "build_metric_anomalies_from_clickhouse",
    "build_metric_correlation_from_clickhouse",
    "build_period_comparison_from_clickhouse",
    "classify_maturity_bands",
    "compute_internal_baselines",
    "compute_metric_correlation",
    "compute_period_comparison",
    "detect_metric_anomalies",
    "generate_benchmark_insights",
]
