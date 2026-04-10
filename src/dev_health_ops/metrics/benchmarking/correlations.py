from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime

from dev_health_ops.metrics.benchmarking._common import (
    MetricPoint,
    align_series,
    fetch_metric_series_by_scope,
    fisher_two_tailed_p_value,
    pearson_correlation,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.testops_schemas import MetricCorrelationRecord

DEFAULT_CORRELATION_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("flake_rate", "cycle_time_hours", "team"),
    ("line_coverage_pct", "defect_rate", "team"),
    ("pipeline_success", "deployment_frequency", "repo"),
)


def _interpretation(metric_name: str, paired_metric_name: str, r_value: float) -> str:
    strength = "weakly"
    magnitude = abs(r_value)
    if magnitude >= 0.8:
        strength = "strongly"
    elif magnitude >= 0.5:
        strength = "moderately"
    direction = "positively" if r_value >= 0 else "negatively"
    return (
        f"{metric_name} appears {strength} and {direction} correlated with "
        f"{paired_metric_name} over this window."
    )


def compute_metric_correlation(
    *,
    metric_name: str,
    paired_metric_name: str,
    scope_type: str,
    left_series_by_scope: Mapping[str, Sequence[MetricPoint]],
    right_series_by_scope: Mapping[str, Sequence[MetricPoint]],
    period_start: date,
    period_end: date,
    computed_at: datetime,
    min_points: int = 5,
    org_id: str = "",
) -> list[MetricCorrelationRecord]:
    results: list[MetricCorrelationRecord] = []
    for scope_key in sorted(set(left_series_by_scope) & set(right_series_by_scope)):
        left_values, right_values, common_days = align_series(
            list(left_series_by_scope[scope_key]),
            list(right_series_by_scope[scope_key]),
        )
        if len(common_days) < min_points:
            continue
        coefficient = pearson_correlation(left_values, right_values)
        p_value = fisher_two_tailed_p_value(coefficient, len(common_days))
        is_significant = abs(coefficient) > 0.5 and p_value < 0.05
        results.append(
            MetricCorrelationRecord(
                metric_name=metric_name,
                paired_metric_name=paired_metric_name,
                scope_type=scope_type,
                scope_key=scope_key,
                period_start=period_start,
                period_end=period_end,
                coefficient=round(coefficient, 4),
                p_value=round(p_value, 6),
                sample_size=len(common_days),
                is_significant=is_significant,
                interpretation=_interpretation(
                    metric_name, paired_metric_name, coefficient
                ),
                computed_at=computed_at,
                org_id=org_id,
            )
        )
    return results


def build_metric_correlation_from_clickhouse(
    sink: BaseMetricsSink,
    *,
    metric_name: str,
    paired_metric_name: str,
    scope_type: str,
    period_start: date,
    period_end: date,
    computed_at: datetime,
    min_points: int = 5,
    org_id: str = "",
) -> list[MetricCorrelationRecord]:
    left_series = fetch_metric_series_by_scope(
        sink,
        metric_name=metric_name,
        start_day=period_start,
        end_day=period_end,
        scope_type=scope_type,
    )
    right_series = fetch_metric_series_by_scope(
        sink,
        metric_name=paired_metric_name,
        start_day=period_start,
        end_day=period_end,
        scope_type=scope_type,
    )
    return compute_metric_correlation(
        metric_name=metric_name,
        paired_metric_name=paired_metric_name,
        scope_type=scope_type,
        left_series_by_scope=left_series,
        right_series_by_scope=right_series,
        period_start=period_start,
        period_end=period_end,
        computed_at=computed_at,
        min_points=min_points,
        org_id=org_id,
    )
