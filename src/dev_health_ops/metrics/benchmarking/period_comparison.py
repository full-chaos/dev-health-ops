from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, datetime

from dev_health_ops.metrics.benchmarking._common import (
    MetricPoint,
    fetch_metric_series,
    mean,
    metric_is_negative,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.testops_schemas import PeriodComparisonRecord


def compute_period_comparison(
    *,
    metric_name: str,
    scope_type: str,
    scope_key: str,
    current_period_start: date,
    current_period_end: date,
    comparison_period_start: date,
    comparison_period_end: date,
    current_period_points: Sequence[MetricPoint],
    comparison_period_points: Sequence[MetricPoint],
    computed_at: datetime,
    org_id: str = "",
) -> PeriodComparisonRecord | None:
    current_values = [point.value for point in current_period_points]
    comparison_values = [point.value for point in comparison_period_points]
    if not current_values or not comparison_values:
        return None

    current_value = mean(current_values)
    comparison_value = mean(comparison_values)
    absolute_delta = current_value - comparison_value

    percentage_change: float | None
    if math.isclose(comparison_value, 0.0, abs_tol=1e-9):
        percentage_change = None
    else:
        percentage_change = (absolute_delta / abs(comparison_value)) * 100.0

    if math.isclose(absolute_delta, 0.0, abs_tol=1e-9):
        trend_direction = "stable"
    elif metric_is_negative(metric_name):
        trend_direction = "improving" if absolute_delta < 0 else "regressing"
    else:
        trend_direction = "improving" if absolute_delta > 0 else "regressing"

    return PeriodComparisonRecord(
        metric_name=metric_name,
        scope_type=scope_type,
        scope_key=scope_key,
        current_period_start=current_period_start,
        current_period_end=current_period_end,
        comparison_period_start=comparison_period_start,
        comparison_period_end=comparison_period_end,
        current_value=round(current_value, 4),
        comparison_value=round(comparison_value, 4),
        absolute_delta=round(absolute_delta, 4),
        percentage_change=(
            round(percentage_change, 4) if percentage_change is not None else None
        ),
        trend_direction=trend_direction,
        computed_at=computed_at,
        org_id=org_id,
    )


def build_period_comparison_from_clickhouse(
    sink: BaseMetricsSink,
    *,
    metric_name: str,
    scope_type: str,
    scope_key: str,
    current_period_start: date,
    current_period_end: date,
    comparison_period_start: date,
    comparison_period_end: date,
    computed_at: datetime,
    org_id: str = "",
) -> PeriodComparisonRecord | None:
    current_series = fetch_metric_series(
        sink,
        metric_name=metric_name,
        start_day=current_period_start,
        end_day=current_period_end,
        scope_type=scope_type,
        scope_key=scope_key,
    )
    comparison_series = fetch_metric_series(
        sink,
        metric_name=metric_name,
        start_day=comparison_period_start,
        end_day=comparison_period_end,
        scope_type=scope_type,
        scope_key=scope_key,
    )
    return compute_period_comparison(
        metric_name=metric_name,
        scope_type=scope_type,
        scope_key=scope_key,
        current_period_start=current_period_start,
        current_period_end=current_period_end,
        comparison_period_start=comparison_period_start,
        comparison_period_end=comparison_period_end,
        current_period_points=current_series,
        comparison_period_points=comparison_series,
        computed_at=computed_at,
        org_id=org_id,
    )
