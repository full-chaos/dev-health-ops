from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta

from dev_health_ops.metrics.benchmarking._common import (
    MetricPoint,
    fetch_metric_series_by_scope,
    mean,
    percentile,
    percentile_rank,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.testops_schemas import BenchmarkBaselineRecord


def _latest_value_on_or_before(
    points: Sequence[MetricPoint], as_of_day: date
) -> float | None:
    eligible = [point for point in points if point.day <= as_of_day]
    if not eligible:
        return None
    return eligible[-1].value


def _window_values(
    points: Sequence[MetricPoint], *, as_of_day: date, window_days: int
) -> list[float]:
    start_day = as_of_day - timedelta(days=window_days - 1)
    return [point.value for point in points if start_day <= point.day <= as_of_day]


def compute_internal_baselines(
    *,
    metric_name: str,
    scope_type: str,
    series_by_scope: Mapping[str, Sequence[MetricPoint]],
    as_of_day: date,
    computed_at: datetime,
    windows: Iterable[int] = (30, 60, 90),
    org_id: str = "",
) -> list[BenchmarkBaselineRecord]:
    current_values_by_scope: dict[str, float] = {}
    for scope_key, points in series_by_scope.items():
        latest_value = _latest_value_on_or_before(points, as_of_day)
        if latest_value is not None:
            current_values_by_scope[scope_key] = latest_value

    if not current_values_by_scope:
        return []

    cross_section_values = list(current_values_by_scope.values())
    p25_value = percentile(cross_section_values, 25.0)
    p50_value = percentile(cross_section_values, 50.0)
    p75_value = percentile(cross_section_values, 75.0)
    p90_value = percentile(cross_section_values, 90.0)

    results: list[BenchmarkBaselineRecord] = []
    for scope_key, points in sorted(series_by_scope.items()):
        latest_value = current_values_by_scope.get(scope_key)
        if latest_value is None:
            continue
        for window_days in windows:
            values = _window_values(
                points, as_of_day=as_of_day, window_days=window_days
            )
            if not values:
                continue
            period_start = as_of_day - timedelta(days=window_days - 1)
            results.append(
                BenchmarkBaselineRecord(
                    metric_name=metric_name,
                    scope_type=scope_type,
                    scope_key=scope_key,
                    period_start=period_start,
                    period_end=as_of_day,
                    rolling_window_days=window_days,
                    current_value=round(latest_value, 4),
                    baseline_value=round(mean(values), 4),
                    percentile_rank=round(
                        percentile_rank(cross_section_values, latest_value), 4
                    ),
                    p25_value=round(p25_value, 4),
                    p50_value=round(p50_value, 4),
                    p75_value=round(p75_value, 4),
                    p90_value=round(p90_value, 4),
                    sample_size=len(cross_section_values),
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )
    return results


def build_internal_baselines_from_clickhouse(
    sink: BaseMetricsSink,
    *,
    metric_name: str,
    scope_type: str,
    as_of_day: date,
    computed_at: datetime,
    windows: Iterable[int] = (30, 60, 90),
    org_id: str = "",
) -> list[BenchmarkBaselineRecord]:
    max_window = max(windows, default=30)
    start_day = as_of_day - timedelta(days=max_window - 1)
    series_by_scope = fetch_metric_series_by_scope(
        sink,
        metric_name=metric_name,
        start_day=start_day,
        end_day=as_of_day,
        scope_type=scope_type,
    )
    return compute_internal_baselines(
        metric_name=metric_name,
        scope_type=scope_type,
        series_by_scope=series_by_scope,
        as_of_day=as_of_day,
        computed_at=computed_at,
        windows=windows,
        org_id=org_id,
    )
