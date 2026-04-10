from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta

from dev_health_ops.metrics.benchmarking._common import (
    MetricPoint,
    fetch_metric_series_by_scope,
    mean,
    metric_is_negative,
    population_stdev,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.testops_schemas import BenchmarkAnomalyRecord


def _severity_from_z_score(z_score: float) -> str:
    magnitude = abs(z_score)
    if magnitude >= 3.0:
        return "critical"
    if magnitude >= 2.0:
        return "warning"
    return "info"


def _anomaly_direction(metric_name: str, delta: float) -> str:
    if math.isclose(delta, 0.0, abs_tol=1e-9):
        return "stable"
    improving = delta < 0 if metric_is_negative(metric_name) else delta > 0
    if improving:
        return "down" if metric_is_negative(metric_name) else "up"
    return "up" if metric_is_negative(metric_name) else "down"


def detect_metric_anomalies(
    *,
    metric_name: str,
    scope_type: str,
    series_by_scope: Mapping[str, Sequence[MetricPoint]],
    as_of_day: date,
    computed_at: datetime,
    rolling_window_days: int = 30,
    z_threshold: float = 2.0,
    volatility_threshold: float = 0.5,
    min_history_points: int = 5,
    org_id: str = "",
) -> list[BenchmarkAnomalyRecord]:
    results: list[BenchmarkAnomalyRecord] = []
    history_start = as_of_day - timedelta(days=rolling_window_days)
    for scope_key, points in sorted(series_by_scope.items()):
        ordered_points = sorted(points, key=lambda point: point.day)
        current_point = next(
            (point for point in reversed(ordered_points) if point.day <= as_of_day),
            None,
        )
        if current_point is None:
            continue
        history = [
            point.value
            for point in ordered_points
            if history_start <= point.day < current_point.day
        ]
        if len(history) < min_history_points:
            continue

        baseline_value = mean(history)
        stdev = population_stdev(history)
        if math.isclose(stdev, 0.0, abs_tol=1e-12):
            z_score = (
                0.0
                if math.isclose(current_point.value, baseline_value, abs_tol=1e-9)
                else 3.0
            )
        else:
            z_score = (current_point.value - baseline_value) / stdev

        delta = current_point.value - baseline_value
        volatility_score = 0.0
        denominator = abs(baseline_value)
        if not math.isclose(denominator, 0.0, abs_tol=1e-12):
            volatility_score = stdev / denominator

        if abs(z_score) >= z_threshold:
            improving = delta < 0 if metric_is_negative(metric_name) else delta > 0
            results.append(
                BenchmarkAnomalyRecord(
                    metric_name=metric_name,
                    scope_type=scope_type,
                    scope_key=scope_key,
                    day=current_point.day,
                    value=round(current_point.value, 4),
                    baseline_value=round(baseline_value, 4),
                    z_score=round(z_score, 4),
                    anomaly_type="improvement" if improving else "regression",
                    direction=_anomaly_direction(metric_name, delta),
                    severity=_severity_from_z_score(z_score),
                    volatility_score=round(volatility_score, 4),
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )

        if volatility_score >= volatility_threshold:
            results.append(
                BenchmarkAnomalyRecord(
                    metric_name=metric_name,
                    scope_type=scope_type,
                    scope_key=scope_key,
                    day=current_point.day,
                    value=round(current_point.value, 4),
                    baseline_value=round(baseline_value, 4),
                    z_score=round(z_score, 4),
                    anomaly_type="volatility",
                    direction="volatile",
                    severity="warning" if volatility_score < 1.0 else "critical",
                    volatility_score=round(volatility_score, 4),
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )
    return results


def build_metric_anomalies_from_clickhouse(
    sink: BaseMetricsSink,
    *,
    metric_name: str,
    scope_type: str,
    as_of_day: date,
    computed_at: datetime,
    rolling_window_days: int = 30,
    z_threshold: float = 2.0,
    volatility_threshold: float = 0.5,
    min_history_points: int = 5,
    org_id: str = "",
) -> list[BenchmarkAnomalyRecord]:
    start_day = as_of_day - timedelta(days=rolling_window_days + min_history_points)
    series_by_scope = fetch_metric_series_by_scope(
        sink,
        metric_name=metric_name,
        start_day=start_day,
        end_day=as_of_day,
        scope_type=scope_type,
    )
    return detect_metric_anomalies(
        metric_name=metric_name,
        scope_type=scope_type,
        series_by_scope=series_by_scope,
        as_of_day=as_of_day,
        computed_at=computed_at,
        rolling_window_days=rolling_window_days,
        z_threshold=z_threshold,
        volatility_threshold=volatility_threshold,
        min_history_points=min_history_points,
        org_id=org_id,
    )
