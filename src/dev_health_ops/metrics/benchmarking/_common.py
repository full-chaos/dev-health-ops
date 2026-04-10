from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

ALIAS_METRICS: dict[str, str] = {
    "pipeline_success": "success_rate",
    "defect_rate": "defect_intro_rate",
}

NEGATIVE_DIRECTION_METRICS = {
    "failure_rate",
    "flake_rate",
    "retry_dependency_rate",
    "failure_recurrence_score",
    "rerun_rate",
    "median_duration_seconds",
    "p95_duration_seconds",
    "avg_queue_seconds",
    "p95_queue_seconds",
    "coverage_regression_count",
    "cycle_time_hours",
    "defect_intro_rate",
}


@dataclass(frozen=True)
class MetricPoint:
    day: date
    value: float


@dataclass(frozen=True)
class MetricDefinition:
    table: str
    value_column: str
    scope_support: frozenset[str]
    inner_group_columns: tuple[str, ...]
    extra_filters: tuple[str, ...] = ()


METRIC_DEFINITIONS: dict[str, MetricDefinition] = {
    "success_rate": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="success_rate",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "failure_rate": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="failure_rate",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "rerun_rate": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="rerun_rate",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "median_duration_seconds": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="median_duration_seconds",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "p95_duration_seconds": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="p95_duration_seconds",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "avg_queue_seconds": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="avg_queue_seconds",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "p95_queue_seconds": MetricDefinition(
        table="testops_pipeline_metrics_daily",
        value_column="p95_queue_seconds",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "pass_rate": MetricDefinition(
        table="testops_test_metrics_daily",
        value_column="pass_rate",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "flake_rate": MetricDefinition(
        table="testops_test_metrics_daily",
        value_column="flake_rate",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "retry_dependency_rate": MetricDefinition(
        table="testops_test_metrics_daily",
        value_column="retry_dependency_rate",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "failure_recurrence_score": MetricDefinition(
        table="testops_test_metrics_daily",
        value_column="failure_recurrence_score",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "line_coverage_pct": MetricDefinition(
        table="testops_coverage_metrics_daily",
        value_column="line_coverage_pct",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "branch_coverage_pct": MetricDefinition(
        table="testops_coverage_metrics_daily",
        value_column="branch_coverage_pct",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "coverage_delta_pct": MetricDefinition(
        table="testops_coverage_metrics_daily",
        value_column="coverage_delta_pct",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "coverage_regression_count": MetricDefinition(
        table="testops_coverage_metrics_daily",
        value_column="coverage_regression_count",
        scope_support=frozenset({"repo", "team", "global"}),
        inner_group_columns=("repo_id", "team_id", "service_id"),
    ),
    "cycle_time_hours": MetricDefinition(
        table="work_item_metrics_daily",
        value_column="cycle_time_p50_hours",
        scope_support=frozenset({"team", "global"}),
        inner_group_columns=("team_id", "work_scope_id", "provider"),
    ),
    "defect_intro_rate": MetricDefinition(
        table="work_item_metrics_daily",
        value_column="defect_intro_rate",
        scope_support=frozenset({"team", "global"}),
        inner_group_columns=("team_id", "work_scope_id", "provider"),
    ),
    "deployment_frequency": MetricDefinition(
        table="dora_metrics_daily",
        value_column="value",
        scope_support=frozenset({"repo", "global"}),
        inner_group_columns=("repo_id",),
        extra_filters=("metric_name = {metric_name:String}",),
    ),
}


def canonical_metric_name(metric_name: str) -> str:
    return ALIAS_METRICS.get(metric_name, metric_name)


def metric_is_negative(metric_name: str) -> bool:
    return canonical_metric_name(metric_name) in NEGATIVE_DIRECTION_METRICS


def percentile(values: list[float], pct: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * (pct / 100.0)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(ordered[lower])
    weight = rank - lower
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * weight)


def percentile_rank(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    lower = sum(1 for candidate in values if candidate < value)
    equal = sum(
        1 for candidate in values if math.isclose(candidate, value, abs_tol=1e-9)
    )
    return ((lower + (equal * 0.5)) / len(values)) * 100.0


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def population_stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def pearson_correlation(xs: list[float], ys: list[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    x_mean = mean(xs)
    y_mean = mean(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=True))
    x_denom = math.sqrt(sum((x - x_mean) ** 2 for x in xs))
    y_denom = math.sqrt(sum((y - y_mean) ** 2 for y in ys))
    if math.isclose(x_denom, 0.0, abs_tol=1e-12) or math.isclose(
        y_denom, 0.0, abs_tol=1e-12
    ):
        return 0.0
    return numerator / (x_denom * y_denom)


def fisher_two_tailed_p_value(r_value: float, sample_size: int) -> float:
    if sample_size < 4:
        return 1.0
    bounded_r = max(min(r_value, 0.999999), -0.999999)
    z_score = 0.5 * math.log((1.0 + bounded_r) / (1.0 - bounded_r))
    z_score *= math.sqrt(sample_size - 3)
    return math.erfc(abs(z_score) / math.sqrt(2.0))


def align_series(
    left: list[MetricPoint], right: list[MetricPoint]
) -> tuple[list[float], list[float], list[date]]:
    left_by_day = {point.day: point.value for point in left}
    right_by_day = {point.day: point.value for point in right}
    common_days = sorted(set(left_by_day) & set(right_by_day))
    return (
        [left_by_day[day] for day in common_days],
        [right_by_day[day] for day in common_days],
        common_days,
    )


def fetch_metric_series(
    sink: BaseMetricsSink,
    *,
    metric_name: str,
    start_day: date,
    end_day: date,
    scope_type: str,
    scope_key: str,
) -> list[MetricPoint]:
    return fetch_metric_series_by_scope(
        sink,
        metric_name=metric_name,
        start_day=start_day,
        end_day=end_day,
        scope_type=scope_type,
        scope_key=scope_key,
    ).get(scope_key, [])


def fetch_metric_series_by_scope(
    sink: BaseMetricsSink,
    *,
    metric_name: str,
    start_day: date,
    end_day: date,
    scope_type: str,
    scope_key: str | None = None,
) -> dict[str, list[MetricPoint]]:
    canonical_name = canonical_metric_name(metric_name)
    definition = METRIC_DEFINITIONS.get(canonical_name)
    if definition is None:
        raise ValueError(f"Unsupported metric: {metric_name}")
    if scope_type not in definition.scope_support:
        raise ValueError(
            f"Metric {canonical_name} does not support scope_type={scope_type}"
        )

    params: dict[str, object] = {
        "start_day": start_day,
        "end_day": end_day,
    }
    filters = ["day >= {start_day:Date}", "day <= {end_day:Date}"]
    for extra_filter in definition.extra_filters:
        filters.append(extra_filter)
    if canonical_name == "deployment_frequency":
        params["metric_name"] = "deployment_frequency"

    if scope_type == "repo":
        scope_expr = "toString(repo_id)"
        scope_filter_column = "repo_id"
    elif scope_type == "team":
        scope_expr = "ifNull(team_id, '')"
        scope_filter_column = "team_id"
        filters.append("ifNull(team_id, '') != ''")
    else:
        scope_expr = "'global'"
        scope_filter_column = ""

    if scope_key is not None:
        if scope_type == "repo":
            params["scope_uuid"] = scope_key
            filters.append(f"{scope_filter_column} = {{scope_uuid:UUID}}")
        elif scope_type == "team":
            params["scope_key"] = scope_key
            filters.append(f"{scope_filter_column} = {{scope_key:String}}")

    inner_group_columns = ["day", *definition.inner_group_columns]
    inner_group = ", ".join(inner_group_columns)
    where_clause = " AND ".join(filters)

    query = f"""
    SELECT
        scope_key,
        day,
        avg(metric_value) AS value
    FROM (
        SELECT
            {scope_expr} AS scope_key,
            day,
            argMax({definition.value_column}, computed_at) AS metric_value
        FROM {definition.table}
        WHERE {where_clause}
        GROUP BY {inner_group}
    )
    WHERE metric_value IS NOT NULL
    GROUP BY scope_key, day
    ORDER BY scope_key, day
    """

    rows = sink.query_dicts(query, params)
    result: dict[str, list[MetricPoint]] = defaultdict(list)
    for row in rows:
        current_scope = str(row.get("scope_key") or "")
        if not current_scope:
            continue
        value = row.get("value")
        row_day = row.get("day")
        if row_day is None or value is None:
            continue
        result[current_scope].append(MetricPoint(day=row_day, value=float(value)))
    return dict(result)
