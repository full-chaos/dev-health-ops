from __future__ import annotations

from datetime import date, datetime, timezone
import math
from typing import Any, Dict, List

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from ..models.filters import MetricFilter
from ..models.schemas import (
    ConstraintCard,
    ConstraintEvidence,
    Coverage,
    EventItem,
    Freshness,
    HomeResponse,
    MetricDelta,
    SparkPoint,
    SummarySentence,
)
from ..queries.client import clickhouse_client
from ..queries.explain import fetch_metric_driver_delta
from ..queries.freshness import fetch_coverage, fetch_last_ingested_at
from ..queries.metrics import (
    fetch_blocked_hours,
    fetch_metric_series,
    fetch_metric_value,
)
from .filtering import filter_cache_key, scope_filter_for_metric, time_window
from .cache import TTLCache


_METRICS = [
    {
        "metric": "cycle_time",
        "label": "Cycle Time",
        "unit": "days",
        "table": "work_item_metrics_daily",
        "column": "cycle_time_p50_hours",
        "aggregator": "avg",
        "transform": lambda v: v / 24.0,
        "scope": "team",
    },
    {
        "metric": "review_latency",
        "label": "Review Latency",
        "unit": "hours",
        "table": "repo_metrics_daily",
        "column": "pr_first_review_p50_hours",
        "aggregator": "avg",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "throughput",
        "label": "Throughput",
        "unit": "items",
        "table": "work_item_metrics_daily",
        "column": "items_completed",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "team",
    },
    {
        "metric": "deploy_freq",
        "label": "Deploy Frequency",
        "unit": "deploys",
        "table": "deploy_metrics_daily",
        "column": "deployments_count",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "churn",
        "label": "Code Churn",
        "unit": "loc",
        "table": "repo_metrics_daily",
        "column": "total_loc_touched",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "wip_saturation",
        "label": "WIP Saturation",
        "unit": "%",
        "table": "work_item_metrics_daily",
        "column": "wip_congestion_ratio",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "team",
    },
    {
        "metric": "blocked_work",
        "label": "Blocked Work",
        "unit": "hours",
        "table": "work_item_state_durations_daily",
        "column": "duration_hours",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "team",
    },
    {
        "metric": "change_failure_rate",
        "label": "Change Failure Rate",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "change_failure_rate",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
    {
        "metric": "rework_ratio",
        "label": "Rework Ratio",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "rework_churn_ratio_30d",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
    {
        "metric": "ci_success",
        "label": "CI Success Rate",
        "unit": "%",
        "table": "cicd_metrics_daily",
        "column": "success_rate",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
]


def _delta_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return (current - previous) / previous * 100.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _safe_transform(transform, value: float) -> float:
    return _safe_float(transform(value))


def _spark_points(rows: List[Dict[str, Any]], transform) -> List[SparkPoint]:
    points = []
    for row in rows:
        value = _safe_float(row.get("value"))
        points.append(
            SparkPoint(ts=row["day"], value=_safe_transform(transform, value))
        )
    return points


def _direction(delta_pct: float) -> str:
    if delta_pct > 0:
        return "rose"
    if delta_pct < 0:
        return "fell"
    return "held steady"


def _format_delta(delta_pct: float) -> str:
    return f"{abs(delta_pct):.0f}%"


async def _metric_deltas(
    sink: BaseMetricsSink,
    filters: MetricFilter,
    start_day: date,
    end_day: date,
    compare_start: date,
    compare_end: date,
) -> List[MetricDelta]:
    deltas: List[MetricDelta] = []

    for metric in _METRICS:
        scope_filter, scope_params = await scope_filter_for_metric(
            sink, metric_scope=metric["scope"], filters=filters
        )

        if metric["metric"] == "blocked_work":
            current_value, current_series = await fetch_blocked_hours(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            previous_value, _ = await fetch_blocked_hours(
                sink,
                start_day=compare_start,
                end_day=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            current_value = _safe_float(current_value)
            previous_value = _safe_float(previous_value)
            spark = _spark_points(current_series, metric["transform"])
        else:
            current_value = await fetch_metric_value(
                sink,
                table=metric["table"],
                column=metric["column"],
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                aggregator=metric["aggregator"],
            )
            previous_value = await fetch_metric_value(
                sink,
                table=metric["table"],
                column=metric["column"],
                start_day=compare_start,
                end_day=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
                aggregator=metric["aggregator"],
            )
            current_value = _safe_float(current_value)
            previous_value = _safe_float(previous_value)
            series = await fetch_metric_series(
                sink,
                table=metric["table"],
                column=metric["column"],
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                aggregator=metric["aggregator"],
            )
            spark = _spark_points(series, metric["transform"])

        delta_pct = _safe_float(_delta_pct(current_value, previous_value))
        deltas.append(
            MetricDelta(
                metric=metric["metric"],
                label=metric["label"],
                value=_safe_transform(metric["transform"], current_value),
                unit=metric["unit"],
                delta_pct=delta_pct,
                spark=spark,
            )
        )

    return deltas


def _select_constraint(deltas: List[MetricDelta]) -> MetricDelta:
    if not deltas:
        return MetricDelta(
            metric="cycle_time",
            label="Cycle Time",
            value=0.0,
            unit="days",
            delta_pct=0.0,
            spark=[],
        )
    return sorted(deltas, key=lambda d: d.delta_pct)[-1]


async def build_home_response(
    *,
    db_url: str,
    filters: MetricFilter,
    cache: TTLCache,
) -> HomeResponse:
    cache_key = filter_cache_key("home", filters)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    start_day, end_day, compare_start, compare_end = time_window(filters)

    async with clickhouse_client(db_url) as sink:
        last_ingested = await fetch_last_ingested_at(sink)
        coverage = await fetch_coverage(sink, start_day=start_day, end_day=end_day)
        deltas = await _metric_deltas(
            sink,
            filters,
            start_day,
            end_day,
            compare_start,
            compare_end,
        )

        sources = {
            "github": "ok" if last_ingested else "down",
            "gitlab": "ok" if last_ingested else "down",
            "jira": "ok" if last_ingested else "down",
            "ci": "ok" if last_ingested else "down",
        }

        summary_sentences: List[SummarySentence] = []
        top_delta = max(deltas, key=lambda d: abs(d.delta_pct), default=None)
        if top_delta:
            scope_filter, scope_params = await scope_filter_for_metric(
                sink, metric_scope=_metric_scope(top_delta.metric), filters=filters
            )

            driver_rows = await fetch_metric_driver_delta(
                sink,
                table=_metric_table(top_delta.metric),
                column=_metric_column(top_delta.metric),
                group_by=_metric_group(top_delta.metric),
                start_day=start_day,
                end_day=end_day,
                compare_start=compare_start,
                compare_end=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            driver_labels = ", ".join(
                [str(row.get("id")) for row in driver_rows if row.get("id")] or []
            )
            driver_text = f" driven by {driver_labels}." if driver_labels else "."
            summary_sentences.append(
                SummarySentence(
                    id="s1",
                    text=(
                        f"{top_delta.label} {_direction(top_delta.delta_pct)} "
                        f"{_format_delta(top_delta.delta_pct)}{driver_text}"
                    ),
                    evidence_link=(
                        f"/api/v1/explain?metric={top_delta.metric}"
                        f"&scope_type={filters.scope.level}"
                        f"&scope_id={_primary_scope_id(filters)}"
                        f"&range_days={filters.time.range_days}"
                        f"&compare_days={filters.time.compare_days}"
                    ),
                )
            )

        constraint_metric = _select_constraint(deltas)
        constraint = ConstraintCard(
            title=f"This week's constraint: {constraint_metric.label}",
            claim=(
                f"{constraint_metric.label} {_direction(constraint_metric.delta_pct)} "
                f"{_format_delta(constraint_metric.delta_pct)} over the last {filters.time.range_days} days."
            ),
            evidence=[
                ConstraintEvidence(
                    label=f"Drill into {constraint_metric.label}",
                    link=(
                        f"/api/v1/explain?metric={constraint_metric.metric}"
                        f"&scope_type={filters.scope.level}"
                        f"&scope_id={_primary_scope_id(filters)}"
                        f"&range_days={filters.time.range_days}"
                        f"&compare_days={filters.time.compare_days}"
                    ),
                )
            ],
            experiments=[
                "Rebalance reviewer rotation to reduce queueing.",
                "Set WIP limits per team and auto-alert at saturation.",
            ],
        )

        events: List[EventItem] = []
        for delta in deltas:
            if abs(delta.delta_pct) >= 25:
                events.append(
                    EventItem(
                        ts=datetime.now(timezone.utc),
                        type="regression" if delta.delta_pct > 0 else "spike",
                        text=(
                            f"{delta.label} shifted {delta.delta_pct:.0f}% "
                            f"over the last {filters.time.range_days} days."
                        ),
                        link=(
                            f"/api/v1/explain?metric={delta.metric}"
                            f"&scope_type={filters.scope.level}"
                            f"&scope_id={_primary_scope_id(filters)}"
                            f"&range_days={filters.time.range_days}"
                            f"&compare_days={filters.time.compare_days}"
                        ),
                    )
                )

        tiles = {
            "understand": {
                "title": "Understand",
                "subtitle": "Flow stages",
                "link": "/explore?view=understand",
            },
            "measure": {
                "title": "Measure",
                "subtitle": "Coverage & freshness",
                "link": "/explore?view=measure",
            },
            "align": {
                "title": "Align",
                "subtitle": "Investment mix",
                "link": "/investment",
            },
            "execute": {
                "title": "Execute",
                "subtitle": "Top opportunities",
                "link": "/opportunities",
            },
        }

        response = HomeResponse(
            freshness=Freshness(
                last_ingested_at=last_ingested,
                sources=sources,
                coverage=Coverage(**coverage),
            ),
            deltas=deltas,
            summary=summary_sentences,
            tiles=tiles,
            constraint=constraint,
            events=events,
        )

    cache.set(cache_key, response)
    return response


def _metric_table(metric: str) -> str:
    return next(
        (cfg["table"] for cfg in _METRICS if cfg["metric"] == metric),
        "repo_metrics_daily",
    )


def _metric_column(metric: str) -> str:
    return next(
        (cfg["column"] for cfg in _METRICS if cfg["metric"] == metric),
        "pr_first_review_p50_hours",
    )


def _metric_group(metric: str) -> str:
    if metric in {"cycle_time", "throughput", "wip_saturation", "blocked_work"}:
        return "team_id"
    return "repo_id"


def _metric_scope(metric: str) -> str:
    if metric in {"cycle_time", "throughput", "wip_saturation", "blocked_work"}:
        return "team"
    return "repo"


def _primary_scope_id(filters: MetricFilter) -> str:
    if filters.scope.ids:
        return filters.scope.ids[0]
    return ""
