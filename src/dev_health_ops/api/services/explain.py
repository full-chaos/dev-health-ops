from __future__ import annotations

import math
from typing import Any, List

from ..models.filters import MetricFilter
from ..models.schemas import Contributor, ExplainResponse
from ..queries.client import clickhouse_client
from ..queries.explain import fetch_metric_contributors, fetch_metric_driver_delta
from ..queries.metrics import fetch_metric_value
from .cache import TTLCache
from .filtering import filter_cache_key, scope_filter_for_metric, time_window


_METRIC_CONFIG = {
    "cycle_time": {
        "label": "Cycle Time",
        "unit": "days",
        "table": "work_item_metrics_daily",
        "column": "cycle_time_p50_hours",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "avg",
        "transform": lambda v: v / 24.0,
    },
    "review_latency": {
        "label": "Review Latency",
        "unit": "hours",
        "table": "repo_metrics_daily",
        "column": "pr_first_review_p50_hours",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "avg",
        "transform": lambda v: v,
    },
    "throughput": {
        "label": "Throughput",
        "unit": "items",
        "table": "work_item_metrics_daily",
        "column": "items_completed",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "deploy_freq": {
        "label": "Deploy Frequency",
        "unit": "deploys",
        "table": "deploy_metrics_daily",
        "column": "deployments_count",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "churn": {
        "label": "Code Churn",
        "unit": "loc",
        "table": "repo_metrics_daily",
        "column": "total_loc_touched",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "wip_saturation": {
        "label": "WIP Saturation",
        "unit": "%",
        "table": "work_item_metrics_daily",
        "column": "wip_congestion_ratio",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
    },
    "blocked_work": {
        "label": "Blocked Work",
        "unit": "hours",
        "table": "work_item_state_durations_daily",
        "column": "duration_hours",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "change_failure_rate": {
        "label": "Change Failure Rate",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "change_failure_rate",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
    },
}


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


async def build_explain_response(
    *,
    db_url: str,
    metric: str,
    filters: MetricFilter,
    cache: TTLCache,
) -> ExplainResponse:
    cache_key = filter_cache_key("explain", filters, extra={"metric": metric})
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    config = _METRIC_CONFIG.get(metric, _METRIC_CONFIG["cycle_time"])
    start_day, end_day, compare_start, compare_end = time_window(filters)

    async with clickhouse_client(db_url) as sink:
        scope_filter, scope_params = await scope_filter_for_metric(
            sink, metric_scope=config["scope"], filters=filters
        )

        current_value = await fetch_metric_value(
            sink,
            table=config["table"],
            column=config["column"],
            start_day=start_day,
            end_day=end_day,
            scope_filter=scope_filter,
            scope_params=scope_params,
            aggregator=config["aggregator"],
        )
        previous_value = await fetch_metric_value(
            sink,
            table=config["table"],
            column=config["column"],
            start_day=compare_start,
            end_day=compare_end,
            scope_filter=scope_filter,
            scope_params=scope_params,
            aggregator=config["aggregator"],
        )

        current_value = _safe_float(current_value)
        previous_value = _safe_float(previous_value)
        delta_pct = _safe_float(_delta_pct(current_value, previous_value))

        drivers = await fetch_metric_driver_delta(
            sink,
            table=config["table"],
            column=config["column"],
            group_by=config["group_by"],
            start_day=start_day,
            end_day=end_day,
            compare_start=compare_start,
            compare_end=compare_end,
            scope_filter=scope_filter,
            scope_params=scope_params,
        )
        contributors = await fetch_metric_contributors(
            sink,
            table=config["table"],
            column=config["column"],
            group_by=config["group_by"],
            start_day=start_day,
            end_day=end_day,
            scope_filter=scope_filter,
            scope_params=scope_params,
        )

    driver_models: List[Contributor] = []
    for row in drivers:
        raw_value = _safe_float(row.get("value"))
        raw_delta = _safe_float(row.get("delta_pct"))
        driver_models.append(
            Contributor(
                id=str(row.get("id") or ""),
                label=str(row.get("id") or "Unknown"),
                value=_safe_transform(config["transform"], raw_value),
                delta_pct=raw_delta,
                evidence_link=(
                    f"/api/v1/drilldown/prs?metric={metric}"
                    f"&scope_type={filters.scope.level}"
                    f"&scope_id={_primary_scope_id(filters)}"
                ),
            )
        )

    contributor_models: List[Contributor] = []
    for row in contributors:
        raw_value = _safe_float(row.get("value"))
        contributor_models.append(
            Contributor(
                id=str(row.get("id") or ""),
                label=str(row.get("id") or "Unknown"),
                value=_safe_transform(config["transform"], raw_value),
                delta_pct=0.0,
                evidence_link=(
                    f"/api/v1/drilldown/prs?metric={metric}"
                    f"&scope_type={filters.scope.level}"
                    f"&scope_id={_primary_scope_id(filters)}"
                ),
            )
        )

    response = ExplainResponse(
        metric=metric,
        label=config["label"],
        unit=config["unit"],
        value=_safe_transform(config["transform"], current_value),
        delta_pct=delta_pct,
        drivers=driver_models,
        contributors=contributor_models,
        drilldown_links={
            "prs": f"/api/v1/drilldown/prs?metric={metric}",
            "issues": f"/api/v1/drilldown/issues?metric={metric}",
        },
    )

    cache.set(cache_key, response)
    return response


def _primary_scope_id(filters: MetricFilter) -> str:
    if filters.scope.ids:
        return filters.scope.ids[0]
    return ""
