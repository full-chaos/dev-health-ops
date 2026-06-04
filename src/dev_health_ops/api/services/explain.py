from __future__ import annotations

from collections.abc import Callable
from typing import TypedDict

from ..models.filters import MetricFilter
from ..models.schemas import Contributor, ExplainResponse
from ..queries.client import clickhouse_client
from ..queries.explain import fetch_metric_contributors, fetch_metric_driver_delta
from ..queries.metrics import fetch_metric_value
from ..utils import delta_pct, safe_float, safe_transform
from .cache import TTLCache
from .filtering import filter_cache_key, scope_filter_for_metric, time_window
from .identity import (
    looks_like_uuid,
    resolve_scope_display_names,
    scope_kind_for_group_by,
)


class _MetricConfig(TypedDict):
    label: str
    unit: str
    table: str
    column: str
    group_by: str
    scope: str
    aggregator: str
    transform: Callable[[float], float]


_METRIC_CONFIG: dict[str, _MetricConfig] = {
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


async def build_explain_response(
    *,
    db_url: str,
    metric: str,
    filters: MetricFilter,
    cache: TTLCache,
    org_id: str = "",
) -> ExplainResponse:
    cache_key = filter_cache_key("explain", org_id, filters, extra={"metric": metric})
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
            org_id=org_id,
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
            org_id=org_id,
        )

        current_value = safe_float(current_value)
        previous_value = safe_float(previous_value)
        pct_change = safe_float(delta_pct(current_value, previous_value))

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
            org_id=org_id,
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
            org_id=org_id,
        )
        # Resolve scope ids -> display names server-side so labels never carry
        # a bare UUID (Framework A7/A8). group_by is repo_id or team_id.
        scope_kind = scope_kind_for_group_by(config["group_by"])
        all_ids = [str(r.get("id") or "") for r in (*drivers, *contributors)]
        display_names = await resolve_scope_display_names(
            sink,
            org_id=org_id,
            scope=scope_kind,
            ids=all_ids,
        )

    driver_models: list[Contributor] = [
        _build_contributor(
            row,
            metric=metric,
            filters=filters,
            transform=config["transform"],
            display_names=display_names,
            delta_value=safe_float(row.get("delta_pct")),
        )
        for row in drivers
    ]

    contributor_models: list[Contributor] = [
        _build_contributor(
            row,
            metric=metric,
            filters=filters,
            transform=config["transform"],
            display_names=display_names,
            delta_value=0.0,
        )
        for row in contributors
    ]

    response = ExplainResponse(
        metric=metric,
        label=config["label"],
        unit=config["unit"],
        value=safe_transform(config["transform"], current_value),
        delta_pct=pct_change,
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


def _short_token(scope_id: str) -> str:
    """Controlled fallback for an unresolved id (never a bare UUID).

    Mirrors the cockpit contract: when the server cannot resolve a human name,
    surface a short, stable, non-UUID token (e.g. ``#698c0211``) plus the
    structured ``display_name=None`` so the client renders its Unresolved badge
    rather than leaking a raw UUID as the primary label (A8).
    """
    token = scope_id.replace("-", "")[:8]
    return f"#{token}" if token else "Unknown"


def _build_contributor(
    row: dict[str, object],
    *,
    metric: str,
    filters: MetricFilter,
    transform: Callable[[float], float],
    display_names: dict[str, str],
    delta_value: float,
) -> Contributor:
    scope_id = str(row.get("id") or "")
    resolved = display_names.get(scope_id)
    # A8: a bare UUID is never a valid label; fall back to a controlled token.
    if resolved and not looks_like_uuid(resolved):
        label = resolved
        display_name: str | None = resolved
    else:
        label = _short_token(scope_id)
        display_name = None
    raw_value = safe_float(row.get("value"))
    return Contributor(
        id=scope_id,
        label=label,
        display_name=display_name,
        value=safe_transform(transform, raw_value),
        delta_pct=delta_value,
        evidence_link=(
            f"/api/v1/drilldown/prs?metric={metric}"
            f"&scope_type={filters.scope.level}"
            f"&scope_id={_primary_scope_id(filters)}"
        ),
    )
