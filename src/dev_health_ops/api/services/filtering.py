from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.utils.datetime import utc_today

from ..models.filters import MetricFilter
from ..queries.scopes import (
    build_scope_filter_multi,
    resolve_repo_ids,
    resolve_repo_ids_for_teams,
)


def filter_cache_key(
    prefix: str, org_id: str, filters: MetricFilter, extra: dict[str, Any] | None = None
) -> str:
    if hasattr(filters, "model_dump"):
        try:
            payload = filters.model_dump(mode="json")
        except TypeError:
            payload = filters.model_dump()
    else:
        payload = filters.dict()
    if extra:
        payload = {**payload, **extra}
    payload["_org_id"] = org_id
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return f"{prefix}:{serialized}"


def time_window(filters: MetricFilter) -> tuple[date, date, date, date]:
    range_days = max(1, filters.time.range_days)
    compare_days = max(1, filters.time.compare_days)
    end_date = filters.time.end_date or utc_today()
    start_date = filters.time.start_date
    end_day = end_date + timedelta(days=1)
    if start_date:
        start_day = start_date
        if start_day >= end_day:
            start_day = end_day - timedelta(days=1)
    else:
        start_day = end_day - timedelta(days=range_days)
    compare_end = start_day
    compare_start = compare_end - timedelta(days=compare_days)
    return start_day, end_day, compare_start, compare_end


async def resolve_repo_filter_ids(
    sink: BaseMetricsSink, filters: MetricFilter, org_id: str = ""
) -> list[str]:
    repo_refs: list[str] = []
    if filters.scope.level == "repo":
        repo_refs.extend(filters.scope.ids)
    if filters.what.repos:
        repo_refs.extend(filters.what.repos)
    if filters.scope.level == "team" and filters.scope.ids:
        team_repo_ids = await resolve_repo_ids_for_teams(
            sink,
            filters.scope.ids,
            org_id=org_id,
        )
        repo_refs.extend(team_repo_ids)
    return await resolve_repo_ids(sink, repo_refs, org_id=org_id)


def work_category_filter(
    filters: MetricFilter, column: str = "investment_area"
) -> tuple[str, dict[str, Any]]:
    raw_categories = filters.why.work_category or []
    categories: list[str] = []
    for category in raw_categories:
        if category is None:
            continue
        category_str = str(category).strip()
        if category_str:
            categories.append(category_str)
    if not categories:
        return "", {}
    return f" AND {column} IN %(work_categories)s", {"work_categories": categories}


async def scope_filter_for_metric(
    sink: BaseMetricsSink,
    *,
    metric_scope: str,
    filters: MetricFilter,
    org_id: str = "",
    team_column: str = "team_id",
    repo_column: str = "repo_id",
) -> tuple[str, dict[str, Any]]:
    if metric_scope == "team" and filters.scope.level == "team":
        return build_scope_filter_multi(
            "team", filters.scope.ids, team_column=team_column, repo_column=repo_column
        )
    if metric_scope == "repo":
        repo_ids = await resolve_repo_filter_ids(sink, filters, org_id=org_id)
        return build_scope_filter_multi(
            "repo", repo_ids, team_column=team_column, repo_column=repo_column
        )
    return "", {}
