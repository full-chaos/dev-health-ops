from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from ..models.filters import MetricFilter
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from ..queries.scopes import (
    build_scope_filter_multi,
    resolve_repo_ids,
    resolve_repo_ids_for_teams,
)


def filter_cache_key(
    prefix: str, filters: MetricFilter, extra: Dict[str, Any] | None = None
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
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return f"{prefix}:{serialized}"


def time_window(filters: MetricFilter) -> Tuple[date, date, date, date]:
    range_days = max(1, filters.time.range_days)
    compare_days = max(1, filters.time.compare_days)
    end_date = filters.time.end_date or date.today()
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
    sink: BaseMetricsSink, filters: MetricFilter
) -> List[str]:
    repo_refs: List[str] = []
    if filters.scope.level == "repo":
        repo_refs.extend(filters.scope.ids)
    if filters.what.repos:
        repo_refs.extend(filters.what.repos)
    if filters.scope.level == "team" and filters.scope.ids:
        team_repo_ids = await resolve_repo_ids_for_teams(sink, filters.scope.ids)
        repo_refs.extend(team_repo_ids)
    return await resolve_repo_ids(sink, repo_refs)


def work_category_filter(
    filters: MetricFilter, column: str = "investment_area"
) -> Tuple[str, Dict[str, Any]]:
    raw_categories = filters.why.work_category or []
    categories: List[str] = []
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
    team_column: str = "team_id",
    repo_column: str = "repo_id",
) -> Tuple[str, Dict[str, Any]]:
    if metric_scope == "team" and filters.scope.level == "team":
        return build_scope_filter_multi(
            "team", filters.scope.ids, team_column=team_column, repo_column=repo_column
        )
    if metric_scope == "repo":
        repo_ids = await resolve_repo_filter_ids(sink, filters)
        return build_scope_filter_multi(
            "repo", repo_ids, team_column=team_column, repo_column=repo_column
        )
    return "", {}
