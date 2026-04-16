from __future__ import annotations

from datetime import date
from typing import Any

from dev_health_ops.api.services.auth import get_current_org_id

from .client import query_dicts


def _assert_org_id(org_id: str) -> None:
    if not org_id:
        raise ValueError("org_id is required for drilldown queries")
    ctx = get_current_org_id()
    if ctx is not None and ctx != org_id:
        raise PermissionError(f"org_id mismatch: contextvar={ctx!r} caller={org_id!r}")


async def fetch_pull_requests(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    limit: int = 50,
    org_id: str = "",
) -> list[dict[str, Any]]:
    _assert_org_id(org_id)
    query = f"""
        SELECT
            repo_id,
            number,
            title,
            author_name,
            created_at,
            merged_at,
            first_review_at,
            if(first_review_at IS NULL, NULL,
               dateDiff('hour', created_at, first_review_at)) AS review_latency_hours
        FROM git_pull_requests
        INNER JOIN repos ON toString(repos.id) = toString(git_pull_requests.repo_id)
        WHERE created_at >= %(start_ts)s AND created_at < %(end_ts)s
          AND repos.org_id = %(org_id)s
        {scope_filter}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    params = {
        "start_ts": start_day,
        "end_ts": end_day,
        "limit": limit,
    }
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(client, query, params)


async def fetch_issues(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    limit: int = 50,
    org_id: str = "",
) -> list[dict[str, Any]]:
    _assert_org_id(org_id)
    query = f"""
        SELECT
            work_item_id,
            provider,
            status,
            team_id,
            cycle_time_hours,
            lead_time_hours,
            started_at,
            completed_at
        FROM work_item_cycle_times
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
        {scope_filter}
        ORDER BY completed_at DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(client, query, params)
