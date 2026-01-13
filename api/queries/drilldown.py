from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from .client import query_dicts


async def fetch_pull_requests(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int = 50,
) -> List[Dict[str, Any]]:
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
        WHERE created_at >= %(start_ts)s AND created_at < %(end_ts)s
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
    return await query_dicts(client, query, params)


async def fetch_issues(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int = 50,
) -> List[Dict[str, Any]]:
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
        {scope_filter}
        ORDER BY completed_at DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    return await query_dicts(client, query, params)
