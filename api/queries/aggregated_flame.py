"""SQL queries for aggregated flame graph data."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from .client import query_dicts


async def fetch_cycle_breakdown(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: Optional[str] = None,
    provider: Optional[str] = None,
    work_scope_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch aggregated state durations for cycle-time breakdown.

    Returns rows with (status, total_duration_hours, items_touched).
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
    }

    filters = ["day >= %(start_day)s", "day < %(end_day)s"]
    if team_id:
        filters.append("team_id = %(team_id)s")
        params["team_id"] = team_id
    if provider:
        filters.append("provider = %(provider)s")
        params["provider"] = provider
    if work_scope_id:
        filters.append("work_scope_id = %(work_scope_id)s")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT
            status,
            sum(duration_hours) AS total_hours,
            sum(items_touched) AS total_items
        FROM work_item_state_durations_daily
        WHERE {where_clause}
        GROUP BY status
        ORDER BY total_hours DESC
    """
    return await query_dicts(client, query, params)


async def fetch_code_hotspots(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    repo_id: Optional[str] = None,
    limit: int = 500,
    min_churn: int = 1,
) -> List[Dict[str, Any]]:
    """
    Fetch aggregated file churn for code hotspot flame.

    Returns rows with (repo_id, file_path, total_churn).
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
        "min_churn": min_churn,
    }

    filters = ["day >= %(start_day)s", "day < %(end_day)s"]
    if repo_id:
        filters.append("repo_id = %(repo_id)s")
        params["repo_id"] = repo_id

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT
            toString(repo_id) AS repo_id,
            path AS file_path,
            sum(churn) AS total_churn
        FROM file_metrics_daily
        WHERE {where_clause}
        GROUP BY repo_id, path
        HAVING total_churn >= %(min_churn)s
        ORDER BY total_churn DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_repo_names(
    client: Any,
    *,
    repo_ids: List[str],
) -> Dict[str, str]:
    """Fetch repo names for given repo IDs."""
    if not repo_ids:
        return {}

    params = {"repo_ids": repo_ids}
    query = """
        SELECT
            toString(id) AS repo_id,
            repo AS repo_name
        FROM repos
        WHERE id IN %(repo_ids)s
    """
    rows = await query_dicts(client, query, params)
    return {row["repo_id"]: row["repo_name"] for row in rows}


async def fetch_throughput(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: Optional[str] = None,
    repo_id: Optional[str] = None,
    provider: Optional[str] = None,
    work_scope_id: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Fetch throughput data for work items completed in window.

    Returns rows with (type, repo_name, items_completed).
    If work item type not available, falls back to 'unclassified'.
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
    }

    filters = ["day >= %(start_day)s", "day < %(end_day)s"]
    if team_id:
        filters.append("team_id = %(team_id)s")
        params["team_id"] = team_id
    if provider:
        filters.append("provider = %(provider)s")
        params["provider"] = provider
    if work_scope_id:
        filters.append("work_scope_id = %(work_scope_id)s")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(filters)

    # Query work_item_metrics_daily for aggregate counts by team
    # Note: No work_type available in this table, so we use 'All' as synthetic type
    query = f"""
        SELECT
            'All' AS work_type,
            coalesce(nullIf(team_name, ''), 'Unassigned') AS team_name,
            sum(items_completed) AS items_completed,
            sum(items_started) AS items_started
        FROM work_item_metrics_daily
        WHERE {where_clause}
        GROUP BY team_name
        HAVING items_completed > 0
        ORDER BY items_completed DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_throughput_by_type(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: Optional[str] = None,
    repo_id: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Fetch throughput by work item type from work_item_cycle_times.
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
    }

    # Filter by completed_at date range, not day column
    filters = [
        "completed_at >= toDateTime(%(start_day)s)",
        "completed_at < toDateTime(%(end_day)s)",
        "completed_at IS NOT NULL",
    ]
    if team_id:
        filters.append("team_id = %(team_id)s")
        params["team_id"] = team_id

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT
            coalesce(nullIf(type, ''), 'unclassified') AS work_type,
            coalesce(nullIf(team_name, ''), 'Unassigned') AS team_name,
            count(*) AS items_completed
        FROM work_item_cycle_times
        WHERE {where_clause}
        GROUP BY work_type, team_name
        HAVING items_completed > 0
        ORDER BY items_completed DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_cycle_milestones(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: Optional[str] = None,
    provider: Optional[str] = None,
    work_scope_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch aggregated cycle time by milestone as a fallback.
    Returns (milestone, avg_hours, total_items).
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
    }

    filters = ["day >= %(start_day)s", "day < %(end_day)s"]
    if team_id:
        filters.append("team_id = %(team_id)s")
        params["team_id"] = team_id
    if provider:
        filters.append("provider = %(provider)s")
        params["provider"] = provider
    if work_scope_id:
        filters.append("work_scope_id = %(work_scope_id)s")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT
            milestone,
            avg(duration_hours) AS avg_hours,
            count(*) AS total_items
        FROM work_item_cycle_milestones_daily
        WHERE {where_clause}
        GROUP BY milestone
        ORDER BY avg_hours DESC
    """
    return await query_dicts(client, query, params)
