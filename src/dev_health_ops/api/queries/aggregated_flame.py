"""SQL queries for aggregated flame graph data."""

from __future__ import annotations

from datetime import date
from typing import Any

from .client import query_dicts
from .investment import PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE


async def fetch_cycle_breakdown(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: str | None = None,
    provider: str | None = None,
    work_scope_id: str | None = None,
    org_id: str = "",
) -> list[dict[str, Any]]:
    """
    Fetch aggregated state durations for cycle-time breakdown.

    Returns rows with (status, total_duration_hours, items_touched).
    """
    params: dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
    }
    params["org_id"] = org_id

    filters = ["day >= %(start_day)s", "day < %(end_day)s", "org_id = %(org_id)s"]
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

    # CHAOS-2377: the daily job appends a fresh row per run to this plain
    # MergeTree, so a re-run/backfill of the same day leaves duplicate rows.
    # Dedup with argMax(..., computed_at) over the full natural key before
    # summing, matching the operating_review reader. Summing raw rows would
    # inflate flow weights and touched counts on every re-run.
    query = f"""
        SELECT
            status,
            sum(duration_hours) AS total_hours,
            sum(items_touched) AS total_items
        FROM (
            SELECT
                day,
                provider,
                work_scope_id,
                team_id,
                status,
                argMax(duration_hours, computed_at) AS duration_hours,
                argMax(items_touched, computed_at) AS items_touched
            FROM work_item_state_durations_daily
            WHERE {where_clause}
            GROUP BY day, provider, work_scope_id, team_id, status
        )
        GROUP BY status
        ORDER BY total_hours DESC
    """
    return await query_dicts(client, query, params)


async def fetch_code_hotspots(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    repo_id: str | None = None,
    limit: int = 500,
    min_churn: int = 1,
    org_id: str = "",
) -> list[dict[str, Any]]:
    """
    Fetch aggregated file churn for code hotspot flame.

    Returns rows with (repo_id, file_path, total_churn).
    """
    params: dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
        "min_churn": min_churn,
    }
    params["org_id"] = org_id

    filters = ["day >= %(start_day)s", "day < %(end_day)s", "org_id = %(org_id)s"]
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
    repo_ids: list[str],
    org_id: str = "",
) -> dict[str, str]:
    """Fetch repo names for given repo IDs."""
    if not repo_ids:
        return {}

    params: dict[str, Any] = {"repo_ids": repo_ids}
    params["org_id"] = org_id
    query = """
        SELECT
            toString(id) AS repo_id,
            repo AS repo_name
        FROM repos
        WHERE id IN %(repo_ids)s
          AND org_id = %(org_id)s
    """
    rows = await query_dicts(client, query, params)
    return {row["repo_id"]: row["repo_name"] for row in rows}


async def fetch_throughput(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: str | None = None,
    repo_id: str | None = None,
    provider: str | None = None,
    work_scope_id: str | None = None,
    limit: int = 500,
    org_id: str = "",
) -> list[dict[str, Any]]:
    """
    Fetch throughput data for work items completed in window.

    Returns rows with (type, repo_name, items_completed).
    If work item type not available, falls back to 'unclassified'.
    """
    params: dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
    }
    params["org_id"] = org_id

    filters = [
        "wct.day >= %(start_day)s",
        "wct.day < %(end_day)s",
        "wct.org_id = %(org_id)s",
    ]
    if team_id:
        filters.append("t.team_id = %(team_id)s")
        params["team_id"] = team_id
    if provider:
        filters.append("wct.provider = %(provider)s")
        params["provider"] = provider
    if work_scope_id:
        filters.append("wct.work_scope_id = %(work_scope_id)s")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(filters)
    team_key_expr = "coalesce(nullIf(t.team_id, ''), 'unassigned')"

    query = f"""
        SELECT
            'All' AS work_type,
            if(
                {team_key_expr} = 'unassigned',
                'Unassigned',
                coalesce(nullIf(any(t.team_name), ''), {team_key_expr})
            ) AS team_name,
            uniqExact(wct.work_item_id) AS items_completed,
            0 AS items_started
        FROM work_item_cycle_times AS wct FINAL
        LEFT JOIN {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE} AS t
          ON t.work_item_id = wct.work_item_id
        WHERE {where_clause}
        GROUP BY {team_key_expr}
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
    team_id: str | None = None,
    repo_id: str | None = None,
    limit: int = 500,
    org_id: str = "",
) -> list[dict[str, Any]]:
    """
    Fetch throughput by work item type from work_item_cycle_times.
    """
    params: dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
    }
    params["org_id"] = org_id

    # Filter by completed_at date range, not day column
    filters = [
        "wct.completed_at >= toDateTime(%(start_day)s)",
        "wct.completed_at < toDateTime(%(end_day)s)",
        "wct.completed_at IS NOT NULL",
        "wct.org_id = %(org_id)s",
    ]
    if team_id:
        filters.append("t.team_id = %(team_id)s")
        params["team_id"] = team_id

    where_clause = " AND ".join(filters)
    team_key_expr = "coalesce(nullIf(t.team_id, ''), 'unassigned')"

    query = f"""
        SELECT
            coalesce(nullIf(wct.type, ''), 'unclassified') AS work_type,
            if(
                {team_key_expr} = 'unassigned',
                'Unassigned',
                coalesce(nullIf(any(t.team_name), ''), {team_key_expr})
            ) AS team_name,
            uniqExact(wct.work_item_id) AS items_completed
        FROM work_item_cycle_times AS wct FINAL
        LEFT JOIN {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE} AS t
          ON t.work_item_id = wct.work_item_id
        WHERE {where_clause}
        GROUP BY work_type, {team_key_expr}
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
    team_id: str | None = None,
    provider: str | None = None,
    work_scope_id: str | None = None,
    org_id: str = "",
) -> list[dict[str, Any]]:
    """
    Fetch aggregated cycle time by milestone as a fallback.
    Returns (milestone, avg_hours, total_items).
    """
    params: dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
    }
    params["org_id"] = org_id

    filters = ["day >= %(start_day)s", "day < %(end_day)s", "org_id = %(org_id)s"]
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
