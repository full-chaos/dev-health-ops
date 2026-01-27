from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from .client import query_dicts


async def fetch_metric_contributors(
    client: Any,
    *,
    table: str,
    column: str,
    group_by: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int = 6,
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            {group_by} AS id,
            avg({column}) AS value
        FROM {table}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
        GROUP BY {group_by}
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    return await query_dicts(client, query, params)


async def fetch_metric_driver_delta(
    client: Any,
    *,
    table: str,
    column: str,
    group_by: str,
    start_day: date,
    end_day: date,
    compare_start: date,
    compare_end: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int = 3,
) -> List[Dict[str, Any]]:
    query = f"""
        WITH
            current AS (
                SELECT {group_by} AS id, avg({column}) AS value
                FROM {table}
                WHERE day >= %(start_day)s AND day < %(end_day)s
                {scope_filter}
                GROUP BY {group_by}
            ),
            previous AS (
                SELECT {group_by} AS id, avg({column}) AS value
                FROM {table}
                WHERE day >= %(compare_start)s AND day < %(compare_end)s
                {scope_filter}
                GROUP BY {group_by}
            )
        SELECT
            current.id AS id,
            current.value AS value,
            CASE WHEN previous.value = 0 THEN 0 ELSE (current.value - previous.value) / previous.value * 100 END AS delta_pct
        FROM current
        LEFT JOIN previous ON current.id = previous.id
        ORDER BY delta_pct DESC
        LIMIT %(limit)s
    """
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "compare_start": compare_start,
        "compare_end": compare_end,
        "limit": limit,
    }
    params.update(scope_params)
    return await query_dicts(client, query, params)
