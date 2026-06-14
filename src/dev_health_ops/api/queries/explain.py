from __future__ import annotations

from datetime import date
from typing import Any

from .client import query_dicts
from .metrics import _DEDUP_BY_COMPUTED_AT, _metric_from_clause


async def fetch_metric_contributors(
    client: Any,
    *,
    table: str,
    column: str,
    group_by: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    limit: int = 6,
    org_id: str = "",
) -> list[dict[str, Any]]:
    if table in _DEDUP_BY_COMPUTED_AT:
        # CHAOS-2377: dedup re-run/backfill rows to the latest computed_at per
        # natural key BEFORE the contributor avg(), matching the deduped
        # /explain headline (fetch_metric_value). Without this the contributor
        # ranking averages over stale + latest duplicates and misranks owners.
        # org_id and scope_filter stay in the inner WHERE (see _metric_from_clause).
        from_clause = _metric_from_clause(
            table=table, column=column, scope_filter=scope_filter
        )
        query = f"""
        SELECT
            {group_by} AS id,
            avg({column}) AS value
        FROM {from_clause}
        GROUP BY {group_by}
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    else:
        query = f"""
        SELECT
            {group_by} AS id,
            avg({column}) AS value
        FROM {table}
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
        {scope_filter}
        GROUP BY {group_by}
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    params["org_id"] = org_id
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
    scope_params: dict[str, Any],
    limit: int = 3,
    org_id: str = "",
) -> list[dict[str, Any]]:
    if table in _DEDUP_BY_COMPUTED_AT:
        # CHAOS-2377: dedup re-run/backfill rows to the latest computed_at per
        # natural key in BOTH windows before the driver avg()/delta, matching
        # the deduped /explain headline. The current window reuses the default
        # start_day/end_day params; the previous CTE binds compare_start/end via
        # the from-clause param overrides. org_id + scope_filter stay inner.
        current_from = _metric_from_clause(
            table=table, column=column, scope_filter=scope_filter
        )
        previous_from = _metric_from_clause(
            table=table,
            column=column,
            scope_filter=scope_filter,
            start_param="compare_start",
            end_param="compare_end",
        )
        query = f"""
        WITH
            current AS (
                SELECT {group_by} AS id, avg({column}) AS value
                FROM {current_from}
                GROUP BY {group_by}
            ),
            previous AS (
                SELECT {group_by} AS id, avg({column}) AS value
                FROM {previous_from}
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
    else:
        query = f"""
        WITH
            current AS (
                SELECT {group_by} AS id, avg({column}) AS value
                FROM {table}
                WHERE day >= %(start_day)s AND day < %(end_day)s
                  AND org_id = %(org_id)s
                {scope_filter}
                GROUP BY {group_by}
            ),
            previous AS (
                SELECT {group_by} AS id, avg({column}) AS value
                FROM {table}
                WHERE day >= %(compare_start)s AND day < %(compare_end)s
                  AND org_id = %(org_id)s
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
    params["org_id"] = org_id
    return await query_dicts(client, query, params)
