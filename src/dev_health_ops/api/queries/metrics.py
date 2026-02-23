from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Tuple

from .client import query_dicts


def _date_params(start_day: date, end_day: date) -> Dict[str, Any]:
    return {"start_day": start_day, "end_day": end_day}


async def fetch_metric_series(
    client: Any,
    *,
    table: str,
    column: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    aggregator: str,
    org_id: str = "",
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            day,
            {aggregator}({column}) AS value
        FROM {table}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
          AND org_id = %(org_id)s
        GROUP BY day
        ORDER BY day
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(client, query, params)


async def fetch_metric_value(
    client: Any,
    *,
    table: str,
    column: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    aggregator: str,
    org_id: str = "",
) -> float:
    query = f"""
        SELECT
            {aggregator}({column}) AS value
        FROM {table}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
          AND org_id = %(org_id)s
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    if not rows:
        return 0.0
    value = rows[0].get("value")
    return float(value or 0.0)


async def fetch_blocked_hours(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    org_id: str = "",
) -> Tuple[float, List[Dict[str, Any]]]:
    query = f"""
        SELECT
            day,
            sum(duration_hours) AS value
        FROM work_item_state_durations_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND status = 'blocked'
        {scope_filter}
          AND org_id = %(org_id)s
        GROUP BY day
        ORDER BY day
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    total = sum(float(row.get("value") or 0.0) for row in rows)
    return total, rows
