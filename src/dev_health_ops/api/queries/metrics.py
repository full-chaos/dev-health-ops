from __future__ import annotations

from datetime import date
from typing import Any

from .client import query_dicts


def _date_params(start_day: date, end_day: date) -> dict[str, Any]:
    return {"start_day": start_day, "end_day": end_day}


async def fetch_metric_series(
    client: Any,
    *,
    table: str,
    column: str,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    aggregator: str,
    org_id: str = "",
) -> list[dict[str, Any]]:
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
    scope_params: dict[str, Any],
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
    scope_params: dict[str, Any],
    org_id: str = "",
) -> tuple[float, list[dict[str, Any]]]:
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


_INVESTMENT_THEME_LABELS = {
    "feature_delivery": "Feature Delivery",
    "operational": "Operational / Support",
    "maintenance": "Maintenance / Tech Debt",
    "quality": "Quality / Reliability",
    "risk": "Risk / Security",
}


async def fetch_rework_theme_allocation(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    work_category_filter: str = "",
    work_category_params: dict[str, Any] | None = None,
    org_id: str = "",
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            investment_area AS theme,
            sum(work_items_completed) AS allocation,
            sum(prs_merged) AS prs_merged,
            sum(churn_loc) AS churn_loc
        FROM (
            SELECT
                day,
                repo_id,
                team_id,
                investment_area,
                project_stream,
                argMax(work_items_completed, computed_at) AS work_items_completed,
                argMax(prs_merged, computed_at) AS prs_merged,
                argMax(churn_loc, computed_at) AS churn_loc
            FROM investment_metrics_daily
            WHERE day >= %(start_day)s AND day < %(end_day)s
            {scope_filter}
            {work_category_filter}
              AND org_id = %(org_id)s
            GROUP BY day, repo_id, team_id, investment_area, project_stream
        )
        GROUP BY investment_area
        ORDER BY allocation DESC
    """
    params = _date_params(start_day, end_day)
    params.update(scope_params)
    params.update(work_category_params or {})
    params["org_id"] = org_id
    rows = await query_dicts(client, query, params)
    total = sum(float(row.get("allocation") or 0.0) for row in rows)
    allocations: list[dict[str, Any]] = []
    for row in rows:
        theme = str(row.get("theme") or "")
        allocation = float(row.get("allocation") or 0.0)
        allocations.append(
            {
                "theme": theme,
                "label": _INVESTMENT_THEME_LABELS.get(theme, theme),
                "allocation": allocation,
                "allocation_pct": (allocation / total * 100.0) if total else 0.0,
                "prs_merged": int(row.get("prs_merged") or 0),
                "churn_loc": int(row.get("churn_loc") or 0),
            }
        )
    return allocations
