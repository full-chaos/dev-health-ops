from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .client import query_dicts


async def fetch_investment_breakdown(
    client: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
    themes: Optional[List[str]] = None,
    subcategories: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    if themes:
        filters.append("theme IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        ARRAY JOIN mapToArray(subcategory_distribution_json) AS subcategory_kv
        WHERE from_ts < %(end_ts)s AND to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY subcategory, theme
        ORDER BY value DESC
    """
    return await query_dicts(client, query, params)


async def fetch_investment_edges(
    client: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
    themes: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    theme_filter = ""
    params = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    if themes:
        theme_filter = " AND theme_kv.1 IN %(themes)s"
        params["themes"] = themes
    query = f"""
        SELECT
            theme_kv.1 AS source,
            ifNull(r.repo, toString(repo_id)) AS target,
            sum(theme_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON r.id = repo_id
        ARRAY JOIN mapToArray(theme_distribution_json) AS theme_kv
        WHERE from_ts < %(end_ts)s AND to_ts >= %(start_ts)s
        {scope_filter}
        {theme_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await query_dicts(client, query, params)


async def fetch_investment_sunburst(
    client: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
    themes: Optional[List[str]] = None,
    subcategories: Optional[List[str]] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    filters: List[str] = []
    params: Dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    }
    params.update(scope_params)
    if themes:
        filters.append("theme IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            ifNull(r.repo, toString(repo_id)) AS scope,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON r.id = repo_id
        ARRAY JOIN mapToArray(subcategory_distribution_json) AS subcategory_kv
        WHERE from_ts < %(end_ts)s AND to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY theme, subcategory, scope
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)
