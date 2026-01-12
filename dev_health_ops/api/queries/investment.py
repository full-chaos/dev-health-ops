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
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
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
        ARRAY JOIN CAST(theme_distribution_json AS Array(Tuple(String, Float32))) AS theme_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {theme_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await query_dicts(client, query, params)


async def fetch_investment_subcategory_edges(
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
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            subcategory_kv.1 AS source,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON r.id = repo_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await query_dicts(client, query, params)


async def fetch_investment_team_edges(
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
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            subcategory_kv.1 AS source,
            ifNull(team_name, 'unassigned') AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN (
            SELECT
                work_item_id,
                argMax(team_name, computed_at) AS team_name
            FROM work_item_cycle_times
            GROUP BY work_item_id
        ) AS t ON t.work_item_id = arrayElement(JSONExtract(structural_evidence_json, 'issues', 'Array(String)'), 1)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
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
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
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
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY theme, subcategory, scope
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_investment_quality_stats(
    client: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
    themes: Optional[List[str]] = None,
    subcategories: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Fetch aggregated evidence quality stats: mean, stddev, band counts."""
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    if themes:
        filters.append(
            "hasAny(mapKeys(CAST(theme_distribution_json AS Map(String, Float32))), %(themes)s)"
        )
        params["themes"] = themes
    if subcategories:
        filters.append(
            "hasAny(mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))), %(subcategories)s)"
        )
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            sum(effort_value) AS total_effort,
            sumIf(effort_value, evidence_quality IS NOT NULL) AS quality_known_effort,
            sumIf(effort_value * evidence_quality, evidence_quality IS NOT NULL) AS quality_weighted,
            countIf(evidence_quality_band = 'high') AS high_count,
            countIf(evidence_quality_band = 'moderate') AS moderate_count,
            countIf(evidence_quality_band = 'low') AS low_count,
            countIf(evidence_quality_band = 'very_low') AS very_low_count,
            countIf(evidence_quality IS NULL OR evidence_quality_band = '') AS unknown_count,
            varPopIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_variance
        FROM work_unit_investments
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
    """
    rows = await query_dicts(client, query, params)
    if not rows:
        return {}
    return dict(rows[0])
