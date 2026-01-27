from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .client import query_dicts
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


async def fetch_investment_breakdown(
    sink: BaseMetricsSink,
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
    return await query_dicts(sink, query, params)


async def fetch_investment_edges(
    sink: BaseMetricsSink,
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
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(theme_distribution_json AS Array(Tuple(String, Float32))) AS theme_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {theme_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await query_dicts(sink, query, params)


async def fetch_investment_subcategory_edges(
    sink: BaseMetricsSink,
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
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY source, target
        ORDER BY value DESC
    """
    return await query_dicts(sink, query, params)


async def fetch_investment_team_edges(
    sink: BaseMetricsSink,
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
    return await query_dicts(sink, query, params)


async def fetch_investment_repo_team_edges(
    sink: BaseMetricsSink,
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
        WITH unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    count() AS cnt
                FROM work_unit_investments
                ARRAY JOIN JSONExtract(structural_evidence_json, 'issues', 'Array(String)') AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                {scope_filter}
                GROUP BY work_unit_id, team
            )
            GROUP BY work_unit_id
        )
        SELECT
            subcategory_kv.1 AS subcategory,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY subcategory, repo, team
        ORDER BY value DESC
    """
    return await query_dicts(sink, query, params)


async def fetch_investment_team_category_repo_edges(
    sink: BaseMetricsSink,
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
        WITH unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    count() AS cnt
                FROM work_unit_investments
                ARRAY JOIN JSONExtract(structural_evidence_json, 'issues', 'Array(String)') AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                {scope_filter}
                GROUP BY work_unit_id, team
            )
            GROUP BY work_unit_id
        )
        SELECT
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            splitByChar('.', subcategory_kv.1)[1] AS category,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY team, category, repo
        ORDER BY value DESC
    """
    return await query_dicts(sink, query, params)


async def fetch_investment_team_subcategory_repo_edges(
    sink: BaseMetricsSink,
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
        WITH unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    count() AS cnt
                FROM work_unit_investments
                ARRAY JOIN JSONExtract(structural_evidence_json, 'issues', 'Array(String)') AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                {scope_filter}
                GROUP BY work_unit_id, team
            )
            GROUP BY work_unit_id
        )
        SELECT
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            subcategory_kv.1 AS subcategory,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY team, subcategory, repo
        ORDER BY value DESC
    """
    return await query_dicts(sink, query, params)


async def fetch_investment_unassigned_counts(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: Dict[str, Any],
    themes: Optional[List[str]] = None,
    subcategories: Optional[List[str]] = None,
) -> Dict[str, int]:
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    if themes:
        filters.append(
            "arrayExists(k -> splitByChar('.', k)[1] IN %(themes)s, mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))))"
        )
        params["themes"] = themes
    if subcategories:
        filters.append(
            "hasAny(mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))), %(subcategories)s)"
        )
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    count() AS cnt
                FROM work_unit_investments
                ARRAY JOIN JSONExtract(structural_evidence_json, 'issues', 'Array(String)') AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                {scope_filter}
                {category_filter}
                GROUP BY work_unit_id, team
            )
            GROUP BY work_unit_id
        )
        SELECT
            countDistinctIf(work_unit_investments.work_unit_id, repo_id IS NULL) AS missing_repo,
            countDistinctIf(
                work_unit_investments.work_unit_id,
                ifNull(nullIf(unit_team.team, ''), '') = ''
            ) AS missing_team
        FROM work_unit_investments
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
    """
    rows = await query_dicts(sink, query, params)
    if not rows:
        return {"missing_team": 0, "missing_repo": 0}
    row = rows[0]
    return {
        "missing_team": int(row.get("missing_team") or 0),
        "missing_repo": int(row.get("missing_repo") or 0),
    }


async def fetch_investment_sunburst(
    sink: BaseMetricsSink,
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
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
        {scope_filter}
        {category_filter}
        GROUP BY theme, subcategory, scope
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    return await query_dicts(sink, query, params)


async def fetch_investment_quality_stats(
    sink: BaseMetricsSink,
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
    rows = await query_dicts(sink, query, params)
    if not rows:
        return {}
    return dict(rows[0])
