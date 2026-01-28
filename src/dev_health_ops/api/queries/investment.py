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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)

    subcat_key = dialect.tuple_element("subcategory_kv", 1)
    subcat_val = dialect.tuple_element("subcategory_kv", 2)
    theme_expr = dialect.split_by_char(".", subcat_key, 1)

    if themes:
        filters.append(f"{theme_expr} IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append(f"{subcat_key} IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            {subcat_key} AS subcategory,
            {theme_expr} AS theme,
            SUM({subcat_val} * effort_value) AS value
        FROM work_unit_investments
        {dialect.array_join("subcategory_distribution_json", "subcategory_kv", "Array(Tuple(String, Float32))")}
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
    dialect = sink.dialect
    theme_filter = ""
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)

    theme_key = dialect.tuple_element("theme_kv", 1)
    theme_val = dialect.tuple_element("theme_kv", 2)

    if themes:
        theme_filter = f" AND {theme_key} IN %(themes)s"
        params["themes"] = themes
    query = f"""
        SELECT
            {theme_key} AS source,
            {dialect.if_null("r.repo", dialect.to_string("repo_id"))} AS target,
            SUM({theme_val} * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON {dialect.to_string("r.id")} = {dialect.to_string("repo_id")}
        {dialect.array_join("theme_distribution_json", "theme_kv", "Array(Tuple(String, Float32))")}
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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)

    subcat_key = dialect.tuple_element("subcategory_kv", 1)
    subcat_val = dialect.tuple_element("subcategory_kv", 2)
    theme_expr = dialect.split_by_char(".", subcat_key, 1)

    if themes:
        filters.append(f"{theme_expr} IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append(f"{subcat_key} IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            {subcat_key} AS source,
            {dialect.if_null("r.repo", dialect.if_null(dialect.to_string("repo_id"), "'unassigned'"))} AS target,
            SUM({subcat_val} * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON {dialect.to_string("r.id")} = {dialect.to_string("repo_id")}
        {dialect.array_join("subcategory_distribution_json", "subcategory_kv", "Array(Tuple(String, Float32))")}
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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)

    subcat_key = dialect.tuple_element("subcategory_kv", 1)
    subcat_val = dialect.tuple_element("subcategory_kv", 2)
    theme_expr = dialect.split_by_char(".", subcat_key, 1)

    if themes:
        filters.append(f"{theme_expr} IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append(f"{subcat_key} IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""

    issue_id_expr = dialect.array_element(
        dialect.json_extract("structural_evidence_json", "issues", "Array(String)"), 1
    )

    query = f"""
        SELECT
            {subcat_key} AS source,
            {dialect.if_null("team_name", "'unassigned'")} AS target,
            SUM({subcat_val} * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN (
            SELECT
                work_item_id,
                {dialect.arg_max("team_name", "computed_at")} AS team_name
            FROM work_item_cycle_times
            GROUP BY work_item_id
        ) AS t ON t.work_item_id = {issue_id_expr}
        {dialect.array_join("subcategory_distribution_json", "subcategory_kv", "Array(Tuple(String, Float32))")}
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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)

    subcat_key = dialect.tuple_element("subcategory_kv", 1)
    subcat_val = dialect.tuple_element("subcategory_kv", 2)
    theme_expr = dialect.split_by_char(".", subcat_key, 1)

    if themes:
        filters.append(f"{theme_expr} IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append(f"{subcat_key} IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""

    issue_id_expr = dialect.json_extract(
        "structural_evidence_json", "issues", "Array(String)"
    )

    query = f"""
        WITH unit_team AS (
            SELECT
                work_unit_id,
                {dialect.arg_max("team", "cnt")} AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    {dialect.if_null(dialect.null_if("t.team_name", "''"), dialect.null_if("t.team_id", "''"))} AS team,
                    count() AS cnt
                FROM work_unit_investments
                {dialect.array_join(issue_id_expr, "issue_id")}
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        {dialect.arg_max("team_id", "computed_at")} AS team_id,
                        {dialect.arg_max("team_name", "computed_at")} AS team_name
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
            {subcat_key} AS subcategory,
            {dialect.if_null("r.repo", dialect.if_null(dialect.to_string("repo_id"), "'unassigned'"))} AS repo,
            {dialect.if_null(dialect.null_if("unit_team.team", "''"), "'unassigned'")} AS team,
            SUM({subcat_val} * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON {dialect.to_string("r.id")} = {dialect.to_string("repo_id")}
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        {dialect.array_join("subcategory_distribution_json", "subcategory_kv", "Array(Tuple(String, Float32))")}
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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    if themes:
        filters.append(
            dialect.json_has_any_key("theme_distribution_json", "%(themes)s")
        )
        params["themes"] = themes
    if subcategories:
        filters.append(
            dialect.json_has_any_key(
                "subcategory_distribution_json", "%(subcategories)s"
            )
        )
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH unit_team AS (
            SELECT
                work_unit_id,
                {dialect.arg_max("team", "cnt")} AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    {dialect.if_null(dialect.null_if("t.team_name", "''"), dialect.null_if("t.team_id", "''"))} AS team,
                    count() AS cnt
                FROM work_unit_investments
                {dialect.array_join(dialect.json_extract("structural_evidence_json", "issues", "Array(String)"), "issue_id")}
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        {dialect.arg_max("team_id", "computed_at")} AS team_id,
                        {dialect.arg_max("team_name", "computed_at")} AS team_name
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
            {dialect.count_distinct("work_unit_investments.work_unit_id")} AS total_count,
            {dialect.count_if("repo_id IS NULL")} AS missing_repo,
            {dialect.count_if("ifNull(nullIf(unit_team.team, ''), '') = ''")} AS missing_team
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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    }
    params.update(scope_params)

    subcat_key = dialect.tuple_element("subcategory_kv", 1)
    subcat_val = dialect.tuple_element("subcategory_kv", 2)
    theme_expr = dialect.split_by_char(".", subcat_key, 1)

    if themes:
        filters.append(f"{theme_expr} IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append(f"{subcat_key} IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            {subcat_key} AS subcategory,
            {theme_expr} AS theme,
            {dialect.if_null("r.repo", dialect.to_string("repo_id"))} AS scope,
            SUM({subcat_val} * effort_value) AS value
        FROM work_unit_investments
        LEFT JOIN repos AS r ON {dialect.to_string("r.id")} = {dialect.to_string("repo_id")}
        {dialect.array_join("subcategory_distribution_json", "subcategory_kv", "Array(Tuple(String, Float32))")}
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
    dialect = sink.dialect
    filters: List[str] = []
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    if themes:
        filters.append(
            dialect.json_has_any_theme("theme_distribution_json", "%(themes)s")
        )
        params["themes"] = themes
    if subcategories:
        filters.append(
            dialect.json_has_any_key(
                "subcategory_distribution_json", "%(subcategories)s"
            )
        )
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        SELECT
            SUM(effort_value) AS total_effort,
            {dialect.sum_if("effort_value", "evidence_quality IS NOT NULL")} AS quality_known_effort,
            {dialect.sum_if("effort_value * evidence_quality", "evidence_quality IS NOT NULL")} AS quality_weighted,
            {dialect.count_if("evidence_quality_band = 'high'")} AS high_count,
            {dialect.count_if("evidence_quality_band = 'moderate'")} AS moderate_count,
            {dialect.count_if("evidence_quality_band = 'low'")} AS low_count,
            {dialect.count_if("evidence_quality_band = 'very_low'")} AS very_low_count,
            {dialect.count_if("evidence_quality IS NULL OR evidence_quality_band = ''")} AS unknown_count,
            {dialect.var_pop_if("evidence_quality", "evidence_quality IS NOT NULL")} AS quality_variance
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
