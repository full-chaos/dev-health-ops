from __future__ import annotations

from datetime import datetime
from typing import Any

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts

# NOTE: This CTE MUST stay tenant-scoped. The ReplacingMergeTree dedup key for
# work_unit_investments is (org_id, work_unit_id) (migration 027), so org_id is
# part of the row identity. We filter org_id BEFORE aggregating and group by
# (org_id, work_unit_id); otherwise two tenants sharing a provider-native
# work_unit_id collapse into a single argMax row and the outer
# `WHERE org_id = %(org_id)s` drops the losing tenant's data entirely
# (cross-org leak / undercount — CHAOS-2374). Every consumer of this CTE already
# supplies the `org_id` query param.
LATEST_WORK_UNIT_INVESTMENTS_CTE = """
        latest_work_unit_investments AS (
            SELECT
                work_unit_id,
                argMax(work_unit_type, computed_at) AS work_unit_type,
                argMax(work_unit_name, computed_at) AS work_unit_name,
                argMax(from_ts, computed_at) AS from_ts,
                argMax(to_ts, computed_at) AS to_ts,
                argMax(repo_id, computed_at) AS repo_id,
                argMax(provider, computed_at) AS provider,
                argMax(effort_metric, computed_at) AS effort_metric,
                argMax(effort_value, computed_at) AS effort_value,
                argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
                argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
                argMax(structural_evidence_json, computed_at) AS structural_evidence_json,
                argMax(evidence_quality, computed_at) AS evidence_quality,
                argMax(evidence_quality_band, computed_at) AS evidence_quality_band,
                argMax(categorization_status, computed_at) AS categorization_status,
                argMax(categorization_model_version, computed_at) AS categorization_model_version,
                argMax(categorization_run_id, computed_at) AS categorization_run_id,
                org_id,
                -- Alias must NOT be ``computed_at``: that name is the ordering
                -- column of every ``argMax(col, computed_at)`` above, and on
                -- ClickHouse 26.5.x an identically-named aggregate alias
                -- shadows the raw column, turning argMax into
                -- ``argMax(col, max(computed_at))`` → ILLEGAL_AGGREGATION (184)
                -- which silently empties the Investment treemap and allocation
                -- sankey. Keep the distinct name.
                max(computed_at) AS latest_computed_at
            FROM work_unit_investments
            WHERE org_id = %(org_id)s
            GROUP BY org_id, work_unit_id
        )
""".rstrip()


async def fetch_investment_breakdown(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {category_filter}
        GROUP BY subcategory, theme
        ORDER BY value DESC
    """
    return await query_dicts(sink, query, params)


async def fetch_mock_fixture_investment_row_count(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> int:
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    filters: list[str] = []
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT count() AS count
        FROM latest_work_unit_investments AS work_unit_investments
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
          AND (
            lower(ifNull(work_unit_investments.provider, '')) IN ('mock', 'fixture', 'fixtures', 'synthetic')
            OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%mock%'
            OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%synthetic%'
            OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%fixture%'
          )
        {scope_filter}
        {category_filter}
    """
    rows = await query_dicts(sink, query, params)
    if not rows:
        return 0
    return int(rows[0].get("count") or 0)


async def fetch_investment_edges(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    scope_filter: str,
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
) -> list[dict[str, Any]]:
    theme_filter = ""
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        theme_filter = " AND theme_kv.1 IN %(themes)s"
        params["themes"] = themes
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            theme_kv.1 AS source,
            ifNull(r.repo, toString(repo_id)) AS target,
            sum(theme_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(theme_distribution_json AS Array(Tuple(String, Float32))) AS theme_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            subcategory_kv.1 AS source,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    [work_unit_investments.work_unit_id]
                )) AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    WHERE org_id = %(org_id)s
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
                {scope_filter}
                GROUP BY work_unit_id, team
            )
            GROUP BY work_unit_id
        )
        SELECT
            subcategory_kv.1 AS source,
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    [work_unit_investments.work_unit_id]
                )) AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    WHERE org_id = %(org_id)s
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
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
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    [work_unit_investments.work_unit_id]
                )) AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    WHERE org_id = %(org_id)s
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
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
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    [work_unit_investments.work_unit_id]
                )) AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    WHERE org_id = %(org_id)s
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
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
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> dict[str, int]:
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
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
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE},
        unit_team AS (
            SELECT
                work_unit_id,
                argMax(team, cnt) AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    [work_unit_investments.work_unit_id]
                )) AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    WHERE org_id = %(org_id)s
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
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
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    }
    params.update(scope_params)
    params["org_id"] = org_id
    if themes:
        filters.append("splitByChar('.', subcategory_kv.1)[1] IN %(themes)s")
        params["themes"] = themes
    if subcategories:
        filters.append("subcategory_kv.1 IN %(subcategories)s")
        params["subcategories"] = subcategories
    category_filter = f" AND ({' OR '.join(filters)})" if filters else ""
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            subcategory_kv.1 AS subcategory,
            splitByChar('.', subcategory_kv.1)[1] AS theme,
            ifNull(r.repo, toString(repo_id)) AS scope,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM latest_work_unit_investments AS work_unit_investments
        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
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
    scope_params: dict[str, Any],
    org_id: str = "",
    themes: list[str] | None = None,
    subcategories: list[str] | None = None,
    team_scope_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Fetch aggregated evidence quality stats: mean, stddev, band counts."""
    filters: list[str] = []
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts}
    params.update(scope_params)
    params["org_id"] = org_id
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
    team_join = ""
    team_filter = ""
    if team_scope_ids:
        params["team_scope_ids"] = team_scope_ids
        team_join = """
        LEFT JOIN (
            SELECT
                work_unit_id,
                argMax(team_id, cnt) AS team_id,
                argMax(team_label, cnt) AS team_label
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    t.team_id AS team_id,
                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team_label,
                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    [work_unit_investments.work_unit_id]
                )) AS issue_id
                LEFT JOIN (
                    SELECT
                        work_item_id,
                        argMax(team_id, computed_at) AS team_id,
                        argMax(team_name, computed_at) AS team_name
                    FROM work_item_cycle_times
                    WHERE org_id = %(org_id)s
                    GROUP BY work_item_id
                ) AS t ON t.work_item_id = issue_id
                WHERE work_unit_investments.from_ts < %(end_ts)s
                  AND work_unit_investments.to_ts >= %(start_ts)s
                  AND work_unit_investments.org_id = %(org_id)s
                GROUP BY work_unit_id, team_id, team_label
            )
            GROUP BY work_unit_id
        ) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        """
        team_filter = """
          AND (
              unit_team.team_label IN %(team_scope_ids)s
              OR unit_team.team_id IN %(team_scope_ids)s
          )
        """
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            count() AS total,
            countIf(evidence_quality IS NOT NULL) AS quality_known_count,
            avgIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_mean,
            stddevPopIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_stddev,
            countIf(evidence_quality_band = 'high') AS high_count,
            countIf(evidence_quality_band = 'moderate') AS moderate_count,
            countIf(evidence_quality_band = 'low') AS low_count,
            countIf(evidence_quality_band = 'very_low') AS very_low_count,
            countIf(evidence_quality IS NULL OR evidence_quality_band = '') AS unknown_count
        FROM latest_work_unit_investments AS work_unit_investments
        {team_join}
        WHERE work_unit_investments.from_ts < %(end_ts)s
          AND work_unit_investments.to_ts >= %(start_ts)s
          AND work_unit_investments.org_id = %(org_id)s
        {scope_filter}
        {team_filter}
        {category_filter}
    """
    rows = await query_dicts(sink, query, params)
    if not rows:
        return {}
    return dict(rows[0])
