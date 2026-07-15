"""SQL compiler for GraphQL analytics queries.

Identifies source tables and compiles to parameterized SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from dev_health_ops.api.queries.investment import (
    LATEST_WORK_UNIT_AUTHORS_CTE,
    LATEST_WORK_UNIT_INVESTMENTS_CTE,
    LATEST_WORK_UNIT_REPO_EFFORT_CTE,
    PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE,
)

from ..authz import enforce_org_scope
from ..errors import ValidationError
from .filter_translation import translate_filters
from .templates import (
    breakdown_template,
    catalog_values_team_template,
    catalog_values_template,
    flow_matrix_repo_edges_template,
    flow_matrix_repo_nodes_template,
    flow_matrix_team_edges_template,
    flow_matrix_team_nodes_template,
    flow_matrix_work_type_edges_template,
    flow_matrix_work_type_nodes_template,
    sankey_edges_template,
    sankey_nodes_template,
    timeseries_template,
)
from .validate import (
    Dimension,
    Measure,
    validate_bucket_interval,
    validate_dimension,
    validate_measure,
    validate_sankey_path,
)

if TYPE_CHECKING:
    from ..models.inputs import FilterInput


# Default query timeout in seconds
DEFAULT_TIMEOUT = 30


# CHAOS-2710: investment_metrics_daily is a plain MergeTree (migration 007), so
# duplicate (re)writes of the same natural key with a newer computed_at do NOT
# self-merge. A Linear-backfill retry (or the scheduled daily recompute) can leave
# multiple rows per (org, day, repo, team, area, stream); a flat SUM() over them
# double-counts. Every other reader (home.py, metrics.py, operating_review.py)
# already collapses with argMax(col, computed_at) over the natural key -- the generic
# analytics templates were the last raw reader, so dedup at the source here. Org-scoped
# only (catalog/value paths bind org_id but not a date range); the template's own
# date_filter still applies to the already-collapsed rows.
_INVESTMENT_METRICS_DAILY_DEDUP = """(
    SELECT
        org_id,
        day,
        repo_id,
        team_id,
        investment_area,
        project_stream,
        argMax(delivery_units, computed_at) AS delivery_units,
        argMax(work_items_completed, computed_at) AS work_items_completed,
        argMax(prs_merged, computed_at) AS prs_merged,
        argMax(churn_loc, computed_at) AS churn_loc,
        argMax(cycle_p50_hours, computed_at) AS cycle_p50_hours
    FROM investment_metrics_daily
    WHERE org_id = %(org_id)s
    GROUP BY org_id, day, repo_id, team_id, investment_area, project_stream
) AS investment_metrics_daily"""


@dataclass
class TimeseriesRequest:
    """Request for a timeseries query."""

    dimension: str
    measure: str
    interval: str
    start_date: date
    end_date: date
    use_investment: bool | None = None


@dataclass
class BreakdownRequest:
    """Request for a breakdown query."""

    dimension: str
    measure: str
    start_date: date
    end_date: date
    top_n: int = 10
    use_investment: bool | None = None


@dataclass
class SankeyRequest:
    """Request for a Sankey flow query."""

    path: list[str]
    measure: str
    start_date: date
    end_date: date
    max_nodes: int = 100
    max_edges: int = 500
    use_investment: bool | None = None


@dataclass
class FlowMatrixRequest:
    """Request for a same-dimension flow matrix query."""

    dimension: str
    measure: str
    start_date: date
    end_date: date
    max_nodes: int = 100
    max_edges: int = 500
    use_investment: bool | None = None


@dataclass
class CatalogValuesRequest:
    """Request for catalog dimension values."""

    dimension: str
    limit: int = 100


def _get_context_params(
    dimensions: list[Dimension],
    force_investment: bool | None = None,
    needs_team_join: bool = False,
    needs_author_join: bool = False,
) -> dict[str, Any]:
    """Determine source table and extra clauses based on dimensions."""
    # WORK_TYPE belongs to the investment-side ``work_unit_investments`` table —
    # the ``investment_metrics_daily`` rollup has no ``work_item_type`` column,
    # so a non-investment WORK_TYPE query is structurally invalid. Treat it as
    # an investment dimension so the compiler auto-routes to the right source.
    investment_dims = {Dimension.THEME, Dimension.SUBCATEGORY, Dimension.WORK_TYPE}
    auto_use_investment = any(d in investment_dims for d in dimensions)
    use_investment = (
        force_investment if force_investment is not None else auto_use_investment
    )

    if use_investment:
        joins = []
        use_repo_allocation = Dimension.REPO in dimensions
        source_table = "latest_work_unit_investments AS work_unit_investments"
        if use_repo_allocation:
            source_table = """
            (
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    wui.work_unit_type AS work_unit_type,
                    wui.work_unit_name AS work_unit_name,
                    wui.from_ts AS from_ts,
                    wui.to_ts AS to_ts,
                    if(wure.work_unit_id != '', wure.repo_id, wui.repo_id) AS repo_id,
                    wui.provider AS provider,
                    if(wure.work_unit_id != '', wure.effort_metric, wui.effort_metric) AS effort_metric,
                    if(wure.work_unit_id != '', wure.repo_effort_value, wui.effort_value) AS effort_value,
                    if(wure.work_unit_id != '', wure.allocation_source, 'scalar_fallback') AS allocation_source,
                    if(wure.work_unit_id != '', if(wui.effort_value > 0, wure.repo_effort_value / wui.effort_value, 0.0), 1.0) AS allocation_weight,
                    wui.theme_distribution_json AS theme_distribution_json,
                    wui.subcategory_distribution_json AS subcategory_distribution_json,
                    wui.structural_evidence_json AS structural_evidence_json,
                    wui.evidence_quality AS evidence_quality,
                    wui.evidence_quality_band AS evidence_quality_band,
                    wui.categorization_status AS categorization_status,
                    wui.categorization_model_version AS categorization_model_version,
                    wui.categorization_run_id AS categorization_run_id,
                    wui.org_id AS org_id
                FROM latest_work_unit_investments AS wui
                LEFT JOIN latest_work_unit_repo_effort AS wure
                    ON wure.org_id = wui.org_id
                    AND wure.work_unit_id = wui.work_unit_id
            ) AS work_unit_investments
            """
        # ALWAYS join subcategory distribution for investment queries
        joins.append(
            "ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv"
        )

        # Add team join if TEAM dimension is used or filters require it
        if Dimension.TEAM in dimensions or needs_team_join:
            team_join = f"""
            LEFT JOIN (
                SELECT
                    work_unit_id,
                    argMax(team_label, cnt) AS team_label,
                    argMax(team_id, cnt) AS team_id
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
                    LEFT JOIN {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE} AS t ON t.work_item_id = issue_id
                    GROUP BY work_unit_id, team_id, team_label
                )
                GROUP BY work_unit_id
            ) AS ut ON ut.work_unit_id = work_unit_investments.work_unit_id
            """
            joins.append(team_join)

        # Add repo join if REPO dimension is used
        if Dimension.REPO in dimensions:
            joins.append("LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)")

        # CHAOS-2492: add developer-identity join if AUTHOR dimension is used
        # or a developer filter (who.developers / scope.level=developer) needs
        # it. Chains LATEST_WORK_UNIT_AUTHORS_CTE onto the WITH clause -- only
        # when actually needed, to avoid the extra join cost otherwise.
        with_parts = [LATEST_WORK_UNIT_INVESTMENTS_CTE]
        if use_repo_allocation:
            with_parts.append(LATEST_WORK_UNIT_REPO_EFFORT_CTE)
        if Dimension.AUTHOR in dimensions or needs_author_join:
            with_parts.append(LATEST_WORK_UNIT_AUTHORS_CTE)
            joins.append(
                "LEFT JOIN work_unit_authors AS au ON au.work_unit_id = work_unit_investments.work_unit_id"
            )

        return {
            "source_table": source_table,
            "date_filter": "work_unit_investments.from_ts < %(end_date)s AND work_unit_investments.to_ts >= %(start_date)s",
            "extra_clauses": "\n".join(joins),
            "with_clause": f"WITH {', '.join(with_parts)}",
            "use_investment": True,
            "use_repo_allocation": use_repo_allocation,
        }

    return {
        "source_table": _INVESTMENT_METRICS_DAILY_DEDUP,
        "date_filter": "day >= %(start_date)s AND day <= %(end_date)s",
        "extra_clauses": "",
        "with_clause": "",
        "use_investment": False,
        "use_repo_allocation": False,
    }


def _needs_team_join(filters: FilterInput | None) -> bool:
    if not filters or not filters.scope or not filters.scope.ids:
        return False
    return filters.scope.level.value == "team"


def _needs_author_join(filters: FilterInput | None) -> bool:
    """CHAOS-2492: does this request need the investment developer-identity join?

    True when a developer/who filter or a developer-scoped view is active, so
    the compiler chains LATEST_WORK_UNIT_AUTHORS_CTE and the ``au`` join onto
    the investment query (see _get_context_params).
    """
    if filters is None:
        return False
    if filters.who is not None and filters.who.developers:
        return True
    return bool(
        filters.scope is not None
        and filters.scope.level.value == "developer"
        and filters.scope.ids
    )


def _has_active_filters(filters: FilterInput | None) -> bool:
    if filters is None:
        return False

    if filters.scope and filters.scope.ids:
        # Any non-org scope with ids would change the result set, but the
        # same-dim TEAM/REPO/WORK_TYPE templates apply no scope predicate
        # (incl. service, which translate_scope_filter no-ops). Treat all of
        # them as active so we reject honestly instead of silently returning
        # org-wide data (CHAOS-2487).
        if filters.scope.level.value != "org":
            return True

    return any(
        (
            filters.who and (filters.who.developers or filters.who.roles),
            filters.what and (filters.what.repos or filters.what.services),
            filters.why and (filters.why.work_category or filters.why.issue_type),
            filters.how and filters.how.flow_stage,
        )
    )


def compile_timeseries(
    request: TimeseriesRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: FilterInput | None = None,  # NEW: Filter support
) -> tuple[str, dict[str, Any]]:
    """
    Compile a timeseries request to parameterized SQL.

    Args:
        request: The timeseries request parameters
        org_id: Organization ID for scoping
        timeout: Query timeout in seconds
        filters: Optional FilterInput for scope/category filtering

    Returns:
        Tuple of (SQL query string, parameters dict)
    """
    dimension = validate_dimension(request.dimension)
    measure = validate_measure(request.measure)
    interval = validate_bucket_interval(request.interval)

    ctx = _get_context_params(
        [dimension],
        force_investment=request.use_investment,
        needs_team_join=_needs_team_join(filters),
        needs_author_join=_needs_author_join(filters),
    )

    # Translate filters to SQL clause
    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    testops_table = Measure.source_table(measure)
    if testops_table:
        ctx["source_table"] = testops_table
        ctx["date_filter"] = "day >= %(start_date)s AND day <= %(end_date)s"

    sql = timeseries_template(
        dimension, measure, interval, filter_clause=filter_clause, **ctx
    )

    params: dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "timeout": timeout,
    }
    params.update(filter_params)
    params = enforce_org_scope(org_id, params)

    return sql, params


def compile_breakdown(
    request: BreakdownRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: FilterInput | None = None,  # NEW: Filter support
) -> tuple[str, dict[str, Any]]:
    """
    Compile a breakdown request to parameterized SQL.
    """
    dimension = validate_dimension(request.dimension)
    measure = validate_measure(request.measure)

    ctx = _get_context_params(
        [dimension],
        force_investment=request.use_investment,
        needs_team_join=_needs_team_join(filters),
        needs_author_join=_needs_author_join(filters),
    )

    # Translate filters to SQL clause
    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    testops_table = Measure.source_table(measure)
    if testops_table:
        ctx["source_table"] = testops_table
        ctx["date_filter"] = "day >= %(start_date)s AND day <= %(end_date)s"

    sql = breakdown_template(dimension, measure, filter_clause=filter_clause, **ctx)

    params: dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "top_n": request.top_n,
        "timeout": timeout,
    }
    params.update(filter_params)
    params = enforce_org_scope(org_id, params)

    return sql, params


def compile_sankey(
    request: SankeyRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: FilterInput | None = None,  # NEW: Filter support
) -> tuple[list[tuple[str, dict[str, Any]]], list[tuple[str, dict[str, Any]]]]:
    """
    Compile a Sankey request to parameterized SQL queries.
    """
    dimensions = validate_sankey_path(request.path)
    measure = validate_measure(request.measure)

    ctx = _get_context_params(
        dimensions,
        force_investment=request.use_investment,
        needs_team_join=_needs_team_join(filters),
        needs_author_join=_needs_author_join(filters),
    )

    # Translate filters to SQL clause
    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    # Calculate per-dimension node limit
    limit_per_dim = max(1, request.max_nodes // len(dimensions))

    # Build nodes query
    nodes_sql = sankey_nodes_template(
        dimensions, measure, filter_clause=filter_clause, **ctx
    )
    nodes_params: dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "limit_per_dim": limit_per_dim,
        "timeout": timeout,
    }
    nodes_params.update(filter_params)
    nodes_params = enforce_org_scope(org_id, nodes_params)

    # Build edges queries (one per adjacent pair in path)
    edges_queries: list[tuple[str, dict[str, Any]]] = []
    for i in range(len(dimensions) - 1):
        source_dim = dimensions[i]
        target_dim = dimensions[i + 1]

        edge_sql = sankey_edges_template(
            source_dim, target_dim, measure, filter_clause=filter_clause, **ctx
        )
        edge_params: dict[str, Any] = {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "max_edges": request.max_edges // (len(dimensions) - 1),
            "timeout": timeout,
        }
        edge_params.update(filter_params)
        edge_params = enforce_org_scope(org_id, edge_params)
        edges_queries.append((edge_sql, edge_params))

    return [(nodes_sql, nodes_params)], edges_queries


def compile_flow_matrix(
    request: FlowMatrixRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: FilterInput | None = None,
) -> tuple[list[tuple[str, dict[str, Any]]], list[tuple[str, dict[str, Any]]]]:
    """Compile a same-dimension flow matrix request to parameterized SQL.

    Produces the same (nodes_queries, edges_queries) tuple shape as
    compile_sankey so _execute_sankey_inner can execute either.

    For TEAM, edges come from a self-join on work_item_cycle_times bridged
    through (work_scope_id, day): every pair of teams that completed work in
    the same scope on the same day becomes an edge, valued by the SOURCE
    team's distinct work_item count. That yields an asymmetric signal that
    unlocks the chord's directional modes.

    For REPO (CHAOS-1292/CHAOS-2848), edges come from cycle-time activity
    joined to work_items (for repo_id) and latest-primary WITA (for team_id),
    then bridge through (team_id, day) — i.e., when the same authoritative team
    touches multiple repos on one day those repos become cross-edges. For
    WORK_TYPE, the bridge is (repo_id, day) — multiple work_types on the same
    repo+day become cross-edges. In all three cases nodes are sourced from the
    same underlying data so node ids and edge endpoints stay consistent.
    """
    dimension = validate_dimension(request.dimension)
    measure = validate_measure(request.measure)

    ctx = _get_context_params(
        [dimension],
        force_investment=request.use_investment,
        needs_team_join=_needs_team_join(filters),
        needs_author_join=_needs_author_join(filters),
    )

    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    common_params: dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "timeout": timeout,
    }

    if dimension == Dimension.TEAM:
        _reject_filtered_same_dimension_flow_matrix(dimension, filters)
        nodes_sql = flow_matrix_team_nodes_template()
        edge_sql = flow_matrix_team_edges_template()
        nodes_params = {**common_params, "limit_per_dim": request.max_nodes}
        nodes_params = enforce_org_scope(org_id, nodes_params)
        edge_params = {**common_params, "max_edges": request.max_edges}
        edge_params = enforce_org_scope(org_id, edge_params)
    elif dimension == Dimension.REPO:
        _reject_filtered_same_dimension_flow_matrix(dimension, filters)
        nodes_sql = flow_matrix_repo_nodes_template()
        edge_sql = flow_matrix_repo_edges_template()
        nodes_params = {**common_params, "limit_per_dim": request.max_nodes}
        nodes_params = enforce_org_scope(org_id, nodes_params)
        edge_params = {**common_params, "max_edges": request.max_edges}
        edge_params = enforce_org_scope(org_id, edge_params)
    elif dimension == Dimension.WORK_TYPE:
        _reject_filtered_same_dimension_flow_matrix(dimension, filters)
        nodes_sql = flow_matrix_work_type_nodes_template()
        edge_sql = flow_matrix_work_type_edges_template()
        nodes_params = {**common_params, "limit_per_dim": request.max_nodes}
        nodes_params = enforce_org_scope(org_id, nodes_params)
        edge_params = {**common_params, "max_edges": request.max_edges}
        edge_params = enforce_org_scope(org_id, edge_params)
    else:
        nodes_sql = sankey_nodes_template(
            [dimension], measure, filter_clause=filter_clause, **ctx
        )
        nodes_params = {**common_params, "limit_per_dim": request.max_nodes}
        nodes_params.update(filter_params)
        nodes_params = enforce_org_scope(org_id, nodes_params)

        edge_sql = sankey_edges_template(
            dimension, dimension, measure, filter_clause=filter_clause, **ctx
        )
        edge_params = {**common_params, "max_edges": request.max_edges}
        edge_params.update(filter_params)
        edge_params = enforce_org_scope(org_id, edge_params)

    return [(nodes_sql, nodes_params)], [(edge_sql, edge_params)]


def _reject_filtered_same_dimension_flow_matrix(
    dimension: Dimension,
    filters: FilterInput | None,
) -> None:
    if not _has_active_filters(filters):
        return

    raise ValidationError(
        "flowMatrix filters are not supported for same-dimension "
        f"{dimension.value} queries yet (CHAOS-2487); remove filters or use "
        "theme/subcategory.",
        field="filters",
        value=dimension.value,
    )


def compile_catalog_values(
    request: CatalogValuesRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: FilterInput | None = None,  # NEW: Filter support
) -> tuple[str, dict[str, Any]]:
    """
    Compile a catalog values request to parameterized SQL.

    CHAOS-1751: The TEAM dimension uses the semantic ``teams`` table as
    the source of truth (LEFT JOINed to an event table for activity
    counts) so the picker reflects the org's actual roster, including
    teams with zero recorded activity. All other dimensions continue to
    derive distinct values from the event table directly.
    """
    dimension = validate_dimension(request.dimension)

    ctx = _get_context_params(
        [dimension],
        needs_team_join=_needs_team_join(filters),
        needs_author_join=_needs_author_join(filters),
    )

    params: dict[str, Any] = {
        "limit": request.limit,
        "timeout": timeout,
    }

    if dimension == Dimension.REPO:
        if _has_active_filters(filters):
            raise ValidationError(
                "repository catalog filters are not supported",
                field="filters",
                value="repo",
            )
        params = enforce_org_scope(org_id, params)
        return (
            """
SELECT
    repo AS value,
    count() AS count
FROM repos FINAL
WHERE org_id = %(org_id)s
  AND repo != ''
GROUP BY value
ORDER BY value
LIMIT %(limit)s
SETTINGS max_execution_time = %(timeout)s
""",
            params,
        )

    if dimension == Dimension.TEAM:
        # Filter scope/category clauses target event-table columns, which
        # do not apply when listing teams from the semantic source of
        # truth. The picker always exposes the full active roster.
        sql = catalog_values_team_template(
            count_source_table=ctx["source_table"],
        )
    else:
        filter_clause, filter_params = translate_filters(
            filters, use_investment=ctx.get("use_investment", False)
        )
        sql = catalog_values_template(dimension, filter_clause=filter_clause, **ctx)
        params.update(filter_params)

    params = enforce_org_scope(org_id, params)

    return sql, params
