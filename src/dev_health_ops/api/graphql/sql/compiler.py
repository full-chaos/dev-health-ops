"""SQL compiler for GraphQL analytics queries.

Identifies source tables and compiles to parameterized SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from ..authz import enforce_org_scope
from .filter_translation import translate_filters
from .templates import (
    breakdown_template,
    catalog_values_template,
    sankey_edges_template,
    sankey_nodes_template,
    timeseries_template,
)
from .validate import (
    Dimension,
    validate_bucket_interval,
    validate_dimension,
    validate_measure,
    validate_sankey_path,
)

if TYPE_CHECKING:
    from ..models.inputs import FilterInput


# Default query timeout in seconds
DEFAULT_TIMEOUT = 30


@dataclass
class TimeseriesRequest:
    """Request for a timeseries query."""

    dimension: str
    measure: str
    interval: str
    start_date: date
    end_date: date
    use_investment: Optional[bool] = None


@dataclass
class BreakdownRequest:
    """Request for a breakdown query."""

    dimension: str
    measure: str
    start_date: date
    end_date: date
    top_n: int = 10
    use_investment: Optional[bool] = None


@dataclass
class SankeyRequest:
    """Request for a Sankey flow query."""

    path: List[str]
    measure: str
    start_date: date
    end_date: date
    max_nodes: int = 100
    max_edges: int = 500
    use_investment: Optional[bool] = None


@dataclass
class CatalogValuesRequest:
    """Request for catalog dimension values."""

    dimension: str
    limit: int = 100


def _get_context_params(
    dimensions: List[Dimension],
    force_investment: Optional[bool] = None,
    needs_team_join: bool = False,
) -> Dict[str, Any]:
    """Determine source table and extra clauses based on dimensions."""
    investment_dims = {Dimension.THEME, Dimension.SUBCATEGORY}
    auto_use_investment = any(d in investment_dims for d in dimensions)
    use_investment = (
        force_investment if force_investment is not None else auto_use_investment
    )

    if use_investment:
        joins = []
        # ALWAYS join subcategory distribution for investment queries
        joins.append(
            "ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv"
        )

        # Add team join if TEAM dimension is used or filters require it
        if Dimension.TEAM in dimensions or needs_team_join:
            team_join = """
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
                    GROUP BY work_unit_id, team_id, team_label
                )
                GROUP BY work_unit_id
            ) AS ut ON ut.work_unit_id = work_unit_investments.work_unit_id
            """
            joins.append(team_join)

        # Add repo join if REPO dimension is used
        if Dimension.REPO in dimensions:
            joins.append("LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)")

        return {
            "source_table": "work_unit_investments",
            "date_filter": "work_unit_investments.from_ts < %(end_date)s AND work_unit_investments.to_ts >= %(start_date)s",
            "extra_clauses": "\n".join(joins),
            "use_investment": True,
        }

    return {
        "source_table": "investment_metrics_daily",
        "date_filter": "day >= %(start_date)s AND day <= %(end_date)s",
        "extra_clauses": "",
        "use_investment": False,
    }


def _needs_team_join(filters: Optional["FilterInput"]) -> bool:
    if not filters or not filters.scope or not filters.scope.ids:
        return False
    return filters.scope.level.value == "team"


def compile_timeseries(
    request: TimeseriesRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: Optional["FilterInput"] = None,  # NEW: Filter support
) -> Tuple[str, Dict[str, Any]]:
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
    )

    # Translate filters to SQL clause
    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    sql = timeseries_template(
        dimension, measure, interval, filter_clause=filter_clause, **ctx
    )

    params: Dict[str, Any] = {
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
    filters: Optional["FilterInput"] = None,  # NEW: Filter support
) -> Tuple[str, Dict[str, Any]]:
    """
    Compile a breakdown request to parameterized SQL.
    """
    dimension = validate_dimension(request.dimension)
    measure = validate_measure(request.measure)

    ctx = _get_context_params(
        [dimension],
        force_investment=request.use_investment,
        needs_team_join=_needs_team_join(filters),
    )

    # Translate filters to SQL clause
    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    sql = breakdown_template(dimension, measure, filter_clause=filter_clause, **ctx)

    params: Dict[str, Any] = {
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
    filters: Optional["FilterInput"] = None,  # NEW: Filter support
) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Tuple[str, Dict[str, Any]]]]:
    """
    Compile a Sankey request to parameterized SQL queries.
    """
    dimensions = validate_sankey_path(request.path)
    measure = validate_measure(request.measure)

    ctx = _get_context_params(
        dimensions,
        force_investment=request.use_investment,
        needs_team_join=_needs_team_join(filters),
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
    nodes_params: Dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "limit_per_dim": limit_per_dim,
        "timeout": timeout,
    }
    nodes_params.update(filter_params)
    nodes_params = enforce_org_scope(org_id, nodes_params)

    # Build edges queries (one per adjacent pair in path)
    edges_queries: List[Tuple[str, Dict[str, Any]]] = []
    for i in range(len(dimensions) - 1):
        source_dim = dimensions[i]
        target_dim = dimensions[i + 1]

        edge_sql = sankey_edges_template(
            source_dim, target_dim, measure, filter_clause=filter_clause, **ctx
        )
        edge_params: Dict[str, Any] = {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "max_edges": request.max_edges // (len(dimensions) - 1),
            "timeout": timeout,
        }
        edge_params.update(filter_params)
        edge_params = enforce_org_scope(org_id, edge_params)
        edges_queries.append((edge_sql, edge_params))

    return [(nodes_sql, nodes_params)], edges_queries


def compile_catalog_values(
    request: CatalogValuesRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
    filters: Optional["FilterInput"] = None,  # NEW: Filter support
) -> Tuple[str, Dict[str, Any]]:
    """
    Compile a catalog values request to parameterized SQL.
    """
    dimension = validate_dimension(request.dimension)

    ctx = _get_context_params(
        [dimension],
        needs_team_join=_needs_team_join(filters),
    )

    # Translate filters to SQL clause
    filter_clause, filter_params = translate_filters(
        filters, use_investment=ctx.get("use_investment", False)
    )

    sql = catalog_values_template(dimension, filter_clause=filter_clause, **ctx)

    params: Dict[str, Any] = {
        "limit": request.limit,
        "timeout": timeout,
    }
    params.update(filter_params)
    params = enforce_org_scope(org_id, params)

    return sql, params
