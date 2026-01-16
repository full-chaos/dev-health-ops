"""SQL compiler for GraphQL analytics queries.

Validates inputs against allowlists and compiles to parameterized SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Tuple

from ..authz import enforce_org_scope
from ..errors import ValidationError
from .templates import (
    breakdown_template,
    catalog_values_template,
    sankey_edges_template,
    sankey_nodes_template,
    timeseries_template,
)
from .validate import (
    BucketInterval,
    Dimension,
    Measure,
    validate_bucket_interval,
    validate_dimension,
    validate_measure,
    validate_sankey_path,
)


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


@dataclass
class BreakdownRequest:
    """Request for a breakdown query."""

    dimension: str
    measure: str
    start_date: date
    end_date: date
    top_n: int = 10


@dataclass
class SankeyRequest:
    """Request for a Sankey flow query."""

    path: List[str]
    measure: str
    start_date: date
    end_date: date
    max_nodes: int = 100
    max_edges: int = 500


@dataclass
class CatalogValuesRequest:
    """Request for catalog dimension values."""

    dimension: str
    limit: int = 100


def compile_timeseries(
    request: TimeseriesRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[str, Dict[str, Any]]:
    """
    Compile a timeseries request to parameterized SQL.

    Args:
        request: TimeseriesRequest with dimension, measure, interval, dates.
        org_id: Organization ID for scoping.
        timeout: Query timeout in seconds.

    Returns:
        Tuple of (sql_string, params_dict).

    Raises:
        ValidationError: If any input is invalid.
    """
    dimension = validate_dimension(request.dimension)
    measure = validate_measure(request.measure)
    interval = validate_bucket_interval(request.interval)

    sql = timeseries_template(dimension, measure, interval)

    params: Dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "timeout": timeout,
    }
    params = enforce_org_scope(org_id, params)

    return sql, params


def compile_breakdown(
    request: BreakdownRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[str, Dict[str, Any]]:
    """
    Compile a breakdown request to parameterized SQL.

    Args:
        request: BreakdownRequest with dimension, measure, dates, top_n.
        org_id: Organization ID for scoping.
        timeout: Query timeout in seconds.

    Returns:
        Tuple of (sql_string, params_dict).

    Raises:
        ValidationError: If any input is invalid.
    """
    dimension = validate_dimension(request.dimension)
    measure = validate_measure(request.measure)

    sql = breakdown_template(dimension, measure)

    params: Dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "top_n": request.top_n,
        "timeout": timeout,
    }
    params = enforce_org_scope(org_id, params)

    return sql, params


def compile_sankey(
    request: SankeyRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Tuple[str, Dict[str, Any]]]]:
    """
    Compile a Sankey request to parameterized SQL queries.

    Returns separate queries for nodes and edges.

    Args:
        request: SankeyRequest with path, measure, dates, limits.
        org_id: Organization ID for scoping.
        timeout: Query timeout in seconds.

    Returns:
        Tuple of (nodes_queries, edges_queries) where each is a list of (sql, params).

    Raises:
        ValidationError: If any input is invalid.
    """
    dimensions = validate_sankey_path(request.path)
    measure = validate_measure(request.measure)

    # Calculate per-dimension node limit
    limit_per_dim = max(1, request.max_nodes // len(dimensions))

    # Build nodes query
    nodes_sql = sankey_nodes_template(dimensions, measure)
    nodes_params: Dict[str, Any] = {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "limit_per_dim": limit_per_dim,
        "timeout": timeout,
    }
    nodes_params = enforce_org_scope(org_id, nodes_params)

    # Build edges queries (one per adjacent pair in path)
    edges_queries: List[Tuple[str, Dict[str, Any]]] = []
    for i in range(len(dimensions) - 1):
        source_dim = dimensions[i]
        target_dim = dimensions[i + 1]

        edge_sql = sankey_edges_template(source_dim, target_dim, measure)
        edge_params: Dict[str, Any] = {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "max_edges": request.max_edges // (len(dimensions) - 1),
            "timeout": timeout,
        }
        edge_params = enforce_org_scope(org_id, edge_params)
        edges_queries.append((edge_sql, edge_params))

    return [(nodes_sql, nodes_params)], edges_queries


def compile_catalog_values(
    request: CatalogValuesRequest,
    org_id: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[str, Dict[str, Any]]:
    """
    Compile a catalog values request to parameterized SQL.

    Args:
        request: CatalogValuesRequest with dimension and limit.
        org_id: Organization ID for scoping.
        timeout: Query timeout in seconds.

    Returns:
        Tuple of (sql_string, params_dict).

    Raises:
        ValidationError: If dimension is invalid.
    """
    dimension = validate_dimension(request.dimension)

    sql = catalog_values_template(dimension)

    params: Dict[str, Any] = {
        "limit": request.limit,
        "timeout": timeout,
    }
    params = enforce_org_scope(org_id, params)

    return sql, params
