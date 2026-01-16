"""Strawberry GraphQL output types for analytics API."""

from __future__ import annotations

from datetime import date
from typing import List, Optional

import strawberry


@strawberry.type
class TimeseriesBucket:
    """A single bucket in a timeseries result."""

    date: date
    value: float


@strawberry.type
class TimeseriesResult:
    """Result of a timeseries query."""

    dimension: str
    dimension_value: str
    measure: str
    buckets: List[TimeseriesBucket]


@strawberry.type
class BreakdownItem:
    """A single item in a breakdown result."""

    key: str
    value: float


@strawberry.type
class BreakdownResult:
    """Result of a breakdown query."""

    dimension: str
    measure: str
    items: List[BreakdownItem]


@strawberry.type
class SankeyNode:
    """A node in a Sankey diagram."""

    id: str
    label: str
    dimension: str
    value: float


@strawberry.type
class SankeyEdge:
    """An edge in a Sankey diagram."""

    source: str
    target: str
    value: float


@strawberry.type
class SankeyResult:
    """Result of a Sankey flow query."""

    nodes: List[SankeyNode]
    edges: List[SankeyEdge]


@strawberry.type
class AnalyticsResult:
    """Combined result of a batch analytics request."""

    timeseries: List[TimeseriesResult]
    breakdowns: List[BreakdownResult]
    sankey: Optional[SankeyResult] = None


@strawberry.type
class CatalogDimension:
    """A dimension available in the catalog."""

    name: str
    description: str


@strawberry.type
class CatalogMeasure:
    """A measure available in the catalog."""

    name: str
    description: str


@strawberry.type
class CatalogLimits:
    """Cost limits for analytics queries."""

    max_days: int
    max_buckets: int
    max_top_n: int
    max_sankey_nodes: int
    max_sankey_edges: int
    max_sub_requests: int


@strawberry.type
class CatalogValueItem:
    """A distinct value for a dimension."""

    value: str
    count: int


@strawberry.type
class CatalogResult:
    """Result of a catalog query."""

    dimensions: List[CatalogDimension]
    measures: List[CatalogMeasure]
    limits: CatalogLimits
    values: Optional[List[CatalogValueItem]] = None
