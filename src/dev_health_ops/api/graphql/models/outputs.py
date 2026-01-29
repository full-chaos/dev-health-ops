"""Strawberry GraphQL output types for analytics API."""

from __future__ import annotations

from datetime import date  # noqa: F401 - used in type annotations
from enum import Enum
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
class SankeyCoverage:
    """Coverage metrics for the Sankey flow."""

    team_coverage: float
    repo_coverage: float


@strawberry.type
class SankeyResult:
    """Result of a Sankey flow query."""

    nodes: List[SankeyNode]
    edges: List[SankeyEdge]
    coverage: Optional[SankeyCoverage] = None


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


# =============================================================================
# Pagination types for cursor-based navigation
# =============================================================================


@strawberry.type
class PageInfo:
    """
    Relay-style pagination info.

    Provides information about the current page and whether more data exists.
    """

    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str] = None
    end_cursor: Optional[str] = None


@strawberry.type
class BreakdownItemEdge:
    """Edge for breakdown item connection."""

    node: BreakdownItem
    cursor: str


@strawberry.type
class BreakdownConnection:
    """Paginated connection for breakdown results."""

    edges: List[BreakdownItemEdge]
    page_info: PageInfo
    total_count: int
    dimension: str
    measure: str


@strawberry.type
class CatalogValueEdge:
    """Edge for catalog value connection."""

    node: CatalogValueItem
    cursor: str


@strawberry.type
class CatalogValueConnection:
    """Paginated connection for catalog dimension values."""

    edges: List[CatalogValueEdge]
    page_info: PageInfo
    total_count: int


# =============================================================================
# Home and summary types
# =============================================================================


@strawberry.type
class SparkPoint:
    """A single point in a sparkline."""

    ts: str
    value: float


@strawberry.type
class MetricDelta:
    """A metric with change over time."""

    metric: str
    label: str
    value: float
    unit: str
    delta_pct: float
    spark: List[SparkPoint]


@strawberry.type
class Coverage:
    """Data coverage metrics."""

    repos_covered_pct: float
    prs_linked_to_issues_pct: float
    issues_with_cycle_states_pct: float


@strawberry.type
class Freshness:
    """Data freshness information."""

    last_ingested_at: Optional[str] = None
    coverage: Optional[Coverage] = None


@strawberry.type
class HomeResult:
    """Result for home dashboard query."""

    freshness: Freshness
    deltas: List[MetricDelta]


# =============================================================================
# Opportunities types
# =============================================================================


@strawberry.type
class OpportunityCard:
    """An opportunity/focus card."""

    id: str
    title: str
    rationale: str
    evidence_links: List[str]
    suggested_experiments: List[str]


@strawberry.type
class OpportunitiesResult:
    """Result for opportunities query."""

    items: List[OpportunityCard]


# =============================================================================
# People types
# =============================================================================


@strawberry.type
class PersonSearchResult:
    """Result for person search."""

    id: str
    name: str
    email: Optional[str] = None
    team: Optional[str] = None


@strawberry.type
class PersonMetric:
    """A metric for a person."""

    metric: str
    value: float
    unit: str


@strawberry.type
class PersonResult:
    """Detailed person information."""

    id: str
    name: str
    email: Optional[str] = None
    team: Optional[str] = None
    metrics: List[PersonMetric]


# =============================================================================
# Drilldown types
# =============================================================================


@strawberry.enum
class DrilldownType(Enum):
    """Type of drilldown data."""

    PRS = "prs"
    ISSUES = "issues"


@strawberry.type
class PullRequestItem:
    """A pull request in drilldown results."""

    repo_id: str
    number: int
    title: Optional[str] = None
    author: Optional[str] = None
    created_at: str
    merged_at: Optional[str] = None
    link: Optional[str] = None


@strawberry.type
class IssueItem:
    """An issue in drilldown results."""

    work_item_id: str
    provider: str
    status: str
    team_id: Optional[str] = None
    cycle_time_hours: Optional[float] = None
    link: Optional[str] = None


@strawberry.type
class DrilldownResult:
    """Result for drilldown query."""

    prs: Optional[List[PullRequestItem]] = None
    issues: Optional[List[IssueItem]] = None


# =============================================================================
# Work Graph types
# =============================================================================


@strawberry.enum
class WorkGraphNodeType(Enum):
    """Types of nodes in the work graph."""

    ISSUE = "issue"
    PR = "pr"
    COMMIT = "commit"
    FILE = "file"


@strawberry.enum
class WorkGraphEdgeType(Enum):
    """Types of edges in the work graph."""

    # Issue-to-issue relationships
    BLOCKS = "blocks"
    RELATES = "relates"
    DUPLICATES = "duplicates"
    IS_BLOCKED_BY = "is_blocked_by"
    IS_RELATED_TO = "is_related_to"
    IS_DUPLICATE_OF = "is_duplicate_of"
    PARENT_OF = "parent_of"
    CHILD_OF = "child_of"

    # Issue-to-PR relationships
    REFERENCES = "references"
    IMPLEMENTS = "implements"
    FIXES = "fixes"

    # PR-to-commit relationships
    CONTAINS = "contains"

    # Commit-to-file relationships
    TOUCHES = "touches"


@strawberry.enum
class WorkGraphProvenance(Enum):
    """How an edge was discovered."""

    NATIVE = "native"
    EXPLICIT_TEXT = "explicit_text"
    HEURISTIC = "heuristic"


@strawberry.type
class WorkGraphEdgeResult:
    """A single edge in the work graph."""

    edge_id: str
    source_type: WorkGraphNodeType
    source_id: str
    target_type: WorkGraphNodeType
    target_id: str
    edge_type: WorkGraphEdgeType
    provenance: WorkGraphProvenance
    confidence: float
    evidence: str
    repo_id: Optional[str] = None
    provider: Optional[str] = None


@strawberry.type
class WorkGraphEdgesResult:
    """Result for work graph edges query."""

    edges: List[WorkGraphEdgeResult]
    total_count: int
    page_info: PageInfo


# =============================================================================
# Capacity Planning types
# =============================================================================


@strawberry.type
class CapacityForecast:
    """Result of a Monte Carlo capacity forecast."""

    forecast_id: str
    computed_at: str
    team_id: Optional[str] = None
    work_scope_id: Optional[str] = None
    backlog_size: int
    target_items: Optional[int] = None
    target_date: Optional[date] = None
    p50_date: Optional[date] = None
    p85_date: Optional[date] = None
    p95_date: Optional[date] = None
    p50_days: Optional[int] = None
    p85_days: Optional[int] = None
    p95_days: Optional[int] = None
    p50_items: Optional[int] = None
    p85_items: Optional[int] = None
    p95_items: Optional[int] = None
    throughput_mean: float
    throughput_stddev: float
    history_days: int
    insufficient_history: bool = False
    high_variance: bool = False


@strawberry.type
class CapacityForecastEdge:
    """Edge for capacity forecast connection."""

    node: CapacityForecast
    cursor: str


@strawberry.type
class CapacityForecastConnection:
    """Paginated connection for capacity forecasts."""

    edges: List[CapacityForecastEdge]
    page_info: PageInfo
    total_count: int
