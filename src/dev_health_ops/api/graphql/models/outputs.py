"""Strawberry GraphQL output types for analytics API."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

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
    buckets: list[TimeseriesBucket]


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
    items: list[BreakdownItem]


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

    nodes: list[SankeyNode]
    edges: list[SankeyEdge]
    coverage: SankeyCoverage | None = None


@strawberry.type
class AnalyticsResult:
    """Combined result of a batch analytics request."""

    timeseries: list[TimeseriesResult]
    breakdowns: list[BreakdownResult]
    sankey: SankeyResult | None = None


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

    dimensions: list[CatalogDimension]
    measures: list[CatalogMeasure]
    limits: CatalogLimits
    values: list[CatalogValueItem] | None = None


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
    start_cursor: str | None = None
    end_cursor: str | None = None


@strawberry.type
class BreakdownItemEdge:
    """Edge for breakdown item connection."""

    node: BreakdownItem
    cursor: str


@strawberry.type
class BreakdownConnection:
    """Paginated connection for breakdown results."""

    edges: list[BreakdownItemEdge]
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

    edges: list[CatalogValueEdge]
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
    spark: list[SparkPoint]


@strawberry.type
class Coverage:
    """Data coverage metrics."""

    repos_covered_pct: float
    prs_linked_to_issues_pct: float
    issues_with_cycle_states_pct: float


@strawberry.type
class Freshness:
    """Data freshness information."""

    last_ingested_at: str | None = None
    coverage: Coverage | None = None


@strawberry.type
class HomeResult:
    """Result for home dashboard query."""

    freshness: Freshness
    deltas: list[MetricDelta]


# =============================================================================
# Opportunities types
# =============================================================================


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
    title: str | None = None
    author: str | None = None
    created_at: str
    merged_at: str | None = None
    link: str | None = None


@strawberry.type
class IssueItem:
    """An issue in drilldown results."""

    work_item_id: str
    provider: str
    status: str
    team_id: str | None = None
    cycle_time_hours: float | None = None
    link: str | None = None


@strawberry.type
class DrilldownResult:
    """Result for drilldown query."""

    prs: list[PullRequestItem] | None = None
    issues: list[IssueItem] | None = None


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
    RELEASE = "release"
    FEATURE_FLAG = "feature_flag"


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

    # Release relationships
    INTRODUCED_BY = "introduced_by"

    # Feature flag relationships
    CONFIG_CHANGED_BY = "config_changed_by"
    GUARDS = "guards"

    # Cross-cutting impact relationships
    IMPACTS = "impacts"


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
    repo_id: str | None = None
    provider: str | None = None


@strawberry.type
class WorkGraphEdgesResult:
    """Result for work graph edges query."""

    edges: list[WorkGraphEdgeResult]
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
    team_id: str | None = None
    work_scope_id: str | None = None
    backlog_size: int
    target_items: int | None = None
    target_date: date | None = None
    p50_date: date | None = None
    p85_date: date | None = None
    p95_date: date | None = None
    p50_days: int | None = None
    p85_days: int | None = None
    p95_days: int | None = None
    p50_items: int | None = None
    p85_items: int | None = None
    p95_items: int | None = None
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

    edges: list[CapacityForecastEdge]
    page_info: PageInfo
    total_count: int


# =============================================================================
# Security alert output types
# =============================================================================


@strawberry.type
class SecurityAlertNode:
    """A single security alert with repo context."""

    alert_id: str
    repo_id: str
    repo_name: str  # joined from repos.repo in ClickHouse
    repo_url: str | None
    source: str  # lowercase string matching SecuritySourceInput enum values
    severity: str
    state: str
    package_name: str | None
    cve_id: str | None
    url: str | None  # upstream link (GitHub/GitLab alert page)
    title: str | None
    description: str | None
    created_at: datetime
    fixed_at: datetime | None
    dismissed_at: datetime | None


@strawberry.type
class SecurityAlertEdge:
    """Edge wrapping a security alert node for cursor-based pagination."""

    node: SecurityAlertNode
    cursor: str


@strawberry.type
class SecurityAlertConnection:
    """Paginated connection for security alert results."""

    edges: list[SecurityAlertEdge]
    total_count: int
    page_info: PageInfo


@strawberry.type
class SecurityKpis:
    """Key performance indicators for the security posture dashboard."""

    open_total: int
    critical: int
    high: int
    mean_days_to_fix_30d: float | None  # null if no alerts fixed in the window
    open_delta_30d: int  # net change in open count over last 30 days


@strawberry.type
class SeverityBucket:
    """Alert count aggregated by severity level."""

    severity: str  # one of low/medium/high/critical/unknown
    count: int


@strawberry.type
class RepoAlertCount:
    """Open alert count for a single repository."""

    repo_id: str
    repo_name: str
    repo_url: str | None
    count: int


@strawberry.type
class TrendPoint:
    """Daily opened/fixed counts for trend charting."""

    day: date
    opened: int
    fixed: int


@strawberry.type
class SecurityOverview:
    """Aggregated security posture for the dashboard."""

    kpis: SecurityKpis
    severity_breakdown: list[SeverityBucket]
    top_repos: list[RepoAlertCount]  # LIMIT 10, count DESC
    trend: list[TrendPoint]  # last 30 days, one point per day
