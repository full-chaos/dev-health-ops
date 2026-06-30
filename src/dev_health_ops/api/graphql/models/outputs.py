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
    """A single item in a breakdown result.

    ``key`` is the stable dimension value (id or slug). ``label`` is the
    server-resolved human-readable name (Framework A7); it falls back to a
    controlled non-UUID token when the entity cannot be resolved (A8) so the
    client never renders a raw id as the primary label.
    """

    key: str
    value: float
    label: str | None = None


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
class FlowMatrixResult:
    """Result of a same-dimension flow matrix query.

    Returns N×N directional flow where both source and target share a single
    dimension (team↔team, repo↔repo, work_type↔work_type). Reuses SankeyNode
    and SankeyEdge shapes so downstream adapters do not need to distinguish.
    """

    nodes: list[SankeyNode]
    edges: list[SankeyEdge]


@strawberry.type
class EvidenceQualityStats:
    mean: float | None = None
    stddev: float | None = None
    total: int = 0
    band_counts: strawberry.scalars.JSON = strawberry.field(default_factory=dict)


@strawberry.type
class AnalyticsResult:
    """Combined result of a batch analytics request."""

    timeseries: list[TimeseriesResult]
    breakdowns: list[BreakdownResult]
    sankey: SankeyResult | None = None
    flow_matrix: FlowMatrixResult | None = None
    evidence_quality_distribution: strawberry.scalars.JSON | None = None
    evidence_quality_stats: EvidenceQualityStats | None = None


@strawberry.type
class ProductTelemetryDailyActiveUsersType:
    day: date
    active_anonymous_users: int


@strawberry.type
class ProductTelemetryRouteUsageType:
    route_pattern: str
    events: int
    sessions: int
    anonymous_users: int


@strawberry.type
class ProductTelemetryFeatureViewType:
    feature: str
    surface: str
    views: int
    anonymous_users: int


@strawberry.type
class ProductTelemetryFilterChangeType:
    view: str
    filter_key: str
    changes: int
    avg_value_count: float | None = None


@strawberry.type
class ProductTelemetryChartInteractionType:
    chart: str
    action: str
    surface: str
    interactions: int
    sessions: int


@strawberry.type
class ProductTelemetryClientErrorType:
    route_pattern: str
    boundary: str
    error_class: str
    errors: int
    affected_anonymous_users: int


@strawberry.type
class ProductTelemetrySessionSummaryType:
    p50_duration_ms: int | None = None
    p75_duration_ms: int | None = None
    p90_duration_ms: int | None = None
    p95_duration_ms: int | None = None
    avg_pages_viewed: float | None = None
    avg_interactions: float | None = None


@strawberry.type
class ProductTelemetryDashboardType:
    daily_active_users: list[ProductTelemetryDailyActiveUsersType]
    top_routes: list[ProductTelemetryRouteUsageType]
    feature_views: list[ProductTelemetryFeatureViewType]
    filter_changes: list[ProductTelemetryFilterChangeType]
    chart_interactions: list[ProductTelemetryChartInteractionType]
    client_errors: list[ProductTelemetryClientErrorType]
    session_summary: ProductTelemetrySessionSummaryType


@strawberry.type
class ProductTelemetryPlatformTotalsType:
    """Cross-org totals shown at the top of the Super-admin dashboard."""

    active_orgs: int
    anonymous_users: int
    sessions: int
    events: int


@strawberry.type
class ProductTelemetryTopOrgType:
    """Per-org rollup row for the Super-admin drilldown table.

    ``org_id`` / ``org_name`` / ``org_slug`` are resolved server-side from
    Postgres by hashing each known organization's id and matching against
    ``org_id_hash``. Unknown hashes (e.g., events ingested from orgs that no
    longer exist) keep those fields ``None`` and the UI falls back to the
    hash prefix.
    """

    org_id_hash: str
    events: int
    sessions: int
    anonymous_users: int
    org_id: str | None = None
    org_name: str | None = None
    org_slug: str | None = None


@strawberry.type
class ProductTelemetryPlatformDashboardType:
    """Platform-admin product telemetry dashboard payload."""

    totals: ProductTelemetryPlatformTotalsType
    daily_active_users: list[ProductTelemetryDailyActiveUsersType]
    top_routes: list[ProductTelemetryRouteUsageType]
    feature_views: list[ProductTelemetryFeatureViewType]
    filter_changes: list[ProductTelemetryFilterChangeType]
    chart_interactions: list[ProductTelemetryChartInteractionType]
    client_errors: list[ProductTelemetryClientErrorType]
    session_summary: ProductTelemetrySessionSummaryType
    top_orgs: list[ProductTelemetryTopOrgType]


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
class ReworkThemeAllocation:
    theme: str
    label: str
    allocation: float
    allocation_pct: float
    prs_merged: int
    churn_loc: int


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
    rework_theme_allocation: list[ReworkThemeAllocation]


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
    AI_WORKFLOW_RUN = "ai_workflow_run"
    DIFF = "diff"
    REVIEW_OUTCOME = "review_outcome"
    DEPLOYMENT = "deployment"
    INCIDENT = "incident"


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
    HAS_AI_WORKFLOW = "has_ai_workflow"
    GENERATES = "generates"
    HAS_REVIEW_OUTCOME = "has_review_outcome"
    DEPLOYS = "deploys"
    LINKED_INCIDENT = "linked_incident"


@strawberry.enum
class WorkGraphProvenance(Enum):
    """How an edge was discovered."""

    NATIVE = "native"
    EXPLICIT_TEXT = "explicit_text"
    HEURISTIC = "heuristic"


@strawberry.enum
class TeamAttributionSource(Enum):
    """Which signal attributed a work item to a team (CHAOS-2600).

    Listed strongest-first (native_team) to the floor (unassigned). Mirrors
    ``work_item_team_attributions.source`` (ClickHouse Enum8) and
    ``compute_work_items.TeamAttributionSource`` — keep all three in lockstep.
    """

    NATIVE_TEAM = "native_team"
    ISSUE_PROJECT = "issue_project"
    PROJECT_OWNERSHIP = "project_ownership"
    REPO_OWNERSHIP = "repo_ownership"
    ASSIGNEE_MEMBERSHIP = "assignee_membership"
    LINKED_ISSUE = "linked_issue"
    MANUAL_FALLBACK = "manual_fallback"
    UNASSIGNED = "unassigned"


@strawberry.enum
class TeamAttributionConfidence(Enum):
    """Confidence of a team attribution. Mirrors
    ``work_item_team_attributions.confidence`` (ClickHouse Enum8)."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    MANUAL = "manual"
    NONE = "none"


@strawberry.type
class WorkItemTeamAttribution:
    """One team-attribution candidate for a work item, with provenance (CHAOS-2600).

    Every applicable source is persisted as a candidate; ``is_primary`` marks the
    winner the precedence resolver selected. The web renders this to explain *why*
    a work item maps to a team (replacing its client-side attribution).
    """

    work_item_id: str
    provider: str
    team_id: str | None
    team_name: str | None
    source: TeamAttributionSource
    confidence: TeamAttributionConfidence
    is_primary: bool
    evidence: str


@strawberry.type
class WorkUnitTeamAttribution:
    """The team a work UNIT rolls up to, with provenance (CHAOS-2600 CS7).

    A work unit (investment cluster) is an aggregation of many work items, each
    with its own per-item ``WorkItemTeamAttribution``. This type collapses those
    member attributions to the ONE team the unit is owned by, chosen by the same
    source precedence the per-item resolver uses (``native_team`` strongest down
    to ``unassigned``; ties broken by how many member items back the team). The
    web renders this as the unit's team badge in the Investment view — it cannot
    derive it client-side because ``work_unit_id`` (a content hash) and
    ``work_item_id`` (a provider key) are disjoint id spaces joined only by
    ``work_unit_membership`` in ClickHouse.
    """

    work_unit_id: str
    team_id: str | None
    team_name: str | None
    source: TeamAttributionSource
    confidence: TeamAttributionConfidence
    # Always true: this IS the unit's selected team. Kept for shape-symmetry with
    # WorkItemTeamAttribution and so the web can reuse its primary-selection hook.
    is_primary: bool
    # Number of member work items whose primary attribution backs the chosen team.
    member_count: int
    evidence: str


@strawberry.type
class WorkGraphEdgeResult:
    """A single edge in the work graph.

    source_display_name / target_display_name are A7/A8 resolved labels:
    a non-UUID id passes through verbatim; a UUID that cannot be looked up
    is None so the client renders a controlled Unresolved badge (never a
    bare UUID).

    theme / subcategory are the dominant investment theme and subcategory of
    the work unit that contains an endpoint of this edge, looked up from
    work_unit_membership (CHAOS-2430). Null when neither endpoint belongs to
    a known work unit.
    """

    edge_id: str
    source_type: WorkGraphNodeType
    source_id: str
    source_display_name: str | None = None
    target_type: WorkGraphNodeType
    target_id: str
    target_display_name: str | None = None
    edge_type: WorkGraphEdgeType
    provenance: WorkGraphProvenance
    confidence: float
    evidence: str
    repo_id: str | None = None
    provider: str | None = None
    theme: str | None = None
    subcategory: str | None = None


@strawberry.type
class WorkGraphEdgesResult:
    """Result for work graph edges query.

    ``degraded_reason`` (wire: ``degradedReason``) is non-null only when a
    theme/subcategory filter was requested, the matched set is empty, and the
    org has ``work_unit_investments`` rows but ZERO ``work_unit_membership`` rows
    (latest-run scoped) — i.e. the post-migration investment materialization
    that populates ``work_unit_membership`` has not run yet. In that case it is
    ``"MEMBERSHIP_NOT_MATERIALIZED"`` so the client can distinguish a transient
    rollout state from a genuine empty result. It is ``None`` in every other
    case, including a genuine empty result when membership data exists
    (CHAOS-2430).
    """

    edges: list[WorkGraphEdgeResult]
    total_count: int
    page_info: PageInfo
    degraded_reason: str | None = None
    is_partial: bool = False
    partial_scope: str | None = None
    partial_repo_ids: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class WorkGraphFlowRow:
    """Inflow/outflow edge counts for one node type, computed over the FULL
    edge set (correct at any scale — NOT derived from a capped edge page).

    ``inflow`` counts edges whose TARGET is this node type; ``outflow`` counts
    edges whose SOURCE is this node type (CHAOS-2442 Inflow/Outflow tab).
    """

    node_type: WorkGraphNodeType
    inflow: int
    outflow: int


@strawberry.type
class WorkGraphFlowResult:
    """Per-node-type inflow/outflow aggregate over the whole work graph.

    ``degraded_reason`` follows the same contract as ``WorkGraphEdgesResult``:
    ``"MEMBERSHIP_NOT_MATERIALIZED"`` only when a theme/subcategory filter is
    active, the aggregate is empty, and the org has investments but no complete
    membership run yet; ``None`` otherwise (CHAOS-2442).
    """

    rows: list[WorkGraphFlowRow]
    degraded_reason: str | None = None
    is_partial: bool = False
    partial_scope: str | None = None
    partial_repo_ids: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class WorkGraphArtifactRow:
    """A single node ranked by degree (edges touching it as source OR target),
    computed over the FULL edge set (CHAOS-2442 Artifacts tab).

    ``display_name`` is the A7/A8-resolved label (None → client Unresolved
    badge, never a raw UUID). ``evidence`` is an opaque sample edge evidence
    string (or None).
    """

    node_type: WorkGraphNodeType
    node_id: str
    display_name: str | None
    degree: int
    evidence: str | None


@strawberry.type
class WorkGraphArtifactsResult:
    """Top-N nodes by degree across the whole work graph.

    ``degraded_reason`` follows the same contract as ``WorkGraphEdgesResult``
    (CHAOS-2442).
    """

    rows: list[WorkGraphArtifactRow]
    degraded_reason: str | None = None
    is_partial: bool = False
    partial_scope: str | None = None
    partial_repo_ids: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class FeatureFlagItem:
    flag_id: str
    flag_key: str
    provider: str
    project_key: str
    flag_type: str
    created_at: str
    archived_at: str | None = None


@strawberry.type
class FeatureFlagRegistryResult:
    flags: list[FeatureFlagItem]
    total_count: int
    degraded_reason: str | None = None


@strawberry.type
class FeatureFlagEventItem:
    flag_key: str
    event_type: str
    prev_state: str
    next_state: str
    actor_type: str
    environment: str
    event_ts: str


@strawberry.type
class FeatureFlagEventsResult:
    events: list[FeatureFlagEventItem]
    total_count: int
    degraded_reason: str | None = None


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


@strawberry.type
class ThroughputRollingWindow:
    """Rolling weekly throughput summary for a forecast window."""

    window_weeks: int
    mean_weekly_throughput: float
    sample_count: int
    insufficient_history: bool


@strawberry.type
class ThroughputRiskOverlay:
    """Risk overlay shown with a throughput forecast."""

    kind: str
    score: float
    label: str
    value: float
    threshold: float
    active: bool


@strawberry.type
class ThroughputStaleWip:
    p50_age_hours: float | None = None
    p90_age_hours: float | None = None


@strawberry.type
class ThroughputForecast:
    """Throughput-based capacity forecast result.

    ``team_id`` is null when the forecast is computed org-wide (no team
    scope; CHAOS-1783).
    """

    forecast_id: str
    computed_at: str
    team_id: str | None = None
    work_scope_id: str | None = None
    backlog_size: int
    history_weeks: int
    p50_weeks: int | None
    p75_weeks: int | None
    p90_weeks: int | None
    rolling_windows: list[ThroughputRollingWindow]
    primary_risk: ThroughputRiskOverlay
    wip_congestion: ThroughputRiskOverlay
    stale_wip: ThroughputStaleWip | None = None
    review_bottleneck: ThroughputRiskOverlay
    incident_load: ThroughputRiskOverlay
    insufficient_history: bool


@strawberry.type
class OperatingReviewDelta:
    """Week-over-week delta for an operating review metric."""

    value: float
    prior_value: float
    absolute: float
    percent: float | None
    status: str


@strawberry.type
class OperatingReviewMetric:
    """A single metric in a weekly operating review section."""

    key: str
    label: str
    value: float
    unit: str
    delta: OperatingReviewDelta


@strawberry.type
class OperatingReviewSection:
    """Fixed weekly operating review section."""

    key: str
    title: str
    metrics: list[OperatingReviewMetric]
    changed: list[str]
    improved: list[str]
    worsened: list[str]


@strawberry.type
class OperatingReview:
    """Weekly Engineering Operating Review for a team or org-wide aggregate.

    ``team_id`` is ``None`` when the report covers all teams (CHAOS-1755).
    """

    org_id: str
    team_id: str | None
    week_start: date
    prior_week_start: date
    sections: list[OperatingReviewSection]
    recommendations: list[str]
    recommendations_empty_state: str


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
