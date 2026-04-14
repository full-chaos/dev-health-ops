"""Strawberry GraphQL input types for analytics API."""

from __future__ import annotations

from datetime import date
from enum import Enum

import strawberry


@strawberry.enum
class DimensionInput(Enum):
    """Allowlisted dimensions for analytics queries."""

    TEAM = "team"
    REPO = "repo"
    AUTHOR = "author"
    WORK_TYPE = "work_type"
    THEME = "theme"
    SUBCATEGORY = "subcategory"


@strawberry.enum
class MeasureInput(Enum):
    """Allowlisted measures for analytics queries."""

    COUNT = "count"
    CHURN_LOC = "churn_loc"
    CYCLE_TIME_HOURS = "cycle_time_hours"
    THROUGHPUT = "throughput"
    PIPELINE_SUCCESS_RATE = "pipeline_success_rate"
    PIPELINE_FAILURE_RATE = "pipeline_failure_rate"
    PIPELINE_DURATION_P95 = "pipeline_duration_p95"
    PIPELINE_QUEUE_TIME = "pipeline_queue_time"
    PIPELINE_RERUN_RATE = "pipeline_rerun_rate"
    TEST_PASS_RATE = "test_pass_rate"
    TEST_FAILURE_RATE = "test_failure_rate"
    TEST_FLAKE_RATE = "test_flake_rate"
    TEST_SUITE_DURATION_P95 = "test_suite_duration_p95"
    COVERAGE_LINE_PCT = "coverage_line_pct"
    COVERAGE_BRANCH_PCT = "coverage_branch_pct"
    COVERAGE_DELTA_PCT = "coverage_delta_pct"


@strawberry.enum
class BucketIntervalInput(Enum):
    """Allowlisted time bucket intervals."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


# =============================================================================
# FilterInput types - Mirror REST MetricFilter for filter parity
# =============================================================================


@strawberry.enum
class ScopeLevelInput(Enum):
    """Scope level for filtering queries."""

    ORG = "org"
    TEAM = "team"
    REPO = "repo"
    SERVICE = "service"
    DEVELOPER = "developer"


@strawberry.input
class ScopeFilterInput:
    """Scope filter for narrowing queries to specific teams/repos/developers.

    Empty ids list means "All" - no filtering applied at this scope level.
    """

    level: ScopeLevelInput = ScopeLevelInput.ORG
    ids: list[str] = strawberry.field(default_factory=list)


@strawberry.input
class WhoFilterInput:
    """Filter by who performed the work."""

    developers: list[str] | None = None
    roles: list[str] | None = None


@strawberry.input
class WhatFilterInput:
    """Filter by what artifacts were affected."""

    repos: list[str] | None = None
    services: list[str] | None = None


@strawberry.input
class WhyFilterInput:
    """Filter by why the work was done (classification/categorization)."""

    work_category: list[str] | None = None
    issue_type: list[str] | None = None


@strawberry.input
class HowFilterInput:
    """Filter by how the work is progressing."""

    flow_stage: list[str] | None = None


@strawberry.input
class FilterInput:
    """Combined filter input matching REST MetricFilter semantics.

    All filter fields are optional. Empty/None values mean "All" - no filtering.
    Filters are ANDed together when multiple are specified.
    """

    scope: ScopeFilterInput | None = None
    who: WhoFilterInput | None = None
    what: WhatFilterInput | None = None
    why: WhyFilterInput | None = None
    how: HowFilterInput | None = None


@strawberry.input
class DateRangeInput:
    """Date range for analytics queries."""

    start_date: date
    end_date: date


@strawberry.input
class TimeseriesRequestInput:
    """Request for a timeseries query."""

    dimension: DimensionInput
    measure: MeasureInput
    interval: BucketIntervalInput
    date_range: DateRangeInput


@strawberry.input
class BreakdownRequestInput:
    """Request for a breakdown (top-N aggregation) query."""

    dimension: DimensionInput
    measure: MeasureInput
    date_range: DateRangeInput
    top_n: int = 10


@strawberry.input
class SankeyRequestInput:
    """Request for a Sankey flow query."""

    path: list[DimensionInput]
    measure: MeasureInput
    date_range: DateRangeInput
    max_nodes: int = 100
    max_edges: int = 500
    use_investment: bool | None = None


@strawberry.input
class PaginationInput:
    """
    Input for cursor-based pagination.

    Supports forward pagination (first/after) and backward pagination (last/before).
    Only one direction should be used at a time.
    """

    first: int | None = None
    after: str | None = None
    last: int | None = None
    before: str | None = None


@strawberry.input
class AnalyticsRequestInput:
    """Batch request for analytics queries.

    The optional `filters` field enables scope/category filtering that matches
    the REST MetricFilter semantics. When provided, filters are applied to all
    queries in the batch.
    """

    timeseries: list[TimeseriesRequestInput] = strawberry.field(default_factory=list)
    breakdowns: list[BreakdownRequestInput] = strawberry.field(default_factory=list)
    sankey: SankeyRequestInput | None = None
    use_investment: bool | None = None
    filters: FilterInput | None = None  # NEW: Filter parity with REST


@strawberry.input
class PaginatedBreakdownRequestInput:
    """Request for a paginated breakdown query with cursor-based pagination."""

    dimension: DimensionInput
    measure: MeasureInput
    date_range: DateRangeInput
    pagination: PaginationInput | None = None


@strawberry.input
class PaginatedCatalogValuesInput:
    """Request for paginated catalog dimension values."""

    dimension: DimensionInput
    pagination: PaginationInput | None = None
    filters: FilterInput | None = None


@strawberry.enum
class WorkGraphNodeTypeInput(Enum):
    """Node type filter for work graph queries."""

    ISSUE = "issue"
    PR = "pr"
    COMMIT = "commit"
    FILE = "file"


@strawberry.enum
class WorkGraphEdgeTypeInput(Enum):
    """Edge type filter for work graph queries."""

    BLOCKS = "blocks"
    RELATES = "relates"
    DUPLICATES = "duplicates"
    IS_BLOCKED_BY = "is_blocked_by"
    IS_RELATED_TO = "is_related_to"
    IS_DUPLICATE_OF = "is_duplicate_of"
    PARENT_OF = "parent_of"
    CHILD_OF = "child_of"
    REFERENCES = "references"
    IMPLEMENTS = "implements"
    FIXES = "fixes"
    CONTAINS = "contains"
    TOUCHES = "touches"


@strawberry.input
class WorkGraphEdgeFilterInput:
    """Filter options for work graph edge queries."""

    repo_ids: list[str] | None = None
    source_type: WorkGraphNodeTypeInput | None = None
    target_type: WorkGraphNodeTypeInput | None = None
    edge_type: WorkGraphEdgeTypeInput | None = None
    node_id: str | None = None
    limit: int = 1000


@strawberry.input
class CapacityForecastInput:
    """Input for on-demand capacity forecast computation."""

    team_id: str | None = None
    work_scope_id: str | None = None
    target_items: int | None = None
    target_date: date | None = None
    history_days: int = 90
    simulations: int = 10000


@strawberry.input
class CapacityForecastFilterInput:
    """Filter for querying persisted capacity forecasts."""

    team_id: str | None = None
    work_scope_id: str | None = None
    from_date: date | None = None
    to_date: date | None = None
    limit: int = 10


# =============================================================================
# Security alert types
# =============================================================================


@strawberry.enum
class SecuritySeverityInput(Enum):
    """Severity levels for security alerts."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@strawberry.enum
class SecuritySourceInput(Enum):
    """Source system for security alerts."""

    DEPENDABOT = "dependabot"
    CODE_SCANNING = "code_scanning"
    ADVISORY = "advisory"
    GITLAB_VULNERABILITY = "gitlab_vulnerability"
    GITLAB_DEPENDENCY = "gitlab_dependency"


@strawberry.enum
class SecurityStateInput(Enum):
    """Lifecycle state of a security alert."""

    OPEN = "open"
    FIXED = "fixed"
    DISMISSED = "dismissed"
    DETECTED = "detected"
    CONFIRMED = "confirmed"
    RESOLVED = "resolved"


@strawberry.input
class SecurityAlertFilterInput:
    """Filter options for security alert queries."""

    repo_ids: list[str] | None = None
    severities: list[SecuritySeverityInput] | None = None
    sources: list[SecuritySourceInput] | None = None
    states: list[SecurityStateInput] | None = None
    since: date | None = None  # created_at >= since
    until: date | None = None  # created_at <= until
    open_only: bool = (
        False  # shorthand for states=OPEN,DETECTED,CONFIRMED; overrides states
    )
    search: str | None = None  # ILIKE on title + package_name + cve_id


@strawberry.input
class SecurityPaginationInput:
    """Cursor-based pagination for security alert queries."""

    first: int = 50
    after: str | None = (
        None  # cursor; offset integer encoded as string, matching work_graph_edges convention
    )
