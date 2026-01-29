"""Strawberry GraphQL input types for analytics API."""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import List, Optional

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
    ids: List[str] = strawberry.field(default_factory=list)


@strawberry.input
class WhoFilterInput:
    """Filter by who performed the work."""

    developers: Optional[List[str]] = None
    roles: Optional[List[str]] = None


@strawberry.input
class WhatFilterInput:
    """Filter by what artifacts were affected."""

    repos: Optional[List[str]] = None
    services: Optional[List[str]] = None


@strawberry.input
class WhyFilterInput:
    """Filter by why the work was done (classification/categorization)."""

    work_category: Optional[List[str]] = None
    issue_type: Optional[List[str]] = None


@strawberry.input
class HowFilterInput:
    """Filter by how the work is progressing."""

    flow_stage: Optional[List[str]] = None


@strawberry.input
class FilterInput:
    """Combined filter input matching REST MetricFilter semantics.

    All filter fields are optional. Empty/None values mean "All" - no filtering.
    Filters are ANDed together when multiple are specified.
    """

    scope: Optional[ScopeFilterInput] = None
    who: Optional[WhoFilterInput] = None
    what: Optional[WhatFilterInput] = None
    why: Optional[WhyFilterInput] = None
    how: Optional[HowFilterInput] = None


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

    path: List[DimensionInput]
    measure: MeasureInput
    date_range: DateRangeInput
    max_nodes: int = 100
    max_edges: int = 500
    use_investment: Optional[bool] = None


@strawberry.input
class PaginationInput:
    """
    Input for cursor-based pagination.

    Supports forward pagination (first/after) and backward pagination (last/before).
    Only one direction should be used at a time.
    """

    first: Optional[int] = None
    after: Optional[str] = None
    last: Optional[int] = None
    before: Optional[str] = None


@strawberry.input
class AnalyticsRequestInput:
    """Batch request for analytics queries.

    The optional `filters` field enables scope/category filtering that matches
    the REST MetricFilter semantics. When provided, filters are applied to all
    queries in the batch.
    """

    timeseries: List[TimeseriesRequestInput] = strawberry.field(default_factory=list)
    breakdowns: List[BreakdownRequestInput] = strawberry.field(default_factory=list)
    sankey: Optional[SankeyRequestInput] = None
    use_investment: Optional[bool] = None
    filters: Optional[FilterInput] = None  # NEW: Filter parity with REST


@strawberry.input
class PaginatedBreakdownRequestInput:
    """Request for a paginated breakdown query with cursor-based pagination."""

    dimension: DimensionInput
    measure: MeasureInput
    date_range: DateRangeInput
    pagination: Optional[PaginationInput] = None


@strawberry.input
class PaginatedCatalogValuesInput:
    """Request for paginated catalog dimension values."""

    dimension: DimensionInput
    pagination: Optional[PaginationInput] = None
    filters: Optional[FilterInput] = None


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

    repo_ids: Optional[List[str]] = None
    source_type: Optional[WorkGraphNodeTypeInput] = None
    target_type: Optional[WorkGraphNodeTypeInput] = None
    edge_type: Optional[WorkGraphEdgeTypeInput] = None
    node_id: Optional[str] = None
    limit: int = 1000


@strawberry.input
class CapacityForecastInput:
    """Input for on-demand capacity forecast computation."""

    team_id: Optional[str] = None
    work_scope_id: Optional[str] = None
    target_items: Optional[int] = None
    target_date: Optional[date] = None
    history_days: int = 90
    simulations: int = 10000


@strawberry.input
class CapacityForecastFilterInput:
    """Filter for querying persisted capacity forecasts."""

    team_id: Optional[str] = None
    work_scope_id: Optional[str] = None
    from_date: Optional[date] = None
    to_date: Optional[date] = None
    limit: int = 10
