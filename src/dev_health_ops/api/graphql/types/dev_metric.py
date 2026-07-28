"""Typed GraphQL surface for the canonical Ask Dev V1 metric services."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

import strawberry


@strawberry.enum
class DevMetricID(Enum):
    ITEMS_COMPLETED = "items_completed"
    CYCLE_TIME_P50_HOURS = "cycle_time_p50_hours"
    AVG_WIP = "avg_wip"
    DEPLOYMENTS_COUNT = "deployments_count"
    CHANGE_FAILURE_RATE = "change_failure_rate"
    INVESTMENT_ALLOCATION_PCT = "investment_allocation_pct"
    CYCLOMATIC_PER_KLOC = "cyclomatic_per_kloc"
    COMPOUNDING_RISK_SCORE = "compounding_risk_score"


@strawberry.enum
class DevMetricDirectScope(Enum):
    ORGANIZATION = "organization"
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"


@strawberry.enum
class DevMetricDataState(Enum):
    VALUE = "value"
    ZERO = "zero"
    NO_MATCH = "no_match"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    PARTIAL = "partial"
    STALE = "stale"
    UNCONFIGURED = "unconfigured"
    UNAVAILABLE = "unavailable"


@strawberry.enum
class DevMetricFreshness(Enum):
    FRESH = "fresh"
    STALE = "stale"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


@strawberry.input
class DevMetricCatalogInput:
    direct_scope: DevMetricDirectScope | None = None
    has_team_filter: bool = False


@strawberry.input
class DevMetricScopeInput:
    direct_scope: DevMetricDirectScope
    start_date: date
    end_date: date
    refs: list[strawberry.ID] = strawberry.field(default_factory=list)
    team_ids: list[strawberry.ID] = strawberry.field(default_factory=list)
    timezone: str = "UTC"


@strawberry.input
class DevMetricQueryInput:
    metric_id: DevMetricID
    scope: DevMetricScopeInput
    dimensions: list[str] = strawberry.field(default_factory=list)
    include_comparison: bool = True
    include_series: bool = False
    max_series_points: int = 90


@strawberry.type
class DevMetricDefinition:
    metric_id: DevMetricID
    label: str
    owner: str
    description: str
    definition_version: str
    source_table: str
    source_version: str
    query_version: str
    unit: str
    aggregation: str
    display_precision: int
    null_semantics: str
    zero_semantics: str
    supported_scopes: list[DevMetricDirectScope]
    supports_team_filter: bool
    supported_dimensions: list[str]
    supported_presets: list[int]
    supported_time_grains: list[str]
    min_range_days: int
    max_range_days: int
    comparison_rule: str
    freshness_policy: str
    expected_materialization: str
    upstream_sources: list[str]
    sensitivity: str
    entitlement: str


@strawberry.type
class DevMetricCatalog:
    metrics: list[DevMetricDefinition]
    registry_version: str


@strawberry.type
class DevMetricDimensionValue:
    key: str
    value: str


@strawberry.type
class DevMetricSeriesPoint:
    timestamp: datetime
    value: float


@strawberry.type
class DevMetricValue:
    dimensions: list[DevMetricDimensionValue]
    value: float
    comparison_value: float | None
    series: list[DevMetricSeriesPoint]


@strawberry.type
class DevMetricSourceRef:
    ref_id: strawberry.ID
    source_table: str
    source_version: str
    watermark: datetime | None
    query_version: str


@strawberry.type
class DevMetricMetadata:
    key: str
    value: str


@strawberry.type
class DevMetricResult:
    definition: DevMetricDefinition
    state: DevMetricDataState
    freshness: DevMetricFreshness
    values: list[DevMetricValue]
    coverage: float
    current_window_start: datetime
    current_window_end: datetime
    comparison_window_start: datetime | None
    comparison_window_end: datetime | None
    timezone: str
    watermark: datetime | None
    source_refs: list[DevMetricSourceRef]
    metadata: list[DevMetricMetadata]
    warnings: list[str]
