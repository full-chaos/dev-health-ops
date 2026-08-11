"""Shared list_metrics.v1 and query_metric.v1 application services."""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from typing import Protocol

from ..contracts import (
    DevMetricPoint,
    DevMetricRef,
    DevScope,
    DirectScope,
    FreshnessState,
    MetricID,
)
from .definitions import (
    MAX_SERIES_POINTS,
    MetricDefinition,
    get_metric,
    list_metrics,
)

MAX_CACHE_ENTRIES = 128
MAX_RESULT_BYTES = 65_536
MAX_RESULT_ROWS = 12

# These are the only metric sources whose ClickHouse projections bind a
# repository-free work scope directly (through ``work_scope_id``). Every other
# non-organization projection is repository-bounded; exposing it from
# ``list_metrics(scope)`` when the committed scope carries no repository would
# make callers enter the validator only to fail, and would tempt a caller to
# widen the scope to make the query pass.
_WORK_SCOPE_METRIC_SOURCES = frozenset(
    {
        "work_item_metrics_daily",
        "work_item_state_durations_daily",
    }
)


class MetricDataState(StrEnum):
    VALUE = "value"
    ZERO = "zero"
    NO_MATCH = "no_match"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    PARTIAL = "partial"
    STALE = "stale"
    UNCONFIGURED = "unconfigured"
    UNAVAILABLE = "unavailable"


class MetricSourceState(StrEnum):
    AVAILABLE = "available"
    UNCONFIGURED = "unconfigured"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class MetricSeriesPoint:
    timestamp: datetime
    value: float


@dataclass(frozen=True, slots=True)
class MetricSourceRef:
    ref_id: str
    source_table: str
    source_version: str
    watermark: datetime | None
    query_version: str


@dataclass(frozen=True, slots=True)
class RawMetricRow:
    dimensions: tuple[tuple[str, str], ...]
    value: float
    series: tuple[MetricSeriesPoint, ...] = ()


@dataclass(frozen=True, slots=True)
class RawMetricResult:
    rows: tuple[RawMetricRow, ...]
    watermark: datetime | None
    latest_materialized_day: date | None
    source_state: MetricSourceState
    covered_days: int
    expected_days: int
    source_refs: tuple[MetricSourceRef, ...]
    metadata: tuple[tuple[str, str], ...] = ()
    coverage: float | None = None


class MetricSourceUnavailableError(RuntimeError):
    """The registered source could not be read for this request."""


class MetricSource(Protocol):
    async def watermark(
        self, org_id: str, definition: MetricDefinition, scope: DevScope
    ) -> str: ...

    async def query(
        self,
        org_id: str,
        definition: MetricDefinition,
        scope: DevScope,
        *,
        comparison: bool,
        include_series: bool,
        max_series_points: int,
    ) -> RawMetricResult: ...


@dataclass(frozen=True, slots=True)
class MetricQueryRequest:
    metric_id: MetricID | str
    scope: DevScope
    dimensions: tuple[str, ...] = ()
    include_comparison: bool = True
    include_series: bool = False
    max_series_points: int = 90


@dataclass(frozen=True, slots=True)
class MetricQueryValue:
    dimensions: tuple[tuple[str, str], ...]
    value: float
    comparison_value: float | None
    series: tuple[MetricSeriesPoint, ...]


@dataclass(frozen=True, slots=True)
class MetricQueryResult:
    definition: MetricDefinition
    state: MetricDataState
    freshness: FreshnessState
    values: tuple[MetricQueryValue, ...]
    coverage: float
    current_window_start: datetime
    current_window_end: datetime
    comparison_window_start: datetime | None
    comparison_window_end: datetime | None
    watermark: datetime | None
    source_refs: tuple[MetricSourceRef, ...]
    metadata: tuple[tuple[str, str], ...] = ()
    warnings: tuple[str, ...] = ()

    def contract_refs(self, scope: DevScope) -> tuple[DevMetricRef, ...]:
        """Project successful rows into the canonical dev_metric_ref.v1 model."""
        refs: list[DevMetricRef] = []
        for value in self.values:
            dimensions = [f"{key}={item}" for key, item in value.dimensions]
            digest = hashlib.sha256(
                json.dumps(
                    {
                        "metric": self.definition.metric_id.value,
                        "dimensions": value.dimensions,
                        "scope": scope.model_dump(mode="json"),
                        "window": [
                            self.current_window_start.isoformat(),
                            self.current_window_end.isoformat(),
                        ],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()[:32]
            refs.append(
                DevMetricRef(
                    schema_version="dev_metric_ref.v1",
                    metric_ref_id=f"metric:{digest}",
                    metric_id=self.definition.metric_id,
                    label=self.definition.label,
                    definition_version=self.definition.definition_version,
                    unit=self.definition.unit,
                    aggregation=self.definition.aggregation,
                    display_precision=self.definition.display_precision,
                    resolved_scope=scope,
                    dimensions=dimensions,
                    current_window=scope.time_range,
                    comparison_window=scope.comparison_range
                    if value.comparison_value is not None
                    else None,
                    value=value.value,
                    comparison_value=value.comparison_value,
                    series=[
                        DevMetricPoint(timestamp=point.timestamp, value=point.value)
                        for point in value.series
                    ],
                    query_version=self.definition.query_version,
                    source_version=self.definition.source_version,
                    freshness=self.freshness,
                    coverage=self.coverage,
                    evidence_ref_ids=[ref.ref_id for ref in self.source_refs],
                )
            )
        return tuple(refs)


class MetricRequestCache:
    """Bounded request-local cache; never share across GraphQL requests."""

    def __init__(self, max_entries: int = MAX_CACHE_ENTRIES) -> None:
        if max_entries < 1 or max_entries > MAX_CACHE_ENTRIES:
            raise ValueError("Metric request cache size is out of bounds")
        self._max_entries = max_entries
        self._values: OrderedDict[str, MetricQueryResult] = OrderedDict()

    def get(self, key: str) -> MetricQueryResult | None:
        result = self._values.get(key)
        if result is not None:
            self._values.move_to_end(key)
        return result

    def put(self, key: str, value: MetricQueryResult) -> None:
        self._values[key] = value
        self._values.move_to_end(key)
        while len(self._values) > self._max_entries:
            self._values.popitem(last=False)


class MetricQueryService:
    """Only application service used by agent tools and GraphQL metric fields."""

    def __init__(
        self,
        source: MetricSource,
        *,
        cache: MetricRequestCache | None = None,
    ) -> None:
        self._source = source
        self._cache = cache or MetricRequestCache()

    def list_metrics(
        self, scope: DevScope | None = None
    ) -> tuple[MetricDefinition, ...]:
        definitions = list_metrics()
        if scope is None:
            return definitions
        return tuple(
            definition
            for definition in definitions
            if scope.direct_scope in definition.supported_scopes
            and (not scope.team_ids or definition.supports_team_filter)
            and self._scope_resolves_definition(definition, scope)
        )

    @staticmethod
    def _scope_resolves_definition(
        definition: MetricDefinition, scope: DevScope
    ) -> bool:
        """Return whether the source can honor ``scope`` without widening it.

        Organization reads are bounded by the source's server-owned
        authorization query. Work-item rollups carry ``work_scope_id`` and
        therefore remain queryable for project/work-unit subjects that have no
        repository dimension. Repository-backed projections require a
        repository on the committed scope.
        """

        if scope.direct_scope is DirectScope.ORGANIZATION:
            return True
        if definition.source_table in _WORK_SCOPE_METRIC_SOURCES:
            return True
        return bool(
            scope.repositories or any(ref.repository_id for ref in scope.entity_refs)
        )

    async def query(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: MetricQueryRequest,
        *,
        now: datetime | None = None,
    ) -> MetricQueryResult:
        definition = get_metric(request.metric_id)
        self._validate_request(org_id, permission_fingerprint, definition, request)
        observed_now = now or datetime.now(timezone.utc)
        if observed_now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        try:
            source_watermark = await self._source.watermark(
                org_id, definition, request.scope
            )
        except Exception:
            return self._unavailable(definition, request)

        cache_key = self._cache_key(
            org_id, permission_fingerprint, definition, request, source_watermark
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            current = await self._source.query(
                org_id,
                definition,
                request.scope,
                comparison=False,
                include_series=request.include_series,
                max_series_points=request.max_series_points,
            )
            comparison = (
                await self._source.query(
                    org_id,
                    definition,
                    request.scope,
                    comparison=True,
                    include_series=False,
                    max_series_points=request.max_series_points,
                )
                if request.include_comparison
                else None
            )
        except MetricSourceUnavailableError:
            result = self._unavailable(definition, request)
            self._cache.put(cache_key, result)
            return result

        result = self._build_result(
            definition, request, current, comparison, observed_now
        )
        self._enforce_result_bounds(result)
        self._cache.put(cache_key, result)
        return result

    @staticmethod
    def _validate_request(
        org_id: str,
        permission_fingerprint: str,
        definition: MetricDefinition,
        request: MetricQueryRequest,
    ) -> None:
        if not org_id or not permission_fingerprint:
            raise ValueError("Tenant and permission fingerprint are required")
        if request.scope.organization_id != org_id:
            raise ValueError("Resolved metric scope must belong to the tenant")
        if request.scope.direct_scope not in definition.supported_scopes:
            raise ValueError("Metric does not support the resolved direct scope")
        if request.scope.team_ids and not definition.supports_team_filter:
            raise ValueError("Metric does not support a team filter")
        if (
            definition.source_table == "compounding_risk_daily"
            and request.scope.team_ids
            and request.scope.direct_scope is not DirectScope.ORGANIZATION
        ):
            raise ValueError(
                "Compounding-risk team filters require organization direct scope"
            )
        if not MetricQueryService._scope_resolves_definition(definition, request.scope):
            raise ValueError("Metric scope does not resolve to a repository")
        unknown_dimensions = set(request.dimensions) - set(
            definition.supported_dimensions
        )
        if unknown_dimensions:
            raise ValueError("Metric does not support the requested dimension")
        if (
            request.max_series_points < 1
            or request.max_series_points > MAX_SERIES_POINTS
        ):
            raise ValueError("Metric series point limit is out of bounds")
        if request.include_series and "day" not in definition.supported_time_grains:
            raise ValueError("Metric does not support a daily series")
        duration = request.scope.time_range.end - request.scope.time_range.start
        range_days = duration.total_seconds() / 86_400
        if not definition.min_range_days <= range_days <= definition.max_range_days:
            raise ValueError("Metric time range is out of bounds")
        if request.include_comparison and request.scope.comparison_range is None:
            raise ValueError("Comparison requested without a resolved comparison range")

    @staticmethod
    def _build_result(
        definition: MetricDefinition,
        request: MetricQueryRequest,
        current: RawMetricResult,
        comparison: RawMetricResult | None,
        now: datetime,
    ) -> MetricQueryResult:
        comparison_by_dimension = (
            {row.dimensions: row.value for row in comparison.rows} if comparison else {}
        )
        values = tuple(
            MetricQueryValue(
                dimensions=row.dimensions,
                value=row.value,
                comparison_value=comparison_by_dimension.get(row.dimensions),
                series=row.series,
            )
            for row in current.rows
        )
        coverage = (
            current.coverage
            if current.coverage is not None
            else (
                min(1.0, current.covered_days / current.expected_days)
                if current.expected_days > 0
                else 0.0
            )
        )
        watermark = current.watermark
        required_day = MetricQueryService._required_completed_day(request, now)
        stale = required_day is not None and (
            current.latest_materialized_day is None
            or current.latest_materialized_day < required_day
        )
        includes_open_day = (
            request.scope.time_range.end.astimezone(timezone.utc).date()
            > now.astimezone(timezone.utc).date()
        )
        metadata = dict(current.metadata)
        if current.source_state is MetricSourceState.UNCONFIGURED:
            state = MetricDataState.UNCONFIGURED
            freshness = FreshnessState.UNAVAILABLE
        elif current.source_state is MetricSourceState.UNAVAILABLE:
            state = MetricDataState.UNAVAILABLE
            freshness = FreshnessState.UNAVAILABLE
        elif not values and metadata.get("empty_reason") == "zero_denominator":
            state = MetricDataState.INSUFFICIENT_EVIDENCE
            freshness = FreshnessState.STALE if stale else FreshnessState.FRESH
        elif not values:
            state = MetricDataState.NO_MATCH
            freshness = (
                FreshnessState.UNKNOWN
                if watermark is None
                else (FreshnessState.STALE if stale else FreshnessState.FRESH)
            )
        elif stale:
            state = MetricDataState.STALE
            freshness = FreshnessState.STALE
        elif all(value.value == 0 for value in values):
            state = MetricDataState.ZERO
            freshness = FreshnessState.FRESH
        else:
            state = (
                MetricDataState.PARTIAL if includes_open_day else MetricDataState.VALUE
            )
            freshness = FreshnessState.FRESH
        return MetricQueryResult(
            definition=definition,
            state=state,
            freshness=freshness,
            values=values,
            coverage=coverage,
            current_window_start=request.scope.time_range.start,
            current_window_end=request.scope.time_range.end,
            comparison_window_start=request.scope.comparison_range.start
            if request.include_comparison and request.scope.comparison_range
            else None,
            comparison_window_end=request.scope.comparison_range.end
            if request.include_comparison and request.scope.comparison_range
            else None,
            watermark=watermark,
            source_refs=current.source_refs,
            metadata=current.metadata,
            warnings=(
                ("source_stale",)
                if stale
                else (("open_utc_day_partial",) if includes_open_day else ())
            ),
        )

    @staticmethod
    def _unavailable(
        definition: MetricDefinition, request: MetricQueryRequest
    ) -> MetricQueryResult:
        return MetricQueryResult(
            definition=definition,
            state=MetricDataState.UNAVAILABLE,
            freshness=FreshnessState.UNAVAILABLE,
            values=(),
            coverage=0.0,
            current_window_start=request.scope.time_range.start,
            current_window_end=request.scope.time_range.end,
            comparison_window_start=request.scope.comparison_range.start
            if request.include_comparison and request.scope.comparison_range
            else None,
            comparison_window_end=request.scope.comparison_range.end
            if request.include_comparison and request.scope.comparison_range
            else None,
            watermark=None,
            source_refs=(),
            warnings=("source_unavailable",),
        )

    @staticmethod
    def _cache_key(
        org_id: str,
        permission_fingerprint: str,
        definition: MetricDefinition,
        request: MetricQueryRequest,
        source_watermark: str,
    ) -> str:
        raw = json.dumps(
            {
                "org_id": org_id,
                "permission": permission_fingerprint,
                "metric": definition.metric_id.value,
                "definition_version": definition.definition_version,
                "query_version": definition.query_version,
                "source_version": definition.source_version,
                "scope": request.scope.model_dump(mode="json"),
                "dimensions": request.dimensions,
                "include_comparison": request.include_comparison,
                "include_series": request.include_series,
                "max_series_points": request.max_series_points,
                "source_watermark": source_watermark,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def _required_completed_day(
        request: MetricQueryRequest, now: datetime
    ) -> date | None:
        """Latest completed UTC day that the half-open window requires."""
        utc_start = request.scope.time_range.start.astimezone(timezone.utc)
        utc_end = request.scope.time_range.end.astimezone(timezone.utc)
        latest_completed = now.astimezone(timezone.utc).date() - timedelta(days=1)
        last_included = (utc_end - timedelta(microseconds=1)).date()
        required = min(latest_completed, last_included)
        return required if required >= utc_start.date() else None

    @staticmethod
    def _enforce_result_bounds(result: MetricQueryResult) -> None:
        if len(result.values) > MAX_RESULT_ROWS:
            raise ValueError("Metric result row limit exceeded")
        if any(len(value.series) > MAX_SERIES_POINTS for value in result.values):
            raise ValueError("Metric series point limit exceeded")
        payload = {
            "metric": result.definition.metric_id.value,
            "state": result.state.value,
            "values": [
                {
                    "dimensions": value.dimensions,
                    "value": value.value,
                    "comparison": value.comparison_value,
                    "series": [
                        (point.timestamp.isoformat(), point.value)
                        for point in value.series
                    ],
                }
                for value in result.values
            ],
            "refs": [ref.ref_id for ref in result.source_refs],
            "metadata": result.metadata,
            "warnings": result.warnings,
        }
        if len(json.dumps(payload, separators=(",", ":")).encode()) > MAX_RESULT_BYTES:
            raise ValueError("Metric result byte limit exceeded")
