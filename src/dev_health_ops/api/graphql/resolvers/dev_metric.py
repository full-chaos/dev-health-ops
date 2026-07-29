"""GraphQL adapters for the shared Ask Dev V1 metric services."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import strawberry
from sqlalchemy import select

from dev_health_ops.api.dev.contracts import DevScope
from dev_health_ops.api.dev.metrics.clickhouse import ClickHouseMetricSource
from dev_health_ops.api.dev.metrics.definitions import MetricDefinition
from dev_health_ops.api.dev.metrics.service import (
    MetricQueryRequest,
    MetricQueryService,
    MetricRequestCache,
    MetricSourceState,
)
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import (
    EntityKind,
    ScopeRef,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeResolveRequest,
    TimeRangeRequest,
)
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.settings import SyncConfiguration

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.dev_metric import (
    DevMetricCatalog,
    DevMetricCatalogInput,
    DevMetricDataState,
    DevMetricDefinition,
    DevMetricDimensionValue,
    DevMetricDirectScope,
    DevMetricFreshness,
    DevMetricID,
    DevMetricMetadata,
    DevMetricQueryInput,
    DevMetricResult,
    DevMetricScopeInput,
    DevMetricSeriesPoint,
    DevMetricSourceRef,
    DevMetricValue,
)
from . import dev_entitlement
from .dev_scope import permission_fingerprint

REGISTRY_VERSION = "ask-dev-metrics.v1"

_UPSTREAM_TARGETS: dict[str, frozenset[str]] = {
    "work_items": frozenset({"work-items"}),
    "investment_classification": frozenset({"work-items"}),
    "deployments": frozenset({"deployments"}),
    "git": frozenset({"git"}),
    "complexity_analysis": frozenset({"git"}),
    "repo_metrics": frozenset({"git"}),
    "ownership": frozenset({"git"}),
    "reviews": frozenset({"git"}),
}


def _definition(value: MetricDefinition) -> DevMetricDefinition:
    return DevMetricDefinition(
        metric_id=DevMetricID(value.metric_id.value),
        label=value.label,
        owner=value.owner,
        description=value.description,
        definition_version=value.definition_version,
        source_table=value.source_table,
        source_version=value.source_version,
        query_version=value.query_version,
        unit=value.unit,
        aggregation=value.aggregation,
        display_precision=value.display_precision,
        null_semantics=value.null_semantics,
        zero_semantics=value.zero_semantics,
        supported_scopes=[
            DevMetricDirectScope(scope.value) for scope in value.supported_scopes
        ],
        supports_team_filter=value.supports_team_filter,
        supported_dimensions=list(value.supported_dimensions),
        supported_presets=list(value.supported_presets),
        supported_time_grains=list(value.supported_time_grains),
        min_range_days=value.min_range_days,
        max_range_days=value.max_range_days,
        comparison_rule=value.comparison_rule,
        freshness_policy=value.freshness_policy,
        expected_materialization=value.expected_materialization,
        upstream_sources=list(value.upstream_sources),
        sensitivity=value.sensitivity,
        entitlement=value.entitlement,
    )


async def resolve_dev_metric_catalog(
    context: GraphQLContext,
    input: DevMetricCatalogInput | None,
) -> DevMetricCatalog:
    org_id = require_org_id(context)
    permission_fingerprint(context)
    await dev_entitlement.require_ask_dev_entitlement(org_id)
    service = MetricQueryService(ClickHouseMetricSource(context.client))
    definitions = service.list_metrics()
    if input is not None and input.direct_scope is not None:
        definitions = tuple(
            definition
            for definition in definitions
            if input.direct_scope.value
            in {scope.value for scope in definition.supported_scopes}
            and (not input.has_team_filter or definition.supports_team_filter)
        )
    return DevMetricCatalog(
        metrics=[_definition(definition) for definition in definitions],
        registry_version=REGISTRY_VERSION,
    )


async def resolve_dev_metric(
    context: GraphQLContext,
    input: DevMetricQueryInput,
) -> DevMetricResult:
    org_id = require_org_id(context)
    fingerprint = permission_fingerprint(context)
    await dev_entitlement.require_ask_dev_entitlement(org_id)
    if context.client is None:
        raise RuntimeError("Database client not available for metric query")
    scope = await _resolve_scope(context, input)
    if context.dev_metric_cache is None:
        context.dev_metric_cache = MetricRequestCache()
    state_resolver = _request_source_state_resolver()
    service = MetricQueryService(
        ClickHouseMetricSource(
            context.client,
            source_state_resolver=state_resolver,
        ),
        cache=context.dev_metric_cache,
    )
    result = await service.query(
        org_id,
        fingerprint,
        MetricQueryRequest(
            metric_id=input.metric_id.value,
            scope=scope,
            dimensions=tuple(input.dimensions),
            include_comparison=input.include_comparison,
            include_series=input.include_series,
            max_series_points=input.max_series_points,
        ),
    )
    return DevMetricResult(
        definition=_definition(result.definition),
        state=DevMetricDataState(result.state.value),
        freshness=DevMetricFreshness(result.freshness.value),
        values=[
            DevMetricValue(
                dimensions=[
                    DevMetricDimensionValue(key=key, value=value)
                    for key, value in row.dimensions
                ],
                value=row.value,
                comparison_value=row.comparison_value,
                series=[
                    DevMetricSeriesPoint(
                        timestamp=point.timestamp,
                        value=point.value,
                    )
                    for point in row.series
                ],
            )
            for row in result.values
        ],
        coverage=result.coverage,
        current_window_start=result.current_window_start,
        current_window_end=result.current_window_end,
        comparison_window_start=result.comparison_window_start,
        comparison_window_end=result.comparison_window_end,
        timezone=scope.time_range.timezone,
        watermark=result.watermark,
        source_refs=[
            DevMetricSourceRef(
                ref_id=strawberry.ID(ref.ref_id),
                source_table=ref.source_table,
                source_version=ref.source_version,
                watermark=ref.watermark,
                query_version=ref.query_version,
            )
            for ref in result.source_refs
        ],
        metadata=[
            DevMetricMetadata(key=key, value=value) for key, value in result.metadata
        ],
        warnings=list(result.warnings),
    )


async def _resolve_scope(
    context: GraphQLContext, input: DevMetricQueryInput
) -> DevScope:
    return await resolve_dev_metric_scope(context, input.scope)


async def resolve_dev_metric_scope(
    context: GraphQLContext, scope_input: DevMetricScopeInput
) -> DevScope:
    """Resolve the shared bounded metric/status GraphQL scope input."""
    org_id = require_org_id(context)
    kind = EntityKind(scope_input.direct_scope.value)
    refs = tuple(str(value) for value in scope_input.refs)
    scope_refs: tuple[ScopeRef, ...]
    if kind is EntityKind.ORGANIZATION:
        if refs:
            raise ValueError("Organization metric scope does not accept entity refs")
        scope_refs = (ScopeRef(EntityKind.ORGANIZATION, org_id),)
    else:
        scope_refs = tuple(ScopeRef(kind, value) for value in refs)
    request = ScopeResolveRequest(
        explicit_refs=scope_refs,
        team_filter_refs=tuple(
            ScopeRef(EntityKind.TEAM, str(value)) for value in scope_input.team_ids
        ),
        time_range=TimeRangeRequest(
            preset_days=None,
            start_date=scope_input.start_date,
            end_date=scope_input.end_date,
            timezone=scope_input.timezone,
        ),
    )
    if context.dev_scope_cache is None:
        context.dev_scope_cache = ScopeRequestCache()
    service = ScopeResolutionService(
        ClickHouseAuthorizedEntityCatalog(context.client),
        cache=context.dev_scope_cache,
    )
    resolution = await service.resolve_contract(
        org_id,
        permission_fingerprint(context),
        request,
    )
    if resolution.resolved_scope is None:
        raise ValueError("Metric scope is unresolved or unauthorized")
    return resolution.resolved_scope


def _request_source_state_resolver() -> Callable[[str, MetricDefinition], Any]:
    cache: dict[tuple[str, tuple[str, ...]], MetricSourceState] = {}

    async def resolve(org_id: str, definition: MetricDefinition) -> MetricSourceState:
        key = (org_id, definition.upstream_sources)
        if key in cache:
            return cache[key]
        required_targets = {
            target
            for source in definition.upstream_sources
            for target in _UPSTREAM_TARGETS.get(source, ())
        }
        if not required_targets:
            cache[key] = MetricSourceState.AVAILABLE
            return cache[key]
        try:
            async with get_postgres_session() as session:
                rows = list(
                    (
                        await session.execute(
                            select(SyncConfiguration).where(
                                SyncConfiguration.org_id == org_id,
                                SyncConfiguration.is_active.is_(True),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
        except Exception:
            cache[key] = MetricSourceState.UNAVAILABLE
            return cache[key]
        matching = [
            config
            for config in rows
            if required_targets.intersection(set(config.sync_targets or []))
        ]
        if not matching:
            state = MetricSourceState.UNCONFIGURED
        elif not any(config.last_sync_success is True for config in matching):
            state = MetricSourceState.UNAVAILABLE
        else:
            state = MetricSourceState.AVAILABLE
        cache[key] = state
        return state

    return resolve


__all__ = [
    "resolve_dev_metric",
    "resolve_dev_metric_catalog",
]
