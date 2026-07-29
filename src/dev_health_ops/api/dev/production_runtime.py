"""Production provider and nine-tool assembly for Ask Dev."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.llm.agent.contracts import AgentLLMProvider
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import (
    READINESS_VERSION,
    OpenAICompatibleAgentProvider,
)
from dev_health_ops.llm.agent.policy import (
    AgentFallbackPolicy,
    AgentProviderCandidate,
    AgentProviderPolicy,
    AgentProviderSource,
    resolve_agent_provider_selection,
)
from dev_health_ops.llm.agent.readiness import SettingsAgentReadinessStore
from dev_health_ops.llm.credentials import LLMCredentials
from dev_health_ops.llm.providers.base import DEFAULT_MODEL_BY_PROVIDER
from dev_health_ops.models.settings import SettingCategory

from .contracts import (
    DevContractVersions,
    DevDataHealth,
    DevEvidenceRef,
    DevGraphEdge,
    DevMetricDefinition,
    DevScope,
    DevScopeResolution,
    DevStatusFact,
    DevToolRequest,
    DevToolResult,
    DirectScope,
    FreshnessState,
    ToolID,
)
from .data_health_service import (
    NATIVE_EVIDENCE_SOURCES,
    DataHealthService,
    DataHealthState,
    NativeDataHealthReader,
)
from .entitlement import CanonicalAskDevEntitlementAuthorizer
from .evidence_service import (
    EvidenceAvailability,
    EvidenceExpansionResult,
    EvidenceReferenceSigner,
    EvidenceService,
)
from .metrics.clickhouse import ClickHouseMetricSource
from .metrics.service import MetricQueryRequest, MetricQueryService
from .native_evidence import native_evidence_adapters
from .native_status_change import ClickHouseStatusChangeSource
from .prompts import PROMPT_VERSION
from .runtime import BoundedDevRuntime, DevRuntimeUnavailable
from .scope_catalog import ClickHouseAuthorizedEntityCatalog
from .scope_service import (
    EntityKind,
    ScopeRef,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeResolveRequest,
    TimeRangeRequest,
)
from .status_change_service import (
    ChangeSummaryRequest,
    StatusChangeService,
    StatusFact,
    StatusSnapshotRequest,
)
from .tool_registry import TOOL_CONTRACT_VERSION, AskDevToolRegistry
from .work_graph_neighbors_service import (
    ALLOWED_RELATIONSHIP_TYPES,
    ClickHouseWorkGraphNeighborSource,
    GraphDirection,
    WorkGraphNeighborsRequest,
    WorkGraphNeighborsService,
    WorkGraphRootRef,
)

_LLM_CATEGORY = SettingCategory.LLM.value
_METRIC_REGISTRY_VERSION = "ask-dev-metrics.v1"
_QUERY_BUNDLE_VERSION = "ask-dev-query-bundle.v1"


@dataclass(frozen=True, slots=True)
class ProductionProviderResolution:
    provider: AgentLLMProvider
    source: AgentProviderSource
    family: str
    model: str
    provider_label: str
    model_label: str


async def resolve_production_provider(
    session: AsyncSession, *, org_id: str
) -> ProductionProviderResolution:
    settings = SettingsService(session, org_id)
    readiness = await SettingsAgentReadinessStore(settings).load()

    byo_provider_name = (await settings.get("provider", _LLM_CATEGORY) or "").strip()
    byo_model = (await settings.get("model", _LLM_CATEGORY) or "").strip()
    if byo_provider_name == "openai" and not byo_model:
        byo_model = DEFAULT_MODEL_BY_PROVIDER["openai"]
    byo_credentials = LLMCredentials(
        api_key=await settings.get("api_key", _LLM_CATEGORY) or "",
        base_url=await settings.get("base_url", _LLM_CATEGORY) or "",
    )
    byo = _candidate(
        provider_name=byo_provider_name,
        model=byo_model,
        credentials=byo_credentials,
        source=AgentProviderSource.BYO,
        readiness=readiness,
    )

    platform_provider_name = os.getenv("LLM_PROVIDER", "").strip().lower()
    platform_model = (
        os.getenv("LLM_MODEL", "").strip() or os.getenv("OPENAI_MODEL", "").strip()
    )
    if platform_provider_name in {"", "openai"} and not platform_model:
        platform_model = DEFAULT_MODEL_BY_PROVIDER["openai"]
    platform_credentials = LLMCredentials(
        api_key=(os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY") or ""),
        base_url=(os.getenv("LLM_BASE_URL") or os.getenv("OPENAI_BASE_URL") or ""),
    )
    platform = _candidate(
        provider_name=platform_provider_name,
        model=platform_model,
        credentials=platform_credentials,
        source=AgentProviderSource.PLATFORM,
        readiness=readiness,
    )
    fallback_setting = (
        (await settings.get("ask_dev_platform_fallback", _LLM_CATEGORY) or "")
        .strip()
        .lower()
    )
    policy = AgentProviderPolicy(
        ask_dev_enabled=True,
        llm_globally_disabled=platform_provider_name == "none",
        fallback=(
            AgentFallbackPolicy.ALLOW_PLATFORM
            if fallback_setting == "true"
            else AgentFallbackPolicy.FAIL_CLOSED
        ),
    )
    try:
        selected = resolve_agent_provider_selection(
            policy=policy, byo=byo, platform=platform
        )
    except AgentProviderError as exc:
        code = {
            AgentProviderErrorCode.DISABLED: "provider_not_configured",
            AgentProviderErrorCode.MODEL_NOT_SUPPORTED: "model_not_supported",
            AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED: "provider_not_configured",
        }.get(exc.code, "provider_not_configured")
        message = (
            "The configured Ask Dev model is not supported."
            if code == "model_not_supported"
            else "No certified Ask Dev model is ready."
        )
        raise DevRuntimeUnavailable(code, message) from None

    provider = _provider(selected)
    return ProductionProviderResolution(
        provider=provider,
        source=selected.source,
        family=selected.provider,
        model=selected.model,
        provider_label="OpenAI compatible",
        model_label=selected.model.replace("\r", "").replace("\n", "")[:256],
    )


def _candidate(
    *,
    provider_name: str,
    model: str,
    credentials: LLMCredentials,
    source: AgentProviderSource,
    readiness: Any,
) -> AgentProviderCandidate | None:
    if (
        not provider_name
        and not model
        and not credentials.api_key
        and not credentials.base_url
    ):
        return None
    provider_name = provider_name or "openai"
    if provider_name != "openai":
        return AgentProviderCandidate(
            provider=provider_name,
            model=model,
            credentials=credentials,
            source=source,
            readiness_current=False,
        )
    provider_fingerprint = hashlib.sha256(
        "\0".join(
            ("openai-compatible", credentials.base_url, READINESS_VERSION)
        ).encode()
    ).hexdigest()[:24]
    current = bool(
        readiness
        and readiness.is_current(
            fingerprint=provider_fingerprint,
            readiness_version=READINESS_VERSION,
        )
    )
    return AgentProviderCandidate(
        provider=provider_name,
        model=model,
        credentials=credentials,
        source=source,
        readiness_current=current,
    )


def _provider(candidate: AgentProviderCandidate) -> AgentLLMProvider:
    if candidate.provider != "openai":
        raise DevRuntimeUnavailable(
            "model_not_supported", "The configured Ask Dev model is not supported."
        )
    return OpenAICompatibleAgentProvider(
        api_key=candidate.credentials.api_key,
        model=candidate.model,
        base_url=candidate.credentials.base_url or None,
    )


def _scope_request(scope: DevScope) -> ScopeResolveRequest:
    refs: tuple[ScopeRef, ...]
    if scope.direct_scope is DirectScope.ORGANIZATION:
        refs = (ScopeRef(EntityKind.ORGANIZATION, scope.organization_id),)
    elif scope.direct_scope is DirectScope.REPOSITORY:
        refs = tuple(
            ScopeRef(EntityKind.REPOSITORY, value) for value in scope.repositories
        )
    else:
        refs = tuple(
            ScopeRef(EntityKind(item.entity_type.value), item.entity_id)
            for item in scope.entity_refs
        )
    return ScopeResolveRequest(
        explicit_refs=refs,
        team_filter_refs=tuple(
            ScopeRef(EntityKind.TEAM, value) for value in scope.team_ids
        ),
        time_range=TimeRangeRequest(
            preset_days=None,
            start_date=date.fromisoformat(scope.time_range.start.date().isoformat()),
            end_date=date.fromisoformat(scope.time_range.end.date().isoformat()),
            timezone=scope.time_range.timezone,
        ),
        allow_organization_fallback=False,
    )


async def _resolve_exact_contract(
    service: ScopeResolutionService,
    *,
    org_id: str,
    permission_fingerprint: str,
    requested_scope: DevScope,
) -> DevScopeResolution:
    resolution = await service.resolve_contract(
        org_id, permission_fingerprint, _scope_request(requested_scope)
    )
    if resolution.resolved_scope is None:
        return resolution.model_copy(update={"requested_scope": requested_scope})
    exact_scope = resolution.resolved_scope.model_copy(
        update={
            "time_range": requested_scope.time_range,
            "comparison_range": requested_scope.comparison_range,
        }
    )
    return resolution.model_copy(
        update={
            "requested_scope": requested_scope,
            "resolved_scope": exact_scope,
        }
    )


def _tool_result(
    request: DevToolRequest,
    *,
    status: str = "success",
    scope_resolution: DevScopeResolution | None = None,
    metric_definitions: list[DevMetricDefinition] | None = None,
    metrics: list[Any] | None = None,
    evidence: list[Any] | None = None,
    status_facts: list[DevStatusFact] | None = None,
    graph_edges: list[DevGraphEdge] | None = None,
    data_health: list[DevDataHealth] | None = None,
    warnings: list[str] | None = None,
) -> DevToolResult:
    return DevToolResult(
        schema_version="dev_tool_result.v1",
        run_id=request.run_id,
        tool_call_id=request.tool_call_id,
        tool_id=request.tool_id,
        status=status,
        scope_resolution=scope_resolution,
        metric_definitions=metric_definitions or [],
        metrics=metrics or [],
        evidence=evidence or [],
        status_facts=status_facts or [],
        graph_edges=graph_edges or [],
        data_health=data_health or [],
        warnings=(warnings or [])[:20],
        serialized_bytes=0,
    )


def _status(state: str) -> str:
    if state in {"unavailable", "unconfigured", "insufficient_evidence"}:
        return "unavailable"
    if state in {"partial", "degraded", "stale"}:
        return "partial"
    return "success"


def _fact(value: StatusFact) -> DevStatusFact | None:
    if not value.evidence_ref_ids:
        return None
    return DevStatusFact(
        fact_id=f"{value.entity_type}:{value.entity_id}",
        text=f"{value.display_label}: {value.status}",
        evidence_ref_ids=list(value.evidence_ref_ids)[:25],
    )


def _work_graph_roots(scope: DevScope) -> tuple[WorkGraphRootRef, ...]:
    aliases = {"pull_request": "pr"}
    roots = []
    for item in scope.entity_refs:
        kind = aliases.get(item.entity_type.value, item.entity_type.value)
        if kind in {"issue", "pr", "commit", "file", "deployment", "incident"}:
            roots.append(WorkGraphRootRef(kind, item.entity_id))
    return tuple(roots)


async def build_production_runtime(
    session: AsyncSession,
    *,
    org_id: str,
    permission_fingerprint: str,
    clickhouse: Any,
) -> BoundedDevRuntime:
    provider = await resolve_production_provider(session, org_id=org_id)
    try:
        return await _assemble_production_runtime(
            session,
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            clickhouse=clickhouse,
            provider=provider,
        )
    except BaseException:
        try:
            await provider.provider.aclose()
        except Exception:
            pass
        raise


async def _assemble_production_runtime(
    session: AsyncSession,
    *,
    org_id: str,
    permission_fingerprint: str,
    clickhouse: Any,
    provider: ProductionProviderResolution,
) -> BoundedDevRuntime:
    entitlement = CanonicalAskDevEntitlementAuthorizer(session)
    scope_service = ScopeResolutionService(
        ClickHouseAuthorizedEntityCatalog(clickhouse), cache=ScopeRequestCache()
    )
    metric_service = MetricQueryService(ClickHouseMetricSource(clickhouse))
    status_service = StatusChangeService(
        ClickHouseStatusChangeSource(clickhouse), metric_service=metric_service
    )
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise DevRuntimeUnavailable(
            "provider_not_configured", "Ask Dev evidence signing is unavailable."
        )
    evidence_service = EvidenceService(
        entitlement=entitlement,
        authorizer=scope_service,
        signer=EvidenceReferenceSigner(secret),
        native_adapters=native_evidence_adapters(clickhouse),
        acr_adapter=None,
    )
    data_health_service = DataHealthService(
        entitlement=entitlement,
        authorizer=scope_service,
        reader=NativeDataHealthReader(clickhouse, session),
    )
    work_graph_service = WorkGraphNeighborsService(
        ClickHouseWorkGraphNeighborSource(clickhouse), entitlement, scope_service
    )
    evidence_by_id: dict[str, Any] = {}

    async def resolve_scope(_context, request):
        resolution = await _resolve_exact_contract(
            scope_service,
            org_id=org_id,
            permission_fingerprint=_context.permission_fingerprint,
            requested_scope=request.scope,
        )
        return _tool_result(request, scope_resolution=resolution)

    async def list_registered_metrics(_context, request):
        definitions = metric_service.list_metrics(request.scope)
        return _tool_result(
            request,
            metric_definitions=[
                DevMetricDefinition(
                    metric_id=item.metric_id,
                    label=item.label,
                    description=item.description,
                    unit=item.unit,
                    supported_dimensions=list(item.supported_dimensions),
                    supported_time_grains=list(item.supported_time_grains),
                    supported_scopes=list(item.supported_scopes),
                    definition_version=item.definition_version,
                    freshness_policy=item.freshness_policy,
                )
                for item in definitions
            ],
        )

    async def query_metric(context, request):
        result = await metric_service.query(
            org_id,
            context.permission_fingerprint,
            MetricQueryRequest(
                metric_id=request.metric_id or "",
                scope=request.scope,
                include_comparison=request.include_comparison,
            ),
        )
        metrics = [
            item.model_copy(update={"evidence_ref_ids": []})
            for item in result.contract_refs(request.scope)
        ]
        return _tool_result(
            request,
            status=_status(result.state.value),
            metrics=metrics,
            warnings=list(result.warnings),
        )

    async def status_snapshot(context, request):
        result = await status_service.status_snapshot(
            org_id,
            context.permission_fingerprint,
            StatusSnapshotRequest(scope=request.scope, max_items=request.limit),
        )
        values = [result.declared, *result.children, *result.blockers]
        facts = [item for value in values if value and (item := _fact(value))]
        return _tool_result(
            request,
            status=_status(result.state.value),
            status_facts=facts,
            warnings=list(result.warnings),
        )

    async def change_summary(context, request):
        comparison = request.scope.comparison_range
        if comparison is None:
            return _tool_result(
                request,
                status="unavailable",
                warnings=["comparison_window_unavailable"],
            )
        result = await status_service.change_summary(
            org_id,
            context.permission_fingerprint,
            ChangeSummaryRequest(
                scope=request.scope,
                current_start=request.scope.time_range.start,
                current_end=request.scope.time_range.end,
                comparison_start=comparison.start,
                comparison_end=comparison.end,
                max_items=request.limit,
            ),
        )
        facts = [
            DevStatusFact(
                fact_id=item.change_id,
                text=(
                    f"{item.display_label}: {item.before or 'unknown'} -> "
                    f"{item.after or 'unknown'}"
                ),
                evidence_ref_ids=list(item.evidence_ref_ids)[:25],
            )
            for item in result.changes
            if item.evidence_ref_ids
        ]
        return _tool_result(
            request,
            status=_status(result.state.value),
            status_facts=facts,
            warnings=list(result.warnings),
        )

    async def work_graph(context, request):
        roots = _work_graph_roots(request.scope)
        if not roots:
            return _tool_result(
                request, status="unavailable", warnings=["work_graph_root_required"]
            )
        result = await work_graph_service.neighbors(
            org_id=org_id,
            permission_fingerprint=context.permission_fingerprint,
            request=WorkGraphNeighborsRequest(
                scope_request=_scope_request(request.scope),
                root_refs=roots,
                relationship_types=tuple(sorted(ALLOWED_RELATIONSHIP_TYPES)),
                direction=GraphDirection.BOTH,
                limit=request.limit,
            ),
        )
        edges = [
            DevGraphEdge(
                source_entity_id=item.source_id,
                relationship=item.relationship_type,
                target_entity_id=item.target_id,
                evidence_ref_ids=[],
            )
            for item in result.edges
        ]
        return _tool_result(
            request,
            status=_status(result.state.value),
            graph_edges=edges,
            warnings=list(result.warnings),
        )

    async def search_evidence(context, request):
        result = await evidence_service.search(
            org_id=org_id,
            permission_fingerprint=context.permission_fingerprint,
            scope_request=_scope_request(request.scope),
            query=request.query or "",
            limit=request.limit,
        )
        for item in result.evidence:
            evidence_by_id[item.evidence_ref_id] = item
        unavailable = all(
            item.state
            in {EvidenceAvailability.UNAVAILABLE, EvidenceAvailability.UNCONFIGURED}
            for item in result.source_states
        )
        return _tool_result(
            request,
            status="unavailable" if unavailable else "success",
            evidence=list(result.evidence),
            warnings=[item.warning for item in result.source_states if item.warning],
        )

    async def get_evidence(context, request):
        known = [
            evidence_by_id[item]
            for item in request.evidence_ref_ids
            if item in evidence_by_id
        ]
        missing = len(request.evidence_ref_ids) - len(known)
        if not known:
            return _tool_result(
                request, status="unavailable", warnings=["evidence_reference_not_found"]
            )
        result = await evidence_service.expand(
            org_id=org_id,
            permission_fingerprint=context.permission_fingerprint,
            scope_request=_scope_request(request.scope),
            evidence=known,
        )
        facts = [
            DevStatusFact(
                fact_id=f"expanded:{item.evidence.evidence_ref_id}",
                text=(item.safe_excerpt or item.warning or item.state.value)[:2_048],
                evidence_ref_ids=[item.evidence.evidence_ref_id],
            )
            for item in result.expansions
        ]
        warnings = [item.warning for item in result.expansions if item.warning]
        if missing:
            warnings.append("some_evidence_references_were_not_found")
        return _tool_result(
            request,
            status="partial" if missing else "success",
            evidence=[item.evidence for item in result.expansions],
            status_facts=facts,
            warnings=warnings,
        )

    async def data_health(context, request):
        result = await data_health_service.inspect(
            org_id=org_id,
            permission_fingerprint=context.permission_fingerprint,
            scope_request=_scope_request(request.scope),
            required_sources=NATIVE_EVIDENCE_SOURCES,
        )
        items = [
            DevDataHealth(
                source_system=item.source_system,
                freshness=(
                    FreshnessState.FRESH
                    if item.state is DataHealthState.COMPLETE
                    else FreshnessState.STALE
                    if item.state is DataHealthState.STALE
                    else FreshnessState.UNAVAILABLE
                ),
                last_successful_at=item.last_successful_at,
                coverage=item.coverage,
                warning=item.warning,
            )
            for item in result.sources
        ]
        return _tool_result(
            request,
            status="success" if result.complete_eligible else "partial",
            data_health=items,
        )

    executors: Mapping[ToolID, Any] = {
        ToolID.RESOLVE_SCOPE: resolve_scope,
        ToolID.LIST_METRICS: list_registered_metrics,
        ToolID.QUERY_METRIC: query_metric,
        ToolID.STATUS_SNAPSHOT: status_snapshot,
        ToolID.CHANGE_SUMMARY: change_summary,
        ToolID.WORK_GRAPH_NEIGHBORS: work_graph,
        ToolID.SEARCH_EVIDENCE: search_evidence,
        ToolID.GET_EVIDENCE: get_evidence,
        ToolID.DATA_HEALTH: data_health,
    }
    registry = AskDevToolRegistry(executors)

    async def scope_resolver(
        *, org_id: str, user_id: str, requested_scope: DevScope
    ) -> DevScopeResolution:
        del user_id
        return await _resolve_exact_contract(
            scope_service,
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            requested_scope=requested_scope,
        )

    return BoundedDevRuntime(
        provider=provider.provider,
        provider_source=provider.source.value,
        provider_family=provider.family,
        registry=registry,
        scope_resolver=scope_resolver,
        versions=DevContractVersions(
            prompt_version=PROMPT_VERSION,
            tool_contract_version=TOOL_CONTRACT_VERSION,
            metric_definition_version=_METRIC_REGISTRY_VERSION,
            query_version=_QUERY_BUNDLE_VERSION,
        ),
    )


async def expand_production_evidence(
    session: AsyncSession,
    *,
    org_id: str,
    permission_fingerprint: str,
    clickhouse: Any,
    scope: DevScope,
    evidence: Sequence[DevEvidenceRef],
) -> EvidenceExpansionResult:
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise DevRuntimeUnavailable(
            "source_unavailable", "Ask Dev evidence expansion is unavailable."
        )
    entitlement = CanonicalAskDevEntitlementAuthorizer(session)
    scope_service = ScopeResolutionService(
        ClickHouseAuthorizedEntityCatalog(clickhouse), cache=ScopeRequestCache()
    )
    service = EvidenceService(
        entitlement=entitlement,
        authorizer=scope_service,
        signer=EvidenceReferenceSigner(secret),
        native_adapters=native_evidence_adapters(clickhouse),
        acr_adapter=None,
    )
    return await service.expand(
        org_id=org_id,
        permission_fingerprint=permission_fingerprint,
        scope_request=_scope_request(scope),
        evidence=evidence,
    )


__all__ = [
    "ProductionProviderResolution",
    "build_production_runtime",
    "expand_production_evidence",
    "resolve_production_provider",
]
