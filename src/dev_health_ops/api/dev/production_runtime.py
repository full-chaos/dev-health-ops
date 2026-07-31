"""Production provider and nine-tool assembly for Ask Dev."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.llm.agent.contracts import AgentLLMProvider
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import (
    READINESS_VERSION,
    OpenAICompatibleAgentProvider,
)
from dev_health_ops.llm.agent.policy import (
    CERTIFIED_PLATFORM_AGENT_PROVIDERS,
    AgentFallbackPolicy,
    AgentProviderCandidate,
    AgentProviderPolicy,
    AgentProviderSource,
    resolve_agent_provider_selection,
)
from dev_health_ops.llm.agent.readiness import SettingsAgentReadinessStore
from dev_health_ops.llm.agent.scripted_openai_service import SCRIPTED_OPENAI_MODEL
from dev_health_ops.llm.budget import attach_agent_budget_guard
from dev_health_ops.llm.credentials import LLMCredentials, resolve_llm_credentials
from dev_health_ops.llm.errors import LLMAuthError
from dev_health_ops.llm.providers import (
    resolve_model_name,
    resolve_provider_name,
)
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
from .org_policy import load_ask_dev_org_policy
from .prompts import PROMPT_VERSION
from .runtime import BoundedDevRuntime, DevRuntimeUnavailable
from .scope_catalog import ClickHouseAuthorizedEntityCatalog
from .scope_service import (
    DIRECT_SCOPE_KINDS,
    EntityKind,
    ScopeRef,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeResolveRequest,
    ScopeSearchRequest,
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
ACCEPTANCE_OPENAI_MODEL = SCRIPTED_OPENAI_MODEL
_ACCEPTANCE_OPENAI_DISCLOSURE_KEY = "ask_dev_scripted_acceptance"
_ACCEPTANCE_OPENAI_HOSTS = frozenset(
    {"127.0.0.1", "localhost", "ask-dev-scripted-openai"}
)
# Named-entity resolution never searches organization scope itself (CHAOS-3256):
# resolve_scope.v1 only disambiguates the direct scopes an organization search
# can return an authorized match for.
_SEARCHABLE_SCOPE_KINDS: tuple[EntityKind, ...] = tuple(
    sorted(DIRECT_SCOPE_KINDS - {EntityKind.ORGANIZATION}, key=lambda kind: kind.value)
)


@dataclass(frozen=True, slots=True)
class ProductionProviderResolution:
    provider: AgentLLMProvider
    source: AgentProviderSource
    family: str
    model: str
    provider_label: str
    model_label: str
    readiness_fingerprint: str = ""


@dataclass(frozen=True, slots=True)
class _AcceptanceOpenAIConfiguration:
    api_key: str
    base_url: str


class _AcceptanceOpenAICandidate(AgentProviderCandidate):
    """Pre-admitted private endpoint for the deterministic acceptance stack only."""

    @property
    def usable(self) -> bool:
        return bool(
            self.provider == "openai"
            and self.model == ACCEPTANCE_OPENAI_MODEL
            and self.credentials.api_key
            and self.credentials.base_url
            and self.readiness_current
        )


def _acceptance_openai_configuration() -> _AcceptanceOpenAIConfiguration | None:
    """Admit the checked-in scripted endpoint only under the full acceptance gate.

    This is deliberately separate from ``LLM_BASE_URL``. Customer and ordinary
    platform endpoints continue through ``validate_llm_base_url`` in provider
    policy; the only private targets admitted here are the exact test-loopback
    hosts and the exact Compose service name used by deterministic acceptance.
    A partial acceptance activation fails closed instead of silently selecting a
    live provider and producing a false-green acceptance run.
    """

    if os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1":
        return None
    environment = os.getenv("ENVIRONMENT", "").strip().lower()
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    api_key = os.getenv("ASK_DEV_ACCEPTANCE_OPENAI_API_KEY", "")
    base_url = os.getenv("ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL", "").strip()
    try:
        parsed = urlsplit(base_url)
        port = parsed.port
    except ValueError:
        parsed = None
        port = None
    valid_url = bool(
        parsed is not None
        and parsed.scheme == "http"
        and parsed.hostname in _ACCEPTANCE_OPENAI_HOSTS
        and parsed.username is None
        and parsed.password is None
        and port is not None
        and parsed.path.rstrip("/") == "/v1"
        and not parsed.query
        and not parsed.fragment
        and not any(ord(char) <= 0x20 or ord(char) == 0x7F for char in base_url)
    )
    if (
        environment != "acceptance"
        or provider != "openai"
        or not api_key
        or not valid_url
    ):
        raise DevRuntimeUnavailable(
            "provider_not_configured", "No certified Ask Dev model is ready."
        )
    return _AcceptanceOpenAIConfiguration(api_key=api_key, base_url=base_url)


async def resolve_production_provider(
    session: AsyncSession, *, org_id: str
) -> ProductionProviderResolution:
    settings = SettingsService(session, org_id)
    readiness = await SettingsAgentReadinessStore(settings).load()

    byo, platform, policy = await _provider_candidates(settings, readiness=readiness)
    return _resolve_provider_selection(
        byo=byo,
        platform=platform,
        policy=policy,
        session=session,
        org_id=org_id,
    )


async def resolve_certification_provider(
    session: AsyncSession, *, org_id: str
) -> ProductionProviderResolution:
    """Resolve the configured candidate without requiring prior certification."""

    settings = SettingsService(session, org_id)
    byo, platform, policy = await _provider_candidates(
        settings, readiness=None, certification=True
    )
    return _resolve_provider_selection(
        byo=byo,
        platform=platform,
        policy=policy,
        session=session,
        org_id=org_id,
    )


async def _provider_candidates(
    settings: SettingsService,
    *,
    readiness: Any,
    certification: bool = False,
) -> tuple[
    AgentProviderCandidate | None,
    AgentProviderCandidate | None,
    AgentProviderPolicy,
]:

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
        certification=certification,
    )

    acceptance = _acceptance_openai_configuration()
    try:
        platform_provider_name = resolve_provider_name("auto", org_id=None)
    except LLMAuthError:
        platform_provider_name = ""
    if acceptance is not None:
        platform_model = ACCEPTANCE_OPENAI_MODEL
        platform_credentials = LLMCredentials(
            api_key=acceptance.api_key, base_url=acceptance.base_url
        )
    else:
        if platform_provider_name == "none":
            platform_model = ""
            platform_credentials = LLMCredentials()
        else:
            platform_model = (
                resolve_model_name(platform_provider_name or "openai") or ""
            )
            try:
                platform_credentials = resolve_llm_credentials(
                    platform_provider_name or "openai", org_id=None
                )
            except LLMAuthError:
                platform_credentials = LLMCredentials()
    platform = _candidate(
        provider_name=platform_provider_name,
        model=platform_model,
        credentials=platform_credentials,
        source=AgentProviderSource.PLATFORM,
        readiness=readiness,
        certification=certification,
        acceptance=acceptance is not None,
    )
    org_policy = await load_ask_dev_org_policy(settings)
    policy = AgentProviderPolicy(
        ask_dev_enabled=True,
        llm_globally_disabled=platform_provider_name == "none",
        fallback=(
            AgentFallbackPolicy.ALLOW_PLATFORM
            if org_policy.fallback_policy == "platform"
            else AgentFallbackPolicy.FAIL_CLOSED
        ),
    )
    return byo, platform, policy


def _resolve_provider_selection(
    *,
    byo: AgentProviderCandidate | None,
    platform: AgentProviderCandidate | None,
    policy: AgentProviderPolicy,
    session: AsyncSession,
    org_id: str,
) -> ProductionProviderResolution:
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
    if selected.source is AgentProviderSource.BYO:
        provider = attach_agent_budget_guard(
            provider,
            session=session,
            org_id=org_id,
            provider=selected.provider,
            model=selected.model,
            base_url=selected.credentials.base_url or None,
        )
    return ProductionProviderResolution(
        provider=provider,
        source=selected.source,
        family=selected.provider,
        model=selected.model,
        provider_label="OpenAI compatible",
        model_label=selected.model.replace("\r", "").replace("\n", "")[:256],
        readiness_fingerprint=_readiness_fingerprint(selected),
    )


def _candidate(
    *,
    provider_name: str,
    model: str,
    credentials: LLMCredentials,
    source: AgentProviderSource,
    readiness: Any,
    certification: bool = False,
    acceptance: bool = False,
) -> AgentProviderCandidate | None:
    if (
        not provider_name
        and not model
        and not credentials.api_key
        and not credentials.base_url
    ):
        return None
    provider_name = provider_name or "openai"
    candidate_type = (
        _AcceptanceOpenAICandidate
        if acceptance and provider_name == "openai"
        else AgentProviderCandidate
    )
    candidate = candidate_type(
        provider=provider_name,
        model=model,
        credentials=credentials,
        source=source,
        readiness_current=certification,
    )
    provider_fingerprint = _readiness_fingerprint(candidate)
    current = bool(
        certification
        or (
            readiness
            and readiness.is_current(
                fingerprint=provider_fingerprint,
                readiness_version=READINESS_VERSION,
            )
        )
    )
    return candidate_type(
        provider=candidate.provider,
        model=candidate.model,
        credentials=candidate.credentials,
        source=candidate.source,
        readiness_current=current,
    )


def _readiness_fingerprint(candidate: AgentProviderCandidate) -> str:
    """Fingerprint every capability input that invalidates certification."""

    return hashlib.sha256(
        "\0".join(
            (
                candidate.source.value,
                candidate.provider,
                candidate.model,
                candidate.credentials.base_url,
                READINESS_VERSION,
            )
        ).encode()
    ).hexdigest()[:24]


def _provider(candidate: AgentProviderCandidate) -> AgentLLMProvider:
    if candidate.provider not in CERTIFIED_PLATFORM_AGENT_PROVIDERS:
        raise DevRuntimeUnavailable(
            "model_not_supported", "The configured Ask Dev model is not supported."
        )
    return OpenAICompatibleAgentProvider(
        api_key=candidate.credentials.api_key or "platform-openai-compatible",
        model=candidate.model,
        base_url=candidate.credentials.base_url or None,
        disclosure_key=(
            _ACCEPTANCE_OPENAI_DISCLOSURE_KEY
            if isinstance(candidate, _AcceptanceOpenAICandidate)
            else "openai_compatible"
        ),
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

    async def resolve_scope(context, request):
        query = (request.query or "").strip()
        if not query:
            resolution = await _resolve_exact_contract(
                scope_service,
                org_id=org_id,
                permission_fingerprint=context.permission_fingerprint,
                requested_scope=request.scope,
            )
            return _tool_result(request, scope_resolution=resolution)
        # A named entity in the model's query is resolved through the same
        # canonical authorized search service the GraphQL scope search field
        # uses, never by re-resolving the caller's already-authorized scope
        # (CHAOS-3256). Exact matches commit a new authorized scope for
        # subsequent tool calls; ambiguous/not-found results never fall back
        # to organization scope.
        resolution = await scope_service.resolve_query_contract(
            org_id,
            context.permission_fingerprint,
            ScopeSearchRequest(
                query=query,
                kinds=_SEARCHABLE_SCOPE_KINDS,
                limit=request.limit,
            ),
            base_scope=request.scope,
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
