"""Production provider and nine-tool assembly for Ask Dev."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.licensing import evaluate_org_feature_async
from dev_health_ops.licensing.registry import ASK_DEV_WAVE_3_1_FEATURE
from dev_health_ops.llm.agent.budget_policy import BUDGET_POLICY_VERSION
from dev_health_ops.llm.agent.contracts import (
    AgentLLMProvider,
    AgentMessage,
    AgentMessageRole,
    AgentToolRequest,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.openai_compatible import (
    READINESS_VERSION,
    OpenAICompatibleAgentProvider,
    PlatformCostMetering,
    build_completion_request,
    platform_cost_metering,
)
from dev_health_ops.llm.agent.policy import (
    CERTIFIED_PLATFORM_AGENT_PROVIDERS,
    AgentFallbackPolicy,
    AgentProviderCandidate,
    AgentProviderPolicy,
    AgentProviderSource,
    resolve_agent_provider_selection,
)
from dev_health_ops.llm.agent.probes.legacy_agent import _probe_registry, _probe_tools
from dev_health_ops.llm.agent.readiness import (
    PLATFORM_READINESS_SETTING_KEY,
    PLATFORM_SETTINGS_ORG_ID,
    AgentReadinessService,
    SettingsAgentReadinessStore,
)
from dev_health_ops.llm.agent.role_readiness import RoleReadinessService
from dev_health_ops.llm.agent.roles import (
    PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    AgentRole,
    RoleCertificationProfile,
    SettingsRoleCertificationStore,
)
from dev_health_ops.llm.agent.scripted_openai_service import SCRIPTED_OPENAI_MODEL
from dev_health_ops.llm.budget import attach_agent_budget_guard
from dev_health_ops.llm.credentials import LLMCredentials, resolve_llm_credentials
from dev_health_ops.llm.errors import LLMAuthError
from dev_health_ops.llm.providers import (
    resolve_model_name,
    resolve_provider_name,
)
from dev_health_ops.llm.providers.base import DEFAULT_MODEL_BY_PROVIDER
from dev_health_ops.llm.qua_shadow_budget import attach_qua_shadow_budget_guard
from dev_health_ops.metrics.prometheus import (
    ASK_DEV_PLATFORM_MODEL_UNPRICED_TOTAL,
)
from dev_health_ops.models.settings import SettingCategory

from .canonical_enrichment import CanonicalEnrichmentAccessor
from .contracts import (
    DevActualCompletion,
    DevAnswer,
    DevCIFact,
    DevContractVersions,
    DevDataHealth,
    DevDeploymentFact,
    DevEvidenceFlags,
    DevEvidenceRef,
    DevGraphEdge,
    DevIncidentFact,
    DevMetricDefinition,
    DevPullRequestFact,
    DevRequiredChildFact,
    DevScope,
    DevScopeResolution,
    DevSourceHealth,
    DevStatusConflict,
    DevStatusFact,
    DevToolRequest,
    DevToolResult,
    DirectScope,
    EntityType,
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
    EvidenceRecord,
    EvidenceReferenceSigner,
    EvidenceService,
)
from .graph_investigation_query import (
    CohortDiscoveryFamily,
    GraphAuthorizationScope,
    GraphInvestigationQuery,
    GraphInvestigationRequest,
    GraphQueryOutcome,
    GraphQueryResult,
)
from .graph_routing_policy import CanonicalGraphRoutingEntitlementAuthorizer
from .investigation_plans import PlanExecutor
from .investigation_plans.plan_documents import CORE_PLANS_BY_INTENT
from .investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
    build_registry_with_wave_3_1,
)
from .metrics.clickhouse import ClickHouseMetricSource
from .metrics.service import MetricQueryRequest, MetricQueryService
from .native_evidence import native_evidence_adapters
from .native_evidence_resolver import NativeEvidenceCandidateResolver
from .native_status_change import ClickHouseStatusChangeSource
from .native_team_workload import ClickHouseTeamWorkloadSource
from .operational_deficiency_service import OperationalDeficiencyService
from .orchestrator import DevRunLimits
from .org_policy import load_ask_dev_org_policy
from .portfolio_status_service import PortfolioStatusService
from .project_health_service import ProjectHealthService
from .prompts import LEGACY_PROMPT_VERSION, PROMPT_VERSION
from .qua_shadow import QUAShadowConfig, QuestionUnderstandingShadow
from .question_interpreter import QuestionInterpreter
from .runtime import BoundedDevRuntime, DevRuntimeUnavailable
from .scope_catalog import ClickHouseAuthorizedEntityCatalog
from .scope_service import (
    MAX_CANDIDATES,
    MODEL_SEARCHABLE_ENTITY_KINDS,
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeSearchRequest,
    scope_request_from_scope,
)
from .status_change_service import (
    ChangeSummaryRequest,
    CIFact,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    StatusChangeService,
    StatusFact,
    StatusSnapshotRequest,
)
from .subject_preflight import SubjectPreflight
from .team_health_service import TeamHealthService
from .team_workload_service import TeamWorkloadService
from .tool_registry import TOOL_CONTRACT_VERSION, AskDevToolRegistry
from .work_graph_neighbors_service import (
    ALLOWED_RELATIONSHIP_TYPES,
    ClickHouseWorkGraphNeighborSource,
    GraphDirection,
    WorkGraphNeighborEdge,
    WorkGraphNeighborsRequest,
    WorkGraphNeighborsResult,
    WorkGraphNeighborsService,
    WorkGraphResultState,
    WorkGraphRootRef,
)
from .work_graph_neighbors_service import (
    QUERY_VERSION as _WORK_GRAPH_QUERY_VERSION,
)
from .work_graph_neighbors_service import (
    SCHEMA_VERSION as _WORK_GRAPH_SCHEMA_VERSION,
)

logger = logging.getLogger(__name__)

_LLM_CATEGORY = SettingCategory.LLM.value
_METRIC_REGISTRY_VERSION = "ask-dev-metrics.v1"
_QUERY_BUNDLE_VERSION = "ask-dev-query-bundle.v1"
ACCEPTANCE_OPENAI_MODEL = SCRIPTED_OPENAI_MODEL
_ACCEPTANCE_OPENAI_DISCLOSURE_KEY = "ask_dev_scripted_acceptance"
_ACCEPTANCE_OPENAI_HOSTS = frozenset(
    {"127.0.0.1", "localhost", "ask-dev-scripted-openai"}
)
# CHAOS-3301 structural pattern: an explicit, independently-pinned constant
# (scope_service.MODEL_SEARCHABLE_ENTITY_KINDS), never derived from
# DIRECT_SCOPE_KINDS — DIRECT_SCOPE_KINDS gained TEAM this issue, and that
# must not silently widen what the model-facing resolve_scope.v1 tool can
# search by free-text query (team is a preflight-committed *subject*, never a
# query-search kind on this surface). Named-entity resolution also never
# searches organization scope itself (CHAOS-3256).
_SEARCHABLE_SCOPE_KINDS: tuple[EntityKind, ...] = tuple(
    sorted(MODEL_SEARCHABLE_ENTITY_KINDS, key=lambda kind: kind.value)
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
    #: CHAOS-3358. True when the SELECTED provider is the platform one and its
    #: stored certification is not current. Deliberately a fact, not a side
    #: effect: resolution runs on unauthenticated-adjacent read paths (the
    #: capabilities projection resolves a provider before Ask Dev entitlement
    #: or emergency-disable is checked), and automatic re-certification spends
    #: real operator provider calls. Only an authorized run that actually
    #: selected the platform provider acts on this -- see the dev router's
    #: message path. Codex CHAOS-3358 review, CONFIRMED reachable.
    platform_certification_stale: bool = False
    #: CHAOS-3452. A SEPARATE, independently-constructed provider instance
    #: already wrapped by ``attach_qua_shadow_budget_guard`` -- never the
    #: SAME object as ``provider`` above, and never touched by
    #: ``attach_agent_budget_guard``. ``None`` whenever the operator flag
    #: ``ASK_DEV_QUA_SHADOW_ENABLED`` is unset (construction is skipped
    #: entirely in that case, preserving CHAOS-3389's "an unset env var is
    #: byte-identical to the seam not existing" invariant) or whenever
    #: building it failed for any reason -- a shadow-provider construction
    #: failure must never surface as a live-path failure; it degrades to the
    #: existing typed ``SKIPPED_NO_PROVIDER`` outcome instead. Only
    #: ``resolve_production_provider`` (the real production runtime path)
    #: populates this; certification/admin routes never do.
    qua_shadow_provider: AgentLLMProvider | None = None


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


def _platform_readiness_store(session: AsyncSession) -> SettingsAgentReadinessStore:
    """Readiness store for the platform-owned (operator env-configured) provider.

    Deliberately scoped by the ``org_id=""`` sentinel and a setting key
    distinct from the ordinary per-org ``ask_dev_agent_readiness`` slot (see
    ``dev_health_ops.llm.agent.readiness`` for the full rationale). Only the
    Platform Admin router ever writes here; every organization's production
    selection just reads it to learn whether the platform candidate is
    currently certified (CHAOS-3265).
    """

    return SettingsAgentReadinessStore(
        SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
        key=PLATFORM_READINESS_SETTING_KEY,
    )


def _platform_role_store(session: AsyncSession) -> SettingsRoleCertificationStore:
    """Per-role certification store for the platform-owned provider -- same
    ``org_id=""`` sentinel scoping and dedicated-key rationale as
    ``_platform_readiness_store`` (CHAOS-3265)."""

    return SettingsRoleCertificationStore(
        SettingsService(session, PLATFORM_SETTINGS_ORG_ID),
        key=PLATFORM_ROLE_CERTIFICATION_SETTING_KEY,
    )


async def resolve_production_provider(
    session: AsyncSession, *, org_id: str
) -> ProductionProviderResolution:
    settings = SettingsService(session, org_id)
    byo_readiness = await SettingsAgentReadinessStore(settings).load()
    platform_readiness = await _platform_readiness_store(session).load()
    # CHAOS-3285: live selection additionally requires a CURRENT, COMPATIBLE
    # legacy_agent role certification -- see _candidate()'s docstring for why
    # the old binary AgentReadinessRecord alone is not sufficient here.
    byo_role_profile = await SettingsRoleCertificationStore(settings).load()
    platform_role_profile = await _platform_role_store(session).load()

    byo, platform, policy = await _provider_candidates(
        settings,
        byo_readiness=byo_readiness,
        platform_readiness=platform_readiness,
        byo_role_profile=byo_role_profile,
        platform_role_profile=platform_role_profile,
    )
    return _resolve_provider_selection(
        byo=byo,
        platform=platform,
        policy=policy,
        session=session,
        org_id=org_id,
        include_qua_shadow=True,
    )


async def resolve_certification_provider(
    session: AsyncSession, *, org_id: str
) -> ProductionProviderResolution:
    """Resolve the configured candidate without requiring prior certification."""

    settings = SettingsService(session, org_id)
    byo, platform, policy = await _provider_candidates(
        settings, byo_readiness=None, platform_readiness=None, certification=True
    )
    return _resolve_provider_selection(
        byo=byo,
        platform=platform,
        policy=policy,
        session=session,
        org_id=org_id,
    )


async def resolve_platform_certification_provider() -> ProductionProviderResolution:
    """Resolve ONLY the platform-owned candidate, with no BYO arbitration.

    Used exclusively by the Platform Admin readiness route. This never reads
    any organization's settings -- the platform candidate is built purely
    from operator environment variables -- so there is nothing to arbitrate
    against a BYO candidate (CHAOS-3265).
    """

    platform, platform_provider_name = _platform_candidate(
        readiness=None, certification=True
    )
    if platform is None or platform_provider_name == "none":
        raise DevRuntimeUnavailable(
            "provider_not_configured", "No certified Ask Dev model is ready."
        )
    if platform.usable:
        provider = _provider(platform)
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.PLATFORM,
            family=platform.provider,
            model=platform.model,
            provider_label="OpenAI compatible",
            model_label=platform.model.replace("\r", "").replace("\n", "")[:256],
            readiness_fingerprint=_readiness_fingerprint(platform),
        )
    if not platform.certified:
        raise DevRuntimeUnavailable(
            "model_not_supported", "The configured Ask Dev model is not supported."
        )
    raise DevRuntimeUnavailable(
        "provider_not_configured", "No certified Ask Dev model is ready."
    )


async def certify_platform_resolution(
    session: AsyncSession, resolution: ProductionProviderResolution
) -> None:
    """Probe an already-resolved platform provider and persist the verdict.

    This is the ONE implementation of "certify the platform-owned provider
    and write the record". Both callers share it deliberately: the Platform
    Admin preflight button (a force-refresh) and the automatic
    re-certification that CHAOS-3358 triggers when the stored record has gone
    stale. Two copies of this sequence would be two things to keep in
    agreement, and the whole point of the automatic path is that the record
    it writes is indistinguishable from the one the button writes -- same
    binary ``AgentReadinessRecord``, same per-role ``legacy_agent`` record,
    same fingerprint.

    The provider probed and the fingerprint written both come off the SAME
    ``ProductionProviderResolution``, so the record cannot claim a
    configuration other than the one actually exercised.
    """

    try:
        await AgentReadinessService(_platform_readiness_store(session)).certify(
            resolution.provider,
            provider_name=resolution.family,
            model=resolution.model,
            fingerprint=resolution.readiness_fingerprint,
        )
        try:
            # CHAOS-3285: certify the legacy_agent role in the new per-role
            # store too, on the same already-resolved provider, so the
            # per-role projection reflects real preflight results rather
            # than staying "not_yet_certified" forever. Best-effort: a bug
            # here must never regress the existing (tested, relied-upon)
            # binary certify() call above -- certify_legacy_agent already
            # turns every AgentProviderError into a FAILED/INCOMPATIBLE
            # record internally, so only a genuinely unexpected failure
            # (e.g. store I/O) reaches this except.
            #
            # CHAOS-3285 round 2 (Codex MEDIUM): the whole attempt runs
            # inside a SAVEPOINT (begin_nested), never bare. A DB error
            # caught by a plain except -- without a rollback or savepoint --
            # leaves the session's transaction unable to accept further
            # operations; the NEXT query on it raises PendingRollbackError,
            # and when the request's own session dependency later tries to
            # commit, THAT failure rolls back the whole transaction --
            # silently discarding the binary certify() write just above,
            # despite this being "caught". begin_nested()'s __aexit__ rolls
            # back only to the savepoint on an exception, leaving the outer
            # transaction (and the binary write already flushed into it)
            # intact and the session usable for the rest of this request.
            async with session.begin_nested():
                await RoleReadinessService(_platform_role_store(session)).certify_role(
                    AgentRole.LEGACY_AGENT,
                    resolution.provider,
                    certification_key=resolution.readiness_fingerprint,
                )
        except Exception:
            logger.warning(
                "Failed to certify the legacy_agent role during platform "
                "Ask Dev preflight",
                exc_info=True,
            )
    finally:
        try:
            await resolution.provider.aclose()
        except Exception:
            # Best-effort cleanup only: the certify() call above has already
            # persisted the readiness result (or its own failure state), so a
            # transport-close error here must never mask that outcome or fail
            # this request. Still worth knowing about (a leaked connection is
            # an operational signal), so log it rather than swallow it.
            logger.warning(
                "Failed to close platform Ask Dev provider connection after preflight",
                exc_info=True,
            )


async def certify_platform_provider(session: AsyncSession) -> bool:
    """Resolve the platform provider and certify it. False when there was
    nothing to certify.

    The entry point for automatic re-certification (CHAOS-3358), which -- unlike
    the admin route -- has no request to answer and so resolves the provider
    itself. A provider that will not resolve at all (unconfigured, or an
    explicit operator "none") leaves the stored record untouched: the badge
    already reports that state from the resolution failure itself, and
    overwriting a real prior verdict with a synthetic one would lose
    information.
    """

    try:
        resolution = await resolve_platform_certification_provider()
    except DevRuntimeUnavailable:
        return False
    await certify_platform_resolution(session, resolution)
    return True


async def resolve_byo_certification_provider(
    session: AsyncSession, *, org_id: str
) -> ProductionProviderResolution:
    """Resolve ONLY the org's own BYO candidate, with no platform fallback.

    This tests the organization's saved LLM credentials specifically,
    independent of whether BYO currently wins Ask Dev's provider-selection
    arbitration (CHAOS-3265).
    """

    settings = SettingsService(session, org_id)
    byo = await _byo_candidate(settings, readiness=None, certification=True)
    if byo is None:
        raise DevRuntimeUnavailable(
            "provider_not_configured",
            "No BYO LLM configuration is saved for this organization.",
        )
    if byo.usable:
        provider = _provider(byo)
        provider = attach_agent_budget_guard(
            provider,
            session=session,
            org_id=org_id,
            provider=byo.provider,
            model=byo.model,
            base_url=byo.credentials.base_url or None,
        )
        return ProductionProviderResolution(
            provider=provider,
            source=AgentProviderSource.BYO,
            family=byo.provider,
            model=byo.model,
            provider_label="OpenAI compatible",
            model_label=byo.model.replace("\r", "").replace("\n", "")[:256],
            readiness_fingerprint=_readiness_fingerprint(byo),
        )
    if not byo.certified:
        raise DevRuntimeUnavailable(
            "model_not_supported", "The configured Ask Dev model is not supported."
        )
    raise DevRuntimeUnavailable(
        "provider_not_configured",
        "No BYO LLM configuration is saved for this organization.",
    )


async def _byo_candidate(
    settings: SettingsService,
    *,
    readiness: Any,
    certification: bool = False,
    role_profile: RoleCertificationProfile | None = None,
) -> AgentProviderCandidate | None:
    byo_provider_name = (await settings.get("provider", _LLM_CATEGORY) or "").strip()
    byo_model = (await settings.get("model", _LLM_CATEGORY) or "").strip()
    if byo_provider_name == "openai" and not byo_model:
        byo_model = DEFAULT_MODEL_BY_PROVIDER["openai"]
    byo_credentials = LLMCredentials(
        api_key=await settings.get("api_key", _LLM_CATEGORY) or "",
        base_url=await settings.get("base_url", _LLM_CATEGORY) or "",
    )
    return _candidate(
        provider_name=byo_provider_name,
        model=byo_model,
        credentials=byo_credentials,
        source=AgentProviderSource.BYO,
        readiness=readiness,
        certification=certification,
        role_profile=role_profile,
    )


def _platform_candidate(
    *,
    readiness: Any,
    certification: bool = False,
    role_profile: RoleCertificationProfile | None = None,
) -> tuple[AgentProviderCandidate | None, str]:
    """Build the platform candidate purely from operator environment state.

    Returns the candidate together with the raw resolved provider name (the
    caller needs the raw name to detect an explicit operator "none" and to
    build ``AgentProviderPolicy.llm_globally_disabled``).
    """

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
        role_profile=role_profile,
    )
    return platform, platform_provider_name


async def _provider_candidates(
    settings: SettingsService,
    *,
    byo_readiness: Any,
    platform_readiness: Any,
    certification: bool = False,
    byo_role_profile: RoleCertificationProfile | None = None,
    platform_role_profile: RoleCertificationProfile | None = None,
) -> tuple[
    AgentProviderCandidate | None,
    AgentProviderCandidate | None,
    AgentProviderPolicy,
]:
    byo = await _byo_candidate(
        settings,
        readiness=byo_readiness,
        certification=certification,
        role_profile=byo_role_profile,
    )
    platform, platform_provider_name = _platform_candidate(
        readiness=platform_readiness,
        certification=certification,
        role_profile=platform_role_profile,
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


def _qua_shadow_flag_enabled() -> bool:
    return os.getenv("ASK_DEV_QUA_SHADOW_ENABLED") == "1"


def _qua_commit_flag_enabled() -> bool:
    """Whether a verified QUA proposal may commit the run's subject (CHAOS-3525).

    Requires BOTH flags. The commit flag alone is inert by construction --
    with the shadow off no proposal is ever produced, so a commit gate that
    read only its own variable would appear armed while doing nothing, which
    is a worse operational state than plainly off.

    A named predicate rather than an inline expression at the construction
    site, mirroring ``_qua_shadow_flag_enabled`` above, so the rollout ladder
    has one definition that tests can assert against instead of restating it.
    """

    return _qua_shadow_flag_enabled() and os.getenv("ASK_DEV_QUA_COMMIT_ENABLED") == "1"


def _build_qua_shadow_provider(
    selected: AgentProviderCandidate, *, org_id: str
) -> AgentLLMProvider | None:
    """A SECOND, independently-constructed provider instance for the QUA
    shadow seam (CHAOS-3452) -- never ``provider.provider``'s object
    identity, and guarded by the isolated shadow quota, never
    ``attach_agent_budget_guard``. Never raises: a construction failure here
    must degrade to the existing typed ``SKIPPED_NO_PROVIDER`` outcome, not
    fail the live run that would otherwise be unaffected by this seam.
    """

    try:
        return attach_qua_shadow_budget_guard(
            _provider(selected),
            org_id=org_id,
            provider=selected.provider,
            model=selected.model,
            base_url=selected.credentials.base_url or None,
        )
    except Exception:
        logger.warning(
            "Failed to construct isolated QUA shadow provider; the shadow "
            "seam will resolve to SKIPPED_NO_PROVIDER for this run",
            exc_info=True,
        )
        return None


def _resolve_provider_selection(
    *,
    byo: AgentProviderCandidate | None,
    platform: AgentProviderCandidate | None,
    policy: AgentProviderPolicy,
    session: AsyncSession,
    org_id: str,
    include_qua_shadow: bool = False,
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
    # CHAOS-3452: constructed ONLY when both this call site opted in
    # (``resolve_production_provider`` -- the real production runtime path;
    # certification/admin routes never do) AND the operator flag is set, so
    # an unset flag costs nothing extra here, preserving CHAOS-3389's
    # "unset env var is byte-identical to the seam not existing" invariant.
    qua_shadow_provider = (
        _build_qua_shadow_provider(selected, org_id=org_id)
        if include_qua_shadow and _qua_shadow_flag_enabled()
        else None
    )
    return ProductionProviderResolution(
        provider=provider,
        qua_shadow_provider=qua_shadow_provider,
        source=selected.source,
        family=selected.provider,
        model=selected.model,
        provider_label="OpenAI compatible",
        model_label=selected.model.replace("\r", "").replace("\n", "")[:256],
        readiness_fingerprint=_readiness_fingerprint(selected),
        # The source check is redundant TODAY and deliberately kept: a BYO
        # candidate is only ever selectable while readiness_current is true
        # (AgentProviderCandidate.usable still requires it for BYO, and
        # selection only returns candidates that are usable), so dropping it
        # is currently an equivalent mutation -- no reachable input
        # distinguishes the two. It is written explicitly anyway so this flag
        # does not silently start meaning something else if BYO's own gate
        # ever changes. Stated rather than claimed as covered: no test kills
        # that mutant, because none can.
        platform_certification_stale=(
            selected.source is AgentProviderSource.PLATFORM
            and not selected.readiness_current
        ),
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
    role_profile: RoleCertificationProfile | None = None,
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
    # CHAOS-3285: live selection (certification=False) additionally requires
    # a CURRENT, COMPATIBLE legacy_agent role certification -- not just the
    # old binary AgentReadinessRecord. Without this, an operator could
    # re-certify through a route that only ever runs the old 512-token echo
    # probe (or the new role probe could simply never have run), the binary
    # store would read "current", and this candidate would become
    # selectable for live traffic having never demonstrated it can handle
    # the real production request shape at all -- the exact gap the role
    # model exists to close (Codex CHAOS-3285 review). `role_profile is
    # None` fails closed (never current), the same direction as every other
    # missing-input default in this function; only ``certification=True``
    # (the "give me a candidate to certify" preflight paths) bypasses this,
    # exactly as it already bypasses the binary check below.
    legacy_agent_record = (
        role_profile.for_role(AgentRole.LEGACY_AGENT)
        if role_profile is not None
        else None
    )
    legacy_agent_current = bool(
        legacy_agent_record is not None
        and legacy_agent_record.is_current(certification_key=provider_fingerprint)
    )
    current = bool(
        certification
        or (
            readiness
            and readiness.is_current(
                fingerprint=provider_fingerprint,
                readiness_version=READINESS_VERSION,
            )
            and legacy_agent_current
        )
    )
    return candidate_type(
        provider=candidate.provider,
        model=candidate.model,
        credentials=candidate.credentials,
        source=candidate.source,
        readiness_current=current,
    )


@functools.lru_cache(maxsize=1)
def _canonical_contract_digest() -> str:
    """Content digest of every capability input a role certification must
    invalidate on, beyond the coarse version-string constants already
    folded into ``_readiness_fingerprint``.

    CHAOS-3285 round 2 (Codex HIGH): the fingerprint previously hashed only
    ``PROMPT_VERSION`` -- the committed-subject prompt shape
    (``subject_committed=True``). ``PromptComposer`` actually selects
    between that and ``LEGACY_PROMPT_VERSION`` (the uncommitted-subject /
    flag-off shape) per run, and the ``legacy_agent`` probe forces
    ``subject_committed=True`` unconditionally -- so the uncommitted-subject
    prompt shape was never probed, and a text-only change to ONLY that
    shape's section (with no version bump, since it was never folded at
    all) never invalidated anything either.

    Rather than track more bare version-string constants (each one an
    opportunity to forget folding the next thing that can change the wire
    shape), this folds actual CONTENT: both prompt version constants, the
    real ``DevAnswer`` response grammar the adapter sends on the wire, the
    real tool registry manifest (every tool's real schema, not just the
    registry's own version string), and the real ``DevRunLimits`` values (a
    limit changing without anyone bumping a version string must still
    invalidate). Computed from the real producers -- ``AskDevToolRegistry``,
    ``DevAnswer.model_json_schema()``, ``DevRunLimits`` -- never hand-
    maintained, and never touches a database or executes a tool.
    Memoized: every input here is fixed at import time by the running code,
    never per-request data, so recomputing per call would be pure overhead
    on the request hot path.
    """

    async def _unused_executor(_context: object, _request: object) -> Any:
        raise AssertionError(  # pragma: no cover - never invoked
            "contract digest must never execute a tool"
        )

    registry = AskDevToolRegistry({tool_id: _unused_executor for tool_id in ToolID})
    manifest = registry.manifest(allowed_tool_ids=None)
    limits = DevRunLimits()
    payload = {
        "prompt_version": PROMPT_VERSION,
        "legacy_prompt_version": LEGACY_PROMPT_VERSION,
        "response_schema": DevAnswer.model_json_schema(mode="validation"),
        "tool_manifest": manifest,
        "limits": {
            "max_output_tokens_per_call": limits.max_output_tokens_per_call,
            "max_total_input_tokens": limits.max_total_input_tokens,
            "max_total_output_tokens": limits.max_total_output_tokens,
            "model_rounds": limits.model_rounds,
            "tool_calls": limits.tool_calls,
        },
    }
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:24]


def _wire_request_probe_messages(*, round_1: bool) -> tuple[AgentMessage, ...]:
    """Fixed, deterministic placeholder messages matching one probe round's
    STRUCTURAL shape (which roles appear, whether a ``tool_calls``/
    ``tool_call_id`` key is present in the built request) -- never real
    conversation content, which varies per call and is neither needed nor
    safe to fingerprint. Prompt CONTENT is separately invalidated via
    ``_canonical_contract_digest``'s ``PROMPT_VERSION``/
    ``LEGACY_PROMPT_VERSION`` folding; this is only about the request's
    mechanical shape. Content is always this exact placeholder so the
    digest is stable across runs and never varies with real conversation
    text."""

    system = AgentMessage(AgentMessageRole.SYSTEM, "")
    user = AgentMessage(AgentMessageRole.USER, "")
    if round_1:
        return (system, user)
    tool_request = AgentToolRequest(
        tool_id="wire_request_digest_scaffold", arguments={}, call_id="scaffold"
    )
    assistant = AgentMessage(AgentMessageRole.ASSISTANT, "", tool_request=tool_request)
    tool_result_message = AgentMessage(
        AgentMessageRole.TOOL, "{}", tool_call_id="scaffold"
    )
    return (system, user, assistant, tool_result_message)


@functools.cache
def _wire_request_digest(model: str) -> str:
    """Content digest of the COMPLETE non-secret wire request
    ``OpenAICompatibleAgentProvider.decide()`` would send for ``model``, at
    both probe round shapes -- built by calling
    ``openai_compatible.build_completion_request`` directly, the SAME
    producer ``decide()`` itself consumes to build its actual request.

    CHAOS-3285 round 5 (Codex HIGH): round 4's ``_wire_policy_digest``
    covered only the capability-gated controls extracted into the narrower
    ``wire_policy_kwargs`` -- ``decide()`` itself still independently
    assembled ``max_completion_tokens`` (via ``model_family_budget``), the
    full generated response_format schema body, and the serialized tool
    payloads, none of which the fingerprint ever hashed; changing the
    response_format wrapper's literal ``"name"``/``"strict"`` values left
    the fingerprint unchanged. This replaces that narrower digest by
    calling the SAME single producer ``decide()`` calls, so no field
    assembled inside it can independently drift from what gets
    fingerprinted again.

    Tools and the response schema are the same real producers
    ``_canonical_contract_digest`` uses (the full 9-tool registry via
    ``_probe_registry``/``_probe_tools``, the real ``DevAnswer`` schema);
    ``max_output_tokens`` is the real ``DevRunLimits`` per-call cap. Cached
    per model (not a single shared cache like ``_canonical_contract_digest``)
    since this genuinely varies by candidate, and models seen per process
    are a small, bounded set.
    """

    registry = _probe_registry()
    tools = _probe_tools(registry)
    response_schema = DevAnswer.model_json_schema(mode="validation")
    max_output_tokens = DevRunLimits().max_output_tokens_per_call
    round_1 = build_completion_request(
        model=model,
        messages=_wire_request_probe_messages(round_1=True),
        tools=tools,
        response_schema=response_schema,
        max_output_tokens=max_output_tokens,
    )
    round_2 = build_completion_request(
        model=model,
        messages=_wire_request_probe_messages(round_1=False),
        tools=tools,
        response_schema=response_schema,
        max_output_tokens=max_output_tokens,
    )
    canonical = json.dumps(
        {"round_1": round_1, "round_2": round_2},
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:24]


def _credential_fingerprint(credentials: LLMCredentials) -> str:
    """Non-reversible fingerprint of ALL identity-bearing credential material
    actually used on the wire -- api_key, organization, project, and
    custom_headers. The raw ``api_key`` and any header value are NEVER
    stored, logged, or included in any return value anywhere; only this
    function's SHA-256 digest is.

    CHAOS-3285 round 5 (Codex HIGH, ratified design): certifying provider
    key A, then saving key B for the exact same provider/model/base_url,
    left the stored certification (keyed only on provider/model/base_url)
    reading as still current -- so live selection kept using it while every
    real request actually carried B's Authorization header, never having
    demonstrated B can handle the production request shape at all. Folding
    this fingerprint into the certification key makes a rotated credential
    unfindable by construction: a prior certification's key can never equal
    the new credential's key, so ``is_current()`` reads False and selection
    fails closed until the NEW credential is re-certified. This does not
    rely on any save/rotation code path remembering to invalidate anything.

    CHAOS-3285 round 6 (Codex HIGH): api_key alone was not the whole
    identity. The OpenAI SDK reads organization/project/custom headers
    AMBIENTLY from OPENAI_ORG_ID/OPENAI_PROJECT_ID/OPENAI_CUSTOM_HEADERS
    when a client is constructed without them -- codex's exact repro:
    flipping OPENAI_PROJECT_ID from one project to another changed the real
    OpenAI-Project wire header while this fingerprint stayed byte-for-byte
    identical, since it never referenced organization/project/headers at
    all. ``LLMCredentials`` now resolves these explicitly (see
    ``credentials.py``) instead of leaving them for the SDK to infer, which
    makes them representable here. Folded via a canonical JSON structure
    (sorted keys, a sorted list of [name, value] header pairs) rather than
    naive string concatenation, so there is no ambiguity between e.g.
    organization="ab" + project="c" and organization="a" + project="bc".
    """

    canonical = json.dumps(
        {
            "api_key": credentials.api_key,
            "organization": credentials.organization,
            "project": credentials.project,
            "custom_headers": [list(pair) for pair in credentials.custom_headers],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:24]


def _readiness_fingerprint(
    candidate: AgentProviderCandidate, *, role: AgentRole = AgentRole.LEGACY_AGENT
) -> str:
    """Fingerprint every capability input that invalidates certification.

    CHAOS-3285: extended to fold ``TOOL_CONTRACT_VERSION``,
    ``BUDGET_POLICY_VERSION``, and ``_canonical_contract_digest()`` (which
    itself folds both prompt shapes, the real response grammar, the real
    tool manifest, and the real run limits -- see its docstring) --
    previously only a manual ``READINESS_VERSION`` bump invalidated a stale
    certification, even though the composed prompt, tool registry, or
    per-family token-budget policy can each independently change the wire
    shape a certified provider was actually tested against. Also folds
    ``role`` now that certification is per-role rather than a single binary
    verdict (see ``llm.agent.roles``), (round 5) ``_wire_request_digest``
    -- the COMPLETE non-secret wire request the adapter would send for this
    model at both probe round shapes (tool_choice, parallel_tool_calls,
    temperature, reasoning_effort, the full response_format wrapper AND
    schema body, serialized tool payloads, max_completion_tokens), see its
    docstring -- and (round 5) ``_credential_fingerprint`` -- see its
    docstring for the credential-rotation gap this closes.

    Migration/invalidation semantics (explicit): this changes the computed
    fingerprint value for the existing single-role (legacy binary) selection
    path too, since every call site below defaults ``role`` to
    ``AgentRole.LEGACY_AGENT`` -- the role today's production runtime
    actually exercises (full tool registry, full ``DevAnswer`` grammar).
    Every previously stored ``AgentReadinessRecord.fingerprint`` was computed
    without these folded inputs, so it no longer equals the newly computed
    value here and ``AgentReadinessRecord.is_current`` returns ``False``:
    existing certifications become stale and the runtime fails closed until
    re-certified -- exactly the same self-invalidating pattern already
    established for the ``READINESS_VERSION`` bump (CHAOS-3254). No stored
    record is silently reinterpreted as still current.
    """

    return hashlib.sha256(
        "\0".join(
            (
                candidate.source.value,
                candidate.provider,
                candidate.model,
                candidate.credentials.base_url,
                _credential_fingerprint(candidate.credentials),
                READINESS_VERSION,
                TOOL_CONTRACT_VERSION,
                BUDGET_POLICY_VERSION,
                role.value,
                _canonical_contract_digest(),
                _wire_request_digest(candidate.model),
            )
        ).encode()
    ).hexdigest()[:24]


def _provider(candidate: AgentProviderCandidate) -> AgentLLMProvider:
    if candidate.provider not in CERTIFIED_PLATFORM_AGENT_PROVIDERS:
        raise DevRuntimeUnavailable(
            "model_not_supported", "The configured Ask Dev model is not supported."
        )
    # CHAOS-3552: an unmeterable model must never be booked SILENTLY.
    #
    # ProviderBudget reserves US$1 per model call and reconciles it down to the
    # real cost afterwards -- but ONLY when a real cost exists.
    # ``_estimated_cost_microusd`` returns None for an unpriced model and the
    # reconciliation branch leaves the reservation standing, so every call
    # permanently books US$1 against the org's monthly allowance. Measured on
    # the dev stack's own ``gpt-5-nano``: US$4.00 booked per run against
    # US$0.018 real -- a 222x overcharge that exhausted the default US$100
    # allowance after 25 runs, against a 1,000-run request cap. That is what
    # CHAOS-3523's "bounds sized for BYO are gating platform runs" actually was.
    #
    # Reported HERE, at construction, rather than per call: once per provider
    # build is enough to tell an operator, and per-call would be log spam on
    # exactly the deployments already paying for the defect.
    #
    # NOTHING RAISES. An earlier revision refused construction; measured
    # availability evidence retired that (see the branch history): with a
    # three-entry price book, refusing would have removed Ask Dev from every
    # organization running gpt-4o, gpt-5, o3 or any other unlisted model --
    # far larger harm than an overstated allowance. Self-hosted providers pass
    # through genuinely unmetered, and the acceptance fixture is carved out.
    # See ``platform_cost_metering``.
    metering = platform_cost_metering(
        provider=candidate.provider,
        model=candidate.model,
        base_url=candidate.credentials.base_url,
    )
    if metering in (
        PlatformCostMetering.UNPRICED_CONFIGURATION_ERROR,
        PlatformCostMetering.UNKNOWN_BILLABILITY,
    ):
        # LOUD, not fatal (team-lead ruling, revising an earlier fail-loud
        # ruling on measured availability evidence). Refusing construction
        # would have taken Ask Dev away from every organization running an
        # OpenAI model outside this build's three-entry price book --
        # gpt-4o, gpt-5, o3 and the rest -- which is a far larger harm than
        # an overstated allowance.
        #
        # So the run proceeds and books the worst-case reservation exactly as
        # it does today, because a platform run spends real operator dollars
        # on the platform key and unmetered openai spend bounded only by the
        # request cap is not an acceptable posture. What changes is that it is
        # never SILENT: the invariant survives as written -- "must never
        # SILENTLY book the reservation as its cost" -- and loud booking
        # satisfies it. Under chris's request-primary ruling the request cap
        # is the control anyway, so a conservative charge against the cost
        # backstop is defensible once it is attributed.
        #
        # Emitted at CONSTRUCTION rather than per call: once per provider
        # build is enough to tell an operator, and per-call would be log spam
        # on the exact deployments already paying for the defect.
        logger.warning(
            "ask_dev.platform_model_unpriced",
            extra={
                "model": candidate.model,
                "provider": candidate.provider,
                "reason": metering.value,
                # Two different operators, two different remedies: a typo'd
                # or new OpenAI model needs a price entry; an Azure/OpenRouter
                # /gateway deployment needs its endpoint priced (CHAOS-3560),
                # and telling them the same thing sends one of them chasing
                # the wrong fix.
                "remedy": (
                    "price the model in _PLATFORM_MODEL_PRICES or set "
                    "LLM_MODEL to a priced model"
                    if metering is PlatformCostMetering.UNPRICED_CONFIGURATION_ERROR
                    else "this endpoint is not api.openai.com, so its billing "
                    "rates are unknown and runs book the conservative "
                    "reservation; pricing known gateways is CHAOS-3560"
                ),
            },
        )
        ASK_DEV_PLATFORM_MODEL_UNPRICED_TOTAL.labels(
            model=candidate.model, reason=metering.value
        ).inc()
    return OpenAICompatibleAgentProvider(
        api_key=candidate.credentials.api_key or "platform-openai-compatible",
        model=candidate.model,
        cost_provider=candidate.provider,
        base_url=candidate.credentials.base_url or None,
        disclosure_key=(
            _ACCEPTANCE_OPENAI_DISCLOSURE_KEY
            if isinstance(candidate, _AcceptanceOpenAICandidate)
            else "openai_compatible"
        ),
        # CHAOS-3285 round 6 (Codex HIGH): passed explicitly so the
        # AsyncOpenAI client never falls back to ambient
        # OPENAI_ORG_ID/OPENAI_PROJECT_ID/OPENAI_CUSTOM_HEADERS -- see
        # OpenAICompatibleAgentProvider.__init__ and
        # LLMCredentials/_credential_fingerprint.
        organization=candidate.credentials.organization,
        project=candidate.credentials.project,
        custom_headers=candidate.credentials.custom_headers,
    )


async def _resolve_exact_contract(
    service: ScopeResolutionService,
    *,
    org_id: str,
    permission_fingerprint: str,
    requested_scope: DevScope,
) -> DevScopeResolution:
    resolution = await service.resolve_contract(
        org_id, permission_fingerprint, scope_request_from_scope(requested_scope)
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
    actual_completion: DevActualCompletion | None = None,
    pull_requests: list[DevPullRequestFact] | None = None,
    ci_checks: list[DevCIFact] | None = None,
    deployments: list[DevDeploymentFact] | None = None,
    incidents: list[DevIncidentFact] | None = None,
    source_health: list[DevSourceHealth] | None = None,
    graph_edges: list[DevGraphEdge] | None = None,
    data_health: list[DevDataHealth] | None = None,
    warnings: list[str] | None = None,
    declared_project_state: str | None = None,
    declared_project_target_date: date | None = None,
    declared_project_evidence_ref_ids: list[str] | None = None,
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
        actual_completion=actual_completion,
        pull_requests=pull_requests or [],
        ci_checks=ci_checks or [],
        deployments=deployments or [],
        incidents=incidents or [],
        source_health=source_health or [],
        graph_edges=graph_edges or [],
        data_health=data_health or [],
        warnings=(warnings or [])[:20],
        serialized_bytes=0,
        declared_project_state=declared_project_state,
        declared_project_target_date=declared_project_target_date,
        declared_project_evidence_ref_ids=declared_project_evidence_ref_ids or [],
    )


def _status(state: str) -> str:
    if state in {"unavailable", "unconfigured", "insufficient_evidence"}:
        return "unavailable"
    if state in {"partial", "degraded", "stale"}:
        return "partial"
    return "success"


# Every status/change/graph fact is projected as its own addressable evidence
# unit, minted through the same signer the ``get_evidence.v1`` route verifies
# against.  This is what makes every evidence ID a status/change/graph fact
# carries independently expandable, instead of forwarding upstream evidence
# IDs verbatim (which today are frequently empty — see CHAOS-3259/CHAOS-3261).
_STATUS_ENTITY_SOURCE_SYSTEM: Mapping[str, str] = {
    "issue": "work_items",
    "pull_request": "pull_requests",
    "work_unit": "work_units",
    # CHAOS-3368 (Codex HIGH fix): a project's own catalog id is never a
    # work_item_id, so routing through "work_items" always expanded to
    # NO_MATCHES. Routes to native_evidence.py's own "projects" adapter,
    # which reads the same table the declared-state facts came from.
    "project": "projects",
}
_CHANGE_CATEGORY_SOURCE_SYSTEM: Mapping[str, str] = {
    "entity": "work_items",
    "status": "work_items",
    # Relationship changes are sourced from work_graph_edges; entity_id for
    # this category is overridden to the edge_id (see change_summary below)
    # so the "work_graph" native evidence adapter can actually resolve it.
    "relationship": "work_graph",
    "blocker": "work_items",
    "dependency": "work_items",
    "pull_request": "pull_requests",
    "review": "reviews",
    "ci": "ci_runs",
    "deployment": "deployments",
    "incident": "incidents",
    # No native evidence adapter backs aggregate metric changes; expansion
    # honestly degrades to "unconfigured" rather than fabricating a match.
    "metric": "metrics",
}
# entity_id is not unique per observed event for these categories (e.g. two
# status transitions, or two dimension rows of one metric, share one
# entity_id) -- mint from (entity_id, change_id) instead of entity_id alone
# so distinct observations never collide onto the same evidence_ref_id.
_CHANGE_COLLISION_PRONE_CATEGORIES = frozenset({"status", "metric"})
# ci_acceptance_checks rows carry a "{repo}#ci{run}#check{key}" entity_id;
# the "ci_runs" adapter only indexes "{repo}#ci{run}". Coarsen to the run so
# expansion resolves the underlying CI run instead of guaranteed NO_MATCHES.
_CI_ACCEPTANCE_CHECK_MARKER = "#check"
# Maps each StatusChangeService blocking-contradiction reason code to the
# fact category that actually produced it, so a conflict cites only the
# evidence that contributed to it rather than every category in the result.
_REASON_CODE_EVIDENCE_CATEGORY: Mapping[str, str] = {
    "required_child_incomplete": "status_facts",
    "open_blocker": "status_facts",
    "required_pull_request_unmerged": "pull_requests",
    "required_review_unresolved": "pull_requests",
    "review_changes_requested": "pull_requests",
    "required_ci_work_skipped": "ci_checks",
    "required_ci_not_passing": "ci_checks",
    "required_deployment_not_succeeded": "deployments",
    "active_blocking_incident": "incidents",
}
_STATUS_EVIDENCE_SOURCE_VERSION = "status-snapshot-evidence.v1"
_CHANGE_EVIDENCE_SOURCE_VERSION = "change-summary-evidence.v1"
_GRAPH_EVIDENCE_SOURCE_VERSION = "work-graph-evidence.v1"
# DevToolResult.evidence is capped at 25 entries (contracts.py); status
# snapshots aggregate six independently-bounded-to-25 categories, so a dense
# scope can mint far more than 25 unique evidence items. Bound defensively
# instead of letting pydantic reject the whole result.
_MAX_RESULT_EVIDENCE = 25


def _mint_evidence(
    signer: EvidenceReferenceSigner,
    org_id: str,
    *,
    source_system: str,
    source_version: str,
    entity_type: str,
    entity_id: str,
    display_label: str,
    observed_at: datetime,
    freshness: FreshnessState,
    confidence: float = 1.0,
    valid_entity_ids: Sequence[str] = (),
    repository_ids: Sequence[str] = (),
) -> DevEvidenceRef:
    """Build a signer-issued, ``get_evidence.v1``-expandable evidence reference.

    Unlike ``EvidenceService._to_ref`` (used by search/get_evidence over free
    text), this mints identity directly from a fact the domain layer already
    authorized and returned, without a second round trip.

    ``valid_entity_ids``/``repository_ids`` must be bound to the CALLER'S
    already-authorized request scope (its direct entity ref / repositories),
    never to the supporting fact's own entity ID: the fact may describe a
    child, linked PR, CI run, deployment, or incident that was never itself
    independently scope-resolved, so binding to it would make expansion
    unauthorized for everything but the direct entity.
    """

    repository_ids = tuple(repository_ids)
    record = EvidenceRecord(
        source_system=source_system,
        source_version=source_version,
        entity_type=entity_type,
        entity_id=entity_id,
        display_label=display_label,
        observed_at=observed_at,
        freshness=freshness,
        provenance=source_system,
        confidence=max(0.0, min(1.0, confidence)),
        repository_ids=repository_ids,
    )
    return DevEvidenceRef(
        schema_version="dev_evidence_ref.v1",
        evidence_ref_id=signer.issue(org_id, record),
        source_system=source_system,
        source_version=source_version,
        entity_type=entity_type,
        entity_id=entity_id,
        display_label=(display_label.strip() or "Evidence")[:256],
        observed_at=observed_at,
        freshness=freshness,
        provenance=source_system,
        confidence=record.confidence,
        repository_ids=list(repository_ids),
        valid_entity_ids=list(valid_entity_ids),
        flags=DevEvidenceFlags(
            stale=freshness is FreshnessState.STALE,
            unavailable=freshness is FreshnessState.UNAVAILABLE,
            untrusted_content=True,
        ),
    )


def _ci_evidence_identity(entity_id: str) -> str:
    return entity_id.split(_CI_ACCEPTANCE_CHECK_MARKER, 1)[0]


def _scope_evidence_binding(scope: DevScope) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return (valid_entity_ids, repository_ids) bound to the request's own
    already-authorized direct scope -- never to a supporting fact's ID."""

    valid_entity_ids = tuple(item.entity_id for item in scope.entity_refs)
    repository_ids = tuple(scope.repositories)
    return valid_entity_ids, repository_ids


def _bounded_result_evidence(
    minted: Mapping[str, DevEvidenceRef],
    *,
    priority_ids: Sequence[str] = (),
    limit: int = _MAX_RESULT_EVIDENCE,
) -> tuple[set[str], bool]:
    """Bound minted evidence to ``limit``, keeping verdict-driving evidence first.

    ``priority_ids`` should cover the declared fact, every required child,
    and every fact that contributed a blocking reason code -- the evidence
    the deterministic verdict actually depends on. Only once that is
    satisfied do arbitrary (alphabetically ordered, for determinism)
    remaining facts fill the rest of the budget. Without this, a dense scope
    could truncate away exactly the evidence the model needs to explain
    *why* the declared/actual states disagree while the verdict itself
    (computed upstream, before truncation) is left unchanged and unexplained.
    """

    if len(minted) <= limit:
        return set(minted), False
    kept: list[str] = []
    seen: set[str] = set()
    for evidence_id in (*priority_ids, *sorted(minted)):
        if len(kept) >= limit:
            break
        if evidence_id in minted and evidence_id not in seen:
            seen.add(evidence_id)
            kept.append(evidence_id)
    return set(kept), True


def _work_graph_roots(scope: DevScope) -> tuple[WorkGraphRootRef, ...]:
    aliases = {"pull_request": "pr"}
    roots = []
    for item in scope.entity_refs:
        kind = aliases.get(item.entity_type.value, item.entity_type.value)
        if kind in {"issue", "pr", "commit", "file", "deployment", "incident"}:
            roots.append(WorkGraphRootRef(kind, item.entity_id))
    return tuple(roots)


@dataclass(slots=True)
class _ProductionPlanExecutorRuntime:
    """CHAOS-3295's ``PlanExecutorRuntime`` over the exact canonical service
    instances ``_assemble_production_runtime`` already constructs for the
    provider tool registry -- never a second, parallel set of services.
    """

    status_service: StatusChangeService
    metric_service: MetricQueryService
    work_graph_service: WorkGraphNeighborsService
    data_health_service: DataHealthService
    #: CHAOS-3296: the same signer the proven v1 tool-call evidence-minting
    #: path (``mint_status_evidence``/``mint_delivery_evidence`` below) uses
    #: -- never a second, parallel evidence-issuing mechanism.
    evidence_signer: EvidenceReferenceSigner

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        ref = _mint_evidence(
            self.evidence_signer,
            org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness,
            confidence=confidence,
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )
        return ref.evidence_ref_id

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return await self.status_service.status_snapshot(
            org_id,
            permission_fingerprint,
            StatusSnapshotRequest(scope=scope, max_items=100),
        )

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        comparison = scope.comparison_range
        assert comparison is not None  # builtin_steps guards this before calling
        return await self.status_service.change_summary(
            org_id,
            permission_fingerprint,
            ChangeSummaryRequest(
                scope=scope,
                current_start=scope.time_range.start,
                current_end=scope.time_range.end,
                comparison_start=comparison.start,
                comparison_end=comparison.end,
                max_items=100,
            ),
        )

    def list_metrics(self, scope):
        return self.metric_service.list_metrics(scope)

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        return await self.metric_service.query(
            org_id,
            permission_fingerprint,
            MetricQueryRequest(metric_id=metric_id, scope=scope),
        )

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        roots = _work_graph_roots(scope)
        if not roots:
            return WorkGraphNeighborsResult(
                schema_version=_WORK_GRAPH_SCHEMA_VERSION,
                state=WorkGraphResultState.EMPTY,
                nodes=(),
                edges=(),
                source_refs=(),
                warnings=(),
                total_count=0,
                returned_count=0,
                truncated=False,
                depth=1,
                query_version=_WORK_GRAPH_QUERY_VERSION,
                watermark=None,
            )
        return await self.work_graph_service.neighbors(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            request=WorkGraphNeighborsRequest(
                scope_request=scope_request_from_scope(scope),
                root_refs=roots,
                relationship_types=tuple(sorted(ALLOWED_RELATIONSHIP_TYPES)),
                direction=GraphDirection.BOTH,
                limit=25,
            ),
        )

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        return await self.data_health_service.inspect(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            scope_request=scope_request_from_scope(scope),
            required_sources=NATIVE_EVIDENCE_SOURCES,
        )


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
        if provider.qua_shadow_provider is not None:
            try:
                await provider.qua_shadow_provider.aclose()
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

    async def resolve_graph_authorization(
        requested_org_id: str,
        requested_permission_fingerprint: str,
        family: CohortDiscoveryFamily,
        authorized_scope: DevScope,
    ) -> GraphAuthorizationScope | None:
        """Derive the bounded graph candidate universe from the canonical catalog.

        The graph arm receives only a complete page from the same
        tenant-filtered catalog used by native Ask Dev scope resolution.  A
        catalog outage, empty roster, or roster larger than the bound is an
        explicit incomplete envelope and therefore falls back to the legacy
        path; no graph membership or model text is consulted.
        """

        if (
            requested_org_id != org_id
            or requested_permission_fingerprint != permission_fingerprint
        ):
            return None

        def requested_candidate_ids() -> frozenset[str] | None:
            """Map the committed request scope to a catalog entity set.

            Team-to-repository/project expansion is deliberately unavailable
            here: the canonical catalog does not grant that relationship, so
            a narrower scope that cannot be represented by the graph family
            fails closed instead of widening to the organization roster.
            An empty set means the committed scope is organization-wide and
            therefore selects the complete catalog page.
            """

            if authorized_scope.organization_id != requested_org_id:
                return None

            expected_type = (
                EntityType.TEAM
                if family is CohortDiscoveryFamily.TEAM_PRESSURE
                else EntityType.PROJECT
            )
            if authorized_scope.surface_context is not None and any(
                ref.entity_type is not expected_type
                for ref in authorized_scope.surface_context.entity_refs
            ):
                return None
            if any(
                ref.entity_type is not expected_type
                for ref in authorized_scope.entity_refs
            ):
                return None

            requested = {
                ref.entity_id
                for ref in authorized_scope.entity_refs
                if ref.entity_type is expected_type
            }
            if authorized_scope.team_ids:
                if family is not CohortDiscoveryFamily.TEAM_PRESSURE:
                    return None
                requested.update(authorized_scope.team_ids)

            if family is CohortDiscoveryFamily.TEAM_PRESSURE:
                if authorized_scope.direct_scope not in {
                    DirectScope.ORGANIZATION,
                    DirectScope.TEAM,
                }:
                    return None
            elif authorized_scope.direct_scope not in {
                DirectScope.ORGANIZATION,
                DirectScope.PROJECT,
            }:
                return None

            return frozenset(requested)

        requested_ids = requested_candidate_ids()
        if requested_ids is None:
            return GraphAuthorizationScope(
                organization_id=requested_org_id,
                permission_fingerprint=requested_permission_fingerprint,
                scope=authorized_scope,
                cohort_discovery_family=family,
                authorized_entity_ids=frozenset(),
                complete=False,
            )
        if family is CohortDiscoveryFamily.TEAM_PRESSURE:
            (
                entities,
                total,
                catalog_available,
            ) = await scope_service.organization_committed_teams(
                requested_org_id,
                requested_permission_fingerprint,
                limit=MAX_CANDIDATES,
            )
        elif family is CohortDiscoveryFamily.PROJECT_CAPACITY:
            (
                entities,
                total,
                catalog_available,
            ) = await scope_service.organization_committed_projects(
                requested_org_id,
                requested_permission_fingerprint,
                limit=MAX_CANDIDATES,
            )
        else:
            return None

        if requested_ids:
            entities = [
                entity for entity in entities if entity.canonical_id in requested_ids
            ]

        ids = frozenset(entity.canonical_id for entity in entities)
        complete = (
            catalog_available
            and total > 0
            and total <= MAX_CANDIDATES
            and (
                (not requested_ids and len(entities) == total and len(ids) == total)
                or (
                    bool(requested_ids)
                    and len(entities) == len(requested_ids)
                    and ids == requested_ids
                )
            )
        )
        return GraphAuthorizationScope(
            organization_id=requested_org_id,
            permission_fingerprint=requested_permission_fingerprint,
            scope=authorized_scope,
            cohort_discovery_family=family,
            authorized_entity_ids=ids,
            complete=complete,
        )

    metric_service = MetricQueryService(ClickHouseMetricSource(clickhouse))
    # CHAOS-3297 stack #3: kept as a NAMED reference (not inlined into
    # StatusChangeService's constructor call the way it was before this
    # stack) so TeamHealthService/TeamWorkloadService/
    # OperationalDeficiencyService can share the SAME source instance --
    # its own (org_id, team_id, as_of) cache then turns their internal
    # team_repository_ids lookup into a hit rather than a second round
    # trip, per those services' own docstrings.
    status_change_source = ClickHouseStatusChangeSource(clickhouse)
    status_service = StatusChangeService(
        status_change_source, metric_service=metric_service
    )
    team_workload_source = ClickHouseTeamWorkloadSource(clickhouse)
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise DevRuntimeUnavailable(
            "provider_not_configured", "Ask Dev evidence signing is unavailable."
        )
    evidence_signer = EvidenceReferenceSigner(secret)
    evidence_service = EvidenceService(
        entitlement=entitlement,
        authorizer=scope_service,
        signer=evidence_signer,
        native_adapters=native_evidence_adapters(clickhouse),
        acr_adapter=None,
        # CHAOS-3675 PR 1/3: the first real (non-fixture) candidate
        # resolver. Registering it does not, by itself, admit anything --
        # nothing in production calls ``EvidenceService.admit`` yet (Lane
        # B's routing work does); this only replaces the "no shipped
        # construction passes a resolver" default the class docstring
        # describes. Handles ``review``-kind candidates only today; every
        # other ``GraphObservationKind`` still refuses as
        # ``source_unconfigured`` until PR 2/3 and PR 3/3 land.
        candidate_resolvers=(NativeEvidenceCandidateResolver(clickhouse),),
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

    def status_source_system(entity_type: str) -> str:
        return _STATUS_ENTITY_SOURCE_SYSTEM.get(entity_type, "work_items")

    def mint_status_evidence(
        fact: StatusFact,
        freshness_by_ref: Mapping[str, FreshnessState],
        *,
        valid_entity_ids: Sequence[str],
        repository_ids: Sequence[str],
    ) -> DevEvidenceRef:
        return _mint_evidence(
            evidence_signer,
            org_id,
            source_system=status_source_system(fact.entity_type),
            source_version=_STATUS_EVIDENCE_SOURCE_VERSION,
            entity_type=fact.entity_type,
            entity_id=fact.entity_id,
            display_label=fact.display_label,
            observed_at=fact.observed_at,
            freshness=freshness_by_ref.get(fact.source_ref_id, FreshnessState.UNKNOWN),
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )

    def mint_delivery_evidence(
        *,
        source_system: str,
        entity_type: str,
        entity_id: str,
        display_label: str,
        observed_at: datetime,
        source_ref_id: str,
        freshness_by_ref: Mapping[str, FreshnessState],
        valid_entity_ids: Sequence[str],
        repository_ids: Sequence[str],
        source_version: str = _STATUS_EVIDENCE_SOURCE_VERSION,
    ) -> DevEvidenceRef:
        return _mint_evidence(
            evidence_signer,
            org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness_by_ref.get(source_ref_id, FreshnessState.UNKNOWN),
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )

    async def status_snapshot(context, request):
        result = await status_service.status_snapshot(
            org_id,
            context.permission_fingerprint,
            StatusSnapshotRequest(scope=request.scope, max_items=request.limit),
        )
        freshness_by_ref: dict[str, FreshnessState] = {
            ref.ref_id: ref.freshness for ref in result.source_refs
        }
        scope_valid_entity_ids, scope_repository_ids = _scope_evidence_binding(
            request.scope
        )
        minted: dict[str, DevEvidenceRef] = {}

        def wire_status_fact(fact: StatusFact) -> DevStatusFact:
            ref = mint_status_evidence(
                fact,
                freshness_by_ref,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
            )
            minted[ref.evidence_ref_id] = ref
            return DevStatusFact(
                fact_id=f"{fact.entity_type}:{fact.entity_id}",
                text=f"{fact.display_label}: {fact.status}",
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        def wire_required_child(fact: StatusFact) -> DevRequiredChildFact:
            ref = mint_status_evidence(
                fact,
                freshness_by_ref,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
            )
            minted[ref.evidence_ref_id] = ref
            return DevRequiredChildFact(
                fact_id=f"{fact.entity_type}:{fact.entity_id}",
                text=fact.display_label,
                status=fact.status,
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        def wire_pull_request(fact: PullRequestFact) -> DevPullRequestFact:
            ref = mint_delivery_evidence(
                source_system="pull_requests",
                entity_type="pull_request",
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                observed_at=fact.observed_at,
                source_ref_id=fact.source_ref_id,
                freshness_by_ref=freshness_by_ref,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
            )
            minted[ref.evidence_ref_id] = ref
            return DevPullRequestFact(
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                state=fact.state,
                review_state=fact.review_state,
                changes_requested=fact.changes_requested,
                merged=fact.merged,
                required=fact.required,
                observed_at=fact.observed_at,
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        def wire_ci(fact: CIFact) -> DevCIFact:
            # ci_acceptance_checks IDs carry a "#check{key}" suffix the
            # ci_runs adapter does not index; coarsen the LOOKUP identity to
            # the run so expansion resolves real content instead of
            # NO_MATCHES. Multiple distinct checks on one run would then
            # mint the same evidence_ref_id -- discriminate the SIGNED
            # identity via source_version (not entity_id) instead, so each
            # check still gets its own evidence_ref_id while expansion still
            # targets the real run.
            lookup_entity_id = _ci_evidence_identity(fact.entity_id)
            source_version = (
                f"{_STATUS_EVIDENCE_SOURCE_VERSION}:{fact.entity_id}"
                if lookup_entity_id != fact.entity_id
                else _STATUS_EVIDENCE_SOURCE_VERSION
            )
            ref = mint_delivery_evidence(
                source_system="ci_runs",
                entity_type="ci_run",
                entity_id=lookup_entity_id,
                display_label=fact.display_label,
                observed_at=fact.observed_at,
                source_ref_id=fact.source_ref_id,
                freshness_by_ref=freshness_by_ref,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
                source_version=source_version,
            )
            minted[ref.evidence_ref_id] = ref
            return DevCIFact(
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                conclusion=fact.conclusion,
                required=fact.required,
                skipped_required_work=fact.skipped_required_work,
                observed_at=fact.observed_at,
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        def wire_deployment(fact: DeploymentFact) -> DevDeploymentFact:
            ref = mint_delivery_evidence(
                source_system="deployments",
                entity_type="deployment",
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                observed_at=fact.observed_at,
                source_ref_id=fact.source_ref_id,
                freshness_by_ref=freshness_by_ref,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
            )
            minted[ref.evidence_ref_id] = ref
            return DevDeploymentFact(
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                status=fact.status,
                environment=fact.environment,
                required=fact.required,
                observed_at=fact.observed_at,
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        def wire_incident(fact: IncidentFact) -> DevIncidentFact:
            ref = mint_delivery_evidence(
                source_system="incidents",
                entity_type="incident",
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                observed_at=fact.observed_at,
                source_ref_id=fact.source_ref_id,
                freshness_by_ref=freshness_by_ref,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
            )
            minted[ref.evidence_ref_id] = ref
            return DevIncidentFact(
                entity_id=fact.entity_id,
                display_label=fact.display_label,
                status=fact.status,
                active=fact.active,
                blocking=fact.blocking,
                observed_at=fact.observed_at,
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        # CHAOS-3368: build the interim display fact HERE, from the typed
        # ``declared_project_state``/``declared_project_target_date``
        # scalars on ``result`` -- never pre-joined text from the source
        # boundary (Codex adversarial review, HIGH, 2026-08-04). A future
        # deterministic renderer (CHAOS-3377) can consume the same two
        # typed fields directly once it lands, without parsing this
        # string back apart.
        declared_project_fact: StatusFact | None = None
        if (
            result.declared_project_state is not None
            or result.declared_project_target_date is not None
        ) and (
            result.declared_project_observed_at is not None
            and request.scope.entity_refs
        ):
            parts = [
                *(
                    [result.declared_project_state]
                    if result.declared_project_state
                    else []
                ),
                *(
                    ["target date " + result.declared_project_target_date.isoformat()]
                    if result.declared_project_target_date is not None
                    else []
                ),
            ]
            # Routes evidence expansion through native_evidence.py's own
            # "projects" adapter (CHAOS-3368 Codex HIGH fix) -- never the
            # "work_items" one, whose identity match a project's catalog
            # id can never satisfy.
            declared_project_ref_id = next(
                (
                    ref.ref_id
                    for ref in result.source_refs
                    if ref.source_system == "projects"
                ),
                "source:projects-unavailable",
            )
            declared_project_fact = StatusFact(
                entity_type="project",
                entity_id=request.scope.entity_refs[0].entity_id,
                display_label="Declared status",
                status="; ".join(parts),
                observed_at=result.declared_project_observed_at,
                source_ref_id=declared_project_ref_id,
                evidence_ref_ids=(),
            )

        status_facts = [
            wire_status_fact(fact)
            for fact in (
                ([result.declared] if result.declared else [])
                + list(result.children)
                # The project's own declared state/target date, wired
                # through the exact same fact->evidence->status_fact path
                # as declared/children/blockers so it reaches the model's
                # context (and, through it, the §10 answer) identically --
                # no separate rendering path.
                + ([declared_project_fact] if declared_project_fact else [])
                + list(result.blockers)
            )
        ]
        pull_requests = [wire_pull_request(fact) for fact in result.pull_requests]
        ci_checks = [wire_ci(fact) for fact in result.ci]
        deployments = [wire_deployment(fact) for fact in result.deployments]
        incidents = [wire_incident(fact) for fact in result.incidents]
        required_children = [
            wire_required_child(fact) for fact in result.actual.required_children
        ]
        # CHAOS-3377 HIGH 3: blockers get the exact same evidence-minting and
        # truncation treatment as required_children below, so the §10
        # deterministic renderer can build a grounded claim from a blocker
        # exactly as it does from a required child.
        blockers = [wire_required_child(fact) for fact in result.actual.blockers]

        # DevToolResult.evidence caps at 25 entries; a dense scope can mint
        # far more across six independently-bounded categories. Bound the
        # evidence set FIRST, prioritizing the declared fact, every required
        # child, and every fact that produced a blocking reason code -- the
        # evidence the deterministic verdict actually depends on -- then
        # drop cut IDs from every fact that referenced one (entire
        # status_facts if that empties their required evidence, or just the
        # ID from the optional-evidence categories), so the result stays
        # internally consistent instead of the whole tool call failing
        # pydantic's list-length validation, AND so truncation preferentially
        # sheds incidental facts rather than the ones explaining the verdict.
        pre_truncation_categories: Mapping[str, Sequence[Any]] = {
            "status_facts": status_facts,
            "pull_requests": pull_requests,
            "ci_checks": ci_checks,
            "deployments": deployments,
            "incidents": incidents,
        }
        contributing_categories = {
            _REASON_CODE_EVIDENCE_CATEGORY[code]
            for code in result.actual.reason_codes
            if code in _REASON_CODE_EVIDENCE_CATEGORY
        }
        priority_ids = [
            *(status_facts[0].evidence_ref_ids if result.declared else []),
            # CHAOS-3368 (Codex MEDIUM fix): reserved a slot BEFORE
            # required_children -- with the old ordering, >=25 required
            # children alone exhausted the entire ``_MAX_RESULT_EVIDENCE``
            # budget and silently dropped this fact even though it was
            # nominally "prioritized" (just prioritized too late to ever
            # actually be reached). At most one small evidence set, so
            # reserving it unconditionally here can never meaningfully
            # starve required_children's own budget.
            *(
                evidence_id
                for fact in status_facts
                if fact.fact_id.startswith("project:")
                for evidence_id in fact.evidence_ref_ids
            ),
            *(
                evidence_id
                for fact in required_children
                for evidence_id in fact.evidence_ref_ids
            ),
            *(
                evidence_id
                for fact in blockers
                for evidence_id in fact.evidence_ref_ids
            ),
            *(
                evidence_id
                for category in contributing_categories
                for item in pre_truncation_categories[category]
                for evidence_id in item.evidence_ref_ids
            ),
        ]
        kept_evidence_ids, truncated = _bounded_result_evidence(
            minted, priority_ids=priority_ids
        )
        warnings = list(result.warnings)
        if truncated:
            warnings.append("status_snapshot_evidence_result_truncated")

        def _kept(evidence_ref_ids: Sequence[str]) -> list[str]:
            return [item for item in evidence_ref_ids if item in kept_evidence_ids]

        filtered_status_facts = []
        for fact in status_facts:
            kept = _kept(fact.evidence_ref_ids)
            if kept:
                filtered_status_facts.append(
                    fact.model_copy(update={"evidence_ref_ids": kept})
                )
        if len(filtered_status_facts) != len(status_facts):
            warnings.append("status_fact_evidence_truncated")
        status_facts = filtered_status_facts
        pull_requests = [
            fact.model_copy(update={"evidence_ref_ids": _kept(fact.evidence_ref_ids)})
            for fact in pull_requests
        ]
        ci_checks = [
            fact.model_copy(update={"evidence_ref_ids": _kept(fact.evidence_ref_ids)})
            for fact in ci_checks
        ]
        deployments = [
            fact.model_copy(update={"evidence_ref_ids": _kept(fact.evidence_ref_ids)})
            for fact in deployments
        ]
        incidents = [
            fact.model_copy(update={"evidence_ref_ids": _kept(fact.evidence_ref_ids)})
            for fact in incidents
        ]
        required_children = [
            fact.model_copy(update={"evidence_ref_ids": _kept(fact.evidence_ref_ids)})
            for fact in required_children
        ]
        blockers = [
            fact.model_copy(update={"evidence_ref_ids": _kept(fact.evidence_ref_ids)})
            for fact in blockers
        ]

        category_facts: Mapping[str, Sequence[Any]] = {
            "status_facts": status_facts,
            "pull_requests": pull_requests,
            "ci_checks": ci_checks,
            "deployments": deployments,
            "incidents": incidents,
        }
        aggregate_evidence_ids = sorted(
            {
                evidence_id
                for group in category_facts.values()
                for item in group
                for evidence_id in item.evidence_ref_ids
            }
        )[:25]
        # ``contributing_categories`` is unchanged by truncation (it only
        # depends on which reason codes fired); reuse it, but recompute the
        # evidence from the post-truncation ``category_facts`` so a conflict
        # never cites evidence that didn't survive the bound. If truncation
        # cut every fact that actually produced this conflict, leave its
        # evidence empty (permitted -- DevStatusConflict has no minimum)
        # rather than falling back to an unrelated aggregate: an honestly
        # under-evidenced conflict is better than a misleadingly-grounded one.
        conflict_evidence_ids = sorted(
            {
                evidence_id
                for category in contributing_categories
                for item in category_facts[category]
                for evidence_id in item.evidence_ref_ids
            }
        )[:25]
        conflicts = [
            DevStatusConflict(
                code=conflict.code,
                message=conflict.message,
                severity=conflict.severity.value,
                evidence_ref_ids=conflict_evidence_ids,
            )
            for conflict in result.actual.conflicts
        ]
        actual_completion = DevActualCompletion(
            state=result.actual.state.value,
            rule_id=result.actual.rule_id,
            rule_version=result.actual.rule_version,
            reason_codes=list(result.actual.reason_codes)[:25],
            required_children=required_children,
            required_child_total=result.actual.required_child_total,
            required_child_complete=result.actual.required_child_complete,
            display_truncated=result.actual.display_truncated,
            conflicts=conflicts,
            evidence_ref_ids=aggregate_evidence_ids,
            blockers=blockers,
            # CHAOS-3409: threads the domain object's own structural-vs-
            # truncation signal onto the wire, so status_answer_render/
            # status_completion_copy see the SAME distinction the real
            # assessment computed, not a re-derived guess.
            required_children_not_applicable=(
                result.actual.required_children_not_applicable
            ),
        )
        source_health = [
            DevSourceHealth(
                ref_id=ref.ref_id,
                source_system=ref.source_system,
                freshness=ref.freshness,
                watermark=ref.watermark,
            )
            for ref in result.source_refs
        ]
        evidence_by_id.update({key: minted[key] for key in kept_evidence_ids})
        # CHAOS-3368 step 2: the SAME post-truncation evidence the interim
        # ``status_facts`` project entry ended up with (never re-minted --
        # one evidence mint, two consumers: the interim narrated fact above
        # and this typed wire field the CHAOS-3377 deterministic renderer
        # reads instead of parsing that fact's display text).
        declared_project_evidence_ref_ids = next(
            (
                list(fact.evidence_ref_ids)
                for fact in status_facts
                if fact.fact_id.startswith("project:")
            ),
            [],
        )
        return _tool_result(
            request,
            status=_status(result.state.value),
            status_facts=status_facts,
            actual_completion=actual_completion,
            pull_requests=pull_requests,
            ci_checks=ci_checks,
            deployments=deployments,
            incidents=incidents,
            source_health=source_health,
            evidence=[minted[key] for key in sorted(kept_evidence_ids)],
            warnings=warnings,
            declared_project_state=result.declared_project_state,
            declared_project_target_date=result.declared_project_target_date,
            declared_project_evidence_ref_ids=declared_project_evidence_ref_ids,
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
        freshness_by_ref: dict[str, FreshnessState] = {
            ref.ref_id: ref.freshness for ref in result.source_refs
        }
        scope_valid_entity_ids, scope_repository_ids = _scope_evidence_binding(
            request.scope
        )

        def change_freshness(item: ObservedChange) -> FreshnessState:
            for ref_id in item.source_ref_ids:
                freshness = freshness_by_ref.get(ref_id)
                if freshness is not None:
                    return freshness
            return FreshnessState.UNKNOWN

        def change_evidence_identity(item: ObservedChange) -> tuple[str, str, str]:
            """Return (source_system, lookup_entity_id, source_version).

            ``item.entity_id`` is already the adapter-matching locator for
            most categories (status/pull_request/incident all resolve to a
            real row) -- it must reach the minted ref UNCHANGED so expansion
            can still resolve real content. It is NOT event-unique for
            status transitions or per-dimension metric rows, though: two
            observations of the same entity would otherwise collide onto one
            evidence_ref_id. Discriminate those through ``source_version``
            instead of corrupting the lookup entity_id -- the native
            adapters never filter on source_version, only on entity_id/
            scope, so this keeps both collision-freedom AND expandability.
            Relationship changes are sourced from work_graph_edges;
            ``change_id`` there *is* the edge_id the "work_graph" adapter
            indexes, and is already unique per edge.
            """

            category = item.category.value
            source_system = _CHANGE_CATEGORY_SOURCE_SYSTEM.get(category, "work_items")
            if category == "relationship":
                return source_system, item.change_id, _CHANGE_EVIDENCE_SOURCE_VERSION
            if category in _CHANGE_COLLISION_PRONE_CATEGORIES:
                return (
                    source_system,
                    item.entity_id,
                    f"{_CHANGE_EVIDENCE_SOURCE_VERSION}:{item.change_id}",
                )
            return source_system, item.entity_id, _CHANGE_EVIDENCE_SOURCE_VERSION

        minted: dict[str, DevEvidenceRef] = {}
        facts: list[DevStatusFact] = []
        for item in result.changes:
            source_system, lookup_entity_id, source_version = change_evidence_identity(
                item
            )
            ref = _mint_evidence(
                evidence_signer,
                org_id,
                source_system=source_system,
                source_version=source_version,
                entity_type=item.entity_type,
                entity_id=lookup_entity_id,
                display_label=item.display_label,
                observed_at=item.observed_at,
                freshness=change_freshness(item),
                confidence=item.confidence if item.confidence is not None else 1.0,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
            )
            minted[ref.evidence_ref_id] = ref
            facts.append(
                DevStatusFact(
                    fact_id=item.change_id,
                    text=(
                        f"{item.display_label}: {item.before or 'unknown'} -> "
                        f"{item.after or 'unknown'}"
                    ),
                    evidence_ref_ids=[ref.evidence_ref_id],
                )
            )
        source_health = [
            DevSourceHealth(
                ref_id=ref.ref_id,
                source_system=ref.source_system,
                freshness=ref.freshness,
                watermark=ref.watermark,
            )
            for ref in result.source_refs
        ]
        evidence_by_id.update(minted)
        return _tool_result(
            request,
            status=_status(result.state.value),
            status_facts=facts,
            source_health=source_health,
            evidence=list(minted.values()),
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
                scope_request=scope_request_from_scope(request.scope),
                root_refs=roots,
                relationship_types=tuple(sorted(ALLOWED_RELATIONSHIP_TYPES)),
                direction=GraphDirection.BOTH,
                limit=request.limit,
            ),
        )
        scope_valid_entity_ids, scope_repository_ids = _scope_evidence_binding(
            request.scope
        )
        minted: dict[str, DevEvidenceRef] = {}

        def wire_edge(item: WorkGraphNeighborEdge) -> DevGraphEdge:
            ref = _mint_evidence(
                evidence_signer,
                org_id,
                source_system="work_graph",
                source_version=_GRAPH_EVIDENCE_SOURCE_VERSION,
                entity_type="work_graph_edge",
                entity_id=item.edge_id,
                display_label=(
                    f"{item.source_type}:{item.source_id} {item.relationship_type} "
                    f"{item.target_type}:{item.target_id}"
                ),
                observed_at=item.observed_at,
                freshness=FreshnessState.UNKNOWN,
                valid_entity_ids=scope_valid_entity_ids,
                repository_ids=scope_repository_ids,
                confidence=item.confidence,
            )
            minted[ref.evidence_ref_id] = ref
            return DevGraphEdge(
                source_entity_id=item.source_id,
                relationship=item.relationship_type,
                target_entity_id=item.target_id,
                provenance=(item.provenance.strip() or "persisted")[:2_048],
                confidence=max(0.0, min(1.0, item.confidence)),
                observed_at=item.observed_at,
                evidence_ref_ids=[ref.evidence_ref_id],
            )

        edges = [wire_edge(item) for item in result.edges]
        evidence_by_id.update(minted)
        return _tool_result(
            request,
            status=_status(result.state.value),
            graph_edges=edges,
            evidence=list(minted.values()),
            warnings=list(result.warnings),
        )

    async def search_evidence(context, request):
        result = await evidence_service.search(
            org_id=org_id,
            permission_fingerprint=context.permission_fingerprint,
            scope_request=scope_request_from_scope(request.scope),
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
            scope_request=scope_request_from_scope(request.scope),
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
            scope_request=scope_request_from_scope(request.scope),
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

    versions = DevContractVersions(
        prompt_version=PROMPT_VERSION,
        tool_contract_version=TOOL_CONTRACT_VERSION,
        metric_definition_version=_METRIC_REGISTRY_VERSION,
        query_version=_QUERY_BUNDLE_VERSION,
    )
    wave_3_1_enabled = await _wave_3_1_enabled(session, org_id)
    preflight = (
        SubjectPreflight(
            # No classifier is wired yet: the deterministic recognizers are the
            # whole interpreter in production for now, and an unrecognized
            # question degrades to bounded investigation exactly as today. The
            # constrained-model fallback seam is built and tested, but turning
            # it on adds a provider call to every low-confidence question and
            # is a separate rollout decision.
            interpreter=QuestionInterpreter(),
            scope_service=scope_service,
            versions=versions,
        )
        if wave_3_1_enabled
        else None
    )
    # CHAOS-3389 shadow phase: a single env flag flip, gated on wave_3_1
    # (the shadow seam runs alongside the deterministic preflight and is
    # meaningless without it). Off by default -- ``QUAShadowConfig.enabled``
    # defaults to ``False``, so an unset env var is byte-identical to this
    # whole seam not existing (see qua_shadow.py's own module docstring and
    # the RED test proving it).
    #
    # CHAOS-3452: ``provider.qua_shadow_provider`` is a SEPARATE,
    # independently-constructed provider instance, guarded by the isolated
    # shadow quota (``llm/qua_shadow_budget.py``) -- never
    # ``provider.provider``'s object identity, and never touched by
    # ``attach_agent_budget_guard`` (llm/budget.py). Before this, the shadow
    # seam always passed ``provider=None`` because the only instance
    # available was the SAME one ``attach_agent_budget_guard`` monkeypatches
    # for the live investigation call -- a shadow call against it would have
    # consumed the SAME org BYO budget/quota the live call needs (Codex
    # adversarial review round 1 on CHAOS-3389, HIGH, confirmed; see design
    # comment 16bd8fed). ``qua_shadow_provider`` is itself only populated
    # when ``ASK_DEV_QUA_SHADOW_ENABLED=1`` (see
    # ``_resolve_provider_selection``), so an unset flag still costs nothing
    # extra and still resolves through the exact same typed
    # ``SKIPPED_NO_PROVIDER`` path as before. The QUA logic itself is fully
    # exercised end-to-end by tests/api/dev/test_chaos_3389_qua_shadow.py's
    # scripted (never budget-shared) providers.
    qua_shadow = (
        QuestionUnderstandingShadow(
            provider=provider.qua_shadow_provider,
            scope_service=scope_service,
            config=QUAShadowConfig(
                enabled=os.getenv("ASK_DEV_QUA_SHADOW_ENABLED") == "1",
                # CHAOS-3525: a SECOND flag, not a widening of the first.
                # ``ASK_DEV_QUA_SHADOW_ENABLED`` keeps meaning exactly what
                # CHAOS-3389's byte-identity tests certify -- the seam
                # observes and never influences -- and this one is what
                # allows a verified proposal to become the run's subject.
                # Meaningless on its own: with the shadow off there is no
                # proposal to promote, which is why it reads the shadow flag
                # as its own precondition rather than being an independent
                # third state.
                commit_enabled=_qua_commit_flag_enabled(),
            ),
        )
        if wave_3_1_enabled
        else None
    )
    # CHAOS-3295: the plan-governed investigation seam rides the same
    # organization gate as preflight -- a plan can only run once a subject
    # is committed, and only the preflight commits one.
    plan_executor: PlanExecutor | None = None
    graph_investigation_query: GraphInvestigationQuery | None = None
    canonical_enrichment: CanonicalEnrichmentAccessor | None = None
    graph_routing_entitlement: CanonicalGraphRoutingEntitlementAuthorizer | None = None
    if wave_3_1_enabled:
        # Keep the optional graph arm out of production module import time.
        # Organization policy is evaluated first, so a policy-off request
        # never imports or constructs the arm.
        from dev_health_ops.context_fabric.graph_arm.query_service import (
            ProductionGraphInvestigationQuery,
        )

        # The graph-assisted enrichment path and authored-plan path share the
        # exact same canonical service instances. Their construction is
        # side-effect free; organization policy controls their presence, and
        # the independent runtime kill switch controls execution.
        plan_executor_runtime = _ProductionPlanExecutorRuntime(
            status_service=status_service,
            metric_service=metric_service,
            work_graph_service=work_graph_service,
            data_health_service=data_health_service,
            evidence_signer=evidence_signer,
        )
        team_health_service = TeamHealthService(
            plan_executor_runtime, status_change_source
        )
        team_workload_service = TeamWorkloadService(
            plan_executor_runtime, status_change_source, team_workload_source
        )
        operational_deficiency_service = OperationalDeficiencyService(
            plan_executor_runtime, status_change_source, team_workload_source
        )
        canonical_enrichment = CanonicalEnrichmentAccessor(
            status=status_service,
            health=team_health_service,
            workload=team_workload_service,
            readiness=operational_deficiency_service,
            metrics=metric_service,
        )
        graph_investigation_query = ProductionGraphInvestigationQuery()
        fallback_question = _acceptance_graph_fallback_question()
        if fallback_question is not None:
            graph_investigation_query = _AcceptanceGraphFallbackQuery(
                graph_investigation_query, fallback_question
            )
        graph_routing_entitlement = CanonicalGraphRoutingEntitlementAuthorizer(session)
        # CHAOS-3297 stack #3: every CHAOS-3303/3304/3305/3393 service is
        # constructed over the SAME PlanExecutorRuntime instance the six
        # core plans' steps use -- never a second, parallel query path.
        # TeamHealthService/TeamWorkloadService/OperationalDeficiencyService
        # share the SAME status_change_source instance so their internal
        # team_repository_ids lookup shares its (org_id, team_id, as_of)
        # cache with the runtime's own calls (see that instance's own
        # construction comment above).
        project_health_service = ProjectHealthService(plan_executor_runtime)
        plan_executor = PlanExecutor(
            # CHAOS-3296 round 5 (structural inversion, 2026-08-02): passing
            # the exact ``EvidenceReferenceSigner`` every builtin step's
            # ``mint_evidence`` already signs through is what turns on
            # executor-side evidence-provenance checking -- the executor
            # recomputes the real signature itself (see
            # ``investigation_plans.executor._evidence_signature_failures``)
            # rather than trusting a receipt of what this run minted, so no
            # runtime-wrapping step is needed here anymore.
            #
            # build_registry_with_wave_3_1 registers the six core plans'
            # steps PLUS health.project.v1/health.team.v1/
            # balance.team_workload.v1/deficiency.operational.v1/
            # status.portfolio.v1's, all against ONE shared registry,
            # validated together.
            registry=build_registry_with_wave_3_1(
                plan_executor_runtime,
                project_health=project_health_service,
                team_health=team_health_service,
                team_workload=team_workload_service,
                operational_deficiency=operational_deficiency_service,
                # CHAOS-3393: PortfolioStatusService batches over the SAME
                # ProjectHealthService instance the health.project.v1 step
                # above uses -- never a second, parallel query path.
                portfolio_status=PortfolioStatusService(project_health_service),
            ),
            evidence_signer=evidence_signer,
        )

    return BoundedDevRuntime(
        provider=provider.provider,
        provider_source=provider.source.value,
        provider_family=provider.family,
        registry=registry,
        scope_resolver=scope_resolver,
        versions=versions,
        platform_certification_stale=provider.platform_certification_stale,
        preflight=preflight,
        qua_shadow=qua_shadow,
        # CHAOS-3452: closed alongside ``provider`` in ``BoundedDevRuntime.
        # aclose()`` -- it is a genuinely separate provider instance/HTTP
        # client and would otherwise leak a connection whenever the shadow
        # flag is on, even on requests where wave_3_1 (and therefore
        # ``qua_shadow`` itself) ends up ``None``.
        qua_shadow_provider=provider.qua_shadow_provider,
        # CHAOS-3297 stack #3: the combined ten-plan lookup -- orchestrator.py's
        # own plan_registry.get(intent.intent_id) is intent-generic and needs
        # no change to reach the four new plans.
        plan_registry=(
            {**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT}
            if wave_3_1_enabled
            else None
        ),
        plan_executor=plan_executor,
        graph_investigation_query=graph_investigation_query,
        evidence_service=evidence_service if wave_3_1_enabled else None,
        canonical_enrichment=canonical_enrichment,
        graph_routing_entitlement=graph_routing_entitlement,
        graph_authorization_resolver=(
            resolve_graph_authorization if wave_3_1_enabled else None
        ),
    )


async def _wave_3_1_enabled(session: AsyncSession, org_id: str) -> bool:
    """Whether this organization gets the CHAOS-3292 preflight.

    Fail-closed **to today's behaviour**: any error evaluating the flag leaves
    the preflight off, which is the pre-3292 run path with the CHAOS-3289
    backstop still terminating. A rollout gate that failed *open* would ship an
    unreviewed path on a storage error.
    """

    try:
        decision = await evaluate_org_feature_async(
            session, uuid.UUID(org_id), ASK_DEV_WAVE_3_1_FEATURE
        )
    except Exception:
        return False
    return bool(decision.allowed)


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
        scope_request=scope_request_from_scope(scope),
        evidence=evidence,
    )


__all__ = [
    "ProductionProviderResolution",
    "build_production_runtime",
    "expand_production_evidence",
    "resolve_byo_certification_provider",
    "resolve_certification_provider",
    "resolve_platform_certification_provider",
    "resolve_production_provider",
]
_GRAPH_ACCEPTANCE_FALLBACK_ARM = "ASK_DEV_GRAPH_ACCEPTANCE_FALLBACK_ARM"
_GRAPH_ACCEPTANCE_FALLBACK_QUESTION = "ASK_DEV_GRAPH_ACCEPTANCE_FALLBACK_QUESTION"


class _AcceptanceGraphFallbackQuery:
    """Induce one real unavailable outcome only in the acceptance runtime.

    The wrapper sits at the production graph-query boundary: the orchestrator
    still performs normal intent, entitlement, scope, SSE, persistence, and
    native-fallback work.  It does not assert the expected state; it causes
    the graph dependency outcome that the browser gate must independently
    observe.
    """

    def __init__(self, delegate: GraphInvestigationQuery, question: str) -> None:
        self._delegate = delegate
        self._question = question

    async def investigate(self, request: GraphInvestigationRequest) -> GraphQueryResult:
        if request.question_text == self._question:
            return GraphQueryResult(
                outcome=GraphQueryOutcome.UNAVAILABLE,
                diagnostic="acceptance graph dependency unavailable",
            )
        return await self._delegate.investigate(request)


def _acceptance_graph_fallback_question() -> str | None:
    if os.getenv("ENVIRONMENT") != "acceptance":
        return None
    if os.getenv(_GRAPH_ACCEPTANCE_FALLBACK_ARM) != "1":
        return None
    question = os.getenv(_GRAPH_ACCEPTANCE_FALLBACK_QUESTION, "").strip()
    if not question:
        raise RuntimeError(
            f"{_GRAPH_ACCEPTANCE_FALLBACK_QUESTION} is required when "
            f"{_GRAPH_ACCEPTANCE_FALLBACK_ARM}=1"
        )
    return question
