"""Canonical provider-neutral Ask Dev v1 wire contracts.

These Pydantic models are the source of truth for the checked-in Draft 2020-12
schemas under ``contracts/ask-dev/v1``. Runtime services may adapt domain
objects into these models, but must not redeclare their wire shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    StringConstraints,
    field_validator,
    model_validator,
)

SchemaVersion = Annotated[str, StringConstraints(min_length=1, max_length=64)]
OpaqueID = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/#-]{0,127}$",
    ),
]
Label = Annotated[str, StringConstraints(min_length=1, max_length=256)]
ShortText = Annotated[str, StringConstraints(min_length=1, max_length=2_048)]
LongText = Annotated[str, StringConstraints(min_length=1, max_length=16_384)]
Version = Annotated[str, StringConstraints(min_length=1, max_length=128)]
TimezoneName = Annotated[str, StringConstraints(min_length=1, max_length=64)]
RelativePath = Annotated[
    str,
    StringConstraints(min_length=1, max_length=2_048, pattern=r"^/[^\s]*$"),
]


class ContractModel(BaseModel):
    """Strict immutable base for every public contract object."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class DirectScope(StrEnum):
    ORGANIZATION = "organization"
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    #: CHAOS-3301. A team as a direct subject, distinct from the pre-existing
    #: ``team_ids`` *filter* on any other direct scope (see
    #: ``DevScope.validate_direct_scope`` below for the invariant that keeps
    #: the two structurally separate).
    TEAM = "team"


class EntityType(StrEnum):
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    TEAM = "team"


class AskDevSurfaceRouteID(StrEnum):
    DIAGNOSE_OVERVIEW = "diagnose_overview"
    FLOW_METRICS = "flow_metrics"
    INVESTMENT = "investment"
    WORK_GRAPH = "work_graph"
    COMPLEXITY = "complexity"
    COGNITIVE_LOAD = "cognitive_load"
    BOTTLENECKS = "bottlenecks"
    REPOSITORY_DETAIL = "repository_detail"
    PROJECT_DETAIL = "project_detail"
    WORK_UNIT_DETAIL = "work_unit_detail"
    ISSUE_DETAIL = "issue_detail"
    PULL_REQUEST_DETAIL = "pull_request_detail"
    DATA_HEALTH = "data_health"


#: CHAOS-3301: deliberately never gains an ``EntityType.TEAM`` entry. Surface
#: context is a total per-route allowlist of what a *page* may assert as the
#: subject (``_validate_surface_context`` below); admitting TEAM here would
#: let page context set ``direct_scope=team`` directly. A page-supplied team
#: reference is instead re-resolved as question text through the subject
#: preflight, exactly like any other named subject the issue describes.
_SURFACE_ENTITY_TYPES: dict[AskDevSurfaceRouteID, frozenset[EntityType]] = {
    AskDevSurfaceRouteID.DIAGNOSE_OVERVIEW: frozenset({EntityType.REPOSITORY}),
    AskDevSurfaceRouteID.FLOW_METRICS: frozenset({EntityType.REPOSITORY}),
    AskDevSurfaceRouteID.INVESTMENT: frozenset({EntityType.REPOSITORY}),
    AskDevSurfaceRouteID.WORK_GRAPH: frozenset(
        {
            EntityType.REPOSITORY,
            EntityType.PROJECT,
            EntityType.WORK_UNIT,
            EntityType.ISSUE,
            EntityType.PULL_REQUEST,
        }
    ),
    AskDevSurfaceRouteID.COMPLEXITY: frozenset({EntityType.REPOSITORY}),
    AskDevSurfaceRouteID.COGNITIVE_LOAD: frozenset({EntityType.REPOSITORY}),
    AskDevSurfaceRouteID.BOTTLENECKS: frozenset(
        {EntityType.REPOSITORY, EntityType.PROJECT}
    ),
    AskDevSurfaceRouteID.REPOSITORY_DETAIL: frozenset({EntityType.REPOSITORY}),
    AskDevSurfaceRouteID.PROJECT_DETAIL: frozenset({EntityType.PROJECT}),
    AskDevSurfaceRouteID.WORK_UNIT_DETAIL: frozenset({EntityType.WORK_UNIT}),
    AskDevSurfaceRouteID.ISSUE_DETAIL: frozenset({EntityType.ISSUE}),
    AskDevSurfaceRouteID.PULL_REQUEST_DETAIL: frozenset({EntityType.PULL_REQUEST}),
    AskDevSurfaceRouteID.DATA_HEALTH: frozenset({EntityType.REPOSITORY}),
}

_SURFACE_ROUTES_ALLOWING_ORGANIZATION_SCOPE = frozenset(
    {
        AskDevSurfaceRouteID.DIAGNOSE_OVERVIEW,
        AskDevSurfaceRouteID.FLOW_METRICS,
        AskDevSurfaceRouteID.INVESTMENT,
        AskDevSurfaceRouteID.COGNITIVE_LOAD,
        AskDevSurfaceRouteID.BOTTLENECKS,
        AskDevSurfaceRouteID.DATA_HEALTH,
    }
)


class QuestionClass(StrEnum):
    STATUS = "status"
    REMAINING_WORK = "remaining_work"
    OBSERVED_CHANGE = "observed_change"
    REGISTERED_STATISTICS = "registered_statistics"
    DATA_TRUST = "data_trust"
    INVESTIGATION = "investigation"


class AnswerStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    DEGRADED = "degraded"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    REFUSED = "refused"
    ERROR = "error"


class ClaimKind(StrEnum):
    OBSERVED = "observed"
    INFERRED = "inferred"
    RECOMMENDATION = "recommendation"


class ScopeResolutionOutcome(StrEnum):
    EXACT = "exact"
    FILTERED = "filtered"
    INHERITED = "inherited"
    ORGANIZATION_FALLBACK = "organization_fallback"
    AMBIGUOUS = "ambiguous"
    UNRESOLVED = "unresolved"
    FORBIDDEN_OR_NOT_FOUND = "forbidden_or_not_found"


class FreshnessState(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class MetricID(StrEnum):
    ITEMS_COMPLETED = "items_completed"
    CYCLE_TIME_P50_HOURS = "cycle_time_p50_hours"
    AVG_WIP = "avg_wip"
    DEPLOYMENTS_COUNT = "deployments_count"
    CHANGE_FAILURE_RATE = "change_failure_rate"
    INVESTMENT_ALLOCATION_PCT = "investment_allocation_pct"
    CYCLOMATIC_PER_KLOC = "cyclomatic_per_kloc"
    COMPOUNDING_RISK_SCORE = "compounding_risk_score"


class ToolID(StrEnum):
    RESOLVE_SCOPE = "resolve_scope.v1"
    LIST_METRICS = "list_metrics.v1"
    QUERY_METRIC = "query_metric.v1"
    STATUS_SNAPSHOT = "status_snapshot.v1"
    CHANGE_SUMMARY = "change_summary.v1"
    WORK_GRAPH_NEIGHBORS = "work_graph_neighbors.v1"
    SEARCH_EVIDENCE = "search_evidence.v1"
    GET_EVIDENCE = "get_evidence.v1"
    DATA_HEALTH = "data_health.v1"


class DevTimeRange(ContractModel):
    start: AwareDatetime
    end: AwareDatetime
    timezone: TimezoneName

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.end <= self.start:
            raise ValueError("time range end must be after start")
        return self


class DevEntityRef(ContractModel):
    entity_type: EntityType
    entity_id: OpaqueID
    display_label: Label
    repository_id: OpaqueID | None = None


#: The bound v1 places on every scope-shaped identifier list -- DevScope's
#: ``repositories``/``entity_refs``/``team_ids`` and DevScopeResolution's
#: ``authorized_repository_ids``/``authorized_entity_ids``.
#:
#: Named rather than repeated as a literal (CHAOS-3534) because a CALLER has
#: to respect it too, and the only safe way to do that is to read the same
#: constant the fields are declared with. ``DevSubjectSet`` independently
#: allows up to 25 committed refs, so "legal upstream, illegal downstream" is
#: a real and reachable state: a fully-resolved 21-25 repository cohort has
#: no v1 scope representation, and a caller that copies its members into
#: these lists raises instead of publishing. Found by adversarial review
#: after the identical shape had already been found once for entity kinds.
V1_SCOPE_LIST_LIMIT = 20


class DevSurfaceContext(ContractModel):
    route_id: AskDevSurfaceRouteID
    entity_refs: list[DevEntityRef] = Field(
        default_factory=list, max_length=V1_SCOPE_LIST_LIMIT
    )
    filter_fingerprint: OpaqueID | None = None


class DevScope(ContractModel):
    schema_version: Literal["dev_scope.v1"]
    organization_id: OpaqueID
    direct_scope: DirectScope
    repositories: list[OpaqueID] = Field(
        default_factory=list, max_length=V1_SCOPE_LIST_LIMIT
    )
    entity_refs: list[DevEntityRef] = Field(
        default_factory=list, max_length=V1_SCOPE_LIST_LIMIT
    )
    team_ids: list[OpaqueID] = Field(
        default_factory=list, max_length=V1_SCOPE_LIST_LIMIT
    )
    time_range: DevTimeRange
    comparison_range: DevTimeRange | None = None
    surface_context: DevSurfaceContext | None = None

    @field_validator("repositories", "team_ids")
    @classmethod
    def require_unique_ids(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("scope identifiers must be unique")
        return value

    @model_validator(mode="after")
    def validate_direct_scope(self) -> Self:
        if self.comparison_range is not None:
            current_duration = self.time_range.end - self.time_range.start
            comparison_duration = (
                self.comparison_range.end - self.comparison_range.start
            )
            if current_duration != comparison_duration:
                raise ValueError("comparison range must have equivalent duration")
        if self.direct_scope is DirectScope.ORGANIZATION and self.entity_refs:
            raise ValueError("organization scope cannot include a direct entity")
        if self.direct_scope is DirectScope.REPOSITORY and not self.repositories:
            raise ValueError("repository scope requires at least one repository")
        entity_scope = {
            DirectScope.PROJECT: EntityType.PROJECT,
            DirectScope.WORK_UNIT: EntityType.WORK_UNIT,
            DirectScope.ISSUE: EntityType.ISSUE,
            DirectScope.PULL_REQUEST: EntityType.PULL_REQUEST,
            DirectScope.TEAM: EntityType.TEAM,
        }
        expected = entity_scope.get(self.direct_scope)
        if expected is not None:
            if (
                len(self.entity_refs) != 1
                or self.entity_refs[0].entity_type != expected
            ):
                raise ValueError("direct entity scope requires one matching entity")
        if self.direct_scope is DirectScope.TEAM:
            # A team filter can never be read as a team subject: team_ids
            # must name exactly the one committed team, never be empty (the
            # metrics path only applies its team_id filter when team_ids is
            # populated) and never carry any other id. An organization scope
            # carrying team_ids stays a filter, structurally — this branch
            # only fires for DirectScope.TEAM.
            # CHAOS-3338: compared as tuples, not against a list literal.
            # ``DevScopeV2`` (contracts_v2.embedded) re-declares ``team_ids``
            # as a ``tuple[OpaqueID, ...]``, and ``("t",) != ["t"]`` is
            # always True in Python -- so this branch rejected *every* v2
            # team scope, including the one the real producer commits.
            # ``investigation_plans.builtin_steps._wire_metric_content``
            # revalidates the committed scope as a ``DevScopeV2``, so a
            # committed team subject raised there for the whole metrics
            # step. Found by exporting the first team golden into the v2
            # tree, which had no positive team example until now.
            if tuple(self.team_ids) != (self.entity_refs[0].entity_id,):
                raise ValueError(
                    "team direct scope requires team_ids to name exactly that team"
                )
            if self.repositories:
                # CHAOS-3301 review fix: a team direct scope has no
                # repository list of its own -- team-to-repository
                # attribution is re-derived at query time from
                # ``team_repo_ownership``, never carried on the wire. Without
                # this, a foreign ``repositories`` list validated as merely
                # unused and was silently consumed by the status source seam
                # instead of being rejected here.
                raise ValueError("team direct scope cannot carry a repository list")
        self._validate_surface_context()
        return self

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        """Revalidating copy (CHAOS-3301 review fix).

        Pydantic's base ``model_copy`` is a raw field copy that never reruns
        ``validate_direct_scope`` — ``model_copy(update={"repositories": [...]})``
        on a TEAM scope returned and serialized a scope the invariant exists
        to forbid, even though ``__init__`` and ``model_validate`` both reject
        it. Every production caller only patches time fields, so
        round-tripping the update through ``model_validate`` costs nothing
        real and closes the construction path.
        """

        copied = super().model_copy(update=update, deep=deep)
        return type(self).model_validate(copied.model_dump())

    def _validate_surface_context(self) -> None:
        context = self.surface_context
        if context is None:
            return

        refs = context.entity_refs
        if len({(ref.entity_type, ref.entity_id) for ref in refs}) != len(refs):
            raise ValueError("surface context entity references must be unique")
        if not refs:
            if context.route_id not in _SURFACE_ROUTES_ALLOWING_ORGANIZATION_SCOPE:
                raise ValueError("surface context route requires a direct entity")
            if self.direct_scope is not DirectScope.ORGANIZATION:
                raise ValueError("empty surface context requires organization scope")
            return

        allowed_types = _SURFACE_ENTITY_TYPES[context.route_id]
        if any(ref.entity_type not in allowed_types for ref in refs):
            raise ValueError("surface context entity is not approved for this route")

        if all(ref.entity_type is EntityType.REPOSITORY for ref in refs):
            if self.direct_scope is not DirectScope.REPOSITORY or {
                ref.entity_id for ref in refs
            } != set(self.repositories):
                raise ValueError("surface repository context must match direct scope")
            return

        if len(refs) != 1:
            raise ValueError("non-repository surface context must be singular")
        surface_ref = refs[0]
        expected_scope = DirectScope(surface_ref.entity_type.value)
        if (
            self.direct_scope is not expected_scope
            or len(self.entity_refs) != 1
            or self.entity_refs[0].entity_type is not surface_ref.entity_type
            or self.entity_refs[0].entity_id != surface_ref.entity_id
        ):
            raise ValueError("surface entity context must match direct scope")


class DevDisambiguationCandidate(ContractModel):
    entity_ref: DevEntityRef
    repository_id: OpaqueID | None = None
    reason: ShortText


#: Outcomes a ``DevScopeResolution`` may carry ``candidates`` for.
#:
#: ``ambiguous`` REQUIRES them (several authorized entities matched and the
#: caller must pick one). ``forbidden_or_not_found`` MAY carry them (CHAOS-3367):
#: the Wave 3.1 PRD's no-match sentence ends "Here are the closest matches, if
#: any", so the closest-match list needs somewhere to live, and the alternative
#: -- a second, parallel candidate field -- would give two producers of the same
#: thing. Every other outcome still forbids them: an ``exact`` commit with a
#: candidate list beside it is a contradiction, not extra context.
#:
#: The set is deliberately narrow. Widening it is a wire-behaviour change, so
#: it is an edit here rather than a condition inlined in the validator.
CANDIDATE_BEARING_OUTCOMES: frozenset[ScopeResolutionOutcome] = frozenset(
    {
        ScopeResolutionOutcome.AMBIGUOUS,
        ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
    }
)


class DevScopeResolution(ContractModel):
    schema_version: Literal["dev_scope_resolution.v1"]
    requested_scope: DevScope
    resolved_scope: DevScope | None = None
    outcome: ScopeResolutionOutcome
    authorized_repository_ids: list[OpaqueID] = Field(
        default_factory=list, max_length=V1_SCOPE_LIST_LIMIT
    )
    authorized_entity_ids: list[OpaqueID] = Field(
        default_factory=list, max_length=V1_SCOPE_LIST_LIMIT
    )
    candidates: list[DevDisambiguationCandidate] = Field(
        default_factory=list, max_length=25
    )
    fallbacks: list[ShortText] = Field(default_factory=list, max_length=10)
    warnings: list[ShortText] = Field(default_factory=list, max_length=20)
    resolved_at: AwareDatetime

    @model_validator(mode="after")
    def validate_outcome_payload(self) -> Self:
        resolved_outcomes = {
            ScopeResolutionOutcome.EXACT,
            ScopeResolutionOutcome.FILTERED,
            ScopeResolutionOutcome.INHERITED,
            ScopeResolutionOutcome.ORGANIZATION_FALLBACK,
        }
        if self.outcome in resolved_outcomes and self.resolved_scope is None:
            raise ValueError("resolved outcome requires resolved_scope")
        if self.outcome is ScopeResolutionOutcome.AMBIGUOUS and not self.candidates:
            raise ValueError("ambiguous outcome requires candidates")
        if self.outcome not in CANDIDATE_BEARING_OUTCOMES and self.candidates:
            raise ValueError(
                "candidates are allowed only for ambiguous and not-found outcomes"
            )
        if self.outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK:
            if (
                self.resolved_scope is None
                or self.resolved_scope.direct_scope != DirectScope.ORGANIZATION
            ):
                raise ValueError(
                    "organization fallback must resolve to organization scope"
                )
        return self


class DevCapabilityLimits(ContractModel):
    active_runs_per_user: int = Field(default=1, ge=1, le=1)
    active_runs_per_organization: int = Field(default=5, ge=1, le=5)
    requests_per_user_per_15_minutes: int = Field(default=20, ge=1, le=20)
    requests_per_organization_per_hour: int = Field(default=100, ge=1, le=100)
    model_decision_rounds: int = Field(default=4, ge=1, le=4)
    total_tool_calls: int = Field(default=6, ge=1, le=6)
    wall_seconds: int = Field(default=45, ge=1, le=45)


def _default_retention_options() -> list[Literal[0, 30]]:
    return [0, 30]


class DevCapabilities(ContractModel):
    schema_version: Literal["dev_capabilities.v1"]
    ask_dev: bool = False
    byo_llm: bool = False
    agent_context_runtime: bool = False
    can_read: bool = False
    can_manage: bool = False
    effective_provider_label: Label | None = None
    effective_model_label: Label | None = None
    provider_source: Literal["platform", "byo"] | None = None
    readiness: Literal[
        "ready",
        "unsupported_model",
        "missing_credentials",
        "disabled",
        "degraded",
    ] = "disabled"
    supported_contract_versions: list[Version] = Field(
        default_factory=list, max_length=20
    )
    retention_options: list[Literal[0, 30]] = Field(
        default_factory=_default_retention_options, min_length=2, max_length=2
    )
    request_limits: DevCapabilityLimits = Field(default_factory=DevCapabilityLimits)
    supported_question_classes: list[QuestionClass] = Field(
        default_factory=lambda: list(QuestionClass), min_length=1, max_length=6
    )
    contextual_entrypoints: bool = False
    evidence_resolver: bool = False
    administrator_safe_failure_reason: ShortText | None = None

    @model_validator(mode="after")
    def validate_capability_sets(self) -> Self:
        if self.retention_options != [0, 30]:
            raise ValueError("retention options must be exactly 0 and 30 days")
        if len(self.supported_question_classes) != len(
            set(self.supported_question_classes)
        ):
            raise ValueError("supported question classes must be unique")
        return self


class DevConversation(ContractModel):
    schema_version: Literal["dev_conversation.v1"]
    conversation_id: OpaqueID
    title: Annotated[str, StringConstraints(min_length=1, max_length=160)] | None = None
    current_scope: DevScope
    retention_days: Literal[0, 30]
    state: Literal["active", "deleted", "expired"]
    message_count: int = Field(ge=0, le=10_000)
    latest_answer_id: OpaqueID | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime
    expires_at: AwareDatetime | None = None


class DevConversationSummary(ContractModel):
    schema_version: Literal["dev_conversation_summary.v1"]
    conversation_id: OpaqueID
    title: Annotated[str, StringConstraints(min_length=1, max_length=160)] | None = None
    direct_scope: DirectScope
    state: Literal["active", "deleted", "expired"]
    message_count: int = Field(ge=0, le=10_000)
    updated_at: AwareDatetime
    expires_at: AwareDatetime | None = None


class DevMessageRequest(ContractModel):
    schema_version: Literal["dev_message_request.v1"]
    request_id: OpaqueID
    client_message_id: OpaqueID
    conversation_id: OpaqueID | None = None
    retry_of_run_id: OpaqueID | None = None
    question: Annotated[
        str,
        StringConstraints(min_length=1, max_length=8_192),
    ] = Field(json_schema_extra={"x-max-utf8-bytes": 8_192})
    question_class: QuestionClass
    scope: DevScope
    requested_metric_ids: list[MetricID] = Field(default_factory=list, max_length=8)

    @field_validator("question")
    @classmethod
    def enforce_utf8_question_bound(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 8_192:
            raise ValueError("question exceeds 8 KiB UTF-8")
        return value


class DevCitationLink(ContractModel):
    internal_path: RelativePath | None = None
    source_url: (
        Annotated[str, StringConstraints(min_length=1, max_length=2_048)] | None
    ) = None

    @model_validator(mode="after")
    def require_link(self) -> Self:
        if self.internal_path is None and self.source_url is None:
            raise ValueError("citation link requires an internal path or source URL")
        return self


class DevEvidenceFlags(ContractModel):
    stale: bool = False
    unavailable: bool = False
    redacted: bool = False
    deleted: bool = False
    uncertain: bool = False
    conflicting: bool = False
    untrusted_content: bool = True


class DevEvidenceRef(ContractModel):
    schema_version: Literal["dev_evidence_ref.v1"]
    evidence_ref_id: OpaqueID
    source_system: OpaqueID
    source_version: Version
    entity_type: OpaqueID
    entity_id: OpaqueID
    display_label: Label
    link: DevCitationLink | None = None
    observed_at: AwareDatetime
    freshness: FreshnessState
    provenance: ShortText
    confidence: FiniteFloat = Field(ge=0, le=1)
    citation_text: (
        Annotated[str, StringConstraints(min_length=1, max_length=2_048)] | None
    ) = None
    repository_ids: list[OpaqueID] = Field(default_factory=list, max_length=20)
    valid_entity_ids: list[OpaqueID] = Field(default_factory=list, max_length=20)
    flags: DevEvidenceFlags


class DevEvidenceExpansion(ContractModel):
    schema_version: Literal["dev_evidence_expansion.v1"]
    evidence: DevEvidenceRef
    state: Literal[
        "available",
        "no_matches",
        "unavailable",
        "unconfigured",
        "redacted",
        "stale",
    ]
    safe_excerpt: (
        Annotated[str, StringConstraints(min_length=1, max_length=65_536)] | None
    ) = Field(default=None, json_schema_extra={"x-max-utf8-bytes": 65_536})
    serialized_bytes: int = Field(ge=0, le=65_536)
    warning: ShortText | None = None
    query_version: Version

    @model_validator(mode="after")
    def enforce_excerpt_byte_count(self) -> Self:
        excerpt_bytes = len((self.safe_excerpt or "").encode("utf-8"))
        if excerpt_bytes > 65_536:
            raise ValueError("safe_excerpt exceeds 64 KiB UTF-8")
        if self.serialized_bytes != excerpt_bytes:
            raise ValueError("serialized_bytes must equal the safe excerpt size")
        return self


class DevMetricPoint(ContractModel):
    timestamp: AwareDatetime
    value: FiniteFloat


class DevMetricDefinition(ContractModel):
    metric_id: MetricID
    label: Label
    description: ShortText
    unit: OpaqueID
    supported_dimensions: list[OpaqueID] = Field(default_factory=list, max_length=12)
    supported_time_grains: list[OpaqueID] = Field(default_factory=list, max_length=12)
    supported_scopes: list[DirectScope] = Field(min_length=1, max_length=8)
    definition_version: Version
    freshness_policy: ShortText


class DevMetricRef(ContractModel):
    schema_version: Literal["dev_metric_ref.v1"]
    metric_ref_id: OpaqueID
    metric_id: MetricID
    label: Label
    definition_version: Version
    unit: OpaqueID
    aggregation: OpaqueID
    display_precision: int = Field(ge=0, le=8)
    resolved_scope: DevScope
    dimensions: list[Label] = Field(default_factory=list, max_length=12)
    current_window: DevTimeRange
    comparison_window: DevTimeRange | None = None
    value: FiniteFloat | None = None
    comparison_value: FiniteFloat | None = None
    series: list[DevMetricPoint] = Field(default_factory=list, max_length=366)
    query_version: Version
    source_version: Version
    freshness: FreshnessState
    coverage: FiniteFloat = Field(ge=0, le=1)
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)

    @model_validator(mode="after")
    def require_metric_value(self) -> Self:
        if self.value is None and not self.series:
            raise ValueError("metric requires a value or bounded series")
        return self


class DevClaimFlags(ContractModel):
    stale: bool = False
    uncertain: bool = False
    conflicting: bool = False
    untrusted_source: bool = False


class DevClaim(ContractModel):
    schema_version: Literal["dev_claim.v1"]
    claim_id: OpaqueID
    kind: ClaimKind
    text: LongText
    confidence: FiniteFloat = Field(ge=0, le=1)
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)
    metric_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=12)
    validity_scope: DevScope
    flags: DevClaimFlags
    recommendation_rule_version: Version | None = None

    @model_validator(mode="after")
    def validate_grounding(self) -> Self:
        if self.kind is ClaimKind.OBSERVED and not (
            self.evidence_ref_ids or self.metric_ref_ids
        ):
            raise ValueError("observed claims require evidence or metric references")
        if self.kind is ClaimKind.INFERRED and self.confidence >= 1:
            raise ValueError("inferred claims cannot use maximum certainty")
        if (
            self.kind is ClaimKind.RECOMMENDATION
            and self.recommendation_rule_version is None
        ):
            raise ValueError("recommendations require a rule version")
        if (
            self.kind is not ClaimKind.RECOMMENDATION
            and self.recommendation_rule_version is not None
        ):
            raise ValueError("only recommendations may carry a rule version")
        return self


class DevConflict(ContractModel):
    summary: ShortText
    evidence_ref_ids: list[OpaqueID] = Field(min_length=2, max_length=10)


class DevCoverage(ContractModel):
    required_source_count: int = Field(ge=0, le=100)
    available_source_count: int = Field(ge=0, le=100)
    unavailable_required_sources: list[OpaqueID] = Field(
        default_factory=list, max_length=25
    )
    stale_required_sources: list[OpaqueID] = Field(default_factory=list, max_length=25)
    #: Required sources that returned real data whose completeness cannot be
    #: established -- distinct from stale, which asserts the data is old
    #: (CHAOS-3334 codex review). A degraded status snapshot means one of its
    #: contributing sources was *unavailable*, and a metric reporting
    #: insufficient evidence can carry ``FreshnessState.FRESH``; filing either
    #: under ``stale_required_sources`` tells a client the wrong failure
    #: cause. Blocks a ``complete`` answer exactly as the other two lists do.
    degraded_required_sources: list[OpaqueID] = Field(
        default_factory=list, max_length=25
    )
    as_of: AwareDatetime

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.available_source_count > self.required_source_count:
            raise ValueError(
                "available source count cannot exceed required source count"
            )
        return self


class DevContractVersions(ContractModel):
    prompt_version: Version
    tool_contract_version: Version
    metric_definition_version: Version
    query_version: Version


class DevModelMetadata(ContractModel):
    provider_source: Literal["platform", "byo"]
    provider_family: OpaqueID
    model_fingerprint: OpaqueID


class DevAnswer(ContractModel):
    schema_version: Literal["dev_answer.v1"]
    answer_id: OpaqueID
    conversation_id: OpaqueID
    generated_at: AwareDatetime
    resolved_scope: DevScopeResolution
    as_of: AwareDatetime
    status: AnswerStatus
    direct_summary: LongText
    claims: list[DevClaim] = Field(default_factory=list, max_length=100)
    metrics: list[DevMetricRef] = Field(default_factory=list, max_length=12)
    evidence: list[DevEvidenceRef] = Field(default_factory=list, max_length=25)
    conflicts: list[DevConflict] = Field(default_factory=list, max_length=20)
    coverage: DevCoverage
    warnings: list[ShortText] = Field(default_factory=list, max_length=20)
    suggested_follow_up_questions: list[ShortText] = Field(
        default_factory=list, max_length=10
    )
    versions: DevContractVersions
    model: DevModelMetadata

    @model_validator(mode="after")
    def validate_answer_invariants(self) -> Self:
        evidence_ids = [item.evidence_ref_id for item in self.evidence]
        metric_ids = [item.metric_ref_id for item in self.metrics]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("evidence reference IDs must be unique")
        if len(metric_ids) != len(set(metric_ids)):
            raise ValueError("metric reference IDs must be unique")
        known_evidence = set(evidence_ids)
        known_metrics = set(metric_ids)
        for claim in self.claims:
            if not set(claim.evidence_ref_ids) <= known_evidence:
                raise ValueError("claim references unknown evidence IDs")
            if not set(claim.metric_ref_ids) <= known_metrics:
                raise ValueError("claim references unknown metric IDs")
        for conflict in self.conflicts:
            if not set(conflict.evidence_ref_ids) <= known_evidence:
                raise ValueError("conflict references unknown evidence IDs")
        for metric in self.metrics:
            if not set(metric.evidence_ref_ids) <= known_evidence:
                raise ValueError("metric references unknown evidence IDs")
        if self.status is AnswerStatus.COMPLETE and (
            self.coverage.available_source_count != self.coverage.required_source_count
            or self.coverage.unavailable_required_sources
            or self.coverage.stale_required_sources
            or self.coverage.degraded_required_sources
        ):
            raise ValueError(
                "complete answer requires all required sources fresh and available"
            )
        return self


DevTranscriptRunState = Literal[
    "accepted",
    "resolving_scope",
    # CHAOS-3292 preflight phases. A transcript can be fetched while a run is
    # still in flight, so omitting these turned an in-progress run into a
    # server error at transcript validation rather than a rejected state.
    "interpreting",
    "resolving_subjects",
    "model_decision",
    "tool_validation",
    "tool_execution",
    "answer_validation",
    "completed",
    "insufficient_evidence",
    "refused",
    "failed",
    "cancelled",
]


class DevTranscriptEntry(ContractModel):
    """One safe persisted turn artifact in the canonical conversation history."""

    schema_version: Literal["dev_transcript_entry.v1"]
    message_id: OpaqueID
    role: Literal["user", "assistant"]
    created_at: AwareDatetime
    run_id: OpaqueID
    retry_of_run_id: OpaqueID | None = None
    run_state: DevTranscriptRunState
    question: (
        Annotated[
            str,
            StringConstraints(min_length=1, max_length=8_192),
        ]
        | None
    ) = Field(default=None, json_schema_extra={"x-max-utf8-bytes": 8_192})
    scope: DevScope | None = None
    answer: DevAnswer | None = None

    @model_validator(mode="after")
    def validate_role_payload(self) -> Self:
        if self.role == "user":
            if self.question is None or self.scope is None or self.answer is not None:
                raise ValueError(
                    "user transcript entries require question and scope only"
                )
            if len(self.question.encode("utf-8")) > 8_192:
                raise ValueError("question exceeds 8 KiB UTF-8")
        elif self.question is not None or self.scope is not None or self.answer is None:
            raise ValueError("assistant transcript entries require answer only")
        return self


class DevConversationTranscript(ContractModel):
    """A bounded page from one retained, owned canonical conversation."""

    schema_version: Literal["dev_conversation_transcript.v1"]
    conversation_id: OpaqueID
    items: list[DevTranscriptEntry] = Field(default_factory=list, max_length=100)
    next_cursor: (
        Annotated[str, StringConstraints(min_length=1, max_length=512)] | None
    ) = None

    @model_validator(mode="after")
    def validate_page(self) -> Self:
        message_ids = [item.message_id for item in self.items]
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("transcript message IDs must be unique")
        ordering = [(item.created_at, item.message_id) for item in self.items]
        if ordering != sorted(ordering):
            raise ValueError("transcript entries must be chronological")
        for item in self.items:
            if (
                item.answer is not None
                and item.answer.conversation_id != self.conversation_id
            ):
                raise ValueError("transcript answer belongs to another conversation")
        return self


class DevStatusFact(ContractModel):
    fact_id: OpaqueID
    text: ShortText
    evidence_ref_ids: list[OpaqueID] = Field(min_length=1, max_length=25)


class DevPullRequestFact(ContractModel):
    entity_id: OpaqueID
    display_label: Label
    state: OpaqueID
    review_state: OpaqueID | None = None
    changes_requested: int = Field(ge=0, le=1_000)
    merged: bool
    required: bool
    observed_at: AwareDatetime
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevCIFact(ContractModel):
    entity_id: OpaqueID
    display_label: Label
    conclusion: OpaqueID
    required: bool | None = None
    skipped_required_work: bool | None = None
    observed_at: AwareDatetime
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevDeploymentFact(ContractModel):
    entity_id: OpaqueID
    display_label: Label
    status: OpaqueID
    environment: OpaqueID | None = None
    required: bool
    observed_at: AwareDatetime
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevIncidentFact(ContractModel):
    entity_id: OpaqueID
    display_label: Label
    status: OpaqueID
    active: bool
    blocking: bool
    observed_at: AwareDatetime
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevStatusConflict(ContractModel):
    code: OpaqueID
    message: ShortText
    severity: Literal["warning", "blocking"]
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevRequiredChildFact(ContractModel):
    fact_id: OpaqueID
    text: ShortText
    status: OpaqueID
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevActualCompletion(ContractModel):
    """Server-computed ``actual-completion`` rule result; the LLM explains, never derives, it."""

    state: Literal["ready", "not_ready", "indeterminate"]
    rule_id: OpaqueID
    rule_version: Version
    reason_codes: list[OpaqueID] = Field(default_factory=list, max_length=25)
    required_children: list[DevRequiredChildFact] = Field(
        default_factory=list, max_length=100
    )
    # The complete denominator/numerator from the server's UNBOUNDED
    # required-work assessment set (CHAOS-3297 stack #2) -- never derived
    # from ``len(required_children)``, which is truncated to the display
    # bound above and would silently undercount once a parent has more
    # than 100 required children.
    #
    # Both are ``None`` together (round 2, codex HIGH) whenever the
    # required-child *source* itself was truncated: an honestly unknown
    # total is not the same claim as a complete one that happens to equal
    # its numerator, and the wire contract must not let a caller mistake
    # one for the other.
    required_child_total: int | None = Field(default=None, ge=0, le=100_000)
    required_child_complete: int | None = Field(default=None, ge=0, le=100_000)
    display_truncated: bool = False
    conflicts: list[DevStatusConflict] = Field(default_factory=list, max_length=20)
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)
    # CHAOS-3377 HIGH 3: blockers (``open_blocker``) previously had no typed
    # wire representation distinct from ``required_children`` -- a NOT_READY
    # verdict caused solely by an open blocker rendered no blocker detail at
    # all. Reuses ``DevRequiredChildFact``'s shape (structurally identical:
    # fact_id/text/status/evidence_ref_ids); mirrors ``required_children``
    # by carrying ALL blockers, not a pre-filtered "open" subset, so a
    # consumer applies its own openness predicate rather than trusting one
    # it cannot verify.
    blockers: list[DevRequiredChildFact] = Field(default_factory=list, max_length=100)
    # CHAOS-3409 codex adversarial review (HIGH): a withheld
    # (``required_child_total is None``) denominator has two structurally
    # distinct causes -- a genuine source truncation (real required items
    # exist, not all were fetched) and CHAOS-3408's structural non-
    # applicability (ORGANIZATION/TEAM scope has no required-child concept
    # at all) -- and reason_codes cannot safely carry that distinction: any
    # new code added there becomes an "unknown reason" that forces
    # ``state`` to INDETERMINATE (status_change_service._assess), which is
    # correct for a real truncation but WRONG for a structural absence
    # (the state/reason codes ARE fully real there; only the required-
    # child count is inapplicable). This is therefore its own, inert
    # field: never read by ``_assess``'s state computation, only by
    # ``is_completion_assessment_untrustworthy``/``render_verdict_summary``
    # to choose the correct copy without conflating the two.
    required_children_not_applicable: bool = False

    @model_validator(mode="after")
    def validate_required_child_counts(self) -> Self:
        if (self.required_child_total is None) != (
            self.required_child_complete is None
        ):
            raise ValueError(
                "required_child_total and required_child_complete must both be "
                "known or both be withheld"
            )
        if self.required_child_total is None or self.required_child_complete is None:
            return self
        if self.required_child_complete > self.required_child_total:
            raise ValueError(
                "required_child_complete cannot exceed required_child_total"
            )
        if len(self.required_children) > self.required_child_total:
            raise ValueError(
                "displayed required_children cannot exceed required_child_total"
            )
        return self


class DevSourceHealth(ContractModel):
    ref_id: OpaqueID
    source_system: OpaqueID
    freshness: FreshnessState
    watermark: AwareDatetime | None = None


class DevGraphEdge(ContractModel):
    source_entity_id: OpaqueID
    relationship: OpaqueID
    target_entity_id: OpaqueID
    provenance: ShortText
    confidence: FiniteFloat = Field(ge=0, le=1)
    observed_at: AwareDatetime
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevDataHealth(ContractModel):
    source_system: OpaqueID
    freshness: FreshnessState
    last_successful_at: AwareDatetime | None = None
    coverage: FiniteFloat = Field(ge=0, le=1)
    warning: ShortText | None = None


class DevToolRequest(ContractModel):
    schema_version: Literal["dev_tool_request.v1"]
    run_id: OpaqueID
    tool_call_id: OpaqueID
    tool_id: ToolID
    scope: DevScope
    query: Annotated[str, StringConstraints(min_length=1, max_length=2_048)] | None = (
        None
    )
    metric_id: MetricID | None = None
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)
    include_comparison: bool = False
    limit: int = Field(default=25, ge=1, le=25)


class DevToolResult(ContractModel):
    schema_version: Literal["dev_tool_result.v1"]
    run_id: OpaqueID
    tool_call_id: OpaqueID
    tool_id: ToolID
    status: Literal["success", "partial", "unavailable", "error"]
    scope_resolution: DevScopeResolution | None = None
    metric_definitions: list[DevMetricDefinition] = Field(
        default_factory=list, max_length=12
    )
    metrics: list[DevMetricRef] = Field(default_factory=list, max_length=12)
    evidence: list[DevEvidenceRef] = Field(default_factory=list, max_length=25)
    status_facts: list[DevStatusFact] = Field(default_factory=list, max_length=100)
    actual_completion: DevActualCompletion | None = None
    pull_requests: list[DevPullRequestFact] = Field(
        default_factory=list, max_length=100
    )
    ci_checks: list[DevCIFact] = Field(default_factory=list, max_length=100)
    deployments: list[DevDeploymentFact] = Field(default_factory=list, max_length=100)
    incidents: list[DevIncidentFact] = Field(default_factory=list, max_length=100)
    source_health: list[DevSourceHealth] = Field(default_factory=list, max_length=25)
    graph_edges: list[DevGraphEdge] = Field(default_factory=list, max_length=100)
    data_health: list[DevDataHealth] = Field(default_factory=list, max_length=25)
    warnings: list[ShortText] = Field(default_factory=list, max_length=20)
    error: DevError | None = None
    serialized_bytes: int = Field(ge=0, le=65_536)
    # CHAOS-3368 step 2: the project's own DECLARED lifecycle state / target
    # date (projects.state/target_date, migration 073), typed -- NEVER
    # pre-joined display prose -- so the CHAOS-3377 deterministic §10
    # renderer (status_answer_render.py) can consume it directly instead of
    # parsing the interim status_facts display text back apart.
    # ``declared_project_state`` is the RAW provider token (e.g. Linear's
    # own "started"/"paused"/"completed"), exactly like ``DevCIFact.
    # conclusion``/``DevPullRequestFact.state`` elsewhere in this contract --
    # translation through a closed-vocabulary table happens at render time,
    # never here, so a future provider's own vocabulary needs no wire change.
    # ``None``/``None`` when the scope is not PROJECT, the catalog row could
    # not be resolved unambiguously, or the provider populated neither
    # column.
    declared_project_state: OpaqueID | None = None
    declared_project_target_date: date | None = None
    declared_project_evidence_ref_ids: list[OpaqueID] = Field(
        default_factory=list, max_length=25
    )

    @model_validator(mode="after")
    def validate_error_state(self) -> Self:
        if self.status == "error" and self.error is None:
            raise ValueError("error tool result requires an error envelope")
        if self.status != "error" and self.error is not None:
            raise ValueError("only error tool results may include an error envelope")
        return self

    @model_validator(mode="after")
    def validate_evidence_closure(self) -> Self:
        """Every evidence ID referenced by a fact/edge must be in ``evidence``.

        This is the wire-level guarantee behind CHAOS-3259: a status, change, or
        graph fact can only cite evidence the model can also expand through
        ``get_evidence.v1`` in the same tool result.
        """

        known = {item.evidence_ref_id for item in self.evidence}
        referenced: set[str] = set()
        for fact in self.status_facts:
            referenced.update(fact.evidence_ref_ids)
        for edge in self.graph_edges:
            referenced.update(edge.evidence_ref_ids)
        for pr in self.pull_requests:
            referenced.update(pr.evidence_ref_ids)
        for ci in self.ci_checks:
            referenced.update(ci.evidence_ref_ids)
        for deployment in self.deployments:
            referenced.update(deployment.evidence_ref_ids)
        for incident in self.incidents:
            referenced.update(incident.evidence_ref_ids)
        if self.actual_completion is not None:
            referenced.update(self.actual_completion.evidence_ref_ids)
            for child in self.actual_completion.required_children:
                referenced.update(child.evidence_ref_ids)
            for blocker in self.actual_completion.blockers:
                referenced.update(blocker.evidence_ref_ids)
            for conflict in self.actual_completion.conflicts:
                referenced.update(conflict.evidence_ref_ids)
        referenced.update(self.declared_project_evidence_ref_ids)
        if not referenced <= known:
            raise ValueError(
                "tool result references evidence IDs missing from its evidence array"
            )
        return self


class DevFeedback(ContractModel):
    schema_version: Literal["dev_feedback.v1"]
    feedback_id: OpaqueID
    answer_id: OpaqueID
    rating: Literal["helpful", "not_helpful"]
    reasons: list[
        Literal[
            "incorrect",
            "missing_evidence",
            "wrong_scope",
            "stale_data",
            "unclear",
            "useful",
        ]
    ] = Field(min_length=1, max_length=6)
    comment: (
        Annotated[str, StringConstraints(min_length=1, max_length=2_048)] | None
    ) = Field(
        default=None,
        json_schema_extra={"x-max-utf8-bytes": 2_048},
    )
    created_at: AwareDatetime

    @field_validator("comment")
    @classmethod
    def enforce_utf8_comment_bound(cls, value: str | None) -> str | None:
        if value is not None and len(value.encode("utf-8")) > 2_048:
            raise ValueError("feedback comment exceeds 2 KiB UTF-8")
        return value


class DevError(ContractModel):
    schema_version: Literal["dev_error.v1"]
    request_id: OpaqueID
    code: Literal[
        "unauthenticated",
        "forbidden",
        "feature_not_enabled",
        "byo_llm_not_enabled",
        "provider_not_configured",
        "model_not_supported",
        "provider_unavailable",
        "rate_limited",
        "concurrency_limited",
        "cost_limit_reached",
        "invalid_request",
        "scope_ambiguous",
        "scope_not_found",
        "scope_forbidden",
        "conversation_not_found",
        "conversation_expired",
        "tool_limit_reached",
        "tool_unavailable",
        "source_unavailable",
        "insufficient_evidence",
        "answer_validation_failed",
        "cancelled",
        "provider_contract_violation",
        "internal_error",
        # CHAOS-3541: a distinct wire code for a genuinely prohibited
        # request (arbitrary execution, a write) -- never
        # "insufficient_evidence", which would mislabel a categorical
        # refusal as an evidence gap the requester could resolve by asking
        # differently. See terminal_frames.PUBLIC_OUTCOME_BY_ERROR_CODE and
        # contracts_v2.base.PublicOutcome.REFUSED.
        "refused",
    ]
    safe_message: ShortText
    retryable: bool
    limit_reset_at: AwareDatetime | None = None
    remediation: list[ShortText] = Field(default_factory=list, max_length=5)


# Safe, stable remediation text keyed by DevError.code, shared by the live
# orchestrator error projection (DevOrchestrator._provider_error) and the
# idempotent-replay projection (router._replayed_result) so a client sees the
# same guidance whether it observes a fresh run or a replayed one (CHAOS-3254).
_DEV_ERROR_REMEDIATION: dict[str, tuple[str, ...]] = {
    "provider_contract_violation": (
        "Confirm the configured OpenAI-compatible endpoint honors "
        "parallel_tool_calls=false and returns exactly one tool call per "
        "decision.",
        "If this is a self-hosted or third-party OpenAI-compatible server, "
        "verify it supports sequential single-tool-call decisions before "
        "certifying it for Ask Dev.",
    ),
}


def dev_error_remediation(code: str) -> list[str]:
    """Return the safe remediation list for a DevError code, or none."""
    return list(_DEV_ERROR_REMEDIATION.get(code, ()))


class StreamEventType(StrEnum):
    RUN_STARTED = "run.started"
    SCOPE_RESOLVED = "scope.resolved"
    PROGRESS = "progress"
    ANSWER_DELTA = "answer.delta"
    ANSWER_COMPLETED = "answer.completed"
    WARNING = "warning"
    ERROR = "error"
    DONE = "done"


class ProgressState(StrEnum):
    RESOLVING_SCOPE = "resolving_scope"
    CHECKING_STATUS = "checking_status"
    QUERYING_METRICS = "querying_metrics"
    CHECKING_DEPENDENCIES = "checking_dependencies"
    CHECKING_EVIDENCE = "checking_evidence"
    CHECKING_DATA_FRESHNESS = "checking_data_freshness"
    PREPARING_ANSWER = "preparing_answer"


class DevStreamEvent(ContractModel):
    schema_version: Literal["dev_stream_event.v1"]
    run_id: OpaqueID
    sequence: int = Field(ge=0, le=100_000)
    event: StreamEventType
    occurred_at: AwareDatetime
    progress: ProgressState | None = None
    scope_resolution: DevScopeResolution | None = None
    delta: Annotated[str, StringConstraints(min_length=1, max_length=8_192)] | None = (
        None
    )
    answer: DevAnswer | None = None
    warning: ShortText | None = None
    error: DevError | None = None
    terminal_kind: Literal["answer", "error"] | None = None

    @model_validator(mode="after")
    def validate_event_payload(self) -> Self:
        required_payload = {
            StreamEventType.SCOPE_RESOLVED: ("scope_resolution", self.scope_resolution),
            StreamEventType.PROGRESS: ("progress", self.progress),
            StreamEventType.ANSWER_DELTA: ("delta", self.delta),
            StreamEventType.ANSWER_COMPLETED: ("answer", self.answer),
            StreamEventType.WARNING: ("warning", self.warning),
            StreamEventType.ERROR: ("error", self.error),
            StreamEventType.DONE: ("terminal_kind", self.terminal_kind),
        }
        required = required_payload.get(self.event)
        if required is not None and required[1] is None:
            raise ValueError(f"{self.event} requires {required[0]}")
        payloads = {
            "progress": self.progress,
            "scope_resolution": self.scope_resolution,
            "delta": self.delta,
            "answer": self.answer,
            "warning": self.warning,
            "error": self.error,
            "terminal_kind": self.terminal_kind,
        }
        allowed = {
            StreamEventType.RUN_STARTED: set(),
            StreamEventType.SCOPE_RESOLVED: {"scope_resolution"},
            StreamEventType.PROGRESS: {"progress"},
            StreamEventType.ANSWER_DELTA: {"delta"},
            StreamEventType.ANSWER_COMPLETED: {"answer"},
            StreamEventType.WARNING: {"warning"},
            StreamEventType.ERROR: {"error"},
            StreamEventType.DONE: {"terminal_kind"},
        }[self.event]
        unexpected = {
            name for name, value in payloads.items() if value is not None
        } - allowed
        if unexpected:
            raise ValueError(
                f"unexpected payloads for {self.event}: {sorted(unexpected)}"
            )
        return self


def validate_stream(events: list[DevStreamEvent]) -> None:
    """Validate one bounded stream: ordered events, one terminal, then done."""

    if not events:
        raise ValueError("stream must not be empty")
    if len(events) > 100_000:
        raise ValueError("stream exceeds event bound")
    run_ids = {event.run_id for event in events}
    if len(run_ids) != 1:
        raise ValueError("stream events must share one run ID")
    if [event.sequence for event in events] != list(range(len(events))):
        raise ValueError("stream sequence must be contiguous and ordered")
    if events[0].event is not StreamEventType.RUN_STARTED:
        raise ValueError("stream must start with run.started")
    terminal_indexes = [
        index
        for index, event in enumerate(events)
        if event.event in {StreamEventType.ANSWER_COMPLETED, StreamEventType.ERROR}
    ]
    if len(terminal_indexes) != 1:
        raise ValueError("stream must contain exactly one terminal result")
    terminal_index = terminal_indexes[0]
    if (
        terminal_index != len(events) - 2
        or events[-1].event is not StreamEventType.DONE
    ):
        raise ValueError("terminal result must be immediately followed by done")
    terminal_kind = (
        "answer"
        if events[terminal_index].event is StreamEventType.ANSWER_COMPLETED
        else "error"
    )
    if events[-1].terminal_kind != terminal_kind:
        raise ValueError("done terminal_kind must match the terminal result")


CONTRACT_MODELS: dict[str, type[ContractModel]] = {
    "dev_capabilities.v1": DevCapabilities,
    "dev_conversation.v1": DevConversation,
    "dev_conversation_summary.v1": DevConversationSummary,
    "dev_conversation_transcript.v1": DevConversationTranscript,
    "dev_message_request.v1": DevMessageRequest,
    "dev_answer.v1": DevAnswer,
    "dev_claim.v1": DevClaim,
    "dev_metric_ref.v1": DevMetricRef,
    "dev_evidence_ref.v1": DevEvidenceRef,
    "dev_evidence_expansion.v1": DevEvidenceExpansion,
    "dev_scope.v1": DevScope,
    "dev_scope_resolution.v1": DevScopeResolution,
    "dev_tool_request.v1": DevToolRequest,
    "dev_tool_result.v1": DevToolResult,
    "dev_feedback.v1": DevFeedback,
    "dev_stream_event.v1": DevStreamEvent,
    "dev_error.v1": DevError,
}

__all__ = [
    "CONTRACT_MODELS",
    "DevAnswer",
    "DevCapabilityLimits",
    "DevCapabilities",
    "DevClaim",
    "DevConversation",
    "DevConversationSummary",
    "DevConversationTranscript",
    "DevError",
    "DevEvidenceExpansion",
    "DevEvidenceRef",
    "DevFeedback",
    "DevMessageRequest",
    "DevMetricDefinition",
    "DevMetricRef",
    "DevScope",
    "DevScopeResolution",
    "DevStreamEvent",
    "DevToolRequest",
    "DevToolResult",
    "DevTranscriptEntry",
    "validate_stream",
]
