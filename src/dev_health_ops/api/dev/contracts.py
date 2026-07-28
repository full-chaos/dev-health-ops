"""Canonical provider-neutral Ask Dev v1 wire contracts.

These Pydantic models are the source of truth for the checked-in Draft 2020-12
schemas under ``contracts/ask-dev/v1``. Runtime services may adapt domain
objects into these models, but must not redeclare their wire shape.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Self

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
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$",
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


class EntityType(StrEnum):
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"


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


class DevSurfaceContext(ContractModel):
    route_id: OpaqueID
    entity_refs: list[DevEntityRef] = Field(default_factory=list, max_length=20)
    filter_fingerprint: OpaqueID | None = None


class DevScope(ContractModel):
    schema_version: Literal["dev_scope.v1"] = "dev_scope.v1"
    organization_id: OpaqueID
    direct_scope: DirectScope
    repositories: list[OpaqueID] = Field(default_factory=list, max_length=20)
    entity_refs: list[DevEntityRef] = Field(default_factory=list, max_length=20)
    team_ids: list[OpaqueID] = Field(default_factory=list, max_length=20)
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
        }
        expected = entity_scope.get(self.direct_scope)
        if expected is not None:
            if (
                len(self.entity_refs) != 1
                or self.entity_refs[0].entity_type != expected
            ):
                raise ValueError("direct entity scope requires one matching entity")
        return self


class DevDisambiguationCandidate(ContractModel):
    entity_ref: DevEntityRef
    repository_id: OpaqueID | None = None
    reason: ShortText


class DevScopeResolution(ContractModel):
    schema_version: Literal["dev_scope_resolution.v1"] = "dev_scope_resolution.v1"
    requested_scope: DevScope
    resolved_scope: DevScope | None = None
    outcome: ScopeResolutionOutcome
    authorized_repository_ids: list[OpaqueID] = Field(
        default_factory=list, max_length=20
    )
    authorized_entity_ids: list[OpaqueID] = Field(default_factory=list, max_length=20)
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
        if self.outcome is not ScopeResolutionOutcome.AMBIGUOUS and self.candidates:
            raise ValueError("candidates are allowed only for ambiguous outcomes")
        if self.outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK:
            if (
                self.resolved_scope is None
                or self.resolved_scope.direct_scope != DirectScope.ORGANIZATION
            ):
                raise ValueError(
                    "organization fallback must resolve to organization scope"
                )
        return self


class DevCapabilities(ContractModel):
    schema_version: Literal["dev_capabilities.v1"] = "dev_capabilities.v1"
    ask_dev: bool = False
    byo_llm: bool = False
    agent_context_runtime: bool = False
    can_read: bool = False
    can_manage: bool = False


class DevConversation(ContractModel):
    schema_version: Literal["dev_conversation.v1"] = "dev_conversation.v1"
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
    schema_version: Literal["dev_conversation_summary.v1"] = (
        "dev_conversation_summary.v1"
    )
    conversation_id: OpaqueID
    title: Annotated[str, StringConstraints(min_length=1, max_length=160)] | None = None
    direct_scope: DirectScope
    state: Literal["active", "deleted", "expired"]
    message_count: int = Field(ge=0, le=10_000)
    updated_at: AwareDatetime
    expires_at: AwareDatetime | None = None


class DevMessageRequest(ContractModel):
    schema_version: Literal["dev_message_request.v1"] = "dev_message_request.v1"
    request_id: OpaqueID
    client_message_id: OpaqueID
    conversation_id: OpaqueID | None = None
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
    schema_version: Literal["dev_evidence_ref.v1"] = "dev_evidence_ref.v1"
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


class DevMetricPoint(ContractModel):
    timestamp: AwareDatetime
    value: FiniteFloat


class DevMetricRef(ContractModel):
    schema_version: Literal["dev_metric_ref.v1"] = "dev_metric_ref.v1"
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
    schema_version: Literal["dev_claim.v1"] = "dev_claim.v1"
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
    schema_version: Literal["dev_answer.v1"] = "dev_answer.v1"
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
        ):
            raise ValueError(
                "complete answer requires all required sources fresh and available"
            )
        return self


class DevStatusFact(ContractModel):
    fact_id: OpaqueID
    text: ShortText
    evidence_ref_ids: list[OpaqueID] = Field(min_length=1, max_length=25)


class DevGraphEdge(ContractModel):
    source_entity_id: OpaqueID
    relationship: OpaqueID
    target_entity_id: OpaqueID
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)


class DevDataHealth(ContractModel):
    source_system: OpaqueID
    freshness: FreshnessState
    last_successful_at: AwareDatetime | None = None
    coverage: FiniteFloat = Field(ge=0, le=1)
    warning: ShortText | None = None


class DevToolRequest(ContractModel):
    schema_version: Literal["dev_tool_request.v1"] = "dev_tool_request.v1"
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
    schema_version: Literal["dev_tool_result.v1"] = "dev_tool_result.v1"
    run_id: OpaqueID
    tool_call_id: OpaqueID
    tool_id: ToolID
    status: Literal["success", "partial", "unavailable", "error"]
    scope_resolution: DevScopeResolution | None = None
    metrics: list[DevMetricRef] = Field(default_factory=list, max_length=12)
    evidence: list[DevEvidenceRef] = Field(default_factory=list, max_length=25)
    status_facts: list[DevStatusFact] = Field(default_factory=list, max_length=100)
    graph_edges: list[DevGraphEdge] = Field(default_factory=list, max_length=100)
    data_health: list[DevDataHealth] = Field(default_factory=list, max_length=25)
    warnings: list[ShortText] = Field(default_factory=list, max_length=20)
    error: DevError | None = None
    serialized_bytes: int = Field(ge=0, le=65_536)

    @model_validator(mode="after")
    def validate_error_state(self) -> Self:
        if self.status == "error" and self.error is None:
            raise ValueError("error tool result requires an error envelope")
        if self.status != "error" and self.error is not None:
            raise ValueError("only error tool results may include an error envelope")
        return self


class DevFeedback(ContractModel):
    schema_version: Literal["dev_feedback.v1"] = "dev_feedback.v1"
    feedback_id: OpaqueID
    answer_id: OpaqueID
    rating: Literal["up", "down"]
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
    schema_version: Literal["dev_error.v1"] = "dev_error.v1"
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
        "internal_error",
    ]
    safe_message: ShortText
    retryable: bool
    remediation: list[ShortText] = Field(default_factory=list, max_length=5)


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
    schema_version: Literal["dev_stream_event.v1"] = "dev_stream_event.v1"
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
    "dev_message_request.v1": DevMessageRequest,
    "dev_answer.v1": DevAnswer,
    "dev_claim.v1": DevClaim,
    "dev_metric_ref.v1": DevMetricRef,
    "dev_evidence_ref.v1": DevEvidenceRef,
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
    "DevCapabilities",
    "DevClaim",
    "DevConversation",
    "DevConversationSummary",
    "DevError",
    "DevEvidenceRef",
    "DevFeedback",
    "DevMessageRequest",
    "DevMetricRef",
    "DevScope",
    "DevScopeResolution",
    "DevStreamEvent",
    "DevToolRequest",
    "DevToolResult",
    "validate_stream",
]
