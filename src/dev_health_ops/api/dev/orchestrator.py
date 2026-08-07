"""Bounded, provider-neutral Ask Dev orchestration state machine.

The model may choose only one of the normalized decisions exposed by
``AgentLLMProvider``. Authorization, scope, limits, tool execution, grounding,
and terminal persistence remain server-owned.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import annotated_types
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentDisambiguation,
    AgentFinalAnswer,
    AgentLLMProvider,
    AgentMessage,
    AgentMessageRole,
    AgentRefusal,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)
from dev_health_ops.llm.budget import budget_idempotency_scope
from dev_health_ops.metrics.prometheus import (
    ASK_DEV_INTERNAL_TOKEN_LEAK_TOTAL,
    ASK_DEV_PLAN_REGISTRY_GAP_TOTAL,
    ASK_DEV_QUA_COMMIT_TOTAL,
    ASK_DEV_QUA_SHADOW_FAULT_TOTAL,
    ASK_DEV_QUA_SHADOW_LATENCY_SECONDS,
    ASK_DEV_QUA_SHADOW_TOTAL,
    ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL,
    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL,
    ASK_DEV_UNREGISTERED_TERMINAL_CODE_TOTAL,
)

from . import terminal_frames
from .answer_frames import narrative_fallback
from .answer_text_sanitizer import sanitize_model_text
from .answer_validator import (
    AnswerValidationContext,
    AnswerValidationError,
    any_tool_result_withheld_its_completion_denominator,
    completion_truncation_detail,
    validate_answer_candidate,
)
from .contracts import (
    V1_SCOPE_LIST_LIMIT,
    AnswerStatus,
    DevAnswer,
    DevClaim,
    DevContractVersions,
    DevCoverage,
    DevEntityRef,
    DevError,
    DevEvidenceRef,
    DevMessageRequest,
    DevMetricRef,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    DirectScope,
    EntityType,
    FreshnessState,
    MetricID,
    QuestionClass,
    ScopeResolutionOutcome,
    ToolID,
    dev_error_remediation,
)
from .contracts_v2 import (
    NO_ANSWER_OUTCOMES,
    Cardinality,
    DevInvestigationResult,
    DevResolutionEntry,
    DevSubjectSet,
    QuestionIntentID,
)
from .contracts_v2 import EntityKind as ContractEntityKind
from .contracts_v2.base import SourceRequirementState
from .contracts_v2.compat import scope_resolution_from_frame
from .contracts_v2.frame import DevAnswerFrame
from .contracts_v2.narrative import DevNarrative
from .contracts_v2.plan import DevInvestigationPlan
from .contracts_v2.subject import DevEntityRefV2
from .investigation_plans import PlanExecutor, StepContext
from .investigation_plans.state_mapping import UNMEASURED_REQUIREMENT_STATES
from .no_match_terminal import (
    attested_strings,
    disclose_scope_widening,
    disclose_subject_match,
    internal_token_leak_field,
    named_subject_not_found_answer,
    scrub_auxiliary_leaks,
    user_supplied_subject_label,
    user_visible_strings_by_field,
)
from .orchestrator_states import TERMINAL_STATES, RunState
from .org_policy import ASK_DEV_RUN_COST_HARD_MAX_MICROUSD
from .preflight_outcomes import (
    LEGACY_ONLY_QUESTION_INTENTS,
    TERMINAL_STATE_BY_OUTCOME,
    project_preflight_error,
)
from .prompts import PromptComposer, PromptConversationTurn
from .qua_promotion import (
    QUA_COMMIT_DIAGNOSTIC,
    QUAPromotion,
    promotable_selection,
    qua_committed_entry,
    verify_still_authorized,
)
from .qua_shadow import QUAShadowRecord, QuestionUnderstandingShadow
from .scope_service import MAX_CANDIDATES, ScopeResolutionService
from .status_answer_render import (
    build_deterministic_status_claims,
    deterministic_answer_status,
    render_declared_project_summary,
    render_portfolio_summary,
    render_verdict_summary,
    status_snapshot_result,
)
from .subject_preflight import (
    ALL_TOOLS,
    SUBJECT_BEARING_TOOLS,
    CommittedSubjects,
    PreflightDecision,
    SubjectPreflight,
    SubjectPreflightResult,
)
from .tool_registry import (
    AskDevToolRegistry,
    ToolExecution,
    ToolExecutionCancelled,
    ToolExecutionContext,
    ToolExecutionTimedOut,
    ToolRegistryError,
    ToolRequestRejected,
)

logger = logging.getLogger(__name__)


def _dev_tool_request_limit_maximum() -> int:
    """The wire-level ceiling on ``dev_tool_request.v1``'s ``limit`` field.

    Derived from the contract itself (not duplicated as a constant) so the
    provider-facing tool schema can never advertise a ``limit`` value the
    server-side ``DevToolRequest`` would reject (CHAOS-3262).
    """
    for constraint in DevToolRequest.model_fields["limit"].metadata:
        if isinstance(constraint, annotated_types.Le) and isinstance(
            constraint.le, int
        ):
            return constraint.le
    raise RuntimeError("dev_tool_request.v1 limit field must declare an upper bound")


# CHAOS-3289: when resolve_scope.v1 was never attempted, the only remaining
# signal that the question named a specific entity is the question text
# itself. This pattern is deliberately narrow (an explicit, capitalized
# entity name adjacent to one of the entity nouns Ask Dev tools actually
# support, in one of a handful of common orderings) to keep false positives
# on ordinary organization-wide prose vanishingly rare -- it is a backstop
# for the exact fabricated-premise shape this issue reproduces (a genuinely
# grounded, evidence-backed answer narrated under a name that was never
# resolved), not a general NLU pass. Known residual gap: lowercase/slug
# names and paraphrases outside these orderings still bypass it (tracked as
# follow-up -- see CHAOS-3289's linked extension issue).
_ENTITY_NAME_PATTERN = r"[A-Z][A-Za-z0-9&/'\-]*(?:\s+[A-Z][A-Za-z0-9&/'\-]*){0,3}"
_ENTITY_NOUN_PATTERN = (
    r"project|repository|repo|team|issue|pull request|deployment|incident|work unit"
)

# "status of/about/regarding/for/with/going on with the <Name> <noun>"
_NAMED_ENTITY_REFERENCE = re.compile(
    r"\b(?:status of|about|regarding|for|with|on|going on with|happening with)\s+"
    r"(?:the\s+)?"
    rf"({_ENTITY_NAME_PATTERN})"
    rf"\s+(?:{_ENTITY_NOUN_PATTERN})\b"
)

# "the <Name> <noun> status" / "the <Name> <noun>'s status" -- the noun and
# "status" trail the name instead of a leading preposition triggering it
# (e.g. "What's the Ask Dev project status?").
_NAMED_ENTITY_NOUN_STATUS = re.compile(
    rf"\b({_ENTITY_NAME_PATTERN})\s+(?:{_ENTITY_NOUN_PATTERN})'?s?\s+status\b"
)

# "<noun> <Name>" -- the noun leads the name instead of trailing a
# preposition (e.g. "Can you check project Ask Dev status?").
_ENTITY_NOUN_LEADING = re.compile(
    rf"\b(?:{_ENTITY_NOUN_PATTERN})\s+({_ENTITY_NAME_PATTERN})\b"
)


def _project_scope_from_ref(
    ref: DevEntityRefV2, *, org_id: str, base_scope: DevScope
) -> DevScope:
    """CHAOS-3393: one committed cohort/org-wide subject-set entry -> a
    real, single-project ``DevScope`` -- the same ``DirectScope.PROJECT``
    shape ``ScopeResolutionService.committed_resolution_for`` builds for a
    SINGULAR commit, adapted for a v2 ``DevEntityRefV2`` input (a subject
    set entry has no ``AuthorizedEntity`` to reuse that constructor with).
    Inherits the run's own ``time_range``/``comparison_range`` from
    ``base_scope`` -- exactly like every other committed scope construction
    in this module. Callers restrict ``ref.entity_kind`` to PROJECT before
    reaching here (the only kind ``status.portfolio.v1`` supports today).
    """

    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=org_id,
        direct_scope=DirectScope.PROJECT,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.PROJECT,
                entity_id=ref.entity_id,
                display_label=ref.display_label,
                repository_id=ref.repository_id,
            )
        ],
        team_ids=[],
        time_range=base_scope.time_range,
        comparison_range=base_scope.comparison_range,
        surface_context=None,
    )


def _named_entity_phrases(text: str) -> frozenset[str]:
    phrases = set(_NAMED_ENTITY_REFERENCE.findall(text))
    phrases.update(_NAMED_ENTITY_NOUN_STATUS.findall(text))
    phrases.update(_ENTITY_NOUN_LEADING.findall(text))
    return frozenset(match.casefold() for match in phrases)


#: The server-owned copy for the legacy CHAOS-3289 backstop, reachable only
#: from the flag-off (no-preflight) path. Split out of the predicate so a
#: telemetry-only firing on the preflight path cannot produce public copy.
#: The subset of guard reasons that say something about the *named entity*
#: rather than about the shape of the answer. Only these keep the backstop
#: terminal on a preflight run with an unresolved bare name.
_NAME_SPECIFIC_GUARD_REASONS = frozenset(
    {
        "resolve_scope_ambiguous",
        "resolve_scope_not_found",
        "narrated_unresolved_entity",
    }
)

#: CHAOS-3297 stack #5 (guard cutover). The server-owned copy a run ships
#: when a demoted legacy guard rejected the model's own summary but the run
#: still holds server-verified material.
#:
#: Deliberately says what the server did and why, and names no source, no
#: entity, and no domain fact -- it is the same class of copy as
#: ``_budget_answer``'s summary (a server statement about the *shape* of what
#: follows), never a claim about the subject. The substance lives in the
#: canonical metrics/evidence beside it and, once the frame is built, in the
#: plan's own findings.
SERVER_GROUNDED_SUMMARY = (
    "This result was assembled by the server from the sources it could "
    "verify for this request. The model's own summary was withheld because "
    "it did not pass a server-side answer check."
)
SERVER_GROUNDED_WARNING = (
    "A server-side answer check rejected the model's own summary; only "
    "server-verified data is reported here."
)

#: ``dev_runs.grounding_validation_status`` values for the two demoted
#: guards (a ``String(32)`` column, no CHECK constraint -- both fit).
#:
#: Distinct values rather than one shared "demoted" marker: the two guards
#: answer different questions (did the answer narrate a name the server never
#: resolved / did it carry any checkable grounding at all), and an operator
#: triaging a cutover regression needs to know which one fired without
#: joining another table. ``legacy_guard_reason`` still carries the
#: CHAOS-3289 backstop's own closed-vocabulary reason code alongside the
#: first of these -- this column says the verdict was *demoted*, that one
#: says what the verdict was.
GUARD_DEMOTED_NAMED_ENTITY_STATUS = "advisory_named_entity"
GUARD_DEMOTED_GROUNDING_FLOOR_STATUS = "advisory_grounding_floor"
#: CHAOS-3377 defect 1: a model that still self-declared ``status=refused``
#: over material grounding after its one repair attempt is demoted the same
#: way a grounding-floor violation is -- never a hard FAILED, which would
#: throw away real evidence the run already retrieved.
GUARD_DEMOTED_REFUSAL_STATUS = "advisory_refused_with_grounding"

_LEGACY_GUARD_TERMINALS: dict[str, tuple[str, str]] = {
    "resolve_scope_ambiguous": (
        "scope_ambiguous",
        "The requested scope is ambiguous.",
    ),
    "resolve_scope_not_found": (
        "scope_not_found",
        "The requested scope was not found.",
    ),
    "no_evidence_backed_claims": (
        "insufficient_evidence",
        "The answer does not include evidence-backed claims for the requested entity.",
    ),
    "narrated_unresolved_entity": (
        "insufficient_evidence",
        "The answer narrates a status for a named entity that was never resolved.",
    ),
}


_HARD_LIMIT_MAXIMA: dict[str, int | float] = {
    "model_rounds": 4,
    "tool_calls": 6,
    "identical_tool_calls": 2,
    "wall_seconds": 45.0,
    "tool_seconds": 15.0,
    "provider_seconds": 30.0,
    "provider_retries": 1,
    "schema_repairs": 1,
    "total_tool_bytes": 256 * 1024,
    "per_tool_bytes": 64 * 1024,
    "evidence_refs": 25,
    "metrics": 12,
    "max_output_tokens_per_call": 4_096,
    "max_total_input_tokens": 100_000,
    "max_total_output_tokens": 16_384,
    "estimated_cost_per_call_microusd": 1_000_000,
    "max_estimated_cost_microusd": ASK_DEV_RUN_COST_HARD_MAX_MICROUSD,
}


@dataclass(frozen=True, slots=True)
class DevRunLimits:
    """TRD defaults; operators may configure only stricter values."""

    model_rounds: int = 4
    tool_calls: int = 6
    identical_tool_calls: int = 2
    wall_seconds: float = 45.0
    tool_seconds: float = 15.0
    provider_seconds: float = 30.0
    provider_retries: int = 1
    schema_repairs: int = 1
    total_tool_bytes: int = 256 * 1024
    per_tool_bytes: int = 64 * 1024
    evidence_refs: int = 25
    metrics: int = 12
    max_output_tokens_per_call: int = 4_096
    max_total_input_tokens: int = 100_000
    max_total_output_tokens: int = 16_384
    estimated_cost_per_call_microusd: int = 1_000_000
    max_estimated_cost_microusd: int = ASK_DEV_RUN_COST_HARD_MAX_MICROUSD

    def __post_init__(self) -> None:
        for name, maximum in _HARD_LIMIT_MAXIMA.items():
            value = getattr(self, name)
            if value < 0 or value > maximum:
                raise ValueError(f"{name} may only be configured downward")
        for name in (
            "max_output_tokens_per_call",
            "max_total_input_tokens",
            "max_total_output_tokens",
            "estimated_cost_per_call_microusd",
            "max_estimated_cost_microusd",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class OrchestratorEvent:
    state: RunState
    safe_code: str | None = None


@dataclass(frozen=True, slots=True)
class OrchestratorResult:
    run_id: str
    state: RunState
    answer: DevAnswer | None
    error: DevError | None
    events: tuple[OrchestratorEvent, ...]
    usage: AgentUsage
    tool_call_count: int
    provider_fingerprint: str | None
    model_fingerprint: str | None
    #: CHAOS-3497: the run's OWN most recent completed scope resolution,
    #: carried on every terminal -- not only the answering ones.
    #:
    #: ``streaming.stream_orchestrator`` used to read the resolution off
    #: ``answer.resolved_scope``, which exists only when the run produced an
    #: answer, so a run that resolved scope and then terminated without one
    #: (insufficient_evidence, a refusal, a not-found) emitted no
    #: ``scope.resolved`` frame at all. Its scope decision -- including a
    #: silent widening from a named subject to organization scope -- was then
    #: permanently unobservable on the wire, which is exactly the run shape
    #: the no-silent-widening audit family exists to catch.
    #:
    #: ``None`` only when the run terminated BEFORE scope resolution
    #: completed (a cancellation on the accepted state, the outer catch-all
    #: on a fault raised before ``_resolve_with_cancellation`` returned).
    scope_resolution: DevScopeResolution | None = None

    def __post_init__(self) -> None:
        if self.state not in TERMINAL_STATES:
            raise ValueError("orchestrator result must be terminal")
        if (self.answer is None) == (self.error is None):
            raise ValueError(
                "orchestrator result requires exactly one terminal payload"
            )


def _deterministic_declined(result: SubjectPreflightResult) -> bool:
    """Whether the deterministic layer failed to commit a NAMED subject.

    The two shapes, and only these two (CHAOS-3525):

    * a ``TERMINATE`` that carries a resolution ledger -- the preflight
      resolved mentions and none of them committed. A ``TERMINATE`` with no
      ledger (an uninterpretable question, an oversized mention set) never
      reached the catalog at all, so there is no named subject for a model to
      match and nothing it could be second-guessing;
    * the bare-name ``PROCEED`` that widens to organization scope, which is a
      *scope* commit standing in for a subject it could not resolve -- see
      ``SubjectPreflightResult.has_committed_subject``, whose whole reason for
      existing is that this branch commits a resolution but emphatically not a
      subject.

    Written as one predicate here rather than inline at the seam so the
    "declined" vocabulary has a single definition, and so a future preflight
    branch that also declines has one obvious place to be added.
    """

    if result.decision is PreflightDecision.TERMINATE:
        return result.ledger is not None
    return result.legacy_guard_required


def _promoted_preflight_result(
    result: SubjectPreflightResult,
    *,
    promotion: QUAPromotion,
    scope_service: Any,
    org_id: str,
    base_scope: DevScope,
    resolved_at: datetime,
) -> tuple[SubjectPreflightResult, tuple[DevResolutionEntry, ...]]:
    """Rewrite a declined preflight result into a QUA-committed PROCEED.

    Returns the rewritten result AND the ledger entries the caller still has
    to persist, because which ones those are depends on the branch being
    replaced and only this function knows both halves:

    * replacing a TERMINATE: nothing was persisted (the whole-ledger write
      is PROCEED-only, and the terminating-entry write lives in the branch
      the promotion now skips), so every entry must be written;
    * replacing the bare-name PROCEED: the ledger already flushed, so only
      the new entry may be written -- re-writing the rest would collide on
      the ``(run_id, entry_ordinal)`` unique constraint.

    Mints through ``committed_resolution_for`` -- the same function the
    deterministic commit uses, whose docstring calls itself "the one
    construction of an exact-match committed scope … so there is one notion
    of committed scope rather than two that can drift". A QUA commit becomes
    a third caller of it, never a second notion.

    The ledger gains a real ``exact_match`` entry for the mention
    (``qua_committed_entry``), stamped with QUA's own ``resolver_version``:
    the run must not later read as though the mention was never resolved,
    and it equally must not read as though the deterministic resolver
    resolved it.

    ``allowed_tools`` matches the deterministic committed-subject branch --
    ``resolve_scope.v1`` is withheld because there is nothing left for the
    model to resolve, which is also what keeps the ``forbidden_or_not_found``
    tool-result leak channel (CHAOS-3421) unreachable here.
    """

    committed = scope_service.committed_resolution_for(
        promotion.entity,
        org_id=org_id,
        base_scope=base_scope,
        resolved_at=resolved_at,
    )
    ledger = result.ledger
    committed_entry: DevResolutionEntry | None = None
    if ledger is not None:
        committed_entry = qua_committed_entry(
            promotion,
            entry_ordinal=len(ledger.entries),
            query_version=committed.requested_scope.schema_version,
            resolved_at=resolved_at,
        )
        ledger = ledger.model_copy(
            update={
                "entries": (*ledger.entries, committed_entry),
                "updated_at": resolved_at,
            }
        )
    pending: tuple[DevResolutionEntry, ...] = ()
    if ledger is not None and committed_entry is not None:
        pending = (
            ledger.entries
            if result.decision is PreflightDecision.TERMINATE
            else (committed_entry,)
        )
    promoted = replace(
        result,
        decision=PreflightDecision.PROCEED,
        ledger=ledger,
        committed_resolution=committed,
        committed_subjects=CommittedSubjects(
            resolution=committed, subject_set=result.subject_set
        ),
        answer=None,
        outcome=None,
        allowed_tools=ALL_TOOLS - {ToolID.RESOLVE_SCOPE},
        diagnostic=QUA_COMMIT_DIAGNOSTIC,
        # The legacy named-entity backstop exists to catch an answer that
        # narrates a name the run never resolved. This run DID resolve it, and
        # discloses the match -- leaving the guard armed would terminate the
        # very answer the promotion exists to produce.
        legacy_guard_required=False,
        unresolved_name_spans=frozenset(),
        blocking_mention_ids=frozenset(),
        terminating_resolution_entry=None,
    )
    return promoted, pending


class ScopeResolver(Protocol):
    async def __call__(
        self, *, org_id: str, user_id: str, requested_scope: DevScope
    ) -> DevScopeResolution: ...


class RunRecorder(Protocol):
    async def transition(self, state: RunState) -> None: ...

    async def record_tool(
        self,
        *,
        ordinal: int,
        request: DevToolRequest,
        canonical_input_hash: str,
        execution: ToolExecution,
    ) -> None: ...

    async def record_answer(self, answer: DevAnswer) -> None: ...

    async def record_error_message(
        self, error: DevError, *, scope_snapshot: Mapping[str, Any]
    ) -> None:
        """Persist one assistant ``dev_messages`` row for a no-answer terminal (CHAOS-3423).

        Called from ``finish()`` for every terminal that carries an ``error``
        and no ``answer`` -- a clarification or any other orchestrator/
        preflight error code -- so the conversation transcript always has a
        row for the turn, mirroring what ``record_answer`` already does for
        a genuine answer.
        """
        ...

    async def record_preflight(
        self,
        *,
        preflight_outcome: str | None,
        legacy_guard_reason: str | None,
    ) -> None:
        """Content-free run diagnostics (CHAOS-3292).

        Both values are members of closed server-owned vocabularies — never
        question text, entity names, or catalog content. There is no metrics,
        logging, tracing or statsd facility anywhere in ``api/dev`` to publish
        a divergence counter to (a real observability stack is CHAOS-3218), so
        these ride on the run row beside the existing ``safe_error_code`` and
        ``grounding_validation_status`` diagnostics.
        """
        ...

    async def record_subject_set(self, subject_set: DevSubjectSet) -> None:
        """Persist one committed ``dev_subject_set.v1`` (CHAOS-3301).

        Called whenever the preflight built a subject set — a homogeneous
        cohort (which then terminates unsupported on the v1 surface, D1) or a
        singular commit reached via duplicate aliases (N4, which still
        proceeds). Never called for an ordinary single-mention commit.
        """
        ...

    async def append_resolution(self, entry: DevResolutionEntry) -> None:
        """Persist one ``dev_resolution_ledger.v1`` entry (CHAOS-3325/3424/3533).

        Called for every entry of the ledger the preflight built, on BOTH
        decisions:

        * ``PROCEED`` — CHAOS-3424, so a widening/wrong-subject incident is
          auditable from data rather than a container log line;
        * ``TERMINATE`` — CHAOS-3533, the same argument for the half where the
          run declined to answer. Until then only the terminating
          ``ambiguous_candidates`` entry was written, so a not-found, an
          unavailable catalog, an unsupported kind, and a committed cohort
          that v1 cannot render all left ZERO rows — and the eight corpus
          cases asserting a resolution path over them were unsatisfiable by
          construction.

        On the ambiguity path the terminating entry is always persisted
        before ``record_frame`` for the same outcome, so the frame's
        ``clarification_candidates`` always has its own mention's ledger row
        to be authorized against
        (``persistence.service._authorize_clarification_candidates``, a
        Codex-review NO-SHIP finding: a schema-valid frame could otherwise
        name an entity the ledger never authorized).
        """
        ...

    async def record_frame(
        self,
        frame: DevAnswerFrame,
        *,
        authorizing_mention_id: str | None = None,
    ) -> None:
        """Persist one already-built ``dev_answer_frame.v1`` (CHAOS-3297).

        Called from the preflight TERMINATE branch with the frame
        ``preflight_outcomes.build_preflight_answer`` already validated, so
        the run tags ``contract_generation = 'v2'`` and the CHAOS-3299 v2
        replay branch (``router._replayed_result``) becomes reachable for a
        preflight-terminated run, not just a full model-round completion.

        ``authorizing_mention_id`` (CHAOS-3533) names the mention whose
        ambiguity terminated the run — the one whose ledger row authorizes
        this frame's ``clarification_candidates``. ``None`` on every other
        terminal, and that absence is itself an assertion: the frame must
        then carry no candidates at all. It has to be named rather than
        inferred now that a TERMINATE persists its whole ledger, because a
        run can legitimately hold an ambiguous row for a mention it never
        offered the user (an omitted cohort member, or an ambiguous mention
        at a higher ordinal than the not-found one that actually terminated).
        """
        ...

    async def rollback(self) -> None:
        """Discard pending writes after a failed record_frame flush (CHAOS-3297).

        A database-level failure during ``record_frame``'s flush (a
        constraint violation, a dropped connection) marks the underlying
        session rollback-only: the next write on it raises
        ``PendingRollbackError`` instead of succeeding. A caller that catches
        a ``record_frame`` failure must call this before any further
        recorder write (``terminal()`` in particular) on the same run, or a
        recoverable frame-write failure strands the run as a nonterminal
        ``accepted``/v1 row that every idempotent retry then 409s against
        forever.
        """
        ...

    async def record_investigation_result(self, result: DevInvestigationResult) -> None:
        """Persist one ``dev_investigation_result.v1`` (CHAOS-3295).

        Called at most once per run, only when a core-question-class plan
        governed this run's investigation — never for a run the model's own
        tool-choice loop drove end to end.
        """
        ...

    async def record_qua_shadow(self, record: QUAShadowRecord) -> None:
        """Persist one CHAOS-3389 QUA shadow evaluation.

        Called at most once per run, only when a ``QuestionUnderstandingShadow``
        is configured -- never affects any live decision; see this module's
        own call site for the full non-influence argument. Best-effort:
        callers must tolerate this raising (mirrors ``record_frame``'s own
        failure-recovery posture) since a shadow-record write is strictly
        secondary to the run it shadows.
        """
        ...

    async def record_narrative(self, narrative: DevNarrative) -> None:
        """Persist one ``dev_narrative.v1`` (CHAOS-3297 stack #4).

        Called at most once per run, only for a frame that already
        persisted (``record_frame`` succeeded) and only for a content-
        bearing outcome — narrative is ABSENT by contract for every
        ``NO_ANSWER_OUTCOMES`` member (``no_answer_policy``), the same
        guardrail that keeps a no-answer outcome from carrying a free-form
        channel.
        """
        ...

    async def terminal(
        self,
        *,
        state: RunState,
        answer: DevAnswer | None,
        error: DevError | None,
        usage: AgentUsage,
        tool_call_count: int,
        provider_fingerprint: str | None,
        model_fingerprint: str | None,
        prompt_checksum: str | None,
        prompt_version: str | None,
        narrative_mode: str | None = None,
        narrative_failure_code: str | None = None,
        grounding_validation_status: str | None = None,
    ) -> None: ...


class NullRunRecorder:
    async def transition(self, state: RunState) -> None:
        del state

    async def record_tool(
        self,
        *,
        ordinal: int,
        request: DevToolRequest,
        canonical_input_hash: str,
        execution: ToolExecution,
    ) -> None:
        del ordinal, request, canonical_input_hash, execution

    async def record_answer(self, answer: DevAnswer) -> None:
        del answer

    async def record_error_message(
        self, error: DevError, *, scope_snapshot: Mapping[str, Any]
    ) -> None:
        del error, scope_snapshot

    async def record_preflight(
        self,
        *,
        preflight_outcome: str | None,
        legacy_guard_reason: str | None,
    ) -> None:
        del preflight_outcome, legacy_guard_reason

    async def record_subject_set(self, subject_set: DevSubjectSet) -> None:
        del subject_set

    async def append_resolution(self, entry: DevResolutionEntry) -> None:
        del entry

    async def record_frame(
        self,
        frame: DevAnswerFrame,
        *,
        authorizing_mention_id: str | None = None,
    ) -> None:
        del frame, authorizing_mention_id

    async def rollback(self) -> None:
        return None

    async def record_investigation_result(self, result: DevInvestigationResult) -> None:
        del result

    async def record_qua_shadow(self, record: QUAShadowRecord) -> None:
        del record

    async def record_narrative(self, narrative: DevNarrative) -> None:
        del narrative

    async def terminal(
        self,
        *,
        state: RunState,
        answer: DevAnswer | None,
        error: DevError | None,
        usage: AgentUsage,
        tool_call_count: int,
        provider_fingerprint: str | None,
        model_fingerprint: str | None,
        prompt_checksum: str | None,
        prompt_version: str | None,
        narrative_mode: str | None = None,
        narrative_failure_code: str | None = None,
        grounding_validation_status: str | None = None,
    ) -> None:
        del (
            state,
            answer,
            error,
            usage,
            tool_call_count,
            provider_fingerprint,
            model_fingerprint,
            prompt_checksum,
            prompt_version,
            narrative_mode,
            narrative_failure_code,
            grounding_validation_status,
        )


class BudgetExceeded(RuntimeError):
    pass


class RunDeadlineExceeded(RuntimeError):
    pass


@dataclass(slots=True)
class ProviderBudget:
    limits: DevRunLimits
    usage: AgentUsage = field(default_factory=AgentUsage)
    pending_input_reservations: list[int] = field(default_factory=list)

    def require(self, *, prompt_bytes: int) -> None:
        estimated_input_tokens = max(1, (prompt_bytes + 3) // 4)
        next_input_tokens = self.usage.input_tokens + estimated_input_tokens
        if next_input_tokens > self.limits.max_total_input_tokens:
            raise BudgetExceeded("input token budget exhausted")
        if (
            self.usage.output_tokens + self.limits.max_output_tokens_per_call
            > self.limits.max_total_output_tokens
        ):
            raise BudgetExceeded("output token budget exhausted")
        reserved_cost = self.limits.estimated_cost_per_call_microusd
        next_cost = (self.usage.estimated_cost_microusd or 0) + reserved_cost
        if next_cost > self.limits.max_estimated_cost_microusd:
            raise BudgetExceeded("provider cost budget exhausted")
        # Reserve before dispatch. Provider failures and responses without a cost
        # estimate must never become free merely because exact billing is unknown.
        self.usage = AgentUsage(
            input_tokens=next_input_tokens,
            output_tokens=self.usage.output_tokens,
            estimated_cost_microusd=next_cost,
        )
        self.pending_input_reservations.append(estimated_input_tokens)

    def add(self, usage: AgentUsage) -> None:
        if usage.input_tokens < 0 or usage.output_tokens < 0:
            raise BudgetExceeded("provider returned invalid token usage")
        if (
            usage.estimated_cost_microusd is not None
            and usage.estimated_cost_microusd < 0
        ):
            raise BudgetExceeded("provider returned invalid cost usage")
        prior_cost = self.usage.estimated_cost_microusd or 0
        if usage.estimated_cost_microusd is None:
            reconciled_cost = prior_cost
        else:
            reconciled_cost = (
                max(
                    0,
                    prior_cost - self.limits.estimated_cost_per_call_microusd,
                )
                + usage.estimated_cost_microusd
            )
        reserved_input = (
            self.pending_input_reservations.pop()
            if self.pending_input_reservations
            else 0
        )
        self.usage = AgentUsage(
            input_tokens=max(0, self.usage.input_tokens - reserved_input)
            + usage.input_tokens,
            output_tokens=self.usage.output_tokens + usage.output_tokens,
            estimated_cost_microusd=reconciled_cost,
        )
        if self.usage.input_tokens > self.limits.max_total_input_tokens:
            raise BudgetExceeded("input token budget exhausted")
        if self.usage.output_tokens > self.limits.max_total_output_tokens:
            raise BudgetExceeded("output token budget exhausted")
        if (
            self.usage.estimated_cost_microusd or 0
        ) > self.limits.max_estimated_cost_microusd:
            raise BudgetExceeded("provider cost budget exhausted")


class EventCancellationSignal:
    def __init__(self, event: asyncio.Event) -> None:
        self._event = event

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()


EventSink = Callable[[OrchestratorEvent], Awaitable[None]]


class DevOrchestrator:
    """Execute one Ask Dev message as a bounded state machine."""

    def __init__(
        self,
        *,
        provider: AgentLLMProvider,
        provider_source: Literal["platform", "byo"],
        provider_family: str,
        registry: AskDevToolRegistry,
        scope_resolver: ScopeResolver,
        versions: DevContractVersions,
        limits: DevRunLimits | None = None,
        recorder: RunRecorder | None = None,
        event_sink: EventSink | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        preflight: SubjectPreflight | None = None,
        plan_registry: Mapping[QuestionIntentID, DevInvestigationPlan] | None = None,
        plan_executor: PlanExecutor | None = None,
        narrative_provider: narrative_fallback.NarrativeProvider | None = None,
        qua_shadow: QuestionUnderstandingShadow | None = None,
    ) -> None:
        self._provider = provider
        self._provider_source = provider_source
        self._provider_family = provider_family
        self._registry = registry
        self._scope_resolver = scope_resolver
        self._versions = versions
        self._limits = limits or DevRunLimits()
        self._recorder = recorder or NullRunRecorder()
        self._event_sink = event_sink
        self._monotonic = monotonic
        # ``None`` is the flag-off path: no interpretation, no subject
        # resolution, every tool advertised, and the CHAOS-3289 backstop
        # terminating exactly as it does today.
        self._preflight = preflight
        # CHAOS-3295: ``None`` is the flag-off path for the plan-governed
        # investigation seam -- an unset registry/executor leaves every run
        # on today's model-tool-choice loop exactly as before, whether or
        # not preflight itself is enabled.
        self._plan_registry = plan_registry or {}
        self._plan_executor = plan_executor
        # CHAOS-3297 stack #4: ``None`` is the only certified state today --
        # no narrative provider has been certified yet (CHAOS-3285's
        # territory). synthesize_narrative treats a ``None`` provider as a
        # configuration state, not a failure, and goes straight to the
        # deterministic fallback. This is the seam CHAOS-3285 populates and
        # the seam C3/C4 live-endpoint controls inject a scripted provider
        # through.
        self._narrative_provider = narrative_provider
        # CHAOS-3389 shadow phase: ``None`` is the flag-off path -- no
        # shadow call, no shadow record, byte-identical to this seam not
        # existing. See the ``qua_shadow`` call site in ``run()`` for the
        # structural argument that even when set, this can never affect any
        # live decision.
        self._qua_shadow = qua_shadow
        self._composer = PromptComposer(registry)

    async def run(
        self,
        *,
        request: DevMessageRequest,
        org_id: str,
        user_id: str,
        permission_fingerprint: str,
        run_id: str,
        conversation_id: str,
        answer_id: str,
        cancellation: asyncio.Event,
        prior_turns: tuple[PromptConversationTurn, ...] = (),
        event_sink: EventSink | None = None,
    ) -> OrchestratorResult:
        started = self._monotonic()
        events: list[OrchestratorEvent] = []
        tool_results: list[DevToolResult] = []
        tool_requests: list[DevToolRequest] = []
        tool_bytes_total = 0
        duplicate_counts: Counter[str] = Counter()
        budget = ProviderBudget(self._limits)
        provider_fingerprint: str | None = None
        model_fingerprint: str | None = None
        prompt_checksum: str | None = None
        prompt_version: str | None = None
        resolution: DevScopeResolution | None = None
        provider_continuation: tuple[AgentMessage, ...] = ()
        terminal_written = False
        repair_count = 0
        retry_count = 0
        # CHAOS-3289: track whether this run ever attempted resolve_scope.v1
        # for a named entity, and that attempt's most recent outcome, so a
        # final answer cannot silently narrate a named entity under
        # organization scope that was never confirmed to exist.
        resolve_scope_attempted = False
        last_resolve_scope_outcome: ScopeResolutionOutcome | None = None
        # CHAOS-3367: the not-found resolution itself, and the query that
        # produced it, so the no-match terminal can render the run's OWN
        # resolution (never the previously committed organization scope --
        # that juxtaposition is the "Scope outcome: exact while a named
        # subject could not be found" the PRD prohibits) and look the query
        # up in the user's own question text.
        last_resolve_scope_resolution: DevScopeResolution | None = None
        last_resolve_scope_query: str | None = None
        # CHAOS-3497: the resolution a preflight TERMINATE actually reached
        # for the subject the question named -- set only on that branch, and
        # the highest-priority thing `published_resolution()` reports.
        #
        # It has to be tracked separately because `_terminate()` commits
        # NOTHING (`committed_resolution=None` on every one of its call
        # sites), so `resolution` there is still the run's original top-level
        # resolve. That value can only ever be healthy -- every unhealthy
        # outcome already returned earlier -- so publishing it would put
        # "scope resolved: exact" one frame ahead of an error saying the
        # named subject could not be found, and would hand
        # `no_unauthorized_candidate_surfaces` an object that STRUCTURALLY
        # cannot carry candidates (v1 permits them only on candidate-bearing
        # outcomes), flipping that security check from a loud "not measured"
        # to a silent vacuous pass. Reproduced live before the fix: an
        # ambiguous "Atlas" and a not-found "Nightfall" both published
        # `inherited`, with zero candidates.
        preflight_terminal_resolution: DevScopeResolution | None = None
        #: CHAOS-3525: ``(user's span, authorized entity label)`` for a subject
        #: committed from a QUA proposal -- set at the promotion seam, read by
        #: ``finish()``'s closure. A free variable rather than a parameter for
        #: the same reason ``investigation_result`` is one: ``finish()`` has
        #: ~35 call sites and none of them should have to remember this.
        qua_subject_disclosure: tuple[str, str] | None = None
        selected_event_sink = event_sink or self._event_sink
        # CHAOS-3297 stack #3 (team-lead boundary ruling, 2026-08-02): set
        # by the CHAOS-3295 plan-execution block below when a plan actually
        # runs, read by `finish()`'s closure (a free variable over this
        # enclosing scope, not `nonlocal` -- `finish()` only reads it) so
        # the legacy answer's frame can embed the plan's findings alongside
        # it, without threading a new parameter through every one of
        # `finish()`'s ~35 call sites.
        investigation_result: DevInvestigationResult | None = None
        # CHAOS-3393: mirrors investigation_result's own free-variable
        # posture -- set below when a PLURAL_COHORT/ORGANIZATION_WIDE
        # status.portfolio.v1 run actually executes against a committed
        # DevSubjectSet, read by `finish()`'s closure so the frame it builds
        # can carry `subject_set_ref` (never alongside `subject_ref`, which
        # `wrap_legacy_answer_as_frame` never sets today).
        portfolio_subject_set_ref: str | None = None
        #: CHAOS-3393: the committed DevSubjectSet's own disclosure strings
        #: (an omitted named mention, or an ORGANIZATION_WIDE enumeration
        #: truncated at the batch cap) -- folded into render_portfolio_
        #: summary's direct_summary below, since wrap_legacy_answer_as_
        #: frame's own `limitations` never reads DevSubjectSet.warnings.
        portfolio_subject_set_warnings: tuple[str, ...] = ()
        #: CHAOS-3393 codex MED-2: the committed DevSubjectSet's own,
        #: catalog-confirmed project display labels -- attested at the
        #: finish() leak-scan seam (the same trust tier attested_strings
        #: already grants evidence/candidate display labels) so a
        #: legitimate project whose real name coincidentally matches an
        #: internal denylisted token is never fail-closed over that
        #: collision.
        portfolio_attested_labels: tuple[str, ...] = ()

        def published_resolution() -> DevScopeResolution | None:
            """The scope decision a no-answer terminal reports on the wire.

            Three sources, in priority order, and the order is the whole
            point -- an auditor must be able to tell a run that resolved to
            an exact subject from one that silently widened, and every
            wrong choice here is a false answer to that question:

            1. ``preflight_terminal_resolution`` -- the preflight
               TERMINATE branch's own outcome for the subject the question
               named. Nothing else describes that failure at all.
            2. ``last_resolve_scope_resolution``, but ONLY while the run is
               still on organization scope. This mirrors the diversion
               guard the answer path already applies (see the
               ``missed_subject_label`` branch): once a ``resolve_scope.v1``
               call has committed a REAL narrower subject, a later failed
               lookup for some other name did not change what this run
               executed under, and reporting that miss as the run's scope
               decision invents a resolution failure that never happened.
            3. ``resolution`` -- the committed scope itself.

            ``None`` only when the run ended before scope resolution
            completed at all.
            """

            if preflight_terminal_resolution is not None:
                return preflight_terminal_resolution
            committed = resolution
            still_organization_wide = (
                committed is None
                or committed.resolved_scope is None
                or committed.resolved_scope.direct_scope is DirectScope.ORGANIZATION
            )
            if still_organization_wide and last_resolve_scope_resolution is not None:
                return last_resolve_scope_resolution
            return committed

        async def transition(state: RunState, safe_code: str | None = None) -> None:
            event = OrchestratorEvent(state=state, safe_code=safe_code)
            events.append(event)
            await self._recorder.transition(state)
            if selected_event_sink is not None:
                await selected_event_sink(event)

        async def finish(
            state: RunState,
            *,
            answer: DevAnswer | None = None,
            error: DevError | None = None,
            frame_already_recorded: bool = False,
            grounding_validation_status: str | None = None,
            extra_attested: tuple[str, ...] = (),
        ) -> OrchestratorResult:
            nonlocal terminal_written
            if terminal_written:
                raise RuntimeError("terminal state already written")
            # CHAOS-3497 part 2: disclose a widening in PROSE, not only in
            # `dev_scope_resolution.v1.fallbacks`.
            #
            # `legacy_guard_required` is true on exactly one branch: a bare
            # name in the question went unresolved and `subject_preflight`
            # committed the organization in its place so no page-derived
            # subject could be narrated as the named one. That run then
            # answers organization-wide, and until this the only trace was
            # machine-readable -- a person reading the answer saw two
            # unrelated limitations and no hint their subject was missed.
            #
            # Placed here, ahead of the leak scan and every record_*() write,
            # so the sentence is scanned like any other user-visible copy and
            # reaches the persisted answer, the persisted frame's
            # `limitations`, and the stream's `warning` frames alike. The
            # committed outcome is re-checked rather than assumed: the
            # disclosure must state something that is actually true of the
            # scope this answer was computed under.
            if (
                answer is not None
                and preflight_result is not None
                and preflight_result.legacy_guard_required
                and answer.resolved_scope.outcome
                is ScopeResolutionOutcome.ORGANIZATION_FALLBACK
            ):
                answer = disclose_scope_widening(answer)
            # CHAOS-3525: a subject an LLM chose is never silent.
            #
            # Same channel as the widening disclosure immediately above, and
            # placed in the same spot for the same reason: ahead of the leak
            # scan and every record_*() write, so the sentence is scanned
            # like any other user-visible copy and reaches the persisted
            # answer, the frame's `limitations`, and the stream's `warning`
            # frames together.
            if answer is not None and qua_subject_disclosure is not None:
                matched_span, matched_label = qua_subject_disclosure
                answer = disclose_subject_match(
                    answer, span=matched_span, label=matched_label
                )
            # CHAOS-3367: the one place every terminal in this module passes
            # through, and therefore the only place a user-visible-copy rule
            # can be enforced structurally rather than by asking ~35 call
            # sites to remember it.
            #
            # It fails CLOSED. A leaked internal token is a producer defect,
            # and the PRD's requirement is "must never show", not "show it
            # with a warning" -- so the offending terminal is discarded and
            # replaced with a canonical internal_error rather than repaired
            # in place, which would leave the rest of a defective payload on
            # the wire. Every increment of the counter is a bug to fix at its
            # source; the denylist is derived from the live enums and is
            # disjoint from the completion reason-code vocabulary that
            # legitimately appears in copy (pinned by
            # test_no_match_terminal.py), so this must never fire on a
            # healthy run.
            #
            # CHAOS-3388 codex re-review (HIGH, confirmed): ``extra_attested``
            # is the narrowly-scoped escape hatch for a caller that built its
            # `error` from server-authorized data ``attested_strings`` cannot
            # see (it reads only off ``answer``, which is ``None`` on an
            # error-only path). The preflight TERMINATE branch is the one
            # caller that passes it today: ``project_preflight_error``
            # interpolates the same catalog-confirmed candidate display
            # labels the persisted frame already carries
            # (``clarification_candidates`` -- CHAOS-3325) into
            # ``safe_message``, so those exact labels are attested here too,
            # the same trust tier as everything else this scan already
            # exempts.
            attested = attested_strings(answer, request.question) + extra_attested
            # CHAOS-3377 leak-hardening: scrub model-authored AUXILIARY
            # fields (warnings/conflicts/suggested_follow_up_questions/
            # resolved_scope.warnings) BEFORE the fail-closed scan below --
            # not after. `_deterministic_status_render` overwrites status/
            # direct_summary/claims once a bound status_snapshot.v1 result
            # exists, but leaves those four fully model-authored; a model
            # that has seen a tool result's raw actual_completion.
            # reason_codes can echo one into any of them, and without this
            # the scan below would destroy the entire safe, deterministic
            # answer over one leaked auxiliary sentence (live incident: run
            # 22f97bee-0b8b-44a5-979f-78d7d7a80a82). direct_summary/claims
            # are deliberately NOT touched here -- see
            # `no_match_terminal.scrub_auxiliary_leaks`'s docstring for why
            # a leak reaching those must still fail the whole terminal
            # closed, exactly as before this scrub existed.
            if answer is not None:
                answer, scrubbed_fields = scrub_auxiliary_leaks(
                    answer, attested=attested
                )
                if scrubbed_fields:
                    logger.warning(
                        "ask_dev.orchestrator.internal_token_leak_scrubbed "
                        f"run_id={run_id} fields={','.join(scrubbed_fields)}",
                        extra={"run_id": run_id, "fields": scrubbed_fields},
                    )
            leaked = internal_token_leak_field(
                user_visible_strings_by_field(answer=answer, error=error),
                # Provenance, so an authorized entity whose real name looks
                # like an enum member does not fail its own answer. See
                # `no_match_terminal.internal_token_leak`.
                attested=attested,
            )
            if leaked is not None:
                leaked_field, leaked_token = leaked
                terminal_kind = "answer" if answer is not None else "error"
                # The token AND the field it was found in, embedded directly
                # in the message string -- not only `extra=`. This dev
                # stack's own LOG_JSON=0 configuration proved `extra=` keys
                # are silently dropped by a bare-message StreamHandler (no
                # formatter references them), which is exactly why the live
                # incident's log line carried no diagnostic detail at all.
                # Internal log only: never in `safe_message` or any
                # persisted user-visible field.
                logger.error(
                    "ask_dev.orchestrator.internal_token_leak "
                    f"run_id={run_id} field={leaked_field} token={leaked_token} "
                    f"terminal_kind={terminal_kind}",
                    extra={
                        "run_id": run_id,
                        "token": leaked_token,
                        "field": leaked_field,
                        "terminal_kind": terminal_kind,
                    },
                )
                ASK_DEV_INTERNAL_TOKEN_LEAK_TOTAL.labels(
                    token=leaked_token,
                    terminal_kind=terminal_kind,
                ).inc()
                answer = None
                # CHAOS-3421: a leak on the ONE preflight branch that sets
                # `legacy_guard_required` (subject_preflight's
                # `unresolved_untyped` -> `proceeded_unresolved_bare_name`,
                # an intentional, disclosed organization-wide fallback for a
                # named subject the catalog could not confirm) is not a
                # producer defect to hide behind a generic, opaque
                # `internal_error` -- it is very often exactly the model
                # echoing the raw `forbidden_or_not_found` outcome
                # `resolve_scope.v1` returned for that same unresolved name
                # (live incident). That run was always heading toward the
                # legacy guard's own `resolve_scope_not_found` terminal
                # (`_LEGACY_GUARD_TERMINALS`) -- reachable today only when
                # the model's prose happens to NARRATE the unresolved name
                # (`_legacy_named_entity_guard_reason`'s own text-similarity
                # check), not when it leaks the raw outcome token instead.
                # This closes that gap at the one shared choke-point every
                # terminal passes through, rather than trying to anticipate
                # every shape a leak on this branch could take.
                if (
                    preflight_result is not None
                    and preflight_result.legacy_guard_required
                ):
                    state = RunState.INSUFFICIENT_EVIDENCE
                    error = DevError(
                        schema_version="dev_error.v1",
                        request_id=request.request_id,
                        code="scope_not_found",
                        safe_message="The requested scope was not found.",
                        retryable=False,
                    )
                else:
                    state = RunState.FAILED
                    error = DevError(
                        schema_version="dev_error.v1",
                        request_id=request.request_id,
                        code="internal_error",
                        safe_message="The request could not be completed.",
                        retryable=False,
                    )
            # CHAOS-3297 Codex review round 3 Finding 2: materialize and
            # validate the terminal error input before any record_*() write
            # is attempted -- not after answer/frame are already flushed on
            # the session. With the persistence-layer fix (round 3 CLASS A:
            # terminal_error_payload's acceptance predicate is
            # DevError.model_validate() succeeding, not a byte cap), this
            # check is cheap and should never actually fire in practice --
            # `error` is already a validated DevError instance by
            # construction everywhere it is built in this module. It exists
            # so a defect that somehow produces an invalid DevError is
            # caught here, before any write, rather than discovered only
            # when update_run's own validation fires after other artifacts
            # are already on the session.
            if error is not None:
                try:
                    DevError.model_validate(error.model_dump(mode="json"))
                except ValidationError as terminal_error_fault:
                    # CHAOS-3334 (folded in from CHAOS-3332's review): the
                    # sibling of the answer-write rewrite below, and silent
                    # for the same reason -- handled locally, so run()'s
                    # catch-all never sees it. If it ever does fire it means a
                    # producer built a DevError this module cannot serialize,
                    # which is a structural defect in *this* package; the
                    # comment above calls it unreachable in practice, and an
                    # unreachable branch that fires unlogged is exactly how a
                    # defect stays invisible for a wave. Logged and counted so
                    # "should never happen" is a claim an operator can check
                    # rather than a claim the code makes about itself.
                    logger.exception(
                        "ask_dev.orchestrator.terminal_error_rewrite",
                        extra={
                            "run_id": run_id,
                            "exception_type": type(terminal_error_fault).__name__,
                            "rejected_code": error.code,
                        },
                    )
                    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                        exception_type=type(terminal_error_fault).__name__
                    ).inc()
                    error = DevError(
                        schema_version="dev_error.v1",
                        request_id=request.request_id,
                        code="internal_error",
                        safe_message="The request could not be completed.",
                        retryable=False,
                    )
                    answer = None
            if answer is not None:
                try:
                    await self._recorder.record_answer(answer)
                except Exception as answer_write_fault:
                    # CHAOS-3332 Codex review (MED): this rewrite is the same
                    # invisible-failure class the rest of this ticket removes,
                    # and it is the one instance the new catch-all cannot
                    # cover -- the exception is handled *locally* and never
                    # propagates to the bottom of run(), so without this log a
                    # validated answer being lost to a database failure is
                    # indistinguishable from any other internal_error, with no
                    # exception type and no traceback anywhere.
                    #
                    # It shares ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL with the
                    # catch-all deliberately: both mean "a run terminated
                    # internal_error for a reason no branch anticipated", which
                    # is the alert an operator actually wants, and the distinct
                    # log message keeps the two separable when triaging.
                    logger.exception(
                        "ask_dev.orchestrator.answer_write_fault",
                        extra={
                            "run_id": run_id,
                            "exception_type": type(answer_write_fault).__name__,
                        },
                    )
                    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                        exception_type=type(answer_write_fault).__name__
                    ).inc()
                    state = RunState.FAILED
                    answer = None
                    error = DevError(
                        schema_version="dev_error.v1",
                        request_id=request.request_id,
                        code="internal_error",
                        safe_message="The validated answer could not be stored.",
                        retryable=True,
                    )
            error_message_written = False
            if answer is None and error is not None:
                # CHAOS-3423: a deliberate second check, not `elif` chained
                # to the block above -- the answer-write-failure branch just
                # above can itself rewrite `answer` to None and mint a fresh
                # `error`, and that terminal needs a transcript row exactly
                # as much as one that started life as an error. Every
                # no-answer terminal in this module reaches here with a
                # non-None `error` (every `finish()` call site either passes
                # one or the outer catch-all builds one), so this is
                # effectively unconditional for that whole class of run --
                # never only the clarification/preflight shape.
                try:
                    await self._recorder.record_error_message(
                        error, scope_snapshot=request.scope.model_dump(mode="json")
                    )
                    # CHAOS-3423 Codex adversarial review (HIGH, confirmed):
                    # tracked so the frame-write-failure rollback below can
                    # tell "nothing written in this flush yet" (the
                    # pre-existing case its own `if answer is None: rollback()`
                    # was written for) apart from "the no-answer transcript
                    # row already flushed" -- a plain session.rollback() here
                    # is NOT commit-scoped (this row isn't durable until the
                    # caller's own session.commit() at the very end of the
                    # request), so it would silently discard this row on a
                    # frame-construction/write failure, reintroducing the
                    # exact "no transcript row for a no-answer terminal" gap
                    # this method exists to close.
                    error_message_written = True
                except Exception as error_message_write_fault:
                    # Best-effort, mirroring record_frame's own failure
                    # handling below: a transcript-row write failure must
                    # never crash or further degrade an otherwise-terminal
                    # run. `error` is already authoritative for replay via
                    # `terminal_error_payload` (persisted by `terminal()`
                    # below, which reads that column directly, never this
                    # row) -- only this turn's transcript/prompt-history
                    # rendering is degraded.
                    logger.exception(
                        "ask_dev.orchestrator.error_message_write_fault",
                        extra={
                            "run_id": run_id,
                            "exception_type": type(error_message_write_fault).__name__,
                        },
                    )
                    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                        exception_type=type(error_message_write_fault).__name__
                    ).inc()
            # CHAOS-3297 stack #4: populated only inside the frame-construction
            # branch below (frame_already_recorded=True has no local `frame`
            # to synthesize a narrative from -- the preflight TERMINATE branch
            # doesn't attach a narrative today, a documented gap, not silently
            # dropped: see the module docstring). Stay None (migration 0078's
            # documented default) for every other path.
            narrative_mode: str | None = None
            narrative_failure_code: str | None = None
            # CHAOS-3297 P1: every terminal path persists a dev_answer_frame.v1,
            # structurally rather than by caller discipline -- the preflight
            # TERMINATE branch already built and recorded a richer frame ahead
            # of this call (frame_already_recorded=True); every other path
            # gets a minimal compatibility frame built here, from exactly the
            # v1 payload above -- never a second, divergent source of truth.
            # Deliberately NOT routed through the v2-to-v1 projector in the
            # other direction (frame -> v1): see terminal_frames.py's module
            # docstring for why that would silently rewrite v1 error codes.
            if not frame_already_recorded:
                terminal_code = error.code if error is not None else "internal_error"
                try:
                    frame = (
                        terminal_frames.wrap_legacy_answer_as_frame(
                            answer,
                            run_id=run_id,
                            investigation_result=investigation_result,
                            subject_set_ref=portfolio_subject_set_ref,
                        )
                        if answer is not None
                        else terminal_frames.build_error_frame(
                            code=terminal_code,
                            run_id=run_id,
                            generated_at=datetime.now(UTC),
                            versions=self._versions,
                        )
                    )
                except Exception as frame_construction_exc:
                    # CHAOS-3297 Codex review HIGH #2 / round 2 MEDIUM #2:
                    # frame *construction* failing must never crash or
                    # discard an otherwise-successful run -- the v1
                    # answer/error above is already recorded and remains
                    # authoritative regardless of whether this compatibility
                    # frame builds cleanly (see the module docstring). But
                    # the two ways construction can fail are different
                    # failure classes and must be distinguishable, not
                    # folded into one silent, unlabeled downgrade:
                    #
                    # * UnregisteredTerminalCode: a closed-registry gap (a
                    #   producer added a new code without updating
                    #   terminal_frames.ORCHESTRATOR_ERROR_CODES) -- a
                    #   structural bug in *this* package, logged and counted
                    #   under its own signal so it is never silently
                    #   invisible to an operator (round 2 finding: the
                    #   original bare `except ValueError` conflated this
                    #   with everything else below and logged nothing).
                    # * anything else (e.g. wrap_legacy_answer_as_frame
                    #   rejecting a validated v1 answer's evidence shape) --
                    #   a genuine, unrelated construction problem, logged
                    #   under its own distinct signal rather than being
                    #   misreported as a registry gap.
                    #
                    # Both fall back to the same always-registered
                    # "internal_error" bucket so the run still gets a frame
                    # either way -- the fallback behavior is identical, only
                    # the operational signal differs.
                    if isinstance(
                        frame_construction_exc, terminal_frames.UnregisteredTerminalCode
                    ):
                        logger.error(
                            "ask_dev.orchestrator.unregistered_terminal_code",
                            extra={"run_id": run_id, "terminal_code": terminal_code},
                        )
                        ASK_DEV_UNREGISTERED_TERMINAL_CODE_TOTAL.labels(
                            code=terminal_code
                        ).inc()
                    else:
                        logger.error(
                            "ask_dev.orchestrator.frame_construction_failed",
                            extra={
                                "run_id": run_id,
                                "exception_type": type(frame_construction_exc).__name__,
                            },
                        )
                    frame = terminal_frames.build_error_frame(
                        code="internal_error",
                        run_id=run_id,
                        generated_at=datetime.now(UTC),
                        versions=self._versions,
                    )
                try:
                    await self._recorder.record_frame(frame)
                except Exception:
                    # A frame-construction or database-layer failure here must
                    # never strand or crash an otherwise-successful run -- the
                    # v1 answer/error above is authoritative and safe to
                    # terminate on alone.
                    if answer is None and not error_message_written:
                        # No TRANSCRIPT row in this flush to protect (the
                        # answer path never reaches here, and
                        # record_error_message above failed), so a rollback
                        # cannot discard the one row that is unrecoverable,
                        # and it buys an inline terminal if the session is
                        # poisoned -- otherwise the terminal() write below
                        # could raise PendingRollbackError (mirrors the
                        # preflight TERMINATE branch, CHAOS-3297 Codex
                        # review).
                        #
                        # CHAOS-3441 Codex adversarial review round 1
                        # (MEDIUM, confirmed as to fact, fix declined with
                        # reasoning): "no prior write to protect" was too
                        # broad a claim and is corrected here. This rollback
                        # DOES discard this run's already-flushed forensic
                        # rows -- stage diagnostics, tool calls, the
                        # resolution ledger, subject sets, intent -- and
                        # unlike the two preflight rollback sites below,
                        # nothing re-persists them afterward. It is kept
                        # anyway, and round 3's frequency objection does not
                        # survive contact with what a poisoned session
                        # actually means. Two cases, and only one costs
                        # anything:
                        #
                        # * Session POISONED -- one earlier failure is enough:
                        #   none of the telemetry writes (record_run_
                        #   diagnostics, append_tool_call, append_resolution,
                        #   record_subject_set, record_intent) is
                        #   savepoint-isolated, and a mid-flush failure in any
                        #   of them makes record_error_message and
                        #   record_frame fail in turn, landing here with
                        #   error_message_written False. Those forensic rows
                        #   are ALREADY unrecoverable: the transaction cannot
                        #   commit, so nothing flushed on it will ever land,
                        #   with or without this rollback. The rollback
                        #   discards nothing that had a future, and buys back
                        #   a specific, user-visible terminal
                        #   (`scope_not_found`, a clarification) in place of a
                        #   generic `internal_error` from
                        #   streaming.stream_orchestrator plus a
                        #   force_terminal_fallback hop. Proven by
                        #   test_chaos_3441_a_poisoned_session_has_already_lost_its_rows.
                        # * Session HEALTHY -- only here does the rollback
                        #   destroy rows that would otherwise have committed,
                        #   and reaching it takes TWO independent failures:
                        #   record_error_message failing inside its own
                        #   savepoint AND record_frame failing inside its own.
                        #
                        # The incremental cost is the two-failure case; the
                        # benefit covers the one-failure case. Telling them
                        # apart in code would mean asking the session whether
                        # it needs a rollback, and that cannot live inside
                        # PersistenceRunRecorder.rollback(): the two preflight
                        # call sites below re-persist rows immediately after
                        # their own rollback, so a rollback that silently
                        # became a no-op would double-write them.
                        await self._recorder.rollback()
                    # else: record_answer (answer is not None) or
                    # record_error_message (CHAOS-3423 Codex adversarial
                    # review, HIGH: error_message_written) already succeeded
                    # in this flush. Rolling back here would discard that
                    # write over an unrelated frame failure -- a dropped
                    # frame is recoverable, a dropped answer/transcript row
                    # is not, so this path deliberately leaves the session
                    # as-is and proceeds frame-less.
                    #
                    # CHAOS-3441: leaving the session as-is is safe for a real
                    # mid-flush database failure too, not just a pre-flush
                    # construction one. `record_frame` -- like
                    # `record_narrative` and `append_assistant_answer`/
                    # `append_assistant_error` -- runs inside a SAVEPOINT
                    # (persistence/service.py), and that savepoint now opens
                    # before its ownership/authorization SELECTs as well as
                    # its flush. Given a caller that enters with nothing
                    # unflushed -- which the transcript writes now guarantee,
                    # each of them touching its conversation inside its own
                    # savepoint (test_chaos_3441_transcript_writes_leave_no_
                    # unflushed_state) -- no failure record_frame can raise
                    # leaves this session rollback-only: the answer/error
                    # transcript row already flushed above survives, and the
                    # `terminal()` write below still lands. The one exception
                    # is stated where it lives: pending state a caller DOES
                    # leave is flushed at savepoint entry, before the
                    # SAVEPOINT is emitted, so a failure there is outside
                    # every savepoint (Codex adversarial review round 2).
                    # Proven for BOTH transcript paths against a REAL
                    # database failure, each proof failing if that
                    # `begin_nested` is removed --
                    # test_chaos_3423_record_frame_integrity_failure_never_poisons_the_session
                    # (no-answer row, duplicate-insert IntegrityError) and
                    # tests/api/dev/test_chaos_3441_record_frame_savepoint.py
                    # (real `DevAnswer` row, a driver-level write rejection
                    # plus the savepoint-boundary and diagnostics-survival
                    # proofs).
                    #
                    # What no savepoint can cover, stated plainly: if the
                    # CONNECTION dies there is no session left to roll back,
                    # and an uncommitted transcript row is lost by
                    # definition -- recovered at the system level only, by
                    # `DevPersistenceService.force_terminal_fallback`'s fresh
                    # session (CHAOS-3297 round 3 Finding 2).
                else:
                    # CHAOS-3297 stack #4: narrative synthesis only runs for
                    # a frame that actually persisted -- record_narrative's
                    # own frame_id cross-check (persistence/service.py)
                    # would otherwise always reject against a frame_id
                    # nothing wrote -- and only for a content-bearing
                    # outcome (see the narrative_mode/narrative_failure_code
                    # declaration above the frame-construction block for the
                    # no-answer-outcome rationale).
                    if frame.public_outcome not in NO_ANSWER_OUTCOMES:
                        (
                            narrative,
                            failure_code,
                        ) = await narrative_fallback.synthesize_narrative(
                            frame=frame,
                            # self._narrative_provider is None until
                            # CHAOS-3285 certifies one (a configuration
                            # state, not a provider failure --
                            # synthesize_narrative goes straight to the
                            # deterministic fallback with no failure
                            # code in that case) or a test injects a
                            # scripted one.
                            provider=self._narrative_provider,
                            generated_at=datetime.now(UTC),
                        )
                        try:
                            await self._recorder.record_narrative(narrative)
                        except Exception as narrative_write_fault:
                            # codex NO-SHIP finding round 1 (HIGH #2b): a
                            # narrative sub-artifact write failure must
                            # never strand or crash an otherwise-successful
                            # run -- but dev_runs must also never CLAIM a
                            # narrative_mode/narrative_failure_code for a
                            # row that was never durably written (the
                            # original defect: the contract's LongText body
                            # cap is looser than persistence's own byte
                            # bound, so a contract-valid narrative could be
                            # rejected here while terminal() still recorded
                            # "deterministic_fallback" as if it had
                            # succeeded). narrative_mode/narrative_failure_code
                            # are set ONLY in the success branch below, so
                            # they stay at their None default here -- an
                            # honest "no narrative recorded" signal, not a
                            # false claim. No rollback needed: record_narrative
                            # isolates its own flush behind a SAVEPOINT
                            # (persistence/service.py), so the session is
                            # already clean by the time this handler runs --
                            # the frame/answer already committed earlier in
                            # this flush are untouched.
                            logger.error(
                                "ask_dev.orchestrator.narrative_persistence_failed",
                                extra={
                                    "run_id": run_id,
                                    "exception_type": type(
                                        narrative_write_fault
                                    ).__name__,
                                },
                            )
                        else:
                            narrative_mode = narrative.mode
                            narrative_failure_code = (
                                failure_code.value if failure_code is not None else None
                            )
            await self._recorder.terminal(
                state=state,
                answer=answer,
                error=error,
                usage=budget.usage,
                tool_call_count=len(tool_results),
                provider_fingerprint=provider_fingerprint,
                model_fingerprint=model_fingerprint,
                prompt_checksum=prompt_checksum,
                prompt_version=prompt_version,
                narrative_mode=narrative_mode,
                narrative_failure_code=narrative_failure_code,
                # CHAOS-3297 stack #5: ``None`` on every ordinary path, which
                # keeps the recorder's own "passed"/"not_applicable" default.
                # Only a demoted-guard terminal names a value here -- a run
                # that shipped a server-grounded result *because* a guard
                # rejected the model's summary must not be recorded as
                # having simply "passed" grounding validation.
                grounding_validation_status=grounding_validation_status,
            )
            terminal_written = True
            event = OrchestratorEvent(
                state=state, safe_code=error.code if error else None
            )
            events.append(event)
            if selected_event_sink is not None:
                await selected_event_sink(event)
            return OrchestratorResult(
                run_id=run_id,
                state=state,
                answer=answer,
                error=error,
                events=tuple(events),
                usage=budget.usage,
                tool_call_count=len(tool_results),
                provider_fingerprint=provider_fingerprint,
                model_fingerprint=model_fingerprint,
                # CHAOS-3497: the run's own scope decision, on EVERY terminal.
                #
                # On the answer path this is byte-identical to what
                # ``streaming`` already published (``answer.resolved_scope``),
                # so that path's wire output does not change at all.
                #
                # On a no-answer terminal it is ``published_resolution()``
                # below -- the resolution this run actually executed under.
                scope_resolution=(
                    answer.resolved_scope
                    if answer is not None
                    else published_resolution()
                ),
            )

        def error(code: str, message: str, *, retryable: bool = False) -> DevError:
            return DevError(
                schema_version="dev_error.v1",
                request_id=request.request_id,
                code=code,
                safe_message=message,
                retryable=retryable,
            )

        def remaining() -> float:
            return self._limits.wall_seconds - (self._monotonic() - started)

        await transition(RunState.ACCEPTED)
        try:
            if cancellation.is_set():
                return await finish(
                    RunState.CANCELLED,
                    error=error("cancelled", "The request was cancelled."),
                )
            await transition(RunState.RESOLVING_SCOPE)
            resolution = await self._resolve_with_cancellation(
                org_id=org_id,
                user_id=user_id,
                requested_scope=request.scope,
                cancellation=cancellation,
                timeout_seconds=max(0, remaining()),
            )
            if resolution.outcome is ScopeResolutionOutcome.AMBIGUOUS:
                return await finish(
                    RunState.INSUFFICIENT_EVIDENCE,
                    error=error("scope_ambiguous", "The requested scope is ambiguous."),
                )
            if resolution.outcome in {
                ScopeResolutionOutcome.UNRESOLVED,
                ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
            }:
                return await finish(
                    RunState.INSUFFICIENT_EVIDENCE,
                    error=error(
                        "scope_not_found", "The requested scope was not found."
                    ),
                )
            authorized_scope = resolution.resolved_scope
            if authorized_scope is None or authorized_scope.organization_id != org_id:
                return await finish(
                    RunState.FAILED,
                    error=error(
                        "scope_forbidden", "The requested scope is not authorized."
                    ),
                )

            # CHAOS-3292 preflight: interpret the question and resolve every
            # named subject against the authorized catalog *before* the first
            # model round, so no evidence-bearing tool can execute without an
            # exact committed subject. Zero provider tokens are spent here.
            preflight_result: SubjectPreflightResult | None = None
            allowed_tools: frozenset[ToolID] = frozenset(ToolID)
            if self._preflight is not None:
                if cancellation.is_set():
                    return await finish(
                        RunState.CANCELLED,
                        error=error("cancelled", "The request was cancelled."),
                    )
                try:
                    preflight_result = await asyncio.wait_for(
                        self._preflight.run(
                            request=request,
                            org_id=org_id,
                            permission_fingerprint=permission_fingerprint,
                            authorized_scope=authorized_scope,
                            run_id=run_id,
                            answer_id=answer_id,
                            conversation_id=conversation_id,
                            on_phase=transition,
                        ),
                        timeout=max(0.0, remaining()),
                    )
                except (TimeoutError, asyncio.TimeoutError):
                    return await finish(
                        RunState.FAILED,
                        error=error(
                            "tool_limit_reached", "The request time limit was reached."
                        ),
                    )
                allowed_tools = preflight_result.allowed_tools
                await self._recorder.record_preflight(
                    preflight_outcome=preflight_result.diagnostic,
                    legacy_guard_reason=None,
                )
                if preflight_result.subject_set is not None:
                    # Persisted regardless of decision: a homogeneous cohort
                    # (D1) still terminates unsupported below, but the set it
                    # committed is recorded either way.
                    await self._recorder.record_subject_set(
                        preflight_result.subject_set
                    )
                if (
                    preflight_result.decision is PreflightDecision.PROCEED
                    and preflight_result.ledger is not None
                ):
                    # CHAOS-3424: until this, dev_run_resolutions was only
                    # ever appended from the TERMINATE branch below (one
                    # entry, the ambiguous mention that produced the
                    # frame's own clarification_candidates) -- every PROCEED
                    # decision left the ledger this preflight already built
                    # entirely unpersisted, including the organization-wide
                    # widening fallback for an unresolved bare name
                    # (`proceeded_unresolved_bare_name`). That is exactly
                    # the run shape a widening/wrong-subject incident needs
                    # to be auditable from data instead of a container log
                    # line (live 2026-08-05 diagnosis, CHAOS-3421). Every
                    # entry, in the ledger's own ordinal order -- not just
                    # the mention(s) that ended up unresolved -- so a
                    # cohort question's exact_match entries stay alongside
                    # the ones that widened, exactly as the ledger recorded
                    # them. Scoped to PROCEED only: TERMINATE's own single
                    # terminating-entry write below already covers that
                    # branch (and shares its `entry_ordinal` with this same
                    # ledger), so persisting the whole ledger there too
                    # would double-insert into the same
                    # (run_id, entry_ordinal) unique constraint.
                    try:
                        for resolution_entry in preflight_result.ledger.entries:
                            await self._recorder.append_resolution(resolution_entry)
                    except Exception as ledger_write_fault:
                        # CHAOS-3424 Codex adversarial review (HIGH,
                        # confirmed): a database-layer failure on ANY entry
                        # here marks the session rollback-only -- every
                        # later write in finish() (record_answer/
                        # record_error_message/terminal) would then raise
                        # PendingRollbackError on its own next flush,
                        # degrading an ordinary PROCEED into an
                        # unrecoverable run over what should be best-effort
                        # forensic telemetry. Mirrors the TERMINATE branch's
                        # own rollback + re-persist pattern immediately
                        # below: a dropped ledger (whole, never partial --
                        # rollback discards every entry this loop already
                        # flushed, not just the one that failed) is
                        # recoverable, a stranded run is not. Re-persists
                        # the preflight diagnostics this same rollback also
                        # discards, for the identical reason the TERMINATE
                        # branch's own comment gives.
                        #
                        # Codex adversarial review round 3 (HIGH, confirmed):
                        # the rollback above was previously silent -- unlike
                        # every sibling failure handler in this module
                        # (answer_write_fault, frame_construction_failed,
                        # error_message_write_fault, ...), a lost ledger
                        # left no log line and no metric, so the exact
                        # forensic gap CHAOS-3424 exists to close could
                        # itself go unnoticed. Logged and counted the same
                        # way every other one of those does.
                        logger.exception(
                            "ask_dev.orchestrator.resolution_ledger_write_fault",
                            extra={
                                "run_id": run_id,
                                "exception_type": type(ledger_write_fault).__name__,
                            },
                        )
                        ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                            exception_type=type(ledger_write_fault).__name__
                        ).inc()
                        await self._recorder.rollback()
                        await self._recorder.record_preflight(
                            preflight_outcome=preflight_result.diagnostic,
                            legacy_guard_reason=None,
                        )
                        if preflight_result.subject_set is not None:
                            await self._recorder.record_subject_set(
                                preflight_result.subject_set
                            )

                if self._qua_shadow is not None:
                    # CHAOS-3389 shadow phase: runs strictly AFTER the
                    # deterministic decision above is fully computed and
                    # persisted, and its result (`shadow_record`) is a local
                    # variable nothing below this block ever reads -- that is
                    # the structural half of "flipping the flag changes zero
                    # live-path behavior" (the RED test in
                    # test_chaos_3389_qua_shadow.py proves the other half:
                    # OrchestratorResult is byte-identical with the shadow on
                    # vs off, even against a provider that always raises).
                    # Every exception from either the evaluation or the
                    # persistence write is caught here -- a shadow-mode bug
                    # must never fail or roll back the run it shadows.
                    try:
                        shadow_record = await self._qua_shadow.evaluate(
                            question=request.question,
                            interpretation=preflight_result.interpretation,
                            org_id=org_id,
                            permission_fingerprint=permission_fingerprint,
                            deterministic_decision=preflight_result.decision,
                            remaining_seconds=remaining(),
                        )
                    except Exception as shadow_fault:
                        logger.exception(
                            "ask_dev.orchestrator.qua_shadow_evaluation_fault",
                            extra={
                                "run_id": run_id,
                                "exception_type": type(shadow_fault).__name__,
                            },
                        )
                        ASK_DEV_QUA_SHADOW_FAULT_TOTAL.labels(
                            exception_type=type(shadow_fault).__name__
                        ).inc()
                        shadow_record = None
                    if shadow_record is not None:
                        ASK_DEV_QUA_SHADOW_TOTAL.labels(
                            status=shadow_record.status.value,
                            deterministic_decision=preflight_result.decision.value,
                        ).inc()
                        if shadow_record.latency_ms > 0:
                            ASK_DEV_QUA_SHADOW_LATENCY_SECONDS.labels(
                                status=shadow_record.status.value
                            ).observe(shadow_record.latency_ms / 1000.0)
                        try:
                            await self._recorder.record_qua_shadow(shadow_record)
                        except Exception as shadow_write_fault:
                            logger.exception(
                                "ask_dev.orchestrator.qua_shadow_write_fault",
                                extra={
                                    "run_id": run_id,
                                    "exception_type": type(shadow_write_fault).__name__,
                                },
                            )
                            ASK_DEV_QUA_SHADOW_FAULT_TOTAL.labels(
                                exception_type=type(shadow_write_fault).__name__
                            ).inc()
                            # Mirrors CHAOS-3424's own ledger-write-fault
                            # recovery immediately above: a failed write can
                            # mark the session rollback-only, so every later
                            # write in finish() (record_answer/terminal/etc.)
                            # must not inherit that state.
                            #
                            # Codex adversarial review round 1 (HIGH,
                            # confirmed): unlike the CHAOS-3424 handler above
                            # -- where the failure IS the ledger write, so
                            # there is nothing valid to replay -- THIS
                            # rollback fires strictly after a PROCEED
                            # decision's resolution ledger already flushed
                            # successfully earlier in this same transaction.
                            # Re-persisting only preflight diagnostics/
                            # subject set (as originally written) would
                            # silently discard those already-good ledger
                            # rows on the rollback below -- the exact
                            # forensic data CHAOS-3424 exists to make
                            # durable, lost by an unrelated OPTIONAL
                            # shadow-record write failing. Replayed here
                            # alongside the other two, in the ledger's own
                            # ordinal order, exactly as the PROCEED block
                            # above first wrote them.
                            # Codex adversarial review round 2 (HIGH,
                            # confirmed): the replay sequence itself used to
                            # be unguarded -- if the underlying fault is a
                            # genuinely dead connection (not just a
                            # constraint violation on the shadow row alone),
                            # `rollback()` or any one of the three replay
                            # calls below can ALSO raise, and that second
                            # exception was not caught here at all. It would
                            # propagate out of this whole block to the
                            # orchestrator's own top-level catch-all
                            # (`unhandled_run_fault`, this module's last
                            # resort), degrading what must stay a routine,
                            # graceful terminal into a generic internal_error
                            # -- turning an OPTIONAL telemetry failure into a
                            # live-outcome change, exactly what this seam
                            # must never do. Wrapped so a second, worse
                            # failure here is recorded distinctly and
                            # swallowed rather than escalated: if recovery
                            # itself cannot succeed, this run's diagnostics
                            # may be incomplete, but its OUTCOME is still
                            # never sacrificed for optional shadow telemetry.
                            try:
                                await self._recorder.rollback()
                                await self._recorder.record_preflight(
                                    preflight_outcome=preflight_result.diagnostic,
                                    legacy_guard_reason=None,
                                )
                                if (
                                    preflight_result.decision
                                    is PreflightDecision.PROCEED
                                    and preflight_result.ledger is not None
                                ):
                                    for (
                                        resolution_entry
                                    ) in preflight_result.ledger.entries:
                                        await self._recorder.append_resolution(
                                            resolution_entry
                                        )
                                if preflight_result.subject_set is not None:
                                    await self._recorder.record_subject_set(
                                        preflight_result.subject_set
                                    )
                            except Exception as shadow_recovery_fault:
                                logger.exception(
                                    "ask_dev.orchestrator.qua_shadow_recovery_fault",
                                    extra={
                                        "run_id": run_id,
                                        "exception_type": type(
                                            shadow_recovery_fault
                                        ).__name__,
                                    },
                                )
                                ASK_DEV_QUA_SHADOW_FAULT_TOTAL.labels(
                                    exception_type=type(shadow_recovery_fault).__name__
                                ).inc()

                    # CHAOS-3525: the promotion seam.
                    #
                    # Deliberately HERE -- after the deterministic decision is
                    # fully computed and persisted, before the branch that acts
                    # on it. CHAOS-3389 built that ordering so the shadow could
                    # observe without influencing; the same position is what lets
                    # a promotion replace the decision without either
                    # re-implementing the preflight or unwinding writes it has
                    # already made.
                    #
                    # Everything below fails closed to "leave the run exactly as
                    # the deterministic layer decided". That is the invariant the
                    # CHAOS-3389 byte-identity tests certify and it must survive
                    # commit mode: an absent, failing, slow, budget-exhausted,
                    # low-confidence or out-of-slice proposal changes nothing.
                    if (
                        shadow_record is not None
                        and self._qua_shadow is not None
                        and self._qua_shadow.commit_enabled
                    ):
                        promotion = promotable_selection(
                            shadow_record,
                            deterministic_declined=_deterministic_declined(
                                preflight_result
                            ),
                        )
                        if promotion is not None and await verify_still_authorized(
                            promotion,
                            scope_service=self._qua_shadow.scope_service,
                            org_id=org_id,
                            permission_fingerprint=permission_fingerprint,
                            limit=MAX_CANDIDATES,
                        ):
                            (
                                preflight_result,
                                pending_resolution_entries,
                            ) = _promoted_preflight_result(
                                preflight_result,
                                promotion=promotion,
                                scope_service=self._qua_shadow.scope_service,
                                org_id=org_id,
                                base_scope=authorized_scope,
                                resolved_at=datetime.now(UTC),
                            )
                            # The disclosure the answer must carry. Held here and
                            # applied in `finish()` rather than written now: the
                            # answer does not exist yet, and a subject chosen by
                            # a model that reached the user unannounced would be
                            # a worse wrong-subject bug than the dead end this
                            # promotion removes.
                            qua_subject_disclosure = (
                                promotion.text_span,
                                promotion.entity.label,
                            )
                            # `allowed_tools` was captured from the ORIGINAL
                            # decision, which for a TERMINATE is the empty
                            # set. Re-read it here or the promoted run
                            # proceeds with a committed subject and no tools
                            # to answer about it -- which fails as an opaque
                            # internal_error, not as anything a reader could
                            # diagnose. (Found by the disclosure test: the
                            # scope committed, the run still did not answer.)
                            allowed_tools = preflight_result.allowed_tools
                            ASK_DEV_QUA_COMMIT_TOTAL.labels(
                                entity_kind=promotion.entity.kind.value
                            ).inc()
                            try:
                                for pending_entry in pending_resolution_entries:
                                    await self._recorder.append_resolution(
                                        pending_entry
                                    )
                                await self._recorder.record_preflight(
                                    preflight_outcome=preflight_result.diagnostic,
                                    legacy_guard_reason=None,
                                )
                            except Exception as qua_commit_write_fault:
                                # Best-effort, exactly like the shadow write
                                # above: the promotion itself already happened in
                                # memory and the run is coherent without this
                                # row. Logged rather than swallowed silently so a
                                # missing `committed_qua_subject` diagnostic is
                                # never mistaken for a run that did not promote.
                                logger.exception(
                                    "ask_dev.orchestrator.qua_commit_write_fault",
                                    extra={
                                        "run_id": run_id,
                                        "exception_type": type(
                                            qua_commit_write_fault
                                        ).__name__,
                                    },
                                )
                                ASK_DEV_QUA_SHADOW_FAULT_TOTAL.labels(
                                    exception_type=type(qua_commit_write_fault).__name__
                                ).inc()

                if preflight_result.decision is PreflightDecision.TERMINATE:
                    assert preflight_result.answer is not None
                    assert preflight_result.outcome is not None
                    preflight_error = project_preflight_error(
                        preflight_result.answer, request_id=request.request_id
                    )
                    # CHAOS-3388 codex re-review (HIGH, confirmed): the
                    # candidate display labels this error's ``safe_message``
                    # may interpolate (``project_preflight_error`` /
                    # ``_name_candidates``) are the SAME catalog-confirmed
                    # entities the frame below persists as
                    # ``clarification_candidates`` -- authorized data, the
                    # same trust tier ``finish()``'s own ``attested_strings``
                    # already exempts off ``DevAnswer``. This path builds no
                    # ``DevAnswer`` (it is error-only), so without this the
                    # exemption never applied and a candidate label that
                    # happened to contain a denylisted token (a real live
                    # shape -- see ``no_match_terminal.internal_token_leak``'s
                    # own docstring example) fail-closed rewrote the terminal
                    # to a bare ``internal_error`` *after* the richer frame
                    # below was already recorded, leaving the two
                    # permanently inconsistent.
                    #
                    # The fix decides frame persistence and the terminal
                    # error from the SAME leak scan ``finish()`` will run, so
                    # the two can never disagree: attest the candidate labels
                    # up front, and if the scan still flags something despite
                    # that (a genuine leak, not a candidate-label collision),
                    # skip the richer frame entirely rather than persist one
                    # the terminal below will contradict.
                    candidate_attestation = tuple(
                        candidate.entity_ref.display_label
                        for candidate in preflight_result.answer.frame.clarification_candidates
                    )
                    # CHAOS-3497: built from the SAME frame the richer
                    # terminal below persists, through the SAME projector
                    # `compat` already uses for a replayed clarification --
                    # so the resolution on the wire and the candidates the
                    # user was shown can never be two different stories.
                    # Set before every `finish()` in this branch, including
                    # the leak-guard bail-out: a run whose terminal was
                    # rewritten to `internal_error` still resolved what it
                    # resolved, and hiding that is how the gap this ticket
                    # closes got there in the first place.
                    #
                    # CHAOS-3534: a COMPLETE committed cohort is the one
                    # terminal the frame cannot describe. `scope_resolution_
                    # from_frame` has three branches -- candidates, a single
                    # `subject_ref`, else UNRESOLVED -- and a cohort frame
                    # carries neither of the first two, so it fell through and
                    # published "unresolved" for a run that resolved EVERY
                    # named subject exactly and committed a real
                    # dev_subject_set.v1. That is the same defect CHAOS-3497
                    # exists to fix (a non-answer terminal misreporting its
                    # own scope decision), one branch short.
                    #
                    # Sourced from the preflight's own subject set rather than
                    # re-derived from the frame, because the frame
                    # STRUCTURALLY cannot carry a cohort -- re-deriving from
                    # it could only ever produce the wrong answer.
                    #
                    # Gated on `cohort_complete`, whose own contract validator
                    # guarantees it is False whenever any mention was omitted:
                    # a partial cohort did NOT resolve everything the question
                    # named, and calling it exact would be the same false
                    # statement pointing the other way.
                    #
                    # Restricted to REPOSITORY cohorts because v1 cannot
                    # represent any other kind as a multi-entity scope --
                    # DevScope requires exactly one entity_ref for
                    # project/team/issue/PR/work-unit, while `repositories`
                    # is a list. A project cohort therefore keeps today's
                    # frame-derived outcome; that residual is real, is NOT
                    # fixed here, and is tracked rather than papered over.
                    #
                    # And bounded by V1_SCOPE_LIST_LIMIT, because the subject
                    # set's own bound is LARGER than v1's: a DevSubjectSet may
                    # commit up to 25 refs, while DevScope.repositories and
                    # DevScopeResolution.authorized_repository_ids both cap at
                    # 20. Without this a fully-resolved, fully-authorized
                    # 21-25 repository cohort raises during terminal
                    # construction -- the SAME "legal upstream, illegal
                    # downstream" defect as the project-kind case above, at a
                    # different boundary, and it would have turned an honest
                    # feature_not_enabled into an opaque internal_error.
                    # (Codex adversarial review, HIGH; reproduced before
                    # fixing.) An oversized cohort keeps the frame-derived
                    # outcome: under-disclosure is the honest failure here,
                    # since v1 genuinely cannot list what was committed.
                    cohort = preflight_result.subject_set
                    if (
                        cohort is not None
                        and cohort.cohort_complete
                        and cohort.entity_kind is ContractEntityKind.REPOSITORY
                        and len(cohort.committed_entity_refs) <= V1_SCOPE_LIST_LIMIT
                    ):
                        preflight_terminal_resolution = (
                            ScopeResolutionService.committed_cohort_resolution_for(
                                cohort,
                                org_id=org_id,
                                base_scope=authorized_scope,
                                resolved_at=datetime.now(UTC),
                            )
                        )
                    else:
                        preflight_terminal_resolution = scope_resolution_from_frame(
                            preflight_result.answer.frame,
                            requested_scope=authorized_scope,
                            resolved_at=datetime.now(UTC),
                        )
                    preflight_leak = internal_token_leak_field(
                        user_visible_strings_by_field(error=preflight_error),
                        attested=attested_strings(None, request.question)
                        + candidate_attestation,
                    )
                    if preflight_leak is not None:
                        leaked_field, leaked_token = preflight_leak
                        logger.error(
                            "ask_dev.orchestrator.preflight_frame_leak_guard "
                            f"run_id={run_id} field={leaked_field} "
                            f"token={leaked_token}",
                            extra={
                                "run_id": run_id,
                                "token": leaked_token,
                                "field": leaked_field,
                            },
                        )
                        ASK_DEV_INTERNAL_TOKEN_LEAK_TOTAL.labels(
                            token=leaked_token, terminal_kind="error"
                        ).inc()
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "internal_error",
                                "The request could not be completed.",
                            ),
                        )
                    # CHAOS-3297: persist the frame the preflight already
                    # built *before* finishing the run, so the terminal
                    # state and the frame's contract_generation='v2' tag
                    # land together -- mirrors record_answer's placement
                    # ahead of terminal() on the completed-answer path.
                    try:
                        # CHAOS-3325 Codex review (NO-SHIP, confirmed): the
                        # terminating ledger entry -- the one that produced
                        # the frame's own clarification_candidates -- is
                        # persisted first, in the same try/rollback unit, so
                        # record_frame's cross-check
                        # (_authorize_clarification_candidates) always has a
                        # real ledger row to authorize against rather than
                        # racing it.
                        #
                        # CHAOS-3533: the WHOLE ledger is written here now,
                        # not only that one entry -- the same forensic
                        # argument CHAOS-3424 made for the PROCEED branch
                        # (see its comment above), applied to the half where
                        # the run declined to answer. A terminate that
                        # resolved "no authorized match", could not reach the
                        # catalog, or committed a cohort v1 cannot render
                        # previously left NO trace of any of it. Ordinals come
                        # from the same ledger, so the (run_id, entry_ordinal)
                        # unique constraint is satisfied by construction, and
                        # the QUA promotion above has already converted any
                        # run it rewrote to PROCEED -- so neither branch can
                        # double-insert.
                        #
                        # The terminating mention is passed EXPLICITLY rather
                        # than left for the persistence layer to infer: with a
                        # whole ledger on disk, "the highest-ordinal ambiguous
                        # row" is no longer a synonym for "the mention this
                        # run offered the user".
                        if preflight_result.ledger is not None:
                            for resolution_entry in preflight_result.ledger.entries:
                                await self._recorder.append_resolution(resolution_entry)
                    except Exception as terminate_ledger_write_fault:
                        # Codex adversarial review (HIGH, CONFIRMED BY
                        # EXECUTION): the ledger write and the frame write
                        # shared ONE try/except, and the finish() below was
                        # told `frame_already_recorded=True` unconditionally.
                        # A failing append_resolution therefore skipped
                        # record_frame AND suppressed the v1 compatibility
                        # frame finish() would otherwise build -- the run
                        # landed with no ledger and no dev_answer_frames row.
                        #
                        # Reachability is what THIS change introduced: before
                        # it a not-found or committed-cohort terminate called
                        # append_resolution zero times, so it could not fail
                        # here and always kept its frame. Widening the ledger
                        # write to every terminate would have widened that
                        # failure mode with it -- a hardening change that
                        # invents a new way to lose a terminal under stress is
                        # the very class CHAOS-3533 exists to close.
                        #
                        # Split out, and given the same operational signal its
                        # PROCEED-branch sibling already carries (CHAOS-3424
                        # round 3: a lost ledger that logs nothing means the
                        # forensic gap it closes can itself go unnoticed).
                        logger.exception(
                            "ask_dev.orchestrator.resolution_ledger_write_fault",
                            extra={
                                "run_id": run_id,
                                "exception_type": type(
                                    terminate_ledger_write_fault
                                ).__name__,
                            },
                        )
                        ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                            exception_type=type(terminate_ledger_write_fault).__name__
                        ).inc()
                        await self._recorder.rollback()
                        # The rollback discards every unflushed write on this
                        # session, not only the poisoned ledger -- including
                        # the record_preflight() diagnostic AND the committed
                        # subject set flushed above, which share this same
                        # transaction. Both are re-persisted, exactly as the
                        # PROCEED-branch handler already does for both.
                        #
                        # The subject set matters most on the committed-cohort
                        # terminate: that run resolved every named member
                        # exactly and committed a real dev_subject_set.v1, so
                        # losing it to an unrelated ledger fault would erase
                        # the only surviving record that the cohort resolved
                        # at all -- the same forensic hole one layer up.
                        await self._recorder.record_preflight(
                            preflight_outcome=preflight_result.diagnostic,
                            legacy_guard_reason=None,
                        )
                        if preflight_result.subject_set is not None:
                            await self._recorder.record_subject_set(
                                preflight_result.subject_set
                            )
                    try:
                        await self._recorder.record_frame(
                            preflight_result.answer.frame,
                            authorizing_mention_id=(
                                preflight_result.terminating_resolution_entry.mention_id
                                if preflight_result.terminating_resolution_entry
                                is not None
                                else None
                            ),
                        )
                    except Exception as record_frame_fault:
                        # A database-layer failure here (constraint
                        # violation, dropped connection) marks the
                        # recorder's session rollback-only; the terminal()
                        # write below would then raise PendingRollbackError
                        # and strand this run as a nonterminal accepted/v1
                        # row that every idempotent retry 409s against
                        # forever (Codex review finding, CHAOS-3297). Roll
                        # back and finish as a coherent v1 terminal run
                        # instead -- a dropped frame is recoverable, a
                        # stranded run is not.
                        #
                        # CHAOS-3550: this used to be a bare `except
                        # Exception`, so a PROGRAMMING error here (a
                        # contract-signature drift, an AttributeError, a
                        # KeyError) was indistinguishable from the database
                        # fault the comment above actually describes -- the
                        # run finished as an ordinary-looking terminal with
                        # its frame silently absent and no signal anywhere.
                        # The rollback + re-persist below are unconditional
                        # (needed for EITHER cause, to keep the session
                        # usable for whichever finish() eventually runs --
                        # this one on the expected-fault path, or the
                        # run-loop's own last-resort handler below on the
                        # re-raise path); only the swallow-vs-surface
                        # decision after them is now typed.
                        await self._recorder.rollback()
                        # The rollback above discards every unflushed write
                        # on this session, not just the poisoned frame --
                        # including the record_preflight() diagnostic
                        # flushed a few lines above, which shares this same
                        # transaction. Re-persist it before finish() writes
                        # the terminal row, or the run lands with
                        # preflight_outcome=None and silently loses the
                        # closed-vocabulary explanation of why it
                        # terminated (Codex review finding, CHAOS-3297).
                        #
                        # CHAOS-3550 / CHAOS-3544 interaction (team-lead
                        # ruling): if THIS run's own dev_runs row was
                        # cascade-deleted out from under it (a wedged run
                        # whose 0-day conversation aged out and was purged
                        # mid-flight -- CHAOS-3544), this re-persist call
                        # itself raises DevPersistenceNotFound before the
                        # typed check below ever runs, and that exception
                        # (not record_frame_fault) is what the run-loop's
                        # own last-resort handler (`except Exception as
                        # unhandled` further down) logs and counts. That is
                        # deliberate, not a gap this ticket closes: a run
                        # whose row no longer exists must not report success
                        # either way, and the zombie-run integration test
                        # below exists to keep proving the last-resort
                        # handler is what actually observes it.
                        await self._recorder.record_preflight(
                            preflight_outcome=preflight_result.diagnostic,
                            legacy_guard_reason=None,
                        )
                        if not isinstance(record_frame_fault, SQLAlchemyError):
                            logger.exception(
                                "ask_dev.orchestrator.record_frame_programming_error",
                                extra={
                                    "run_id": run_id,
                                    "exception_type": type(record_frame_fault).__name__,
                                },
                            )
                            ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                                exception_type=type(record_frame_fault).__name__
                            ).inc()
                            raise
                        # Expected path: a genuine database-layer fault,
                        # recovered and swallowed exactly as before this
                        # ticket. Previously this branch emitted NO signal
                        # at all -- unlike its sibling ledger-write-fault
                        # handler above (CHAOS-3533), which has always
                        # logged and counted its own DB-adjacent faults
                        # uniformly, expected or not. Matched here for the
                        # same reason: a recovered fault is still a fault an
                        # operator should be able to see happened, and a
                        # log line costs nothing compared to the silence
                        # this whole ticket exists to close.
                        logger.warning(
                            "ask_dev.orchestrator.record_frame_recovered_fault",
                            extra={
                                "run_id": run_id,
                                "exception_type": type(record_frame_fault).__name__,
                            },
                        )
                        ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                            exception_type=type(record_frame_fault).__name__
                        ).inc()
                    return await finish(
                        TERMINAL_STATE_BY_OUTCOME[preflight_result.outcome],
                        error=preflight_error,
                        # Deliberately still unconditional, and NOT switched to
                        # "did the frame actually land". Making it honest was
                        # tried and reverted: when record_frame ITSELF is what
                        # failed, finish() would retry the same doomed insert,
                        # fail again, and roll back the record_preflight()
                        # diagnostic the handler above just re-persisted --
                        # regressing the CHAOS-3297 guarantee that a run never
                        # loses its closed-vocabulary reason for terminating
                        # (caught by test_preflight_frame_flush_failure_reaches
                        # _terminal_state_not_stranded, which is exactly why
                        # that test exists). The Codex HIGH is closed by the
                        # SPLIT above instead: a ledger fault no longer skips
                        # record_frame, so the frame lands on the path that
                        # previously lost it. A frame-write fault remains what
                        # CHAOS-3297 decided it should be -- a dropped frame,
                        # never a stranded run and never a lost diagnostic.
                        frame_already_recorded=True,
                        extra_attested=candidate_attestation,
                    )
                if preflight_result.committed_resolution is not None:
                    committed = preflight_result.committed_resolution
                    committed_scope = committed.resolved_scope
                    if committed_scope is None or (
                        committed_scope.organization_id != org_id
                    ):  # pragma: no cover - committed_resolution_for always commits
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "scope_forbidden",
                                "The requested scope is not authorized.",
                            ),
                        )
                    # The model is *given* the subject rather than asked to
                    # earn it: composer serializes ``resolved_scope`` into the
                    # prompt, so this is the commit that removes the "narrate
                    # an unresolved name" opportunity entirely.
                    resolution = committed
                    authorized_scope = committed_scope

            # CHAOS-3295: deterministic plan-governed investigation. Placed
            # strictly after the CHAOS-3292 preflight block above (so a
            # SINGULAR-cardinality plan can never reach a canonical service
            # before the named subject is committed — the literal
            # acceptance criterion) and strictly before the model round
            # loop below (so mandatory server steps never consume a
            # provider tool-decision round). Additive by construction: it
            # calls canonical services directly, never through
            # ``self._registry``/``tool_results``/``budget``, so it cannot
            # change what the model loop below observes or how many tool
            # calls it is charged for.
            if self._plan_executor is not None and preflight_result is not None:
                intent = preflight_result.interpretation.intent
                plan = self._plan_registry.get(intent.intent_id)
                # CHAOS-3300 finding (2026-08-02): a plan-eligible-by-vocabulary
                # intent whose plan_registry.get(...) returns None silently
                # falls through to the legacy model-tool-choice loop below --
                # a real capability downgrade (a status_snapshot.v1-only
                # answer instead of a governed health/deficiency/portfolio
                # evaluation) that produced no signal anywhere. Distinguish it
                # from BOUNDED_INVESTIGATION, whose fallthrough is the
                # DESIGNED behavior (preflight_outcomes.LEGACY_ONLY_QUESTION_INTENTS'
                # own docstring) -- only the genuine gap is loud.
                #
                # Team-lead ratification (2026-08-02, superseding an earlier,
                # reverted attempt at an honest "feature_not_enabled" early
                # termination here): the legacy fallback stays the terminal
                # behavior for BOTH cases -- PORTFOLIO_STATUS recognition is
                # new this wave; before it, these questions degraded to
                # BOUNDED_INVESTIGATION and got a legacy answer, so
                # terminating unsupported now would regress live free-form
                # traffic to a refusal. That is exactly the behavioral cliff
                # the epic's own §g sequencing defers to the stack-5 guard
                # cutover, once frames are proven -- not a side effect stack
                # 3 introduces alone. One rule until then: a recognized-but-
                # unwired intent falls back loudly (this log + counter),
                # never terminally.
                if (
                    plan is None
                    and intent.intent_id not in LEGACY_ONLY_QUESTION_INTENTS
                ):
                    logger.warning(
                        "ask_dev.orchestrator.plan_registry_gap",
                        extra={"run_id": run_id, "intent_id": intent.intent_id.value},
                    )
                    ASK_DEV_PLAN_REGISTRY_GAP_TOTAL.labels(
                        intent=intent.intent_id.value
                    ).inc()
                cardinality = intent.cardinality
                plan_eligible = (
                    plan is not None and cardinality in plan.supported_cardinalities
                )
                if cardinality is Cardinality.SINGULAR:
                    plan_eligible = (
                        plan_eligible and preflight_result.has_committed_subject
                    )
                elif cardinality in (
                    Cardinality.PLURAL_COHORT,
                    Cardinality.ORGANIZATION_WIDE,
                ):
                    # CHAOS-3393: a cohort/org-wide plan step (status.
                    # portfolio.v1) needs the several committed per-subject
                    # scopes StepContext.subject_set_scopes carries -- there
                    # is nothing to batch over without a committed
                    # DevSubjectSet, mirroring the SINGULAR branch's
                    # has_committed_subject gate one line up.
                    plan_eligible = (
                        plan_eligible and preflight_result.subject_set is not None
                    )
                if plan_eligible:
                    assert plan is not None
                    await transition(RunState.TOOL_EXECUTION)
                    subject_entity_id = (
                        authorized_scope.entity_refs[0].entity_id
                        if cardinality is Cardinality.SINGULAR
                        and authorized_scope.entity_refs
                        else None
                    )
                    # CHAOS-3393: a subject_set only drives batched execution
                    # for a cohort/org-wide run -- a SINGULAR commit can
                    # still carry an AUDIT-ONLY subject_set (duplicate
                    # aliases of the one committed entity; see
                    # CommittedSubjects's own docstring), which must never
                    # also populate subject_set_scopes/subject_set_fingerprint
                    # alongside subject_entity_id (DevInvestigationResult.
                    # validate_result_invariants forbids both being set).
                    portfolio_subject_set = (
                        preflight_result.subject_set
                        if cardinality is not Cardinality.SINGULAR
                        else None
                    )
                    subject_set_scopes = (
                        tuple(
                            _project_scope_from_ref(
                                ref, org_id=org_id, base_scope=authorized_scope
                            )
                            for ref in portfolio_subject_set.committed_entity_refs
                        )
                        if portfolio_subject_set is not None
                        else ()
                    )
                    investigation_result = await self._plan_executor.run(
                        plan=plan,
                        context=StepContext(
                            org_id=org_id,
                            permission_fingerprint=permission_fingerprint,
                            scope=authorized_scope,
                            run_id=run_id,
                            now=datetime.now(UTC),
                            requested_metric_ids=tuple(intent.requested_metric_ids),
                            subject_set_scopes=subject_set_scopes,
                        ),
                        run_id=run_id,
                        subject_entity_id=subject_entity_id,
                        subject_set_fingerprint=(
                            portfolio_subject_set.fingerprint
                            if portfolio_subject_set is not None
                            else None
                        ),
                    )
                    await self._recorder.record_investigation_result(
                        investigation_result
                    )
                    if portfolio_subject_set is not None:
                        portfolio_subject_set_ref = portfolio_subject_set.set_id
                        portfolio_subject_set_warnings = portfolio_subject_set.warnings
                        portfolio_attested_labels = tuple(
                            ref.display_label
                            for ref in portfolio_subject_set.committed_entity_refs
                        )

            for round_index in range(self._limits.model_rounds):
                del round_index
                if cancellation.is_set():
                    return await finish(
                        RunState.CANCELLED,
                        error=error("cancelled", "The request was cancelled."),
                    )
                if remaining() <= 0:
                    return await finish(
                        RunState.FAILED,
                        error=error(
                            "tool_limit_reached", "The request time limit was reached."
                        ),
                    )

                await transition(RunState.MODEL_DECISION)
                try:
                    composed = self._composer.compose(
                        question=request.question,
                        scope=resolution,
                        prior_turns=prior_turns,
                        tool_results=tuple(tool_results),
                        allowed_tools=allowed_tools,
                        subject_committed=(
                            preflight_result is not None
                            and preflight_result.has_committed_subject
                        ),
                        # CHAOS-3421 codex adversarial review (MED-1): the
                        # ONE preflight branch that sets legacy_guard_required
                        # (subject_preflight's proceeded_unresolved_bare_name)
                        # withholds resolve_scope.v1 from allowed_tools -- the
                        # prompt must say so, never instruct the model to
                        # call a tool the registry will reject.
                        resolution_unavailable=(
                            preflight_result is not None
                            and preflight_result.legacy_guard_required
                        ),
                    )
                except ValueError as exc:
                    # A synthetic repair turn (CHAOS-3288) added on top of an
                    # already-large caller-supplied conversation history can
                    # push PromptComposer over its byte budget. That is a
                    # bounded-budget condition, not an unexpected server
                    # error: classify it the same way as the other prompt/
                    # tool budget limits in this run loop instead of falling
                    # through to the generic internal_error handler below,
                    # which would misrepresent a repairable rejection as an
                    # opaque failure. Only this specific, known budget
                    # message is reclassified; any other ValueError from the
                    # composer (e.g. a malformed prior-turn role) still
                    # surfaces as internal_error so it gets investigated
                    # rather than silently mislabeled as a budget limit.
                    if "exceeds prompt budget" not in str(exc):
                        raise
                    return await finish(
                        RunState.FAILED,
                        error=error(
                            "tool_limit_reached",
                            "The conversation history budget was reached.",
                        ),
                    )
                prompt_checksum = composed.checksum
                prompt_version = composed.version
                prompt_bytes = len(composed.system_text.encode()) + len(
                    composed.user_text.encode()
                )
                messages = (
                    AgentMessage(AgentMessageRole.SYSTEM, composed.system_text),
                    AgentMessage(AgentMessageRole.USER, composed.user_text),
                    *provider_continuation,
                )
                tools = self._provider_tools(allowed_tools)
                while True:
                    budget.require(prompt_bytes=prompt_bytes)
                    provider_timeout = min(self._limits.provider_seconds, remaining())
                    if provider_timeout <= 0:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The request time limit was reached.",
                            ),
                        )
                    try:
                        with budget_idempotency_scope(
                            f"ask-dev:{request.request_id}:{retry_count}:{len(tool_results)}"
                        ):
                            decision_result = await self._decide_with_cancellation(
                                messages=messages,
                                tools=tools,
                                timeout_seconds=provider_timeout,
                                cancellation=cancellation,
                            )
                        break
                    except Exception as exc:
                        provider_error = safe_agent_provider_error(exc)
                        if (
                            provider_error.retryable
                            and retry_count < self._limits.provider_retries
                            and not cancellation.is_set()
                            and remaining() > 0
                        ):
                            retry_count += 1
                            continue
                        state = (
                            RunState.CANCELLED
                            if provider_error.code is AgentProviderErrorCode.CANCELLED
                            or cancellation.is_set()
                            else RunState.FAILED
                        )
                        # A non-retryable failure that still returned a
                        # response (e.g. OUTPUT_EXHAUSTED) billed real
                        # tokens for it; the guarded provider already
                        # reconciled that cost against the BYO budget
                        # reservation (CHAOS-3285), so the run's own terminal
                        # usage total must include it too rather than
                        # silently reporting the exhausted call as free.
                        if provider_error.usage is not None:
                            budget.add(provider_error.usage)
                        return await finish(
                            state,
                            error=self._provider_error(
                                request.request_id, provider_error
                            ),
                        )

                budget.add(decision_result.usage)
                provider_fingerprint = decision_result.provider_fingerprint
                model_fingerprint = decision_result.model_fingerprint
                decision = decision_result.decision

                if isinstance(decision, AgentToolRequest):
                    if len(tool_results) >= self._limits.tool_calls:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached", "The tool-call limit was reached."
                            ),
                        )
                    await transition(RunState.TOOL_VALIDATION)
                    try:
                        tool_id_for_call = ToolID(decision.tool_id)
                    except ValueError:
                        # The model named a tool that is not registered at
                        # all -- distinct from a registered tool receiving
                        # invalid arguments. Not degradable: there is no
                        # tool contract to report a bounded per-call error
                        # against.
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_unavailable",
                                "The requested tool was not available.",
                            ),
                        )
                    # Second enforcement point, deliberately redundant with the
                    # pre-loop gate. The pre-loop gate decides whether the run
                    # proceeds at all; this one decides whether *this call*
                    # executes, and it is the seam a mutation can defeat — so
                    # each must independently keep a subject-bearing tool from
                    # running while any mention lacks a committed exact match.
                    gate_rejection = self._subject_gate_rejection(
                        tool_id=tool_id_for_call,
                        allowed_tools=allowed_tools,
                        preflight=preflight_result,
                    )
                    try:
                        tool_request, canonical_hash = self._canonical_tool_request(
                            decision=decision,
                            run_id=run_id,
                            authorized_scope=authorized_scope,
                        )
                        construction_rejection: ToolRequestRejected | None = None
                    except ToolRequestRejected as exc:
                        # The model's arguments did not conform to the tool's
                        # own contract even though the *tool* is registered
                        # (e.g. an advertised-as-open-string field, such as
                        # query_metric.v1's metric_id, that DevToolRequest
                        # itself constrains to a closed enum). Build a safe
                        # placeholder request so this degrades to one failed
                        # tool result below instead of killing the run
                        # (CHAOS-3262).
                        construction_rejection = exc
                        tool_request = DevToolRequest(
                            schema_version="dev_tool_request.v1",
                            run_id=run_id,
                            tool_call_id=decision.call_id,
                            tool_id=tool_id_for_call,
                            scope=authorized_scope,
                        )
                        canonical_hash = (
                            "sha256:"
                            + hashlib.sha256(
                                json.dumps(
                                    {
                                        "tool_id": tool_id_for_call.value,
                                        "arguments": decision.arguments,
                                    },
                                    sort_keys=True,
                                    separators=(",", ":"),
                                    default=str,
                                ).encode()
                            ).hexdigest()
                        )
                    duplicate_counts[canonical_hash] += 1
                    if (
                        duplicate_counts[canonical_hash]
                        > self._limits.identical_tool_calls
                    ):
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "A repeated tool-call loop was stopped.",
                            ),
                        )
                    if gate_rejection is not None:
                        # Degraded per-call rather than fatal (CHAOS-3262's
                        # posture): the model is told the tool is unavailable
                        # and can still answer from the committed subject. The
                        # executor is never reached either way.
                        execution = self._rejected_tool_execution(
                            tool_request=tool_request,
                            code="tool_unavailable",
                            message=gate_rejection,
                        )
                    elif construction_rejection is not None:
                        execution = self._rejected_tool_execution(
                            tool_request=tool_request,
                            code="invalid_request",
                            message=(
                                "The tool request did not match the tool's "
                                f"contract: {construction_rejection}"
                            ),
                        )
                    else:
                        await transition(RunState.TOOL_EXECUTION)
                        tool_remaining = min(remaining(), self._limits.tool_seconds)
                        if tool_remaining <= 0:
                            return await finish(
                                RunState.FAILED,
                                error=error(
                                    "tool_limit_reached",
                                    "The request time limit was reached.",
                                ),
                            )
                        context = ToolExecutionContext(
                            org_id=org_id,
                            user_id=user_id,
                            permission_fingerprint=permission_fingerprint,
                            authorized_scope=authorized_scope,
                            cancellation=cancellation,
                            remaining_seconds=tool_remaining,
                        )
                        try:
                            execution = await self._registry.execute(
                                tool_request, context
                            )
                        except ToolRequestRejected as exc:
                            # The model's arguments passed construction but
                            # violated the tool's own scope/shape contract
                            # (validate_request). Degrade this one call
                            # instead of failing the whole run (CHAOS-3262).
                            execution = self._rejected_tool_execution(
                                tool_request=tool_request,
                                code="invalid_request",
                                message=(
                                    "The tool request did not match the "
                                    f"tool's contract: {exc}"
                                ),
                            )
                        except ToolExecutionTimedOut:
                            # A registered tool with a valid request simply
                            # did not answer in time. This is a per-call
                            # source-availability failure, not a registry
                            # defect: degrade it too, so prior successful
                            # results are not discarded (CHAOS-3262).
                            execution = self._rejected_tool_execution(
                                tool_request=tool_request,
                                code="source_unavailable",
                                message=(
                                    "The tool did not respond within its "
                                    "execution deadline."
                                ),
                            )
                        except ToolRegistryError:
                            # Run-level registry failures (unknown tool,
                            # malformed executor output, cancellation) stay
                            # fatal by design -- see
                            # _rejected_tool_execution's docstring. Listed
                            # explicitly so the catch-all below cannot
                            # silently demote them to a per-call error.
                            raise
                        except Exception as executor_fault:
                            # CHAOS-3332: an executor that raises OUTSIDE its
                            # declared contract -- neither a rejection, a
                            # timeout, nor a registry fault -- used to escape
                            # every handler here and land in the catch-all at
                            # the bottom of run(), which mapped it to a
                            # terminal internal_error while logging nothing
                            # at all. AskDevToolRegistry.execute re-raises
                            # whatever the executor raised verbatim, so this
                            # is the only place that class can be caught.
                            #
                            # The observed instance: a committed TEAM subject
                            # (a real v1 direct scope since CHAOS-3301)
                            # reaching MetricQueryService._validate_request,
                            # which raises a bare ValueError because no
                            # registered metric lists DirectScope.TEAM in its
                            # supported_scopes. Every status question naming a
                            # team died on its first tool call with zero
                            # operator signal.
                            #
                            # One failed tool call must not discard a run that
                            # can still answer from its other evidence, so
                            # this degrades exactly like the timeout above
                            # (CHAOS-3262's posture). It is NOT a silent
                            # downgrade: an executor breaking its contract is
                            # a server defect, so it is logged with a
                            # traceback and counted under its own signal.
                            # The safe_message is fixed text -- the exception
                            # string is operator-only and never echoed to the
                            # model or the user.
                            logger.exception(
                                "ask_dev.orchestrator.tool_executor_fault",
                                extra={
                                    "run_id": run_id,
                                    "tool_id": tool_request.tool_id.value,
                                    "exception_type": type(executor_fault).__name__,
                                },
                            )
                            ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL.labels(
                                tool_id=tool_request.tool_id.value,
                                exception_type=type(executor_fault).__name__,
                            ).inc()
                            execution = self._rejected_tool_execution(
                                tool_request=tool_request,
                                code="source_unavailable",
                                message="The tool did not produce a result.",
                            )
                    if execution.serialized_bytes > self._limits.per_tool_bytes:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The tool-result budget was reached.",
                            ),
                        )
                    next_total = tool_bytes_total + execution.serialized_bytes
                    if next_total > self._limits.total_tool_bytes:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "tool_limit_reached",
                                "The tool-result budget was reached.",
                            ),
                        )
                    tool_results.append(execution.result)
                    tool_requests.append(tool_request)
                    tool_bytes_total = next_total
                    committed_resolution = execution.result.scope_resolution
                    committed_scope = (
                        committed_resolution.resolved_scope
                        if committed_resolution is not None
                        else None
                    )
                    if (
                        tool_request.tool_id is ToolID.RESOLVE_SCOPE
                        and committed_resolution is not None
                    ):
                        # CHAOS-3289: record every resolve_scope.v1 attempt
                        # that actually produced an outcome (not just
                        # successful ones -- ambiguous/not-found count too)
                        # so a final answer can be judged against the run's
                        # most recent named-entity resolution attempt.
                        #
                        # An attempt that produced NO outcome at all (the
                        # call was rejected as malformed, or timed out --
                        # execution.result.scope_resolution is None) must
                        # NOT flip resolve_scope_attempted: doing so would
                        # silently disarm every check below (neither the
                        # ambiguous/not-found branch nor the
                        # never-attempted/no-evidence branch would fire),
                        # which is a worse outcome than never having called
                        # the tool at all. Leaving resolve_scope_attempted
                        # false here makes an errored call fall through to
                        # the same empty-claims/named-phrase backstop that
                        # covers "resolve_scope.v1 was never called".
                        resolve_scope_attempted = True
                        last_resolve_scope_outcome = committed_resolution.outcome
                        last_resolve_scope_resolution = committed_resolution
                        last_resolve_scope_query = tool_request.query
                    if (
                        tool_request.tool_id is ToolID.RESOLVE_SCOPE
                        and execution.result.status == "success"
                        and committed_resolution is not None
                        and committed_scope is not None
                        and committed_scope.organization_id == org_id
                    ):
                        # A named-entity resolve_scope.v1 match commits a new
                        # server-authorized scope for every subsequent tool
                        # call in this run (CHAOS-3256) — the prior immutable
                        # authorized_scope must not keep being reused once the
                        # model has resolved a more specific entity.
                        resolution = committed_resolution
                        authorized_scope = committed_scope
                    provider_continuation += (
                        AgentMessage(
                            AgentMessageRole.ASSISTANT,
                            "",
                            tool_request=decision,
                        ),
                        AgentMessage(
                            AgentMessageRole.TOOL,
                            execution.result.model_dump_json(),
                            tool_call_id=decision.call_id,
                        ),
                    )
                    await self._recorder.record_tool(
                        ordinal=len(tool_results) - 1,
                        request=tool_request,
                        canonical_input_hash=canonical_hash,
                        execution=execution,
                    )
                    continue

                # CHAOS-3367: the run's most recent named-subject resolution
                # came back not-found, and the model is now trying to close
                # the run. Whatever it wants to say, the server owns this
                # terminal.
                #
                # This runs BEFORE the decision dispatch, so it covers both
                # shapes the model can take here, which previously diverged:
                #
                # * AgentFinalAnswer -- the CHAOS-3289 backstop below already
                #   treats `resolve_scope_not_found` as terminal, but only for
                #   an answer whose own status is substantive. A model that
                #   set status=refused/insufficient_evidence walked straight
                #   past it with its prose intact -- that is the reported live
                #   defect, where the summary read "Scope resolution ...
                #   returned forbidden_or_not_found" under a "Refused" chip.
                # * AgentRefusal -- terminated with the generic
                #   "not supported by Ask Dev" refusal, which labels a
                #   no-match as a refusal (PRD Wave 3.1 §12).
                #
                # Two preconditions keep this from diverting a run that is
                # legitimately about something else:
                #
                # * `last_resolve_scope_outcome` is the LAST outcome, not any
                #   outcome -- a run that failed to resolve "A" and then
                #   resolved "B" exactly is answering about B.
                # * the run is still on organization scope -- if an earlier
                #   resolve_scope.v1 committed a real entity scope, the answer
                #   is about that entity and a later miss on some other name
                #   must not erase it. This is the same precondition
                #   `_legacy_named_entity_guard_reason` uses, for the same
                #   reason.
                #
                # Deliberately NOT restricted to `QuestionClass.STATUS` the
                # way that guard is: §12's prohibition is about a named
                # subject that could not be found, and a class restriction
                # would leave the identical defect reachable from every other
                # question class.
                #
                # The third precondition is what makes this safe, and an
                # earlier revision without it was wrong (codex adversarial
                # review, round 1 HIGH, with a repro from this file's own
                # fixtures): "the last lookup missed" does NOT imply "the
                # answer depended on it". A model can speculatively resolve a
                # name the user never wrote, miss, and then answer a genuinely
                # organization-wide question correctly -- diverting that run
                # replaces a good answer with a no-match about a subject
                # nobody asked for. Requiring the failed query to correspond
                # to a whole word the USER typed is the cheapest available
                # proof that the miss was about the question's own subject,
                # and it is the same span the copy would name.
                missed_subject_label = (
                    user_supplied_subject_label(
                        request.question, last_resolve_scope_query
                    )
                    if last_resolve_scope_outcome
                    is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
                    else None
                )
                if (
                    missed_subject_label is not None
                    and last_resolve_scope_resolution is not None
                    and authorized_scope.direct_scope is DirectScope.ORGANIZATION
                    and isinstance(decision, (AgentFinalAnswer, AgentRefusal))
                ):
                    await transition(RunState.ANSWER_VALIDATION)
                    return await finish(
                        RunState.INSUFFICIENT_EVIDENCE,
                        answer=named_subject_not_found_answer(
                            answer_id=answer_id,
                            conversation_id=conversation_id,
                            question=request.question,
                            query=last_resolve_scope_query,
                            resolution=last_resolve_scope_resolution,
                            versions=self._versions,
                            model=DevModelMetadata(
                                provider_source=self._provider_source,
                                provider_family=self._provider_family,
                                model_fingerprint=decision_result.model_fingerprint,
                            ),
                            now=datetime.now(UTC),
                        ),
                    )

                if isinstance(decision, AgentFinalAnswer):
                    await transition(RunState.ANSWER_VALIDATION)
                    model = DevModelMetadata(
                        provider_source=self._provider_source,
                        provider_family=self._provider_family,
                        model_fingerprint=decision_result.model_fingerprint,
                    )
                    validation_context = AnswerValidationContext(
                        conversation_id=conversation_id,
                        answer_id=answer_id,
                        scope_resolution=resolution,
                        versions=self._versions,
                        model=model,
                        tool_results=tuple(tool_results),
                    )
                    candidate = dict(decision.value)
                    canonical_data = self._canonical_answer_data(tuple(tool_results))
                    if canonical_data is None:
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "answer_validation_failed",
                                "The answer failed grounding validation.",
                            ),
                        )
                    canonical_metrics, canonical_evidence = canonical_data
                    now = datetime.now(UTC)
                    # CHAOS-3297 stack #5: computed once, into a local, because
                    # the demoted-guard path below builds a server-owned answer
                    # from the SAME coverage this candidate is judged against.
                    # Recomputing it there would be a second producer of the
                    # run's coverage accounting that could silently disagree
                    # with the one the validator saw.
                    server_coverage = self._coverage_with_plan_sources(
                        self._coverage_from_tool_results(
                            tuple(tool_requests), tuple(tool_results), now
                        ),
                        investigation_result,
                    )
                    candidate.update(
                        {
                            "schema_version": "dev_answer.v1",
                            "answer_id": answer_id,
                            "conversation_id": conversation_id,
                            "generated_at": now,
                            "resolved_scope": resolution.model_dump(mode="json"),
                            "as_of": now,
                            "metrics": [
                                item.model_dump(mode="json")
                                for item in canonical_metrics
                            ],
                            "evidence": [
                                item.model_dump(mode="json")
                                for item in canonical_evidence
                            ],
                            # CHAOS-3334: the model-chosen tool results are
                            # only half the coverage picture. When a plan
                            # governed this run, its mandatory source
                            # observations are merged in here so a failed
                            # required step is judged by the answer contract's
                            # existing completeness invariant rather than
                            # being silently discarded.
                            "coverage": server_coverage.model_dump(mode="json"),
                            "versions": self._versions.model_dump(mode="json"),
                            "model": model.model_dump(mode="json"),
                        }
                    )
                    # CHAOS-3377 defect 3: strip a trailing JSON-structural
                    # artifact from every model-authored free-text field at
                    # this seam -- before anything else reads or validates
                    # them -- rather than special-casing the one reported
                    # string later.
                    if isinstance(candidate.get("direct_summary"), str):
                        candidate["direct_summary"] = sanitize_model_text(
                            candidate["direct_summary"]
                        )
                    if isinstance(candidate.get("claims"), list):
                        candidate["claims"] = [
                            {**claim, "text": sanitize_model_text(claim["text"])}
                            if isinstance(claim, dict)
                            and isinstance(claim.get("text"), str)
                            else claim
                            for claim in candidate["claims"]
                        ]
                    if isinstance(candidate.get("warnings"), list):
                        candidate["warnings"] = [
                            sanitize_model_text(warning)
                            if isinstance(warning, str)
                            else warning
                            for warning in candidate["warnings"]
                        ]
                    # CHAOS-3377 defects 1/2/5 -- the §10 deterministic
                    # renderer. When this run executed status_snapshot.v1
                    # for the CURRENT resolved scope, the server already
                    # computed a real completion verdict (`actual_completion`
                    # bound to that scope -- see `_deterministic_status_
                    # render`/`status_snapshot_result`); the direct verdict/
                    # completion/blockers are rendered from THAT here,
                    # overwriting whatever the model proposed for status/
                    # direct_summary/claims -- exactly the same
                    # "server-derived fields overwrite the model's" pattern
                    # `metrics`/`evidence`/`coverage` already follow above.
                    # The model cannot mislabel a substantive answer as
                    # refused, narrate a raw internal token, or contradict
                    # the frame's own blocker list, because none of its own
                    # text for this content reaches the candidate at all.
                    rendered_status = self._deterministic_status_render(
                        resolution=resolution,
                        tool_requests=tuple(tool_requests),
                        tool_results=tuple(tool_results),
                        server_coverage=server_coverage,
                        canonical_evidence=canonical_evidence,
                    )
                    if (
                        rendered_status is None
                        and resolution.resolved_scope is not None
                    ):
                        # CHAOS-3393: a status.portfolio.v1 batch has no
                        # status_snapshot.v1 tool call to bind to (the plan
                        # executor evaluated it directly), so it rides this
                        # same §10 seam via investigation_result instead --
                        # the model's own narrative for the batch is
                        # likewise never what reaches the wire.
                        rendered_status = render_portfolio_summary(
                            investigation_result,
                            validity_scope=resolution.resolved_scope,
                            subject_set_warnings=portfolio_subject_set_warnings,
                        )
                    if (
                        portfolio_subject_set_ref is not None
                        and rendered_status is None
                    ):
                        # CHAOS-3393 codex HIGH-2: a committed portfolio
                        # subject set ran the plan executor (subject_set_
                        # scopes was passed in), but the resulting
                        # investigation_result carries no usable rows at
                        # all -- the evaluator raised, the whole step hit
                        # the plan's own per_step_timeout_seconds ceiling,
                        # or some other TOTAL failure the isolated
                        # per-project PortfolioProjectFailure path never
                        # reaches (that path always still produces rows).
                        # render_portfolio_summary returning None here must
                        # NEVER be read as "nothing to override" -- the
                        # model's own unconstrained prose about a
                        # portfolio it may never have actually seen must
                        # not reach the wire. A deterministic, zero-claim
                        # DEGRADED result always wins in this case,
                        # regardless of what the model wrote, closing the
                        # gap the validator's own PARTIAL-with-a-coverage-
                        # gap allowance would otherwise leave open.
                        rendered_status = (
                            AnswerStatus.DEGRADED,
                            (
                                "Portfolio status could not be determined: "
                                "the evaluation did not complete for any "
                                "project in this batch."
                            ),
                            [],
                        )
                    if rendered_status is not None:
                        det_status, det_direct_summary, det_claims = rendered_status
                        candidate["status"] = det_status.value
                        candidate["direct_summary"] = det_direct_summary
                        candidate["claims"] = [
                            claim.model_dump(mode="json") for claim in det_claims
                        ]
                    try:
                        answer = validate_answer_candidate(
                            candidate, validation_context
                        )
                    except AnswerValidationError as exc:
                        if (
                            exc.repairable
                            and repair_count < self._limits.schema_repairs
                        ):
                            repair_count += 1
                            # Tell the model exactly what was wrong instead of
                            # a generic "fix your JSON" instruction it cannot
                            # act on -- e.g. it claimed status=complete while
                            # a queried metric was stale, and a bare "schema
                            # validation failed" note gives it nothing to
                            # change (CHAOS-3288). `exc.detail` is bounded and
                            # never contains echoed tool/evidence content.
                            prior_turns = prior_turns + (
                                PromptConversationTurn(
                                    role="assistant",
                                    content=(
                                        "The previous response failed validation: "
                                        f"{exc.detail}. Return one corrected "
                                        "dev_answer.v1 object that fixes exactly "
                                        "that issue and keeps everything else "
                                        "grounded in the same tool results."
                                    ),
                                ),
                            )
                            continue
                        if exc.code == "completion_denominator_withheld":
                            # CHAOS-3297 s2 round 5 (codex MEDIUM): the
                            # bounded repair pass above already gave the
                            # model one chance to reissue this honestly; if
                            # it still fabricated a completion total, the
                            # user must not be left with only a generic
                            # "validation failed" -- surface WHY (the
                            # reason codes) and WHAT was actually assessed
                            # (how many required items were displayed), the
                            # same truncation detail the repair prompt
                            # itself never leaked beyond bounded safe text.
                            # DevError.code stays the closed-vocabulary
                            # "answer_validation_failed" (this internal
                            # exc.code is orchestrator-only branching, not
                            # the wire code -- see "answer_grounding_floor_
                            # not_met" just below for the same pattern).
                            return await finish(
                                RunState.FAILED,
                                error=error(
                                    "answer_validation_failed",
                                    "The answer stated a completion total "
                                    "that could not be verified. "
                                    + completion_truncation_detail(tuple(tool_results)),
                                ),
                            )
                        if exc.code == "answer_grounding_floor_not_met":
                            # CHAOS-3290: a complete/substantive answer with
                            # no claim, metric, or evidence grounding at all
                            # is a silent non-answer, not a malformed one --
                            # surface it as the same honest "nothing usable
                            # was found" outcome the run already uses
                            # elsewhere (AgentDisambiguation/AgentRefusal
                            # below), not the scarier "validation failed"
                            # error a real grounding violation gets.
                            #
                            # CHAOS-3297 stack #5 (guard cutover): routed
                            # through the same demotion seam as the
                            # CHAOS-3289 backstop below, so there is one
                            # rule for "a guard rejected the model's answer"
                            # rather than two that could drift.
                            #
                            # In practice this seam cannot fire for THIS
                            # guard, and that is a property worth stating
                            # rather than leaving a reader to rediscover:
                            # `answer.metrics`/`answer.evidence` are
                            # OVERWRITTEN above with the canonical tuples, so
                            # `answer_grounding_floor_not_met` implies both
                            # are empty -- which is exactly the condition
                            # `_server_grounded_answer` refuses to build on.
                            # The CHAOS-3290 floor therefore never had server
                            # material to erase in the first place; the guard
                            # that did is the named-entity backstop below.
                            # `test_chaos_3297_s5_guard_cutover.py::test_c4_*`
                            # pins that implication so a future change to the
                            # floor's trigger cannot silently make this branch
                            # live without anyone noticing.
                            demoted = self._server_grounded_answer(
                                answer_id=answer_id,
                                conversation_id=conversation_id,
                                resolution=resolution,
                                coverage=server_coverage,
                                tool_results=tuple(tool_results),
                                investigation_result=investigation_result,
                                model=model,
                                now=now,
                                cutover_active=self._frame_cutover_active(
                                    preflight_result
                                ),
                            )
                            if demoted is not None:
                                return await finish(
                                    RunState.COMPLETED,
                                    answer=demoted,
                                    grounding_validation_status=(
                                        GUARD_DEMOTED_GROUNDING_FLOOR_STATUS
                                    ),
                                )
                            return await finish(
                                RunState.INSUFFICIENT_EVIDENCE,
                                error=error(
                                    "insufficient_evidence",
                                    "The answer did not include enough "
                                    "grounded detail to present as a "
                                    "result.",
                                ),
                            )
                        if exc.code == "refused_with_material_grounding":
                            # CHAOS-3377 defect 1: the model kept self-
                            # declaring REFUSED over real grounding even
                            # after the bounded repair pass. Same demotion
                            # seam as the grounding floor above -- the run
                            # has genuine server-verified material
                            # (`_server_grounded_answer`'s own third guard
                            # refuses to build on nothing), so it terminates
                            # COMPLETED with that material rather than a
                            # hard FAILED that would discard evidence the
                            # run already retrieved.
                            demoted = self._server_grounded_answer(
                                answer_id=answer_id,
                                conversation_id=conversation_id,
                                resolution=resolution,
                                coverage=server_coverage,
                                tool_results=tuple(tool_results),
                                investigation_result=investigation_result,
                                model=model,
                                now=now,
                                cutover_active=self._frame_cutover_active(
                                    preflight_result
                                ),
                            )
                            if demoted is not None:
                                return await finish(
                                    RunState.COMPLETED,
                                    answer=demoted,
                                    grounding_validation_status=(
                                        GUARD_DEMOTED_REFUSAL_STATUS
                                    ),
                                )
                            return await finish(
                                RunState.INSUFFICIENT_EVIDENCE,
                                error=error(
                                    "insufficient_evidence",
                                    "The answer did not include enough "
                                    "grounded detail to present as a "
                                    "result.",
                                ),
                            )
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "answer_validation_failed",
                                "The answer failed grounding validation.",
                            ),
                        )
                    if (
                        len(answer.evidence) > self._limits.evidence_refs
                        or len(answer.metrics) > self._limits.metrics
                    ):
                        return await finish(
                            RunState.FAILED,
                            error=error(
                                "answer_validation_failed",
                                "The answer exceeds grounded-result limits.",
                            ),
                        )
                    guard_reason = self._legacy_named_entity_guard_reason(
                        question=request.question,
                        question_class=request.question_class,
                        authorized_scope=authorized_scope,
                        resolve_scope_attempted=resolve_scope_attempted,
                        last_resolve_scope_outcome=last_resolve_scope_outcome,
                        answer=answer,
                        extra_named_phrases=(
                            preflight_result.unresolved_name_spans
                            if preflight_result is not None
                            else frozenset()
                        ),
                    )
                    if guard_reason is not None:
                        if preflight_result is not None and not (
                            self._legacy_guard_is_terminal(
                                preflight_result, guard_reason
                            )
                        ):
                            # TRD §10: kept as defense in depth, but it must
                            # not create public copy or delete an answer once
                            # the server owns the subject. A firing here is a
                            # cutover defect, so it is recorded and alerted on
                            # rather than acted on. Until CHAOS-3297 lands real
                            # answer frames, "a valid frame exists" is
                            # substituted by: the preflight committed an exact
                            # subject for every mention (or the question named
                            # none) **and** the answer passed the existing
                            # grounding validator, both true at this line.
                            await self._recorder.record_preflight(
                                preflight_outcome=preflight_result.diagnostic,
                                legacy_guard_reason=guard_reason,
                            )
                        else:
                            # CHAOS-3297 stack #5 (guard cutover, TRD §15
                            # Phase D). This is the branch where the CHAOS-3289
                            # backstop still *decides*: a name-specific reason
                            # on a preflight run that saw an unresolved bare
                            # name, or any reason at all on the flag-off path.
                            #
                            # Frames are now proven end to end (stack #1 makes
                            # every terminal persist one, stack #3 builds it,
                            # stack #4 renders it deterministically), so the
                            # remaining question is no longer "is the model's
                            # answer trustworthy" -- it is "does the server
                            # hold anything of its own to show instead". The
                            # model's prose stays rejected either way; what
                            # changes is that its rejection no longer erases
                            # the run's server-verified material.
                            #
                            # ``_frame_cutover_active`` is the flag gate:
                            # ``preflight_result`` is None exactly when
                            # ``ask_dev_wave_3_1`` is off for this
                            # organization (production_runtime builds
                            # preflight/plan_registry/plan_executor together
                            # or not at all), and that path has no proven
                            # frame path behind it -- so it keeps every reason
                            # terminal, exactly as today.
                            demoted = self._server_grounded_answer(
                                answer_id=answer_id,
                                conversation_id=conversation_id,
                                resolution=resolution,
                                coverage=server_coverage,
                                tool_results=tuple(tool_results),
                                investigation_result=investigation_result,
                                model=model,
                                now=now,
                                cutover_active=self._frame_cutover_active(
                                    preflight_result
                                ),
                            )
                            if demoted is None:
                                code, message = _LEGACY_GUARD_TERMINALS[guard_reason]
                                return await finish(
                                    RunState.INSUFFICIENT_EVIDENCE,
                                    error=error(code, message),
                                )
                            # Demoted, not deleted: the verdict is recorded on
                            # the run as a content-free diagnostic (the same
                            # closed vocabulary the telemetry branch above
                            # writes), and the terminal marks grounding
                            # validation as advisory rather than "passed".
                            await self._recorder.record_preflight(
                                preflight_outcome=(
                                    preflight_result.diagnostic
                                    if preflight_result is not None
                                    else None
                                ),
                                legacy_guard_reason=guard_reason,
                            )
                            return await finish(
                                RunState.COMPLETED,
                                answer=demoted,
                                grounding_validation_status=(
                                    GUARD_DEMOTED_NAMED_ENTITY_STATUS
                                ),
                            )
                    return await finish(
                        RunState.COMPLETED,
                        answer=answer,
                        extra_attested=portfolio_attested_labels,
                    )

                if isinstance(decision, AgentDisambiguation):
                    return await finish(
                        RunState.INSUFFICIENT_EVIDENCE,
                        error=error(
                            "scope_ambiguous",
                            "The request requires scope clarification.",
                        ),
                    )
                if isinstance(decision, AgentRefusal):
                    # CHAOS-3377 HIGH 2 (codex adversarial review): a
                    # refusal reached AFTER a real status_snapshot.v1 result
                    # already exists for the run's current resolved scope is
                    # the same defect class the §10 deterministic renderer
                    # exists to close -- the model declined to answer over
                    # material the server already retrieved and can report
                    # honestly. Checked before the unconditional REFUSED
                    # terminal below, never after.
                    deterministic_refusal_answer = self._deterministic_status_answer(
                        answer_id=answer_id,
                        conversation_id=conversation_id,
                        resolution=resolution,
                        tool_requests=tuple(tool_requests),
                        tool_results=tuple(tool_results),
                        now=datetime.now(UTC),
                        model=DevModelMetadata(
                            provider_source=self._provider_source,
                            provider_family=self._provider_family,
                            model_fingerprint=decision_result.model_fingerprint,
                        ),
                    )
                    if deterministic_refusal_answer is not None:
                        return await finish(
                            RunState.COMPLETED, answer=deterministic_refusal_answer
                        )
                    return await finish(
                        RunState.REFUSED,
                        error=error(
                            "insufficient_evidence",
                            "The request is not supported by Ask Dev.",
                        ),
                    )
                return await finish(
                    RunState.FAILED,
                    error=error(
                        "internal_error",
                        "The provider returned an unsupported decision.",
                    ),
                )

            return await finish(
                RunState.FAILED,
                error=error(
                    "tool_limit_reached", "The model-decision limit was reached."
                ),
            )
        except ToolExecutionCancelled:
            return await finish(
                RunState.CANCELLED,
                error=error("cancelled", "The request was cancelled."),
            )
        except BudgetExceeded:
            if (
                resolution is not None
                and provider_fingerprint is not None
                and model_fingerprint is not None
            ):
                partial = self._budget_answer(
                    answer_id=answer_id,
                    conversation_id=conversation_id,
                    resolution=resolution,
                    tool_requests=tuple(tool_requests),
                    tool_results=tuple(tool_results),
                    model_fingerprint=model_fingerprint,
                )
                if partial is not None:
                    return await finish(RunState.COMPLETED, answer=partial)
            return await finish(
                RunState.FAILED,
                error=error("cost_limit_reached", "The provider budget was reached."),
            )
        except RunDeadlineExceeded:
            return await finish(
                RunState.FAILED,
                error=error(
                    "tool_limit_reached", "The request time limit was reached."
                ),
            )
        except ToolRegistryError:
            return await finish(
                RunState.FAILED,
                error=error(
                    "tool_unavailable", "The requested tool was not available."
                ),
            )
        except Exception as unhandled:
            # CHAOS-3332: this handler is the run's last resort, and until now
            # it mapped anything at all to a terminal internal_error while
            # emitting nothing -- no log line, no metric, no exception type.
            # A 100%-reproducible production crash (every TEAM-subject status
            # question) was therefore indistinguishable from a transient blip
            # in every operator surface there is. An increment here is always
            # an unclassified server defect: every *expected* terminal
            # condition in this run loop has its own typed handler above, so
            # anything reaching this line is by construction something no
            # branch anticipated, and it must be loud.
            #
            # Logged before finish() rather than after: finish() itself does
            # database writes that can raise, and the diagnosis for the
            # original exception must survive that.
            logger.exception(
                "ask_dev.orchestrator.unhandled_run_fault",
                extra={
                    "run_id": run_id,
                    "exception_type": type(unhandled).__name__,
                    "terminal_written": terminal_written,
                },
            )
            ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL.labels(
                exception_type=type(unhandled).__name__
            ).inc()
            if terminal_written:
                raise
            return await finish(
                RunState.FAILED,
                error=error("internal_error", "The request could not be completed."),
            )

    async def _resolve_with_cancellation(
        self,
        *,
        org_id: str,
        user_id: str,
        requested_scope: DevScope,
        cancellation: asyncio.Event,
        timeout_seconds: float,
    ) -> DevScopeResolution:
        if timeout_seconds <= 0:
            raise RunDeadlineExceeded("scope resolution exceeded the run deadline")
        resolver_task: asyncio.Future[DevScopeResolution] = asyncio.ensure_future(
            self._scope_resolver(
                org_id=org_id, user_id=user_id, requested_scope=requested_scope
            )
        )
        cancellation_task = asyncio.create_task(cancellation.wait())
        wait_set: set[asyncio.Future[Any]] = {resolver_task, cancellation_task}
        try:
            done, _ = await asyncio.wait(
                wait_set,
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_task in done:
                resolver_task.cancel()
                await asyncio.gather(resolver_task, return_exceptions=True)
                raise ToolExecutionCancelled("scope resolution cancelled")
            if resolver_task not in done:
                resolver_task.cancel()
                await asyncio.gather(resolver_task, return_exceptions=True)
                raise RunDeadlineExceeded("scope resolution exceeded the run deadline")
            return resolver_task.result()
        finally:
            cancellation_task.cancel()
            await asyncio.gather(cancellation_task, return_exceptions=True)

    async def _decide_with_cancellation(
        self,
        *,
        messages: tuple[AgentMessage, ...],
        tools: tuple[AgentToolDefinition, ...],
        timeout_seconds: float,
        cancellation: asyncio.Event,
    ) -> AgentDecisionResult:
        provider_task: asyncio.Future[AgentDecisionResult] = asyncio.ensure_future(
            self._provider.decide(
                messages=messages,
                tools=tools,
                response_schema=DevAnswer.model_json_schema(mode="validation"),
                timeout_seconds=timeout_seconds,
                max_output_tokens=self._limits.max_output_tokens_per_call,
                signal=EventCancellationSignal(cancellation),
            )
        )
        cancellation_task = asyncio.create_task(cancellation.wait())
        wait_set: set[asyncio.Future[Any]] = {provider_task, cancellation_task}
        try:
            done, _ = await asyncio.wait(
                wait_set,
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_task in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
            if provider_task not in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
            return provider_task.result()
        finally:
            cancellation_task.cancel()
            await asyncio.gather(cancellation_task, return_exceptions=True)

    def _provider_tools(
        self, allowed_tools: frozenset[ToolID] | None = None
    ) -> tuple[AgentToolDefinition, ...]:
        manifest = self._registry.manifest(allowed_tool_ids=allowed_tools)
        tools = manifest["tools"]
        assert isinstance(tools, list)
        return tuple(
            AgentToolDefinition(
                tool_id=str(item["tool_id"]),
                description=str(item["description"]),
                input_schema=self._provider_tool_input_schema(
                    ToolID(str(item["tool_id"])), int(item["max_items"])
                ),
            )
            for item in tools
            if isinstance(item, Mapping)
        )

    @staticmethod
    def _provider_tool_input_schema(tool_id: ToolID, max_items: int) -> dict[str, Any]:
        """Expose only model-owned arguments accepted by the exact tool contract.

        The advertised ``limit`` enum must never exceed what ``DevToolRequest``
        itself accepts on the wire (``dev_tool_request.v1.limit`` caps at 25
        regardless of a tool's own registered ``max_items``, e.g.
        status_snapshot.v1's 100). Advertising an unreachable upper bound is
        the same class of provider/server schema drift as CHAOS-3262.
        """

        request_limit_ceiling = _dev_tool_request_limit_maximum()
        properties: dict[str, Any] = {
            "limit": {
                "type": "integer",
                "enum": list(range(1, min(max_items, request_limit_ceiling) + 1)),
            }
        }
        if tool_id is ToolID.QUERY_METRIC:
            properties = {
                # dev_tool_request.v1's metric_id is the closed MetricID
                # enum, not an open string: advertise exactly that enum so a
                # schema-compliant model can never request an unregistered
                # metric (CHAOS-3262).
                "metric_id": {
                    "type": "string",
                    "enum": [item.value for item in MetricID],
                },
                "include_comparison": {"type": "boolean"},
                **properties,
            }
        elif tool_id in {ToolID.RESOLVE_SCOPE, ToolID.SEARCH_EVIDENCE}:
            properties = {"query": {"type": "string"}, **properties}
        elif tool_id is ToolID.GET_EVIDENCE:
            properties = {
                "evidence_ref_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                **properties,
            }
        elif tool_id in {ToolID.STATUS_SNAPSHOT, ToolID.CHANGE_SUMMARY}:
            properties = {
                "include_comparison": {"type": "boolean"},
                **properties,
            }
        return {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
            "required": sorted(properties),
        }

    @staticmethod
    def _deterministic_status_render(
        *,
        resolution: DevScopeResolution,
        tool_requests: tuple[DevToolRequest, ...],
        tool_results: tuple[DevToolResult, ...],
        server_coverage: DevCoverage,
        canonical_evidence: Sequence[DevEvidenceRef],
    ) -> tuple[AnswerStatus, str, list[DevClaim]] | None:
        """The §10 deterministic (status, direct_summary, claims), or
        ``None`` if this run has no ``status_snapshot.v1`` result bound to
        the CURRENT resolved scope.

        The one seam every terminal that can carry an answer -- a validated
        ``AgentFinalAnswer``, an ``AgentRefusal``, or a budget-exhaustion
        partial -- renders §10 content through (CHAOS-3377 HIGH 2, codex
        adversarial review: a prior revision only applied the override
        inside the ``AgentFinalAnswer`` branch, so a provider that emitted
        ``AgentRefusal`` -- or a run that hit ``BudgetExceeded`` -- AFTER a
        real ``status_snapshot.v1`` result already existed could still
        terminate REFUSED, or with generic budget boilerplate, without ever
        inspecting the retrieved material. The reported defect class stayed
        reachable from those two paths).
        """

        if resolution.resolved_scope is None:
            return None
        status_result = status_snapshot_result(
            tool_requests, tool_results, authorized_scope=resolution.resolved_scope
        )
        if status_result is None or status_result.actual_completion is None:
            return None
        actual = status_result.actual_completion
        canonical_evidence_ids = frozenset(
            item.evidence_ref_id for item in canonical_evidence
        )
        claims = build_deterministic_status_claims(
            actual=actual,
            status_result=status_result,
            validity_scope=resolution.resolved_scope,
            canonical_evidence_ids=canonical_evidence_ids,
            tool_results=tool_results,
        )
        status = deterministic_answer_status(
            coverage=server_coverage, tool_results=tool_results
        )
        direct_summary = render_verdict_summary(
            actual,
            denominator_withheld=any_tool_result_withheld_its_completion_denominator(
                tool_results
            ),
        )
        # CHAOS-3368 step 2: the project's own declared state/target date,
        # appended to the same verdict/summary section -- ``status_result``
        # is the identical scope-verified DevToolResult
        # ``status_snapshot_result`` already selected above, so this rides
        # that binding for free (no extra scope check needed: a
        # declared_project_state set on a DIFFERENT tool result could never
        # reach here, since only THIS result's fields are read).
        #
        # ``canonical_evidence_ids`` -- the SAME frozenset already passed to
        # ``build_deterministic_status_claims`` above -- gates this exactly
        # like it gates the claim (Codex HIGH, delta review, 2026-08-04):
        # this run's OWN 25-entry canonical evidence cap
        # (``Orchestrator._canonical_answer_data``) can truncate the
        # declared-state evidence out even when its per-tool-call priority
        # reservation let it survive onto the wire -- without this gate the
        # summary sentence could assert a declared state with no claim and
        # no evidence behind it anywhere in the answer.
        declared_project_summary = render_declared_project_summary(
            status_result, canonical_evidence_ids
        )
        if declared_project_summary is not None:
            direct_summary = f"{direct_summary} {declared_project_summary}"
        return status, direct_summary, claims

    def _deterministic_status_answer(
        self,
        *,
        answer_id: str,
        conversation_id: str,
        resolution: DevScopeResolution,
        tool_requests: tuple[DevToolRequest, ...],
        tool_results: tuple[DevToolResult, ...],
        now: datetime,
        model: DevModelMetadata,
    ) -> DevAnswer | None:
        """A full, server-rendered §10 ``DevAnswer`` for this run, or
        ``None`` if there is nothing to render (no bound status_snapshot
        result, or no canonical grounding to attach it to). Used by the
        ``AgentRefusal`` and ``BudgetExceeded`` terminals -- see
        ``_deterministic_status_render`` for why those need this too, not
        only the ``AgentFinalAnswer`` branch (which merges the same render
        into an existing candidate instead of building a fresh answer).
        """

        canonical_data = self._canonical_answer_data(tool_results)
        if canonical_data is None:
            return None
        canonical_metrics, canonical_evidence = canonical_data
        server_coverage = self._coverage_with_plan_sources(
            self._coverage_from_tool_results(tool_requests, tool_results, now),
            None,
        )
        rendered = self._deterministic_status_render(
            resolution=resolution,
            tool_requests=tool_requests,
            tool_results=tool_results,
            server_coverage=server_coverage,
            canonical_evidence=canonical_evidence,
        )
        if rendered is None:
            return None
        status, direct_summary, claims = rendered
        return DevAnswer(
            schema_version="dev_answer.v1",
            answer_id=answer_id,
            conversation_id=conversation_id,
            generated_at=now,
            resolved_scope=resolution,
            as_of=now,
            status=status,
            direct_summary=direct_summary,
            claims=claims,
            metrics=canonical_metrics,
            evidence=canonical_evidence,
            conflicts=[],
            coverage=server_coverage,
            warnings=[],
            suggested_follow_up_questions=[],
            versions=self._versions,
            model=model,
        )

    def _budget_answer(
        self,
        *,
        answer_id: str,
        conversation_id: str,
        resolution: DevScopeResolution,
        tool_requests: tuple[DevToolRequest, ...],
        tool_results: tuple[DevToolResult, ...],
        model_fingerprint: str,
    ) -> DevAnswer | None:
        """Return canonical retrieved data when a later model call is
        blocked -- the §10 deterministic render if this run has a
        status_snapshot.v1 result bound to the current resolved scope
        (CHAOS-3377 HIGH 2: budget exhaustion after a real completion
        assessment must not discard it for generic boilerplate), otherwise
        the prior generic "budget reached" summary.
        """

        deterministic = self._deterministic_status_answer(
            answer_id=answer_id,
            conversation_id=conversation_id,
            resolution=resolution,
            tool_requests=tool_requests,
            tool_results=tool_results,
            now=datetime.now(UTC),
            model=DevModelMetadata(
                provider_source=self._provider_source,
                provider_family=self._provider_family,
                model_fingerprint=model_fingerprint,
            ),
        )
        if deterministic is not None:
            return deterministic

        canonical_data = self._canonical_answer_data(tool_results)
        if canonical_data is None:
            return None
        canonical_metrics, canonical_evidence = canonical_data
        if not canonical_evidence and not canonical_metrics:
            return None
        now = datetime.now(UTC)
        degraded = any(
            result.status in {"unavailable", "error"} for result in tool_results
        )
        return DevAnswer(
            schema_version="dev_answer.v1",
            answer_id=answer_id,
            conversation_id=conversation_id,
            generated_at=now,
            resolved_scope=resolution,
            as_of=now,
            status=AnswerStatus.DEGRADED if degraded else AnswerStatus.PARTIAL,
            direct_summary=(
                "The provider budget was reached. This answer contains only the "
                "validated data retrieved before the limit."
            ),
            claims=[],
            metrics=canonical_metrics,
            evidence=canonical_evidence,
            conflicts=[],
            coverage=DevCoverage(
                required_source_count=1,
                available_source_count=0 if degraded else 1,
                unavailable_required_sources=["tool_results"] if degraded else [],
                stale_required_sources=[],
                as_of=now,
            ),
            warnings=[
                "The provider budget was reached; no additional model call was made."
            ],
            suggested_follow_up_questions=[],
            versions=self._versions,
            model=DevModelMetadata(
                provider_source=self._provider_source,
                provider_family=self._provider_family,
                model_fingerprint=model_fingerprint,
            ),
        )

    def _canonical_answer_data(
        self, tool_results: tuple[DevToolResult, ...]
    ) -> tuple[list[DevMetricRef], list[DevEvidenceRef]] | None:
        evidence: dict[str, DevEvidenceRef] = {}
        metrics: dict[str, DevMetricRef] = {}
        for result in tool_results:
            for evidence_item in result.evidence:
                current_evidence = evidence.setdefault(
                    evidence_item.evidence_ref_id, evidence_item
                )
                if current_evidence != evidence_item:
                    return None
            for metric_item in result.metrics:
                current_metric = metrics.setdefault(
                    metric_item.metric_ref_id, metric_item
                )
                if current_metric != metric_item:
                    return None
        canonical_evidence = list(evidence.values())[: self._limits.evidence_refs]
        allowed_evidence_ids = {item.evidence_ref_id for item in canonical_evidence}
        canonical_metrics = [
            item
            for item in metrics.values()
            if set(item.evidence_ref_ids) <= allowed_evidence_ids
        ][: self._limits.metrics]
        return canonical_metrics, canonical_evidence

    @staticmethod
    def _tool_result_has_usable_data(result: DevToolResult) -> bool:
        """Whether a tool result carries any fact an answer could ground on.

        A ``success``/``partial`` status alone is not evidence of coverage: a
        source that returned zero facts because a required upstream source
        (e.g. the authorized repository set) was unavailable must not be
        reported as an available required source (CHAOS-3257). A
        ``data_health`` list is only usable if at least one entry reports
        something other than ``unavailable`` -- a partial data-health result
        that exhaustively lists every source as unavailable is itself the
        "nothing is available" case, not evidence of availability.
        """
        return bool(
            result.metrics
            or result.evidence
            or result.status_facts
            or result.graph_edges
            or result.metric_definitions
            or any(
                item.freshness is not FreshnessState.UNAVAILABLE
                for item in result.data_health
            )
        )

    @staticmethod
    def _required_source_key(
        request: DevToolRequest,
    ) -> tuple[str, str | None, str | None, tuple[str, ...]]:
        """Identify the distinct required source one tool request represents.

        Two calls to the *same* tool are the same required source only if
        they ask for the same discriminating data. `query_metric.v1` called
        for ``items_completed`` and again for ``cycle_time_p50_hours`` are
        two distinct required sources, not one, even though a retry of the
        identical ``items_completed`` request is one (CHAOS-3257).
        """
        return (
            request.tool_id.value,
            request.metric_id.value if request.metric_id is not None else None,
            request.query,
            tuple(sorted(request.evidence_ref_ids)),
        )

    @classmethod
    def _coverage_from_tool_results(
        cls,
        tool_requests: tuple[DevToolRequest, ...],
        tool_results: tuple[DevToolResult, ...],
        as_of: datetime,
    ) -> DevCoverage:
        if len(tool_requests) != len(tool_results):
            raise ValueError("tool requests and results must be paired one-to-one")
        # `required_source_count` counts required *source classes* -- the
        # distinct (tool, discriminating-argument) asks the model made, not
        # raw tool invocations: a retried, identical request is one required
        # source; two different requests to the same tool are two.
        usable_by_source: dict[
            tuple[str, str | None, str | None, tuple[str, ...]], bool
        ] = {}
        label_by_source: dict[
            tuple[str, str | None, str | None, tuple[str, ...]], str
        ] = {}
        for request, result in zip(tool_requests, tool_results, strict=True):
            key = cls._required_source_key(request)
            usable = result.status == "success" or (
                result.status == "partial" and cls._tool_result_has_usable_data(result)
            )
            usable_by_source[key] = usable_by_source.get(key, False) or usable
            label_by_source.setdefault(key, result.tool_id.value)
        # Sort by (label, stringified key): the key tuple mixes `str | None`
        # components (e.g. metric_id is None for tools that don't take one),
        # and `None` is not orderable against `str` in Python -- comparing
        # two raw keys directly raises TypeError as soon as two sources
        # share a label but differ in which discriminating field is unset.
        required_sources = sorted(
            label_by_source, key=lambda key: (label_by_source[key], repr(key))
        )
        available = [
            label_by_source[key] for key in required_sources if usable_by_source[key]
        ]
        unavailable = [
            label_by_source[key]
            for key in required_sources
            if not usable_by_source[key]
        ]
        stale = sorted(
            {
                result.tool_id.value
                for result in tool_results
                if any(
                    item.freshness is FreshnessState.STALE
                    for item in result.data_health
                )
                or any(
                    item.freshness is FreshnessState.STALE for item in result.metrics
                )
                or any(
                    item.freshness is FreshnessState.STALE for item in result.evidence
                )
            }
        )
        return DevCoverage(
            required_source_count=len(required_sources),
            available_source_count=len(available),
            unavailable_required_sources=unavailable,
            stale_required_sources=stale,
            as_of=as_of,
        )

    @staticmethod
    def _coverage_with_plan_sources(
        coverage: DevCoverage,
        investigation_result: DevInvestigationResult | None,
    ) -> DevCoverage:
        """Fold the plan's **mandatory** source observations into ``coverage``.

        CHAOS-3334. ``_coverage_from_tool_results`` above sees only what the
        *model* chose to call. When a plan governs the investigation, the
        server independently required a set of sources and observed how each
        one actually resolved -- and that half was persisted to
        ``dev_runs.plan_step_partition`` and then dropped on the floor for
        answer validation. A run whose mandatory ``required_source_health``
        step failed could still return ``status="complete"`` over a coverage
        of 1/1, because the one tool the model happened to call succeeded.
        That is a confident answer standing on a required production source
        that was never read: the laundering sibling of CHAOS-3332's crash.

        Merging here rather than at the validator is deliberate: coverage is
        already the server-owned field the answer contract judges
        completeness against (``DevAnswer.validate_status_consistency``
        refuses ``complete`` while any required source is unavailable or
        stale), so a mandatory plan failure becomes a refusal through the
        invariant that already exists, with no second completeness rule to
        keep in sync.

        ``mandatory`` observations always participate. ``conditional`` ones
        participate **unless they were never applicable** -- the distinction
        the plan itself draws, and one already present in the data rather
        than inferred:

        * ``not_applicable`` means the step's applicability predicate said
          this subject does not need the source at all. It must never block:
          ``status.entity.v2``'s two conditional requirements are
          ``not_applicable`` on every project-subject run, so counting them
          would refuse ``complete`` everywhere.
        * any *other* unmeasured state on a conditional means the predicate
          said the source **was** needed for this subject, the step ran, and
          the data did not arrive. ``_work_graph_applicable`` fires for
          issue/pull-request subjects, so this is reachable in production,
          not hypothetical: before this clause an issue-subject run whose
          ``work_graph_expansion`` failed still answered ``complete`` at
          coverage 3/3 with an empty unavailable list -- the same laundering
          this function exists to stop, one requirement level over.

        ``optional`` never participates: optional means the answer does not
        need it even when it is available.

        Only a *complete* claim is blocked either way -- ``partial`` remains
        reachable, and now names the missing source.

        Usability reuses ``UNMEASURED_REQUIREMENT_STATES``, the closed set
        the plan executor already treats as "never actually measured", so
        this cannot drift into a second, parallel vocabulary for the same
        question. ``truncated`` counts as unavailable because bounded,
        partial data cannot back a *complete* claim.

        ``available_unknown`` is grouped with ``available_stale``: the data
        is real and countable, but a ``complete`` claim over it is refused
        and the source is named in ``stale_required_sources`` so the
        disclosure machinery reports it. An earlier revision let it fall
        through to plain "available", citing the dimension adapters'
        severity ranking (``available_unknown`` outranks
        ``available_stale``) -- but that ranking is about *display* order,
        not completeness eligibility, and the two are not interchangeable.

        Every reachable producer of this state is a degradation, traced by
        executing all five ``state_mapping`` functions over their full source
        enums (CHAOS-3334 codex review, finding 2):

        * ``status_result_state_to_requirement_state(DEGRADED)`` -- its own
          docstring: "at least one contributing source is itself
          unavailable". Reachable on the mandatory ``status_snapshot`` step.
        * ``metric_data_state_to_requirement_state(PARTIAL)`` and
          ``(INSUFFICIENT_EVIDENCE)`` -- reachable on the mandatory
          ``registered_metric_query`` step.
        * ``freshness_state_to_requirement_state(UNKNOWN)`` is the only
          producer that would mean "healthy data, freshness merely unproven",
          and it has **no callers anywhere in the repository** -- it cannot
          put this state on a live observation today.

        So no legitimately-healthy run reaches ``available_unknown`` on a
        participating source, and blocking needs no split-by-cause. If
        ``freshness_state_to_requirement_state`` is ever wired up, that
        conclusion changes and the split becomes necessary --
        ``test_available_unknown_has_no_healthy_producer`` fails the moment a
        new producer appears, so the decision gets revisited rather than
        silently inherited.

        Labels are ``source_class`` values -- short, stable, content-free,
        and disjoint from the tool-id labels the tool-result half emits, so
        the union below cannot collide two different sources onto one entry.
        """

        if investigation_result is None:
            return coverage

        unavailable = list(coverage.unavailable_required_sources)
        stale = list(coverage.stale_required_sources)
        degraded = list(coverage.degraded_required_sources)
        seen = set(unavailable) | set(stale) | set(degraded)
        required_added = 0
        available_added = 0
        for observation in investigation_result.observations:
            if observation.requirement_level not in {"mandatory", "conditional"}:
                continue
            if (
                observation.requirement_level == "conditional"
                and observation.observed_state is SourceRequirementState.NOT_APPLICABLE
            ):
                continue
            label = observation.source_class.value
            required_added += 1
            if observation.observed_state in UNMEASURED_REQUIREMENT_STATES:
                if label not in seen:
                    unavailable.append(label)
                    seen.add(label)
            elif observation.observed_state is SourceRequirementState.AVAILABLE_STALE:
                available_added += 1
                if label not in seen:
                    stale.append(label)
                    seen.add(label)
            elif observation.observed_state is SourceRequirementState.AVAILABLE_UNKNOWN:
                available_added += 1
                if label not in seen:
                    degraded.append(label)
                    seen.add(label)
            else:
                available_added += 1
        if required_added == 0:
            return coverage
        # DevCoverage bounds its counts (<=100) and both id lists (<=25). No
        # plan declares anywhere near that many mandatory sources, but bounding
        # here keeps a future oversized one from turning an honest coverage
        # downgrade into a ValidationError that fails the whole run. What the
        # completeness invariant actually reads is *non-emptiness*, and
        # truncating a non-empty list cannot empty it, so the refusal survives
        # either bound.
        #
        # Constructed rather than `model_copy(update=...)`: model_copy skips
        # validators, so an edit that broke `validate_counts` (available must
        # not exceed required) would silently produce an invalid coverage that
        # only surfaced somewhere downstream. The invariant holds by
        # construction today -- available_added <= required_added, and `min`
        # is monotonic -- and this makes a future violation fail here.
        return DevCoverage(
            required_source_count=min(
                100, coverage.required_source_count + required_added
            ),
            available_source_count=min(
                100, coverage.available_source_count + available_added
            ),
            unavailable_required_sources=unavailable[:25],
            stale_required_sources=stale[:25],
            degraded_required_sources=degraded[:25],
            as_of=coverage.as_of,
        )

    @staticmethod
    def _legacy_named_entity_guard_reason(
        *,
        question: str,
        question_class: QuestionClass,
        authorized_scope: DevScope,
        resolve_scope_attempted: bool,
        last_resolve_scope_outcome: ScopeResolutionOutcome | None,
        answer: DevAnswer,
        extra_named_phrases: frozenset[str] = frozenset(),
    ) -> str | None:
        """CHAOS-3289: a status answer must never narrate a named entity that
        was never confirmed to exist.

        A **pure predicate** returning a closed-vocabulary reason code, never a
        message: CHAOS-3292 demoted this to telemetry on the preflight path,
        and telemetry must not be able to become public copy. The four user
        messages now live in ``_LEGACY_GUARD_TERMINALS`` and are reachable only
        from the flag-off path, where this remains the terminating
        defense-in-depth check it is today. Removal is blocked on the cutover
        issue (TRD §15 Phase D); what ships instead is the proof that disabling
        it does not reduce the new path's correctness.

        Organization scope is a legitimate, fully executable answer target on
        its own (CHAOS-3255) -- this only fires for STATUS-class questions
        still running under organization scope, and only in the cases that
        actually indicate a fabricated premise:

        * the run's own most recent resolve_scope.v1 attempt for a named
          entity came back ambiguous or not-found, yet the model still tried
          to finalize a substantive answer instead of disclosing that; or
        * resolve_scope.v1 was never attempted at all and the answer carries
          no evidence-backed claims -- an organization-wide answer with real
          support has real claims; one with neither a resolution attempt nor
          any claim is exactly the fabricated-narrative shape reported in
          CHAOS-3289 (the model skipped resolving "the X project" entirely
          and narrated organization-wide tool output under that name); or
        * resolve_scope.v1 was never attempted and the question names a
          specific entity (``_named_entity_phrases``) that the answer's own
          text still narrates -- this is the mixed variant of the same
          defect: the model can hold genuine, evidence-backed organization-
          wide claims (passing the empty-claims check above) while still
          attributing them to a name it never resolved. Both are fabricated
          premises; only the second needs the model's own words as evidence
          since the run produced no resolve_scope.v1 call to judge instead.

        Returns a reason code from ``_LEGACY_GUARD_TERMINALS``, or ``None``
        when the answer is not this failure shape.
        """
        if question_class is not QuestionClass.STATUS:
            return None
        if authorized_scope.direct_scope is not DirectScope.ORGANIZATION:
            return None
        if answer.status in {
            AnswerStatus.INSUFFICIENT_EVIDENCE,
            AnswerStatus.REFUSED,
            AnswerStatus.ERROR,
        }:
            return None
        if last_resolve_scope_outcome is ScopeResolutionOutcome.AMBIGUOUS:
            return "resolve_scope_ambiguous"
        if last_resolve_scope_outcome is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND:
            return "resolve_scope_not_found"
        if not resolve_scope_attempted:
            if not answer.claims:
                return "no_evidence_backed_claims"
            # The legacy grammar only recognizes a name adjacent to an entity
            # noun ("the X project"), so it is blind to a bare "X" — exactly
            # the shape the preflight re-arms it for. The preflight's own
            # unresolved spans are supplied here so the narration check can
            # actually see them (CHAOS-3292).
            named_phrases = _named_entity_phrases(question) | frozenset(
                phrase.casefold() for phrase in extra_named_phrases
            )
            if named_phrases:
                narrated_texts = (
                    answer.direct_summary,
                    *(claim.text for claim in answer.claims),
                )
                if any(
                    phrase in text.casefold()
                    for phrase in named_phrases
                    for text in narrated_texts
                ):
                    return "narrated_unresolved_entity"
        return None

    @staticmethod
    def _frame_cutover_active(preflight: SubjectPreflightResult | None) -> bool:
        """Whether CHAOS-3297's server-owned frame path governs this run.

        A named seam rather than an inline condition, for the same reason
        ``_legacy_guard_is_terminal`` is one: a mutation test can defeat
        exactly this decision and observe which acceptance case notices.

        The gate is ``ask_dev_wave_3_1``, not a new flag of this stack's own.
        ``production_runtime.build_runtime`` constructs ``preflight``,
        ``plan_registry`` and ``plan_executor`` together under that one org
        flag or not at all (and ``_wave_3_1_enabled`` fails *closed* to the
        flag-off path on any error), so ``preflight_result is not None`` is
        precisely "this organization is in the Wave 3.1 cohort". The
        flag-off path has no preflight, no plan, and no proven frame
        pipeline behind it -- demoting its backstop there would widen the
        cutover past the cohort the rollout gate defines.
        """

        return preflight is not None

    def _server_grounded_answer(
        self,
        *,
        answer_id: str,
        conversation_id: str,
        resolution: DevScopeResolution,
        coverage: DevCoverage,
        tool_results: tuple[DevToolResult, ...],
        investigation_result: DevInvestigationResult | None,
        model: DevModelMetadata,
        now: datetime,
        cutover_active: bool,
    ) -> DevAnswer | None:
        """The answer a demoted guard firing ships, or ``None`` to fail safe.

        CHAOS-3297 stack #5. Carries ONLY server-owned material: the
        canonical metrics/evidence the tool registry itself returned (the
        same tuples ``_canonical_answer_data`` hands the validator, so they
        are the objects the run actually retrieved, not a re-derivation),
        the server's own coverage accounting, and -- once ``finish()`` wraps
        this into a frame -- the plan's health/deficiency findings. The
        model's ``claims`` and ``direct_summary`` are exactly what the guard
        rejected and are structurally unreachable from here: this function
        takes no ``DevAnswer`` parameter at all, so no reviewed-and-rejected
        prose has a path into what it returns. That mirrors
        ``narrative_fallback.build_deterministic_fallback_narrative``'s own
        signature-level guarantee one layer down.

        Returns ``None`` -- meaning "terminate exactly as the pre-cutover
        code did" -- in three cases, each of which is a case where shipping
        would be worse than failing:

        * the cutover is not active for this run (``ask_dev_wave_3_1`` off);
        * the tool results disagree about a canonical object
          (``_canonical_answer_data`` returns ``None``), which is an
          integrity failure, not a grounding one;
        * there is no canonical metric and no canonical evidence. An
          "answer" built from nothing but this module's own copy would be a
          substantive-looking shell, which is the precise failure the
          CHAOS-3290 floor exists to prevent.

        On that last point, ``investigation_result`` is deliberately NOT
        part of the predicate, and the parameter is kept only to document
        that (codex adversarial review, round 1 HIGH -- an earlier revision
        did count plan findings as sufficient material). The plan's
        health/deficiency findings are real server-computed content, but
        ``finish()`` embeds them into the FRAME, and no client surface reads
        a frame today: ``streaming.py`` sends ``result.answer`` live, and
        ``router``'s replay prefers the stored v1 answer. So a run demoted
        on the strength of findings alone would terminate COMPLETED while
        the client received an answer with no claim, no metric and no
        evidence -- the exact empty shell this function's third guard
        exists to refuse, and a worse outcome than the honest
        ``insufficient_evidence`` it replaced. When canonical material IS
        present the findings ride along on the frame for free, which is the
        only case where they add anything a caller can reach. Revisit once
        CHAOS-3298 puts v2 on the wire and a findings-only frame is
        genuinely readable.
        """

        del investigation_result  # see the docstring: documented non-input
        if not cutover_active:
            return None
        canonical_data = self._canonical_answer_data(tool_results)
        if canonical_data is None:
            return None
        canonical_metrics, canonical_evidence = canonical_data
        if not (canonical_metrics or canonical_evidence):
            return None
        degraded = any(
            result.status in {"unavailable", "error"} for result in tool_results
        )
        return DevAnswer(
            schema_version="dev_answer.v1",
            answer_id=answer_id,
            conversation_id=conversation_id,
            generated_at=now,
            resolved_scope=resolution,
            as_of=now,
            # Never COMPLETE: a run whose model summary was withheld has by
            # construction not reported everything it could have, and
            # COMPLETE additionally asserts every required source was fresh
            # and available (DevAnswer.validate_answer_invariants).
            status=AnswerStatus.DEGRADED if degraded else AnswerStatus.PARTIAL,
            direct_summary=SERVER_GROUNDED_SUMMARY,
            claims=[],
            metrics=canonical_metrics,
            evidence=canonical_evidence,
            conflicts=[],
            coverage=coverage,
            warnings=[SERVER_GROUNDED_WARNING],
            suggested_follow_up_questions=[],
            versions=self._versions,
            model=model,
        )

    @staticmethod
    def _legacy_guard_is_terminal(
        preflight: SubjectPreflightResult | None, reason: str
    ) -> bool:
        """Whether a CHAOS-3289 guard firing may still terminate the run.

        A named seam rather than an inline condition so a mutation test can
        defeat exactly this decision and observe which acceptance case notices
        (TRD §10: on the preflight path a firing is a cutover defect to record,
        not an outcome to act on).

        The flag-off path keeps every reason terminal — the backstop is the
        only check there is. A preflight run that saw a bare name it could not
        resolve keeps only the reasons that are *evidence about that name*: the
        answer narrating it, or a resolution attempt that came back ambiguous
        or not-found. ``no_evidence_backed_claims`` stays telemetry even then,
        because it is a proxy for the shape of the answer rather than a
        statement about the unresolved name, and terminating on it would fail
        an ordinary organization-wide question that merely happens to contain
        a capitalized acronym.
        """

        if preflight is None:
            return True
        return (
            preflight.legacy_guard_required and reason in _NAME_SPECIFIC_GUARD_REASONS
        )

    @staticmethod
    def _subject_gate_rejection(
        *,
        tool_id: ToolID,
        allowed_tools: frozenset[ToolID],
        preflight: SubjectPreflightResult | None,
    ) -> str | None:
        """Deny-by-default: why this tool call must not execute, or ``None``.

        Two independent clauses, mutated separately in the mutation suite:
        the per-run availability allowlist, and the "every mention holds an
        exact match" requirement for a subject-bearing tool.
        """

        if tool_id not in allowed_tools:
            return "The requested tool is not available for this run."
        if preflight is None or preflight.ledger is None:
            return None
        if tool_id not in SUBJECT_BEARING_TOOLS:
            return None
        if not preflight.all_subjects_committed:
            return "The requested subject was not resolved."
        return None

    @staticmethod
    def _canonical_tool_request(
        *, decision: AgentToolRequest, run_id: str, authorized_scope: DevScope
    ) -> tuple[DevToolRequest, str]:
        try:
            tool_id = ToolID(decision.tool_id)
        except ValueError as exc:
            raise ToolRegistryError("tool is not registered") from exc
        allowed = {
            "query",
            "metric_id",
            "evidence_ref_ids",
            "include_comparison",
            "limit",
        }
        server_owned = {"schema_version", "run_id", "tool_call_id", "tool_id", "scope"}
        unknown = set(decision.arguments) - allowed - server_owned
        if unknown:
            # An invalid model-generated argument shape, not a registry defect;
            # classify it with the same degradable error as validate_request
            # rejections (CHAOS-3262) instead of the generic registry error.
            raise ToolRequestRejected("tool request contains unsupported arguments")
        arguments = {
            key: value for key, value in decision.arguments.items() if key in allowed
        }
        payload: dict[str, Any] = {
            "schema_version": "dev_tool_request.v1",
            "run_id": run_id,
            "tool_call_id": decision.call_id,
            "tool_id": tool_id.value,
            "scope": authorized_scope.model_dump(mode="json"),
            **arguments,
        }
        try:
            request = DevToolRequest.model_validate(payload)
        except ValidationError as exc:
            # The model produced schema-valid-looking but semantically invalid
            # arguments (e.g. an empty-string query). Classify uniformly with
            # ToolRequestRejected so the run loop can degrade this single
            # tool call instead of treating it as an internal error.
            raise ToolRequestRejected(
                "tool request does not conform to the tool contract"
            ) from exc
        canonical = json.dumps(
            {"tool_id": tool_id.value, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
        return request, digest

    @staticmethod
    def _rejected_tool_execution(
        *, tool_request: DevToolRequest, code: str, message: str
    ) -> ToolExecution:
        """Degrade one failed per-call tool attempt instead of failing the run.

        CHAOS-3262: an advertised tool that the model called with arguments
        outside its contract, or that timed out, must not terminate the
        whole run as ``tool_unavailable``. The model is told, via a normal
        failed tool result, what happened, so it can correct course (or the
        answer can proceed degraded) within the existing tool-call and
        wall-clock budgets. Run-level registry failures (unknown tools,
        malformed executor output, cancellation) remain fatal.
        """
        result = DevToolResult(
            schema_version="dev_tool_result.v1",
            run_id=tool_request.run_id,
            tool_call_id=tool_request.tool_call_id,
            tool_id=tool_request.tool_id,
            status="error",
            error=DevError(
                schema_version="dev_error.v1",
                request_id=tool_request.run_id,
                code=code,
                safe_message=message,
                retryable=True,
            ),
            serialized_bytes=0,
        )
        serialized = json.dumps(
            result.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return ToolExecution(
            result=result, serialized_bytes=len(serialized), latency_ms=0
        )

    @staticmethod
    def _provider_error(request_id: str, exc: AgentProviderError) -> DevError:
        code_map = {
            AgentProviderErrorCode.DISABLED: "feature_not_enabled",
            AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED: "provider_not_configured",
            AgentProviderErrorCode.MODEL_NOT_SUPPORTED: "model_not_supported",
            AgentProviderErrorCode.PROVIDER_UNAVAILABLE: "provider_unavailable",
            AgentProviderErrorCode.INVALID_REQUEST: "invalid_request",
            AgentProviderErrorCode.RATE_LIMITED: "rate_limited",
            AgentProviderErrorCode.INVALID_RESPONSE: "internal_error",
            AgentProviderErrorCode.TIMEOUT: "provider_unavailable",
            AgentProviderErrorCode.CANCELLED: "cancelled",
            AgentProviderErrorCode.BUDGET_EXHAUSTED: "cost_limit_reached",
            AgentProviderErrorCode.BUDGET_UNAVAILABLE: "provider_unavailable",
            AgentProviderErrorCode.PROVIDER_CONTRACT_VIOLATION: (
                "provider_contract_violation"
            ),
            # CHAOS-3285: output/reasoning-budget exhaustion is a structural,
            # non-retryable model-capability mismatch, not an opaque
            # application failure -- reuse the existing "model_not_supported"
            # public code rather than the internal_error bucket
            # INVALID_RESPONSE previously fell into for this exact symptom.
            # A dedicated dev_error.v1 code is CHAOS-3294's v2 vocabulary to
            # own, not invented here.
            AgentProviderErrorCode.OUTPUT_EXHAUSTED: "model_not_supported",
        }
        code = code_map[exc.code]
        return DevError(
            schema_version="dev_error.v1",
            request_id=request_id,
            code=code,
            safe_message=str(exc),
            retryable=exc.retryable,
            remediation=dev_error_remediation(code),
        )


__all__ = [
    "DevOrchestrator",
    "DevRunLimits",
    "EventCancellationSignal",
    "NullRunRecorder",
    "OrchestratorEvent",
    "OrchestratorResult",
    "ProviderBudget",
    "RunRecorder",
    "RunState",
    "ScopeResolver",
]
