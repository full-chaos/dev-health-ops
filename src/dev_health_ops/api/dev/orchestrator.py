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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import annotated_types
from pydantic import ValidationError

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
    ASK_DEV_PLAN_REGISTRY_GAP_TOTAL,
    ASK_DEV_TOOL_EXECUTOR_FAULT_TOTAL,
    ASK_DEV_UNHANDLED_RUN_FAULT_TOTAL,
    ASK_DEV_UNREGISTERED_TERMINAL_CODE_TOTAL,
)

from . import terminal_frames
from .answer_validator import (
    AnswerValidationContext,
    AnswerValidationError,
    completion_truncation_detail,
    validate_answer_candidate,
)
from .contracts import (
    AnswerStatus,
    DevAnswer,
    DevContractVersions,
    DevCoverage,
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
    FreshnessState,
    MetricID,
    QuestionClass,
    ScopeResolutionOutcome,
    ToolID,
    dev_error_remediation,
)
from .contracts_v2 import (
    Cardinality,
    DevInvestigationResult,
    DevResolutionEntry,
    DevSubjectSet,
    QuestionIntentID,
)
from .contracts_v2.base import SourceRequirementState
from .contracts_v2.frame import DevAnswerFrame
from .contracts_v2.plan import DevInvestigationPlan
from .investigation_plans import PlanExecutor, StepContext
from .investigation_plans.state_mapping import UNMEASURED_REQUIREMENT_STATES
from .orchestrator_states import TERMINAL_STATES, RunState
from .org_policy import ASK_DEV_RUN_COST_HARD_MAX_MICROUSD
from .preflight_outcomes import (
    LEGACY_ONLY_QUESTION_INTENTS,
    TERMINAL_STATE_BY_OUTCOME,
    project_preflight_error,
)
from .prompts import PromptComposer, PromptConversationTurn
from .subject_preflight import (
    SUBJECT_BEARING_TOOLS,
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

    def __post_init__(self) -> None:
        if self.state not in TERMINAL_STATES:
            raise ValueError("orchestrator result must be terminal")
        if (self.answer is None) == (self.error is None):
            raise ValueError(
                "orchestrator result requires exactly one terminal payload"
            )


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
        """Persist one ``dev_resolution_ledger.v1`` entry (CHAOS-3325).

        Called only for the terminating ``ambiguous_candidates`` entry of a
        preflight ambiguity — the exact entry
        ``SubjectPreflightResult.terminating_resolution_entry`` carries —
        always immediately before ``record_frame`` for the same outcome, so
        the persisted frame's ``clarification_candidates`` always has a
        matching ledger row to be authorized against
        (``persistence.service._authorize_clarification_candidates``, a
        Codex-review NO-SHIP finding: a schema-valid frame could otherwise
        name an entity the ledger never authorized).
        """
        ...

    async def record_frame(self, frame: DevAnswerFrame) -> None:
        """Persist one already-built ``dev_answer_frame.v1`` (CHAOS-3297).

        Called from the preflight TERMINATE branch with the frame
        ``preflight_outcomes.build_preflight_answer`` already validated, so
        the run tags ``contract_generation = 'v2'`` and the CHAOS-3299 v2
        replay branch (``router._replayed_result``) becomes reachable for a
        preflight-terminated run, not just a full model-round completion.
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

    async def record_frame(self, frame: DevAnswerFrame) -> None:
        del frame

    async def rollback(self) -> None:
        return None

    async def record_investigation_result(self, result: DevInvestigationResult) -> None:
        del result

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
        selected_event_sink = event_sink or self._event_sink
        # CHAOS-3297 stack #3 (team-lead boundary ruling, 2026-08-02): set
        # by the CHAOS-3295 plan-execution block below when a plan actually
        # runs, read by `finish()`'s closure (a free variable over this
        # enclosing scope, not `nonlocal` -- `finish()` only reads it) so
        # the legacy answer's frame can embed the plan's findings alongside
        # it, without threading a new parameter through every one of
        # `finish()`'s ~35 call sites.
        investigation_result: DevInvestigationResult | None = None

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
        ) -> OrchestratorResult:
            nonlocal terminal_written
            if terminal_written:
                raise RuntimeError("terminal state already written")
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
                    if answer is None:
                        # No prior write in this flush to protect, so it is
                        # safe to roll back a poisoned session -- otherwise
                        # the terminal() write below could raise
                        # PendingRollbackError (mirrors the preflight
                        # TERMINATE branch, CHAOS-3297 Codex review).
                        await self._recorder.rollback()
                    # else: record_answer already succeeded in this flush.
                    # Rolling back here would discard that write over an
                    # unrelated frame failure -- a dropped frame is
                    # recoverable, a dropped answer is not, so this path
                    # deliberately leaves the session as-is and proceeds
                    # frame-less.
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
                if preflight_result.decision is PreflightDecision.TERMINATE:
                    assert preflight_result.answer is not None
                    assert preflight_result.outcome is not None
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
                        if preflight_result.terminating_resolution_entry is not None:
                            await self._recorder.append_resolution(
                                preflight_result.terminating_resolution_entry
                            )
                        await self._recorder.record_frame(preflight_result.answer.frame)
                    except Exception:
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
                        await self._recorder.record_preflight(
                            preflight_outcome=preflight_result.diagnostic,
                            legacy_guard_reason=None,
                        )
                    return await finish(
                        TERMINAL_STATE_BY_OUTCOME[preflight_result.outcome],
                        error=project_preflight_error(
                            preflight_result.answer, request_id=request.request_id
                        ),
                        frame_already_recorded=True,
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
                if plan_eligible:
                    assert plan is not None
                    await transition(RunState.TOOL_EXECUTION)
                    subject_entity_id = (
                        authorized_scope.entity_refs[0].entity_id
                        if cardinality is Cardinality.SINGULAR
                        and authorized_scope.entity_refs
                        else None
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
                        ),
                        run_id=run_id,
                        subject_entity_id=subject_entity_id,
                    )
                    await self._recorder.record_investigation_result(
                        investigation_result
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
                            "coverage": self._coverage_with_plan_sources(
                                self._coverage_from_tool_results(
                                    tuple(tool_requests), tuple(tool_results), now
                                ),
                                investigation_result,
                            ).model_dump(mode="json"),
                            "versions": self._versions.model_dump(mode="json"),
                            "model": model.model_dump(mode="json"),
                        }
                    )
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
                            code, message = _LEGACY_GUARD_TERMINALS[guard_reason]
                            return await finish(
                                RunState.INSUFFICIENT_EVIDENCE,
                                error=error(code, message),
                            )
                    return await finish(RunState.COMPLETED, answer=answer)

                if isinstance(decision, AgentDisambiguation):
                    return await finish(
                        RunState.INSUFFICIENT_EVIDENCE,
                        error=error(
                            "scope_ambiguous",
                            "The request requires scope clarification.",
                        ),
                    )
                if isinstance(decision, AgentRefusal):
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

    def _budget_answer(
        self,
        *,
        answer_id: str,
        conversation_id: str,
        resolution: DevScopeResolution,
        tool_results: tuple[DevToolResult, ...],
        model_fingerprint: str,
    ) -> DevAnswer | None:
        """Return only canonical retrieved data when a later model call is blocked."""

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
        seen = set(unavailable) | set(stale)
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
            elif observation.observed_state in {
                SourceRequirementState.AVAILABLE_STALE,
                SourceRequirementState.AVAILABLE_UNKNOWN,
            }:
                available_added += 1
                if label not in seen:
                    stale.append(label)
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
