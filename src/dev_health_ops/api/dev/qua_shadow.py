"""CHAOS-3389 shadow phase: the Question Understanding Agent (QUA) shadow seam.

Runs a bounded, no-tool, structured-output LLM call **alongside** the
deterministic ``SubjectPreflight`` resolver, over a candidate shortlist the
caller is already authorized to see (the same ``ScopeResolutionService``
catalog boundary the deterministic path itself uses, never a new subject
directory). The proposal is recorded as a ``QUAShadowRecord`` and **never**
fed back into any live decision -- see ``orchestrator.py``'s call site for
the structural half of that guarantee (nothing downstream reads the record
this module returns).

Hardening conditions this module builds in from the start (CHAOS-3389
adversarial critique, comment 7d1368d9):

* **Permission fingerprint in any cache key.** This module introduces no new
  cache. It calls ``ScopeResolutionService.search()`` directly, which is
  already keyed by ``(org_id, permission_fingerprint, payload, watermark)``
  (``scope_service.py:716-719``) -- reusing that boundary is what makes "the
  shadow shortlist is exactly what the caller is authorized to see" true
  without inventing a second cache to get wrong.
* **Org-wide-cardinality widening needs deterministic corroboration.** See
  ``_cardinality_corroborated``: a model-proposed ``organization_wide``
  cardinality is trusted (for shadow analytics -- again, never for a live
  decision) only when the deterministic interpreter independently reached
  the same cardinality. This is what stops "how is the Contxt Fabric doing?"
  from reading as a corroborated org-wide proposal just because the model
  found no candidate id to name.
* **Never-widen is enforced in TWO places, and the split is exact.** This
  bullet has now been wrong in both directions, so it states the boundary
  rather than a slogan.

  *Call-wide, structurally, on the wire.* ``_response_schema`` enumerates the
  authorized indices as ``enum``, rebuilt per call, so a provider cannot
  express an index this CALL did not authorize. Read the mechanism, because
  the keyword choice is the whole point: ``OpenAICompatibleAgentProvider``
  projects every schema through ``_structural_schema``, which keeps only
  ``_STRUCTURAL_SCHEMA_KEYS``. ``minimum``/``maximum``/``minItems``/
  ``maxItems``/``maxLength`` are NOT in that set; ``enum`` is. The original
  bullet claimed this guarantee on the strength of ``[0, len(combined) - 1]``
  range bounds, and was FALSE for as long as it was written (CHAOS-3536) --
  those bounds were stripped before dispatch on every call that ever ran.
  CHAOS-3537 re-expressed the bound in a keyword that survives, which is also
  strictly stronger: a range admits every integer between its ends, an enum
  admits exactly the shortlist.

  *Per-mention, at runtime.* ``_verify`` re-checks every accepted index
  against its OWN mention's authorized slice, which the wire schema cannot
  express at all -- the schema is built once per call from the COMBINED
  shortlist, so a mention whose own slice is empty (past
  ``max_total_candidates``) still sees the call's full range. CHAOS-3525
  additionally hardened the singular path to resolve by identity.

  **These are two STAGES, not two coverages** -- the first version of this
  bullet claimed the latter and was wrong. Every mention slice is a subset of
  ``[0, len(combined))``, so any index the enum rejects is also outside every
  mention's slice: ``_verify``'s rejection set strictly SUBSUMES the enum's.
  The enum is worth having because it acts BEFORE generation (an unauthorized
  index is never produced, rather than produced and then rejected, which also
  means the proposal survives instead of being discarded) and because it ends
  ``_verify``'s status as a single point of failure. Per-mention ownership is
  ``_verify``'s alone and always will be: the schema cannot express it.

  With zero candidates authorized, ``selected_candidate_index`` is
  ``{"type": "null"}``. ``candidate_indices`` stays a plain integer array and
  is bounded only by ``_verify`` -- see ``_response_schema`` for the
  empty-enum option that was measured and deliberately not adopted.
* **LLM unavailable degrades to silent skip, never a block.** Every branch
  of ``evaluate()`` returns a ``QUAShadowRecord`` (never raises); the
  orchestrator's own call site additionally wraps the call and the
  recorder write in defensive ``try/except`` so a bug in either can never
  roll back or fail the run it is shadowing.
* **Latency instrumented, never gated.** ``latency_ms`` is captured on
  every branch that reaches the provider call (success, timeout, and
  provider error alike), for the future probe-certification's budget
  evidence -- nothing in this module or its caller conditions the live run
  on it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentLLMProvider,
    AgentMessage,
    AgentMessageRole,
)
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.metrics.prometheus import (
    ASK_DEV_QUA_SHADOW_CARDINALITY_UNCORROBORATED_TOTAL,
)

from .contracts_v2.base import Cardinality, QuestionIntentID
from .contracts_v2.question_understanding import (
    QUESTION_UNDERSTANDING_SCHEMA_VERSION,
    DevQuestionUnderstanding,
    QUAOutcome,
)
from .contracts_v2.subject import DevSubjectMention
from .question_interpreter import InterpretedQuestion
from .scope_service import (
    SEARCHABLE_ENTITY_KINDS,
    AuthorizedEntity,
    EntityKind,
    ScopeResolutionService,
    ScopeSearchRequest,
)
from .subject_preflight import PreflightDecision

logger = logging.getLogger(__name__)

__all__ = [
    "QUA_SHADOW_RESOLUTION_PATH",
    "QUAShadowConfig",
    "QUAShadowMentionAssessment",
    "QUAShadowRecord",
    "QUAShadowStatus",
    "QuestionUnderstandingShadow",
]

#: The exact ``resolution_path`` value lane 2a's ``wave4_case_result.v1``
#: corpus receipt reserves for a case this shadow seam evaluated (team-lead
#: coordination ruling, 2026-08-05: fixed by decree as kebab-case
#: ``"qua-shadow"``, with ``"qua-committed"`` reserved for the future rank/
#: commit modes, so no reconciliation is needed once 2a's receipt type
#: lands). This module does not write that receipt itself -- 2a's runner
#: does not exist on any branch yet as of this writing -- but the string is
#: pinned here, in one place, so 2a's runner can import it instead of
#: hand-typing a literal that could drift from this module's own vocabulary.
QUA_SHADOW_RESOLUTION_PATH = "qua-shadow"

_SYSTEM_PROMPT = (
    "You are the Ask Dev Question Understanding Agent, running in SHADOW "
    "MODE. Your output is recorded for offline comparison only and NEVER "
    "affects any live answer, tool call, or scope decision. You are given "
    "the user's question and, for each named mention, a closed list of "
    "already-authorized candidate entities identified only by an integer "
    'index. For each mention, propose an outcome ("resolved" if exactly '
    'one candidate is clearly the one named, "ambiguous" if more than one '
    'plausibly is, "no_match" if none is) and, only for "resolved", the '
    "single candidate index. You must never propose an index that was not "
    "listed for that exact mention, and you must never invent an entity "
    "name, id, or index. Also propose the overall intent_id and cardinality "
    "for the question as a whole."
)


class QUAShadowStatus(StrEnum):
    """How one shadow evaluation went.

    Entirely internal to the shadow audit trail -- distinct from, and never
    merged with, ``ResolutionOutcome`` or ``PreflightDecision`` (the live,
    closed vocabularies the deterministic resolver owns). Adding a member
    here can never widen what the live resolution path can express.
    """

    EVALUATED = "evaluated"
    SKIPPED_DISABLED = "skipped_disabled"
    SKIPPED_NO_PROVIDER = "skipped_no_provider"
    SKIPPED_NO_MENTIONS = "skipped_no_mentions"
    SKIPPED_BUDGET_EXHAUSTED = "skipped_budget_exhausted"
    SKIPPED_CATALOG_UNAVAILABLE = "skipped_catalog_unavailable"
    SKIPPED_TIMEOUT = "skipped_timeout"
    SKIPPED_PROVIDER_ERROR = "skipped_provider_error"
    SKIPPED_UNEXPECTED_DECISION = "skipped_unexpected_decision"
    SKIPPED_INVALID_OUTPUT = "skipped_invalid_output"


@dataclass(frozen=True, slots=True)
class QUAShadowConfig:
    #: The single flag flip CHAOS-3389 requires: off is byte-identical to
    #: the seam not existing at all (see ``evaluate``'s first branch).
    enabled: bool = False
    #: CHAOS-3525: whether an EVALUATED proposal may be promoted to the run's
    #: committed subject, instead of only being recorded.
    #:
    #: A SECOND gate on purpose, rather than widening what ``enabled`` means.
    #: ``enabled`` is the shadow's own contract -- "this seam observes and
    #: never influences" -- and that contract is what
    #: ``test_chaos_3389_qua_shadow.py``'s byte-identity tests certify. Making
    #: ``enabled`` also mean "and may now change the answer" would retire
    #: those proofs by redefinition rather than by evidence, and would leave
    #: no flag state in which the shadow can still be run purely for
    #: evidence. Two flags keep the rollout ladder (shadow -> commit) real:
    #: ``commit_enabled`` is meaningless unless ``enabled`` is also set, since
    #: there is no proposal to promote otherwise.
    commit_enabled: bool = False
    #: Platform-spec hard cap (comment 6fa38d88, "Performance and budgets").
    #: Also bounded by the run's own remaining wall-clock budget at the call
    #: site -- whichever is smaller governs.
    hard_timeout_seconds: float = 2.5
    max_candidates_per_mention: int = 25
    max_total_candidates: int = 50
    max_output_tokens: int = 1024


@dataclass(frozen=True, slots=True)
class QUAShadowMentionAssessment:
    """One mention's shadow proposal, verified against its OWN authorized
    shortlist slice -- never the call's combined shortlist.

    ``rejected_reason`` is set (and ``selected_entity`` forced ``None``)
    whenever the model's claimed index fell outside this mention's own
    slice, however schema-valid the integer was for the call as a whole
    (e.g. it named a candidate that was only ever shown for a DIFFERENT
    mention). The rest of the assessment is still recorded -- a rejection
    is shadow-analytics signal, not a reason to discard the row.
    """

    mention_id: str
    text_span: str
    outcome: QUAOutcome
    selected_entity: AuthorizedEntity | None
    candidate_entities: tuple[AuthorizedEntity, ...]
    confidence: float
    rejected_reason: str | None = None


@dataclass(frozen=True, slots=True)
class QUAShadowRecord:
    schema_version: str = "qua_shadow_record.v1"
    status: QUAShadowStatus = QUAShadowStatus.SKIPPED_DISABLED
    deterministic_decision: PreflightDecision | None = None
    #: Captured whenever a provider call was actually attempted (success,
    #: timeout, or provider error) -- zero on every branch that skipped
    #: before reaching the provider.
    latency_ms: float = 0.0
    model_fingerprint: str | None = None
    intent_id: QuestionIntentID | None = None
    cardinality: Cardinality | None = None
    cardinality_corroborated: bool | None = None
    requires_clarification: bool | None = None
    mentions: tuple[QUAShadowMentionAssessment, ...] = ()
    error_class: str | None = None

    @property
    def evaluated(self) -> bool:
        return self.status is QUAShadowStatus.EVALUATED


class QuestionUnderstandingShadow:
    """Runs the QUA shadow call alongside the deterministic resolver.

    Structural non-influence: ``evaluate()`` returns a ``QUAShadowRecord``
    and has no other effect -- no argument is mutated, nothing is written
    anywhere, no exception escapes. The caller decides what (if anything)
    to do with the record; production only ever persists it.
    """

    def __init__(
        self,
        *,
        provider: AgentLLMProvider | None,
        scope_service: ScopeResolutionService,
        config: QUAShadowConfig,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._provider = provider
        self._scope_service = scope_service
        self._config = config
        self._now = now
        self._monotonic = monotonic

    @property
    def commit_enabled(self) -> bool:
        """Whether a proposal from this shadow may be promoted (CHAOS-3525).

        Read by the orchestrator's promotion seam. Exposed as a property
        rather than reaching into ``_config`` so the gate has one reader and
        one name, and so a caller cannot accidentally consult ``enabled``
        (which means only "a proposal was produced") when it meant this.
        """

        return self._config.commit_enabled

    @property
    def scope_service(self) -> ScopeResolutionService:
        """The SAME authorization boundary this shadow built its shortlist from.

        CHAOS-3525's commit-time re-verification has to ask the boundary
        again, and it must be the identical service instance -- a different
        one would carry a different request-scoped, permission-fingerprint-
        keyed cache, so "is this entity authorized for this caller" could be
        answered from a snapshot the proposal was never checked against.
        """

        return self._scope_service

    async def evaluate(
        self,
        *,
        question: str,
        interpretation: InterpretedQuestion,
        org_id: str,
        permission_fingerprint: str,
        deterministic_decision: PreflightDecision,
        remaining_seconds: float,
    ) -> QUAShadowRecord:
        if not self._config.enabled:
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_DISABLED,
                deterministic_decision=deterministic_decision,
            )
        if self._provider is None:
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_NO_PROVIDER,
                deterministic_decision=deterministic_decision,
            )
        mentions = interpretation.mentions
        if not mentions:
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_NO_MENTIONS,
                deterministic_decision=deterministic_decision,
            )
        budget = min(self._config.hard_timeout_seconds, max(0.0, remaining_seconds))
        if budget <= 0:
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED,
                deterministic_decision=deterministic_decision,
            )

        # Codex adversarial review round 1 (HIGH, confirmed): `budget` above
        # is the deadline for the WHOLE call, not just the provider request
        # -- catalog search runs first and was previously uncapped, so a
        # slow (or hanging) catalog could blow through `hard_timeout_seconds`
        # entirely before the provider call even starts, directly
        # contradicting "never blocks or degrades the run" (this whole
        # method is awaited synchronously in the orchestrator's critical
        # path). Bounded here, and the provider call below gets what's
        # actually left, not the stale up-front `budget`.
        shortlist_started = self._monotonic()
        try:
            per_mention_candidates = await asyncio.wait_for(
                self._shortlist(
                    interpretation=interpretation,
                    org_id=org_id,
                    permission_fingerprint=permission_fingerprint,
                ),
                timeout=budget,
            )
        except (TimeoutError, asyncio.TimeoutError):
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_CATALOG_UNAVAILABLE,
                deterministic_decision=deterministic_decision,
                latency_ms=(self._monotonic() - shortlist_started) * 1000.0,
            )
        except Exception:
            logger.exception(
                "ask_dev.qua_shadow.catalog_fault", extra={"org_id": org_id}
            )
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_CATALOG_UNAVAILABLE,
                deterministic_decision=deterministic_decision,
                latency_ms=(self._monotonic() - shortlist_started) * 1000.0,
            )

        remaining_budget = budget - (self._monotonic() - shortlist_started)
        if remaining_budget <= 0:
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED,
                deterministic_decision=deterministic_decision,
                latency_ms=(self._monotonic() - shortlist_started) * 1000.0,
            )

        combined, mention_ranges = self._combine_shortlists(
            mentions=mentions, per_mention_candidates=per_mention_candidates
        )
        messages = self._build_messages(
            question=question,
            mentions=mentions,
            combined=combined,
            mention_ranges=mention_ranges,
        )
        response_schema = self._response_schema(
            mention_count=len(mentions), candidate_count=len(combined)
        )

        started = self._monotonic()
        try:
            result = await asyncio.wait_for(
                self._provider.decide(
                    messages=messages,
                    tools=(),
                    response_schema=response_schema,
                    timeout_seconds=remaining_budget,
                    max_output_tokens=self._config.max_output_tokens,
                    signal=None,
                ),
                timeout=remaining_budget,
            )
        except (TimeoutError, asyncio.TimeoutError):
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_TIMEOUT,
                deterministic_decision=deterministic_decision,
                latency_ms=(self._monotonic() - started) * 1000.0,
            )
        except AgentProviderError as exc:
            # Codex round 1 (MEDIUM, confirmed): every AgentProviderError
            # used to collapse onto SKIPPED_PROVIDER_ERROR, so the
            # provider's OWN internal timeout (raised as
            # AgentProviderErrorCode.TIMEOUT -- e.g.
            # ScriptedAgentProvider's/the real OpenAI-compatible adapter's
            # internal wait racing this call's own timeout_seconds) was
            # indistinguishable from a genuine provider failure. Mapped to
            # the SAME closed status this method's own asyncio.wait_for
            # timeout already uses -- both really are "the call did not
            # finish in time", just raised from two different layers.
            # `error_class` carries the specific code (rate_limited, ...)
            # for every OTHER AgentProviderError kind, so those stay
            # distinguishable in the persisted record/logs without growing
            # the closed status vocabulary itself.
            #
            # CHAOS-3452: BUDGET_EXHAUSTED/BUDGET_UNAVAILABLE -- raised by
            # ``attach_qua_shadow_budget_guard`` (llm/qua_shadow_budget.py)
            # when the ISOLATED shadow quota (never the live BYO budget) is
            # exhausted or its accounting fails -- get their own case here
            # rather than falling into the generic provider-error bucket:
            # the isolated shadow quota running out is an expected,
            # frequent, typed-skip outcome (this seam's OWN budget
            # discipline working as designed), not a provider fault.
            # Reuses ``SKIPPED_BUDGET_EXHAUSTED`` (previously only reached
            # via the wall-clock deadline check above) since both really are
            # "this call's own budget ran out", just in two different
            # currencies (seconds vs. micro-USD) -- ``error_class`` keeps
            # them distinguishable in the persisted record/logs.
            status = (
                QUAShadowStatus.SKIPPED_TIMEOUT
                if exc.code is AgentProviderErrorCode.TIMEOUT
                else (
                    QUAShadowStatus.SKIPPED_BUDGET_EXHAUSTED
                    if exc.code
                    in (
                        AgentProviderErrorCode.BUDGET_EXHAUSTED,
                        AgentProviderErrorCode.BUDGET_UNAVAILABLE,
                    )
                    else QUAShadowStatus.SKIPPED_PROVIDER_ERROR
                )
            )
            logger.warning(
                "ask_dev.qua_shadow.provider_error",
                extra={"provider_error_code": exc.code.value},
            )
            return QUAShadowRecord(
                status=status,
                deterministic_decision=deterministic_decision,
                latency_ms=(self._monotonic() - started) * 1000.0,
                error_class=exc.code.value,
            )
        except Exception as exc:  # defense in depth: never propagate
            logger.exception("ask_dev.qua_shadow.provider_fault")
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_PROVIDER_ERROR,
                deterministic_decision=deterministic_decision,
                latency_ms=(self._monotonic() - started) * 1000.0,
                error_class=type(exc).__name__,
            )
        latency_ms = (self._monotonic() - started) * 1000.0

        if not isinstance(result.decision, AgentFinalAnswer):
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_UNEXPECTED_DECISION,
                deterministic_decision=deterministic_decision,
                latency_ms=latency_ms,
                model_fingerprint=result.model_fingerprint,
            )
        try:
            parsed = DevQuestionUnderstanding.model_validate(result.decision.value)
        except ValidationError:
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_INVALID_OUTPUT,
                deterministic_decision=deterministic_decision,
                latency_ms=latency_ms,
                model_fingerprint=result.model_fingerprint,
            )
        if len(parsed.mentions) != len(mentions):
            # Totality: the model must account for every mention it was
            # shown, in order -- a short or padded ``mentions`` list cannot
            # be zipped against the real mentions without silently
            # mismatching which proposal belongs to which named subject.
            return QUAShadowRecord(
                status=QUAShadowStatus.SKIPPED_INVALID_OUTPUT,
                deterministic_decision=deterministic_decision,
                latency_ms=latency_ms,
                model_fingerprint=result.model_fingerprint,
            )

        assessments = self._verify(
            mentions=mentions,
            proposal=parsed,
            combined=combined,
            mention_ranges=mention_ranges,
        )
        cardinality_corroborated = self._cardinality_corroborated(
            interpretation=interpretation, proposed=parsed.cardinality
        )
        if not cardinality_corroborated:
            ASK_DEV_QUA_SHADOW_CARDINALITY_UNCORROBORATED_TOTAL.labels(
                intent=parsed.intent_id.value
            ).inc()

        return QUAShadowRecord(
            status=QUAShadowStatus.EVALUATED,
            deterministic_decision=deterministic_decision,
            latency_ms=latency_ms,
            model_fingerprint=result.model_fingerprint,
            intent_id=parsed.intent_id,
            cardinality=parsed.cardinality,
            cardinality_corroborated=cardinality_corroborated,
            requires_clarification=parsed.requires_clarification,
            mentions=assessments,
        )

    async def _shortlist(
        self,
        *,
        interpretation: InterpretedQuestion,
        org_id: str,
        permission_fingerprint: str,
    ) -> dict[str, tuple[AuthorizedEntity, ...]]:
        """Per-mention authorized candidates, via the SAME
        ``ScopeResolutionService.search`` boundary (and therefore the same
        ``permission_fingerprint``-keyed cache, CHAOS-3389 hardening
        condition #1) the deterministic resolver itself uses. No new
        subject directory, no new cache, no RapidFuzz prefilter -- all
        explicitly deferred per the adversarial critique.
        """

        untyped_ids = interpretation.untyped_mention_ids
        all_kinds = tuple(sorted(SEARCHABLE_ENTITY_KINDS, key=lambda kind: kind.value))

        async def one(
            mention: DevSubjectMention,
        ) -> tuple[str, tuple[AuthorizedEntity, ...]]:
            kinds = (
                all_kinds
                if mention.mention_id in untyped_ids
                else (EntityKind(mention.requested_entity_kind.value),)
            )
            result = await self._scope_service.search(
                org_id,
                permission_fingerprint,
                ScopeSearchRequest(
                    query=mention.normalized_lookup_text,
                    kinds=kinds,
                    limit=self._config.max_candidates_per_mention,
                    allowed_kinds=SEARCHABLE_ENTITY_KINDS,
                    include_alias_matches=True,
                    # CHAOS-3525: a TYPED mention's own kind ranks first, the
                    # same key CHAOS-3422 added for the deterministic close-
                    # match search.
                    #
                    # Set now because the shortlist stopped being audit-only.
                    # While nothing read the record, ordering was cosmetic;
                    # now the model picks from this list and the list is
                    # truncated at a cap, so an unranked shortlist can push
                    # the one real project below issue/PR substring hits --
                    # which is CHAOS-3422's exact defect re-entering through
                    # the QUA door after being fixed at the deterministic
                    # one. An untyped mention names no kind, so it gets no
                    # preference rather than an invented one.
                    preferred_kinds=(
                        frozenset()
                        if mention.mention_id in untyped_ids
                        else frozenset(kinds)
                    ),
                ),
            )
            return mention.mention_id, result.candidates

        pairs = await asyncio.gather(
            *(one(mention) for mention in interpretation.mentions)
        )
        return dict(pairs)

    def _combine_shortlists(
        self,
        *,
        mentions: Sequence[DevSubjectMention],
        per_mention_candidates: dict[str, tuple[AuthorizedEntity, ...]],
    ) -> tuple[tuple[AuthorizedEntity, ...], dict[str, tuple[int, int]]]:
        """One combined, globally-indexed candidate list plus each mention's
        own half-open ``[start, end)`` slice into it.

        Bounded by ``max_total_candidates`` across the WHOLE call, not per
        mention -- a mention beyond the bound gets an empty slice (never a
        partial one straddling the truncation point, which would let an
        in-range index silently resolve to a candidate that belonged to a
        different, already-truncated mention).
        """

        combined: list[AuthorizedEntity] = []
        ranges: dict[str, tuple[int, int]] = {}
        max_total = self._config.max_total_candidates
        for mention in mentions:
            if len(combined) >= max_total:
                ranges[mention.mention_id] = (len(combined), len(combined))
                continue
            start = len(combined)
            candidates = per_mention_candidates.get(mention.mention_id, ())
            room = max_total - start
            combined.extend(candidates[:room])
            ranges[mention.mention_id] = (start, len(combined))
        return tuple(combined), ranges

    def _build_messages(
        self,
        *,
        question: str,
        mentions: Sequence[DevSubjectMention],
        combined: Sequence[AuthorizedEntity],
        mention_ranges: dict[str, tuple[int, int]],
    ) -> tuple[AgentMessage, ...]:
        lines = [f'Question: "{question}"', "", "Named mentions:"]
        for mention in mentions:
            start, end = mention_ranges.get(mention.mention_id, (0, 0))
            lines.append(
                f'- "{mention.original_text_span}" '
                f"(requested kind: {mention.requested_entity_kind.value}):"
            )
            if start == end:
                lines.append("    (no authorized candidates)")
            for index in range(start, end):
                entity = combined[index]
                lines.append(f"    [{index}] {entity.kind.value}: {entity.label}")
        return (
            AgentMessage(role=AgentMessageRole.SYSTEM, content=_SYSTEM_PROMPT),
            AgentMessage(role=AgentMessageRole.USER, content="\n".join(lines)),
        )

    def _response_schema(
        self, *, mention_count: int, candidate_count: int
    ) -> dict[str, Any]:
        """The response schema for THIS call's authorized shortlist.

        Index bounds are expressed as ``enum``, not as ``minimum``/
        ``maximum``, and that is load-bearing rather than stylistic.
        ``OpenAICompatibleAgentProvider`` projects every schema through
        ``_structural_schema``, which keeps only ``_STRUCTURAL_SCHEMA_KEYS``.
        ``minimum``/``maximum``/``minItems``/``maxItems``/``maxLength`` are
        NOT in that set and are stripped before dispatch; ``enum`` IS, and
        survives untouched. CHAOS-3536 found the range bounds had therefore
        never reached any provider, leaving ``_verify`` as the sole guard;
        CHAOS-3537 is this repair.

        An enumeration is also strictly STRONGER than the range it replaces:
        ``[0, n-1]`` admits every integer between the ends, while the enum
        admits exactly the indices this call authorized, rebuilt per call.

        Measured live before adopting (CHAOS-3537, gpt-5-nano): strict mode
        accepts an integer enum at 3 and at 50 candidates and accepts the
        empty enum below; and given a prompt instructing it to return
        out-of-range index 7, the pre-fix schema returned
        ``selected_candidate_index: 7`` at confidence 0.92 in 3 runs of 3,
        while the enum-bound schema could not express it in any of 3.

        With zero candidates, ``selected_candidate_index`` is
        ``{"type": "null"}`` (parseable -- the contract field is
        ``_StrictIndex | None``). ``candidate_indices`` deliberately stays a
        plain integer array. An EMPTY item enum would close it structurally,
        and was measured working against OpenAI -- but ``enum: []`` is an
        unsatisfiable choice set, and ``CERTIFIED_PLATFORM_AGENT_PROVIDERS``
        also includes ``local``/``ollama``/``lmstudio``, whose constrained
        decoders were NOT probed. A decoder that rejects it would turn every
        zero-candidate call into a provider error and silently lose exactly
        the no-match evidence the shadow exists to collect -- a worse failure
        than the residual it closes. Adopt it only with per-endpoint
        certification evidence (CHAOS-3538 is the natural home).

        What the enum does NOT do, stated because the first version of this
        docstring got it wrong: it does not add coverage ``_verify`` lacks.
        Every mention's slice is a subset of ``[0, len(combined))``, so an
        index the enum would reject is outside every mention's slice and
        ``_verify`` rejects it too -- ``_verify``'s rejection set strictly
        SUBSUMES the enum's. The enum earns its place by acting at a
        different STAGE, not over a different set: it constrains generation,
        so an unauthorized index is never produced rather than produced and
        then rejected, and a ``_verify`` regression is no longer a
        single point of failure. Per-mention ownership remains ``_verify``'s
        alone -- the schema is built once per call from the COMBINED
        shortlist and cannot express it.

        ``maxItems``/``maxLength`` below are stripped in transit and are kept
        only as intent, and as a real bound for any future provider that does
        not project. Nothing may be claimed on their strength.
        """

        authorized_indices = list(range(candidate_count))
        index_schema: dict[str, Any] = (
            {"type": "null"}
            if candidate_count == 0
            else {
                "type": ["integer", "null"],
                "enum": [*authorized_indices, None],
            }
        )
        mention_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["text_span", "outcome", "confidence"],
            "properties": {
                "text_span": {"type": "string", "maxLength": 512},
                "outcome": {
                    "type": "string",
                    "enum": [outcome.value for outcome in QUAOutcome],
                },
                "selected_candidate_index": index_schema,
                "candidate_indices": {
                    "type": "array",
                    "maxItems": 25,
                    # Enumerated when there is anything to enumerate. With
                    # NOTHING authorized this stays a plain integer array
                    # rather than carrying an empty enum -- see the docstring
                    # for why that exotic shape was measured, then dropped.
                    "items": (
                        {"type": "integer", "enum": authorized_indices}
                        if authorized_indices
                        else {"type": "integer"}
                    ),
                },
                "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            },
        }
        return {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "schema_version",
                "intent_id",
                "cardinality",
                "mentions",
                "requires_clarification",
            ],
            "properties": {
                # CHAOS-3536: ``type`` is REQUIRED here, and its absence took
                # the whole QUA path down against every real provider --
                # strict structured-output mode rejects the request with
                # "In context=('properties', 'schema_version'), schema must
                # have a 'type' key". ``const`` alone is not a type
                # declaration. The pin stays: the field must still be v1 by
                # VALUE, not merely a string.
                "schema_version": {
                    "type": "string",
                    "const": QUESTION_UNDERSTANDING_SCHEMA_VERSION,
                },
                "intent_id": {
                    "type": "string",
                    "enum": [intent.value for intent in QuestionIntentID],
                },
                "cardinality": {
                    "type": "string",
                    "enum": [card.value for card in Cardinality],
                },
                "requires_clarification": {"type": "boolean"},
                "mentions": {
                    "type": "array",
                    "minItems": mention_count,
                    "maxItems": mention_count,
                    "items": mention_schema,
                },
            },
        }

    def _verify(
        self,
        *,
        mentions: Sequence[DevSubjectMention],
        proposal: DevQuestionUnderstanding,
        combined: Sequence[AuthorizedEntity],
        mention_ranges: dict[str, tuple[int, int]],
    ) -> tuple[QUAShadowMentionAssessment, ...]:
        assessments: list[QUAShadowMentionAssessment] = []
        for mention, proposed in zip(mentions, proposal.mentions, strict=True):
            start, end = mention_ranges.get(mention.mention_id, (0, 0))
            rejected_reason: str | None = None
            selected_entity: AuthorizedEntity | None = None
            if proposed.selected_candidate_index is not None:
                if start <= proposed.selected_candidate_index < end:
                    selected_entity = combined[proposed.selected_candidate_index]
                else:
                    rejected_reason = "index_outside_mention_shortlist"
            candidate_entities = tuple(
                combined[index]
                for index in proposed.candidate_indices
                if start <= index < end
            )
            if len(candidate_entities) != len(proposed.candidate_indices):
                rejected_reason = (
                    rejected_reason or "candidate_index_outside_mention_shortlist"
                )
            # Codex round 1 (MEDIUM, confirmed): outcome and
            # selected_candidate_index are independently-typed fields on the
            # wire, so a provider can propose a self-contradictory pair --
            # "resolved" with no index, or "ambiguous"/"no_match" WITH one --
            # and nothing above catches it; both would otherwise persist as
            # EVALUATED with inconsistent shadow evidence. Cross-checked
            # here rather than in the pydantic model itself: whether an
            # index is "present" depends on it ALSO passing the shortlist
            # bounds check above, which the model has no way to know.
            if (
                proposed.outcome is QUAOutcome.RESOLVED
                and selected_entity is None
                and rejected_reason is None
            ):
                rejected_reason = "resolved_outcome_missing_index"
            elif (
                proposed.outcome is not QUAOutcome.RESOLVED
                and selected_entity is not None
            ):
                rejected_reason = "non_resolved_outcome_has_index"
                selected_entity = None
            assessments.append(
                QUAShadowMentionAssessment(
                    mention_id=mention.mention_id,
                    text_span=mention.original_text_span,
                    outcome=proposed.outcome,
                    selected_entity=selected_entity,
                    candidate_entities=candidate_entities,
                    confidence=proposed.confidence,
                    rejected_reason=rejected_reason,
                )
            )
        return tuple(assessments)

    def _cardinality_corroborated(
        self, *, interpretation: InterpretedQuestion, proposed: Cardinality
    ) -> bool:
        """CHAOS-3389 critique §2 hardening condition: an org-wide
        cardinality proposal is only trusted when the DETERMINISTIC
        interpreter independently reached the same cardinality. This is
        what makes "how is the Contxt Fabric doing?" -- a named subject the
        model failed to match to any candidate id -- read as uncorroborated
        rather than silently equivalent to a genuine organization-wide
        question. Shadow mode never acts on this either way; it is recorded
        so the eventual verified-commit phase can enforce it as a real gate.
        """

        if proposed is not Cardinality.ORGANIZATION_WIDE:
            return True
        return interpretation.intent.cardinality is Cardinality.ORGANIZATION_WIDE
