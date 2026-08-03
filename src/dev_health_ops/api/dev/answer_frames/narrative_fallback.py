"""Narrative provider call, layered validation, deterministic fallback.

CHAOS-3297 issue requirements 7-8 ("Optional narrative provider" /
"Layered validation") and plan §a P6 ("Layered validation with a one-way
failure rule"). This module is the only place a narrative provider is
called from a frame that has already been decided; it can only ever
*downgrade* the resulting ``dev_answer.v2``'s ``narrative.mode`` to
``deterministic_fallback`` and never touch ``public_outcome``, a number, a
fact, or the terminal run state.

Two closed-vocabulary, import-time-total mechanisms make the failure and
fallback machinery structural rather than a matter of caller discipline:

* ``NarrativeFailureCode`` — the seven provider-failure modes the issue's
  acceptance criteria name (timeout, refusal, empty content, schema
  violation, output-budget exhaustion, unsafe prose, narrative grounding
  failure), plus ``PROVIDER_UNKNOWN_FAILURE``. ``classify_provider_exception``
  is total over ``Exception`` by construction (an ``isinstance`` chain over
  the closed ``_KNOWN_PROVIDER_EXCEPTIONS`` tuple ending in the unknown
  bucket, never a ``raise``) — a provider exception this module has never
  seen before still produces a safe fallback and an observable counter
  increment, not a crash and not a silent pass-through of unvalidated
  content. ``test_chaos_3297_narrative_fallback.py`` pins
  ``_KNOWN_PROVIDER_EXCEPTIONS``' membership the same way CHAOS-3300 pins
  ``LEGACY_ONLY_QUESTION_INTENTS``.

* The deterministic fallback narrative is built by
  ``build_deterministic_fallback_narrative``, whose signature accepts only
  ``frame`` (plus the deterministic identity/time inputs P3 requires) —
  structurally, not by discipline, it cannot see any provider output at
  all, so a provider response that fails validation *after* producing
  plausible-looking prose cannot leak so much as one token of it into the
  fallback. See that function's docstring and
  ``test_a_provider_leak_token_never_reaches_the_fallback_narrative`` for
  the planted-defect proof.

P6's identity requirement -- narrative choice may only change the
``narrative`` field of the produced ``DevAnswerV2`` -- is enforced at
runtime, not only in tests, by ``synthesize_narrative_answer``: every
answer it returns is compared field-for-field (except ``narrative``)
against a second ``DevAnswerV2`` built from the *same* frame with no
narrative at all. A future change that let narrative selection also alter
``frame``, ``public_outcome``, or ``outcome_display_label`` fails this
assertion immediately rather than shipping silently.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Protocol

from dev_health_ops.api.dev.contracts import DevModelMetadata
from dev_health_ops.api.dev.contracts_v2 import validators as _validators
from dev_health_ops.api.dev.contracts_v2.answer import (
    DevAnswerV2,
    outcome_display_label,
)
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame
from dev_health_ops.api.dev.contracts_v2.narrative import DevNarrative
from dev_health_ops.metrics.prometheus import ASK_DEV_NARRATIVE_FALLBACK_TOTAL

from .narrative_request import build_narrative_brief

__all__ = [
    "NARRATIVE_BRIEF_MAX_SECTIONS",
    "NarrativeFailureCode",
    "NarrativeProvider",
    "NarrativeProviderBudgetExceededError",
    "NarrativeProviderEmptyContentError",
    "NarrativeProviderError",
    "NarrativeProviderRefusalError",
    "NarrativeProviderResult",
    "NarrativeProviderSchemaViolationError",
    "NarrativeProviderTimeoutError",
    "NarrativeProviderUnsafeContentError",
    "assert_narrative_choice_is_the_only_delta",
    "build_deterministic_fallback_narrative",
    "classify_provider_exception",
    "synthesize_narrative",
    "synthesize_narrative_answer",
]

#: Not enforced here (bounded already by the frame's own field caps), named
#: only so a future size-budget guard has an obvious constant to extend.
NARRATIVE_BRIEF_MAX_SECTIONS = 20


# ---------------------------------------------------------------------------
# Provider boundary: a minimal, prompt-agnostic protocol. Prompt content,
# model selection, and role/budget certification are CHAOS-3285's territory
# (plan §f non-goal); this module only defines the typed failure contract a
# concrete provider must honor and the fallback behavior when it doesn't.
# ---------------------------------------------------------------------------


class NarrativeProviderError(Exception):
    """Base for the closed set of provider failure exceptions below."""


class NarrativeProviderTimeoutError(NarrativeProviderError):
    """The provider did not respond within its allotted budget."""


class NarrativeProviderRefusalError(NarrativeProviderError):
    """The provider declined to produce narrative content."""


class NarrativeProviderEmptyContentError(NarrativeProviderError):
    """The provider returned no usable content."""


class NarrativeProviderSchemaViolationError(NarrativeProviderError):
    """The provider's response did not match the required output shape."""


class NarrativeProviderBudgetExceededError(NarrativeProviderError):
    """The provider's output exceeded the allowed length/cost budget."""


class NarrativeProviderUnsafeContentError(NarrativeProviderError):
    """The provider's response failed a content-safety check."""


#: Closed, ordered set of recognized provider exceptions -> failure code.
#: ``classify_provider_exception`` walks this in order (first match wins,
#: relevant only if a future subclass relationship is introduced between
#: two entries). Order-independent today: the six exception types share no
#: subclass relationship other than ``NarrativeProviderError`` itself.
_KNOWN_PROVIDER_EXCEPTIONS: tuple[type[NarrativeProviderError], ...] = (
    NarrativeProviderTimeoutError,
    NarrativeProviderRefusalError,
    NarrativeProviderEmptyContentError,
    NarrativeProviderSchemaViolationError,
    NarrativeProviderBudgetExceededError,
    NarrativeProviderUnsafeContentError,
)


@dataclass(frozen=True, slots=True)
class NarrativeProviderResult:
    """A provider's raw response, before frame-grounding validation."""

    body: str
    referenced_fact_ids: tuple[str, ...]
    referenced_section_ids: tuple[str, ...]
    provider_family: str
    model_fingerprint: str
    provider_source: Literal["platform", "byo"] = "platform"


class NarrativeProvider(Protocol):
    """The narrative provider boundary. A real implementation lives outside
    this module (CHAOS-3285); it must raise one of the six
    ``NarrativeProviderError`` subclasses above on failure, never return a
    result that silently represents one.
    """

    async def generate_narrative(
        self, brief: Mapping[str, Any]
    ) -> NarrativeProviderResult: ...


class NarrativeFailureCode(StrEnum):
    """The closed, run-persisted vocabulary for ``dev_runs.narrative_failure_code``.

    Migration 0078 reserved the column and its docstring names this module
    as the owner of its vocabulary. Seven members are the issue's own
    acceptance-criteria failure modes (timeout, refusal, empty content,
    schema violation, output-budget exhaustion, unsafe prose, narrative
    grounding failure); the eighth, ``PROVIDER_UNKNOWN_FAILURE``, is the
    closed-vocabulary totality guard's catch-all -- see
    ``classify_provider_exception``.
    """

    PROVIDER_TIMEOUT = "provider_timeout"
    PROVIDER_REFUSED = "provider_refused"
    PROVIDER_EMPTY_CONTENT = "provider_empty_content"
    PROVIDER_SCHEMA_VIOLATION = "provider_schema_violation"
    PROVIDER_OUTPUT_BUDGET_EXCEEDED = "provider_output_budget_exceeded"
    PROVIDER_UNSAFE_CONTENT = "provider_unsafe_content"
    NARRATIVE_GROUNDING_FAILED = "narrative_grounding_failed"
    #: A provider raised something outside ``_KNOWN_PROVIDER_EXCEPTIONS``.
    #: Always safe to fall back on (the fallback narrative never depends on
    #: knowing *why* the provider failed) -- this member exists so an
    #: unrecognized failure is still classified, counted, and answered
    #: instead of propagating as an unhandled exception.
    PROVIDER_UNKNOWN_FAILURE = "provider_unknown_failure"


_PROVIDER_EXCEPTION_FAILURE_CODES: dict[
    type[NarrativeProviderError], NarrativeFailureCode
] = {
    NarrativeProviderTimeoutError: NarrativeFailureCode.PROVIDER_TIMEOUT,
    NarrativeProviderRefusalError: NarrativeFailureCode.PROVIDER_REFUSED,
    NarrativeProviderEmptyContentError: NarrativeFailureCode.PROVIDER_EMPTY_CONTENT,
    NarrativeProviderSchemaViolationError: (
        NarrativeFailureCode.PROVIDER_SCHEMA_VIOLATION
    ),
    NarrativeProviderBudgetExceededError: (
        NarrativeFailureCode.PROVIDER_OUTPUT_BUDGET_EXCEEDED
    ),
    NarrativeProviderUnsafeContentError: NarrativeFailureCode.PROVIDER_UNSAFE_CONTENT,
}


def classify_provider_exception(exc: Exception) -> NarrativeFailureCode:
    """Total classification of any exception a provider call raised.

    Never raises. A member of ``_KNOWN_PROVIDER_EXCEPTIONS`` maps to its own
    code; anything else -- a bug in this module's provider adapter, a
    library exception a future provider implementation didn't wrap, network
    plumbing -- maps to ``PROVIDER_UNKNOWN_FAILURE`` rather than propagating.
    That fallback branch is what makes this function total over ``Exception``
    without needing an import-time totality assertion the way a closed enum
    mapping does (``Exception`` subclasses are not an enumerable set); the
    regression fence is instead the requirement (tested) that *every*
    concrete provider failure this module defines maps to its own distinct
    code, and that anything else still returns a value rather than raising.
    """

    for exc_type in _KNOWN_PROVIDER_EXCEPTIONS:
        if isinstance(exc, exc_type):
            return _PROVIDER_EXCEPTION_FAILURE_CODES[exc_type]
    return NarrativeFailureCode.PROVIDER_UNKNOWN_FAILURE


# ---------------------------------------------------------------------------
# Deterministic fallback narrative -- built ONLY from the frame.
# ---------------------------------------------------------------------------

#: Deterministic uuid5 namespace for narrative identifiers, distinct from
#: preflight_outcomes._HANDLE_NAMESPACE and terminal_frames' own namespace so
#: a collision between packages minting server handles is structurally
#: impossible, not merely unlikely.
_NARRATIVE_NAMESPACE = uuid.UUID("2c6f9b1a-3f5f-58f1-9e1e-6b3d2f8a4c7e")


def _mint_narrative_id(run_id: str, frame_id: str) -> str:
    """Deterministic narrative_id (plan §a P3): pure function of run+frame,
    never ``uuid4()``, so a fallback narrative built twice from the same
    inputs is byte-identical -- see
    ``test_deterministic_fallback_is_pure``.
    """

    return str(uuid.uuid5(_NARRATIVE_NAMESPACE, f"narrative:{run_id}:{frame_id}"))


def build_deterministic_fallback_narrative(
    frame: DevAnswerFrame, *, generated_at: datetime
) -> DevNarrative:
    """Build a ``dev_narrative.v1`` (``mode="deterministic_fallback"``) from
    ``frame`` alone.

    Structural guarantee, not a discipline one: this function's signature
    accepts no provider output whatsoever, so there is no parameter through
    which a rejected or partially-parsed provider response could reach the
    body it constructs. The body is composed from exactly the frame's own
    public-copy fields -- the same set ``validators._public_copy_fields``
    already treats as the frame's disclosed content -- in the order the
    renderer would present them: the direct answer, then every section's
    facts, then disclosed limitations.
    """

    sentences: list[str] = [frame.direct_answer]
    referenced_fact_ids: list[str] = []
    referenced_section_ids: list[str] = []
    for section in frame.sections:
        referenced_section_ids.append(section.section_id)
        for fact_id in section.fact_ids:
            fact = next((f for f in frame.facts if f.fact_id == fact_id), None)
            if fact is None:
                continue
            sentences.append(fact.text)
            referenced_fact_ids.append(fact.fact_id)
    for limitation in frame.limitations:
        sentences.append(limitation)
    body = " ".join(sentence.strip() for sentence in sentences if sentence.strip())

    return DevNarrative(
        schema_version="dev_narrative.v1",
        narrative_id=_mint_narrative_id(frame.run_id, frame.frame_id),
        run_id=frame.run_id,
        frame_id=frame.frame_id,
        mode="deterministic_fallback",
        body=body,
        referenced_fact_ids=tuple(dict.fromkeys(referenced_fact_ids)),
        referenced_section_ids=tuple(dict.fromkeys(referenced_section_ids)),
        provider_metadata=None,
        generated_at=generated_at,
        validation_warnings=(),
    )


# ---------------------------------------------------------------------------
# P6 identity check: narrative choice is the only permitted delta.
# ---------------------------------------------------------------------------

#: Every DevAnswerV2 field except ``narrative`` -- the set P6 requires to be
#: identical between "what the answer would be with this narrative" and
#: "what the answer would be with no narrative at all". Read off the model
#: itself, not hand-duplicated, so a field added to DevAnswerV2 is covered
#: by this comparison automatically.
_ANSWER_FIELDS_EXCLUDING_NARRATIVE = tuple(
    name for name in DevAnswerV2.model_fields if name != "narrative"
)


def assert_narrative_choice_is_the_only_delta(
    frame: DevAnswerFrame,
    *,
    answer_id: str,
    conversation_id: str,
    run_id: str,
    generated_at: datetime,
    narrative: DevNarrative | None,
) -> DevAnswerV2:
    """Build the ``DevAnswerV2`` for ``narrative`` and prove it is the only
    field that differs from the same answer built with no narrative at all.

    This is plan §a P6 ("narrative validation failure may only downgrade
    narrative.mode ... never change public_outcome, any number, any fact, or
    the terminal run state") made a runtime invariant rather than only a
    test: both objects are constructed from the *same* ``frame`` argument
    here, in the same call, so a defect that let narrative selection also
    swap in a different frame, outcome, or label raises immediately instead
    of shipping.
    """

    label = outcome_display_label(frame.public_outcome)
    baseline = DevAnswerV2(
        schema_version="dev_answer.v2",
        answer_id=answer_id,
        conversation_id=conversation_id,
        run_id=run_id,
        generated_at=generated_at,
        public_outcome=frame.public_outcome,
        outcome_display_label=label,
        frame=frame,
        narrative=None,
    )
    if narrative is None:
        return baseline
    candidate = DevAnswerV2(
        schema_version="dev_answer.v2",
        answer_id=answer_id,
        conversation_id=conversation_id,
        run_id=run_id,
        generated_at=generated_at,
        public_outcome=frame.public_outcome,
        outcome_display_label=label,
        frame=frame,
        narrative=narrative,
    )
    drifted = [
        field_name
        for field_name in _ANSWER_FIELDS_EXCLUDING_NARRATIVE
        if getattr(candidate, field_name) != getattr(baseline, field_name)
    ]
    if drifted:
        raise RuntimeError(
            "narrative selection altered non-narrative answer field(s) "
            f"{drifted}; P6 requires narrative choice to be the only delta"
        )
    return candidate


# ---------------------------------------------------------------------------
# Orchestration: request the narrative, validate it, fall back on failure.
# ---------------------------------------------------------------------------


async def synthesize_narrative(
    *,
    frame: DevAnswerFrame,
    provider: NarrativeProvider | None,
    generated_at: datetime,
) -> tuple[DevNarrative, NarrativeFailureCode | None]:
    """Produce the ``dev_narrative.v1`` for one frame, with fallback.

    The leaner entry point: needs only ``frame`` (which already carries
    ``run_id``/``frame_id``) and no answer-level identifiers, so an
    orchestrator call site that has a frame but has not yet assembled a
    ``dev_answer.v2`` can call this directly. ``synthesize_narrative_answer``
    below is a convenience wrapper for callers (tests, any future v2-native
    answer assembly) that also want the full ``DevAnswerV2`` with P6's
    identity check applied.

    Returns ``(narrative, failure_code)`` -- ``failure_code`` is ``None``
    when the provider narrative was accepted, and one of
    ``NarrativeFailureCode`` (persisted as
    ``dev_runs.narrative_failure_code``) when the deterministic fallback was
    selected. ``provider=None`` (no certified provider configured) goes
    straight to the deterministic fallback with no failure code -- that is a
    configuration state, not a provider failure.

    Layered validation (plan §a P6, issue requirement 8): a provider result
    is accepted only if it survives the same narrative/frame binding checks
    ``DevAnswerV2.validate_answer_invariants`` already runs
    (``validate_narrative_fact_references``,
    ``validate_narrative_frame_consistency``,
    ``validators.scan_public_text`` on the body) -- run explicitly here
    first, so a rejection is classified as ``NARRATIVE_GROUNDING_FAILED``
    and routed to deterministic fallback rather than raised as an
    unhandled ``ValidationError`` out of ``DevAnswerV2(...)`` construction.
    """

    fallback_narrative = build_deterministic_fallback_narrative(
        frame, generated_at=generated_at
    )

    if provider is None:
        return fallback_narrative, None

    brief = build_narrative_brief(frame)
    failure_code: NarrativeFailureCode | None = None

    # Layer 1: the provider call itself. Caught as broad ``Exception``, not
    # only ``NarrativeProviderError`` -- classify_provider_exception is total
    # over *any* exception (requirement: "an unrecognized provider failure
    # must map to fallback and increment a counter, never raise or pass
    # through"), so a provider implementation that lets a raw transport
    # exception escape instead of wrapping it in one of the six typed errors
    # above still falls back safely rather than crashing the run.
    try:
        result = await provider.generate_narrative(brief)
    except Exception as exc:  # noqa: BLE001 -- total classification below
        failure_code = classify_provider_exception(exc)
    else:
        # Layer 2/3: the response parsed and its frame-grounding, exactly
        # the checks DevAnswerV2.validate_answer_invariants already runs for
        # an *accepted* narrative -- run explicitly here so a rejection is
        # classified and routed to fallback rather than raised as an
        # unhandled pydantic ValidationError out of DevAnswerV2(...).
        try:
            candidate_narrative = DevNarrative(
                schema_version="dev_narrative.v1",
                narrative_id=_mint_narrative_id(frame.run_id, frame.frame_id),
                run_id=frame.run_id,
                frame_id=frame.frame_id,
                mode="provider",
                body=result.body,
                referenced_fact_ids=tuple(result.referenced_fact_ids),
                referenced_section_ids=tuple(result.referenced_section_ids),
                provider_metadata=DevModelMetadata(
                    provider_source=result.provider_source,
                    provider_family=result.provider_family,
                    model_fingerprint=result.model_fingerprint,
                ),
                generated_at=generated_at,
                validation_warnings=(),
            )
            _validators.validate_narrative_fact_references(candidate_narrative, frame)
            hits = _validators.scan_public_text(candidate_narrative.body)
            if hits:
                raise ValueError(
                    f"narrative body leaks internal token(s) {sorted(hits)}"
                )
            _validators.validate_narrative_frame_consistency(candidate_narrative, frame)
        except (ValueError, TypeError):
            # A response that parsed into a syntactically valid DevNarrative
            # but failed frame-grounding (numeric containment, readiness/
            # subject claim, fact-reference, or internal-leakage scan) is a
            # narrative-grounding failure, not a provider-transport failure
            # -- distinct code, same fallback destination.
            failure_code = NarrativeFailureCode.NARRATIVE_GROUNDING_FAILED
        else:
            return candidate_narrative, None

    # Every path that reaches here (provider call raised, or the response
    # failed grounding validation) set failure_code above; the only path
    # that leaves it None returned early.
    assert failure_code is not None, "fallback reached with no failure_code set"
    ASK_DEV_NARRATIVE_FALLBACK_TOTAL.labels(failure_code=failure_code.value).inc()
    return fallback_narrative, failure_code


async def synthesize_narrative_answer(
    *,
    frame: DevAnswerFrame,
    provider: NarrativeProvider | None,
    answer_id: str,
    conversation_id: str,
    run_id: str,
    generated_at: datetime,
) -> tuple[DevAnswerV2, NarrativeFailureCode | None]:
    """Produce the ``dev_answer.v2`` for one frame, with narrative fallback.

    Convenience wrapper over :func:`synthesize_narrative` for a caller that
    already has (or wants) the answer-level identifiers and P6's runtime
    identity check. See that function for the failure/fallback semantics.
    """

    narrative, failure_code = await synthesize_narrative(
        frame=frame, provider=provider, generated_at=generated_at
    )
    answer = assert_narrative_choice_is_the_only_delta(
        frame,
        answer_id=answer_id,
        conversation_id=conversation_id,
        run_id=run_id,
        generated_at=generated_at,
        narrative=narrative,
    )
    return answer, failure_code
