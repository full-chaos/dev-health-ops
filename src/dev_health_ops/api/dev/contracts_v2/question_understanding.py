"""``dev_question_understanding.v1`` -- the Question Understanding Agent's
structured-output contract (CHAOS-3389 shadow phase).

This is deliberately NOT one of the frontend-facing v2 contracts (it never
reaches ``dev_answer_frame.v1`` or any API response): it is the shape one
QUA shadow call's model output is validated against before being folded into
a ``qua_shadow.QUAShadowRecord`` and persisted as a Postgres audit row. It
lives in ``contracts_v2`` anyway, alongside ``dev_resolution_ledger.v1`` and
``dev_subject_mention.v1``, because the platform spec (CHAOS-3389 comment
6fa38d88) treats it as a first-class pinned schema id and the closed
vocabularies it reuses (``QuestionIntentID``, ``Cardinality``) are defined
here.

**The never-widen invariant is NOT enforced by this static model.** This
class only checks *shape*: is ``selected_candidate_index`` an int or null,
is ``confidence`` a probability, is ``outcome`` one of three closed values.
Whether a given index actually falls within the specific shortlist shown for
that mention is a **per-call** fact this static schema cannot know (two
different questions show different candidate counts).

**That bound is enforced in two places, split by scope.** This paragraph has
been wrong in both directions -- it once claimed defence in depth that did not
exist (CHAOS-3536), then, while that was true, understated what came back
(CHAOS-3537) -- so it now states the boundary precisely rather than a slogan:

1. **Call-wide, structurally, on the wire.** ``qua_shadow._response_schema``
   enumerates this call's authorized indices as ``enum``, so a provider cannot
   express an index the CALL never authorized. The keyword matters:
   ``OpenAICompatibleAgentProvider`` projects every schema through
   ``_structural_schema``, which keeps only ``_STRUCTURAL_SCHEMA_KEYS``.
   ``minimum``/``maximum``/``minItems``/``maxItems``/``maxLength`` are not in
   it and are stripped in transit; ``enum`` is, and survives. The original
   claim rested on ``[0, total_candidates - 1]`` range bounds and was false
   for as long as it stood -- those never reached any provider.
2. **Per-mention, at runtime.** ``_verify`` re-checks every index against the
   exact per-mention slice after parsing, independent of whether the provider
   honored anything. A violation marks that one mention's proposal
   ``rejected``, never the whole record. CHAOS-3525 additionally hardened the
   singular commit path to resolve by identity.

**These are two STAGES, not two coverages.** Every mention slice is a subset
of ``[0, len(combined))``, so any index the enum rejects is outside every
mention's slice and ``_verify`` rejects it too -- ``_verify``'s rejection set
strictly SUBSUMES the schema's. The enum earns its place by acting before
generation (an unauthorized index is never produced, so the proposal survives
rather than being discarded) and by ending ``_verify``'s status as a single
point of failure. Per-mention ownership is ``_verify``'s alone: the schema is
built once per call from the COMBINED shortlist and cannot express which
mention an index belongs to.

With zero candidates authorized, ``selected_candidate_index`` is sent as
``{"type": "null"}``. ``candidate_indices`` stays a plain integer array --
a null would fail parsing here (non-optional tuple), and the ``enum: []``
that would close it structurally is an unsatisfiable choice set that has not
been certified against the local/ollama/lmstudio decoders. It is bounded by
``_verify`` alone. See ``qua_shadow._response_schema``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt

from .base import Cardinality, QuestionIntentID, ShortText

__all__ = [
    "QUESTION_UNDERSTANDING_SCHEMA_VERSION",
    "DevQuestionUnderstanding",
    "DevQuestionUnderstandingMention",
    "QUAOutcome",
]

QUESTION_UNDERSTANDING_SCHEMA_VERSION = "dev_question_understanding.v1"

#: Bound applied to every candidate-index field regardless of shortlist size.
#:
#: CHAOS-3536 found this calling itself the "belt" to a tighter per-call wire
#: bound that had been stripped in transit and did not exist. CHAOS-3537
#: restored that bound as an ``enum``, which survives the projection -- so the
#: braces are real again and this genuinely is the belt. Kept as the absolute
#: ceiling a hand-authored ``max_length``/``le`` must never exceed, and as the
#: bound that still applies if the per-call enum is ever widened.
_MAX_MENTIONS = 25
_MAX_CANDIDATE_INDICES = 25


class QUAOutcome(StrEnum):
    """What the model proposes for one mention, over the shortlist it was
    shown -- mirrors ``ResolutionOutcome``'s three-way shape
    (committed / ambiguous / unresolved) without reusing that enum: this is
    a MODEL proposal, never a live resolution outcome, and keeping the
    vocabularies distinct is what keeps "closed vocabularies stay closed on
    the live path" true by construction -- there is no shared enum a shadow
    bug could accidentally widen.
    """

    RESOLVED = "resolved"
    AMBIGUOUS = "ambiguous"
    NO_MATCH = "no_match"


class _QUAStrictModel(BaseModel):
    """Shared strict posture for this schema only.

    Not ``ContractModelV2``: that base is for frozen, wire-stable objects
    the frontend or another service depends on. This schema is parsed once
    per shadow call, immediately projected into ``QUAShadowRecord``, and
    never handed to a client -- freezing it buys nothing. ``extra="forbid"``
    is what matters: a field this build does not recognize must fail
    validation, not silently pass through as an unvalidated proposal.

    Model-wide ``strict=True`` was tried and reverted: pydantic's strict
    mode also rejects the ordinary JSON-string spelling of an ``Enum``
    field in *Python*-mode validation (``model_validate`` on an
    already-parsed dict, which is what ``qua_shadow.py`` calls) -- every
    real provider response would then fail ``intent_id``/``cardinality``
    validation outright, which is worse than the coercion bug it would
    have fixed. Strictness is applied per-field instead, only to the
    numeric fields actually at risk (see ``_StrictIndex``/``_StrictScore``
    below); ``QUAOutcome``/``QuestionIntentID``/``Cardinality`` stay in the
    default lax mode that accepts their own string values.
    """

    model_config = ConfigDict(extra="forbid")


#: Codex adversarial review round 2 (MEDIUM, confirmed): pydantic's default
#: LAX mode coerces cross-type input -- a provider response with
#: ``selected_candidate_index: true`` or ``candidate_indices: ["0"]`` would
#: otherwise validate cleanly (``bool`` is an ``int`` subclass in Python;
#: numeric strings coerce silently) instead of failing as
#: SKIPPED_INVALID_OUTPUT, contaminating shadow-vs-deterministic analytics
#: with a malformed provider response the schema was supposed to catch.
_StrictIndex = Annotated[StrictInt, Field(ge=0)]
_StrictScore = Annotated[StrictFloat, Field(ge=0.0, le=1.0)]


class DevQuestionUnderstandingMention(_QUAStrictModel):
    #: Not re-validated as a literal substring here (question_interpreter's
    #: deterministic ``_validate_proposal`` does that for its own
    #: classifier). Shadow mode never acts on this text either way; it is
    #: recorded for shadow-vs-deterministic analytics only.
    text_span: ShortText
    outcome: QUAOutcome
    selected_candidate_index: _StrictIndex | None = None
    candidate_indices: tuple[_StrictIndex, ...] = Field(
        default_factory=tuple, max_length=_MAX_CANDIDATE_INDICES
    )
    confidence: _StrictScore


class DevQuestionUnderstanding(_QUAStrictModel):
    schema_version: Literal["dev_question_understanding.v1"]
    intent_id: QuestionIntentID
    cardinality: Cardinality
    mentions: tuple[DevQuestionUnderstandingMention, ...] = Field(
        max_length=_MAX_MENTIONS
    )
    requires_clarification: bool
