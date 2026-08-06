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
different questions show different candidate counts) -- that bound is
enforced two other ways, both in ``qua_shadow.py``:

1. The wire-level JSON Schema handed to the provider (``_response_schema``)
   bounds every index field to ``[0, total_candidates - 1]`` for THIS call,
   built fresh per call. A provider honoring the schema cannot even
   *generate* an out-of-range integer, and when there are zero authorized
   candidates the bound is ``[0, -1]`` -- empty, so no integer satisfies it.
2. ``_verify`` re-checks every index against the exact per-mention slice
   after parsing, in case a provider does not honor the schema strictly.
   A violation there marks that one mention's proposal ``rejected``, never
   the whole record, and it is never a live-path decision either way --
   shadow mode never acts on any of this.
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

#: Bound applied to every candidate-index field regardless of shortlist size
#: (belt: the wire schema's own per-call bound is tighter; this is the
#: absolute ceiling a hand-authored ``max_length``/``le`` must never exceed).
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
