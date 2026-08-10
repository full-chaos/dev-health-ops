"""``dev_answer.v2`` — projects the frame, optional narrative, and safe outcome.

Amendment TRD v2 §4.5: "Projects the frame, optional narrative, and safe
outcome to both surfaces [app-wide window and /dev]. Internal resolver and
authorization enums are absent by schema." Because ``DevAnswerFrame`` and
``DevNarrative`` are themselves ``extra="forbid"`` contract models, and
``DevAnswerFrame`` already scans every public copy field for internal
tokens (``validators.validate_no_internal_leakage``), embedding them here
(rather than re-deriving a looser projection) is what makes "absent by
schema" true rather than aspirational.

``run_id`` closure (Codex adversarial-review hardening, CHAOS-3294): an
answer's ``run_id`` must equal both its embedded frame's ``run_id`` and (when
present) its narrative's ``run_id``, so an answer/metadata shell from one run
can never be paired with a frame or narrative produced by a different run.
The stream layer (``stream.py``) enforces the matching half of this closure
at the wire boundary: an ``answer.completed`` event's ``run_id`` must equal
its embedded answer's ``run_id``.

A no-answer outcome (``validators.NO_ANSWER_OUTCOMES``) additionally carries
**no narrative at all**: an optional provider narrative is a free-form
channel with no structured field to check it against, and adversarial review
used one on a ``denied`` answer to disclose a private project's existence
while every structured field was correctly empty. That, and the
classification of every other field of this model, is enforced by
``validators.validate_no_answer_projection`` through the total field policy
registered at the bottom of this module.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AwareDatetime, model_validator

from . import no_answer_policy as _policy
from . import validators as _validators
from .base import ContractModelV2, Label, PublicOutcome, ServerHandle
from .frame import DevAnswerFrame
from .narrative import DevNarrative

__all__ = ["DevAnswerV2", "outcome_display_label"]

#: Safe, stable display labels for each public outcome. Kept separate from
#: the enum code itself (CHAOS-3294: "safe display labels separate from
#: internal enums") so the UI never has to derive prose from a wire code.
_OUTCOME_DISPLAY_LABELS: dict[PublicOutcome, str] = {
    PublicOutcome.ANSWERED: "Answered",
    PublicOutcome.ANSWERED_WITH_GAPS: "Answered with some gaps",
    PublicOutcome.NEEDS_CLARIFICATION: "Needs clarification",
    PublicOutcome.NOT_FOUND: "Not found",
    PublicOutcome.TEMPORARILY_UNAVAILABLE: "Temporarily unavailable",
    PublicOutcome.UNSUPPORTED: "Not supported yet",
    PublicOutcome.DENIED: "Not permitted",
    PublicOutcome.FAILED: "Something went wrong",
    PublicOutcome.REFUSED: "Not something Ask Dev can do",
}


def outcome_display_label(outcome: PublicOutcome) -> str:
    """The canonical ``dev_answer.v2`` label for one public outcome.

    The single public accessor for ``_OUTCOME_DISPLAY_LABELS`` (CHAOS-3297
    stack #4, narrative fallback: any caller that assembles a
    ``DevAnswerV2`` needs this exact table rather than a second, possibly
    drifted copy of it).
    """

    return _OUTCOME_DISPLAY_LABELS[outcome]


class DevAnswerV2(ContractModelV2):
    schema_version: Literal["dev_answer.v2"]
    answer_id: ServerHandle
    conversation_id: ServerHandle
    run_id: ServerHandle
    generated_at: AwareDatetime
    public_outcome: PublicOutcome
    outcome_display_label: Label
    frame: DevAnswerFrame
    narrative: DevNarrative | None = None

    @model_validator(mode="after")
    def validate_answer_invariants(self) -> Self:
        if self.public_outcome is not self.frame.public_outcome:
            raise ValueError(
                "answer public_outcome must match its frame's public_outcome"
            )
        if self.outcome_display_label != _OUTCOME_DISPLAY_LABELS[self.public_outcome]:
            raise ValueError(
                "outcome_display_label does not match the canonical safe label"
            )
        _validators.validate_no_answer_projection(self)
        if self.frame.run_id != self.run_id:
            # Codex adversarial review (CHAOS-3294): nothing previously
            # required the frame embedded in an answer to have been produced
            # by the *same* run as the answer itself, so an answer could
            # splice a frame from run A into an answer/metadata shell for
            # run B. Closing this is what makes ``run_id`` an actual
            # provenance closure key rather than a decorative field.
            raise ValueError("answer run_id must match its frame's run_id")
        if self.narrative is not None:
            if self.narrative.run_id != self.run_id:
                raise ValueError("narrative run_id must match the answer's run_id")
            _validators.validate_narrative_fact_references(self.narrative, self.frame)
            hits = _validators.scan_public_text(self.narrative.body)
            if hits:
                raise ValueError(
                    f"narrative body leaks internal token(s) {sorted(hits)}"
                )
            _validators.validate_narrative_frame_consistency(self.narrative, self.frame)
        return self


# Import-time totality for the no-answer field policy — see frame.py and the
# validators module docstring. The canonical display labels are the same
# table the answer already validates against, restricted to the no-answer
# outcomes; asserting the two agree keeps one source of truth.
_drifted_labels = sorted(
    outcome
    for outcome, label in _policy.CANONICAL_NO_ANSWER_DISPLAY_LABELS.items()
    if _OUTCOME_DISPLAY_LABELS[PublicOutcome(outcome)] != label
)
if _drifted_labels:
    raise RuntimeError(
        "canonical no-answer display labels drifted from the outcome label "
        f"table: {_drifted_labels}"
    )

_policy.register_no_answer_policy(
    DevAnswerV2,
    _policy.NO_ANSWER_ANSWER_FIELD_POLICY,
    {"outcome_display_label": _policy.CANONICAL_NO_ANSWER_DISPLAY_LABELS},
    vocabularies={
        "schema_version": _policy.literal_vocabulary(DevAnswerV2, "schema_version"),
        "public_outcome": _policy.NO_ANSWER_OUTCOMES,
    },
)
