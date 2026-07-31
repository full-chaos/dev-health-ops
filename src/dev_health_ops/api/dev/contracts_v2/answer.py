"""``dev_answer.v2`` — projects the frame, optional narrative, and safe outcome.

Amendment TRD v2 §4.5: "Projects the frame, optional narrative, and safe
outcome to both surfaces [app-wide window and /dev]. Internal resolver and
authorization enums are absent by schema." Because ``DevAnswerFrame`` and
``DevNarrative`` are themselves ``extra="forbid"`` contract models, and
``DevAnswerFrame`` already scans every public copy field for internal
tokens (``validators.validate_no_internal_leakage``), embedding them here
(rather than re-deriving a looser projection) is what makes "absent by
schema" true rather than aspirational.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AwareDatetime, model_validator

from . import validators as _validators
from .base import ContractModelV2, Label, OpaqueID, PublicOutcome
from .frame import DevAnswerFrame
from .narrative import DevNarrative

__all__ = ["DevAnswerV2"]

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
}


class DevAnswerV2(ContractModelV2):
    schema_version: Literal["dev_answer.v2"]
    answer_id: OpaqueID
    conversation_id: OpaqueID
    run_id: OpaqueID
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
        if self.narrative is not None:
            if self.narrative.run_id != self.run_id:
                raise ValueError("narrative run_id must match the answer's run_id")
            _validators.validate_narrative_fact_references(self.narrative, self.frame)
            hits = _validators.scan_public_text(self.narrative.body)
            if hits:
                raise ValueError(
                    f"narrative body leaks internal token(s) {sorted(hits)}"
                )
        return self
