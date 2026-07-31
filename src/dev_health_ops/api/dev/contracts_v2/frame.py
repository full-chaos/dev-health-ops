"""``dev_answer_frame.v1`` — the server-owned canonical answer, Amendment TRD v2 §4.5.

A valid answer frame is independent of whether a provider narrative exists
(CHAOS-3294 guardrail): everything a user needs is representable here
without ``dev_narrative.v1``. The frame's own ``model_validator`` wires in
the five acceptance-criteria semantic validators, plus the post-merge
Codex-review guardrail (f) ``validate_no_answer_content_leaks`` (a
no-content outcome must carry nothing beyond the bare outcome — see that
function's docstring), via the ``validators`` *module* (not bound-method
references) so each can be disabled independently in a mutation test — see
``validators`` module docstring for why that indirection matters.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AwareDatetime, Field, FiniteFloat, model_validator

from dev_health_ops.api.dev.contracts import DevCoverage, DevEvidenceRef, DevMetricRef

from . import validators as _validators
from .base import (
    ContractModelV2,
    Label,
    LongText,
    OpaqueID,
    PublicOutcome,
    ShortText,
    Version,
)
from .result import DevRelationshipPath, DevSourceObservation
from .subject import DevEntityRefV2

__all__ = [
    "DevAnswerFact",
    "DevAnswerFrame",
    "DevAnswerSection",
    "DevCompletionBlock",
    "DevComparisonPoint",
    "DevFrameConflict",
    "DevFrameVersions",
    "DevReadinessBlock",
]


class DevCompletionBlock(ContractModelV2):
    """Deterministic completion numerator/denominator/rate/calculability.

    ``calculable`` is never inferred True from a partial numerator or
    denominator: see ``validators.validate_completion_denominator``, which
    is the sole enforcement point (deliberately not duplicated here — see
    that module's docstring on why enforcement lives in one monkeypatchable
    place).
    """

    numerator: int | None = Field(default=None, ge=0, le=100_000)
    denominator: int | None = Field(default=None, ge=0, le=100_000)
    rate: FiniteFloat | None = Field(default=None, ge=0, le=1)
    calculable: bool
    rule_id: OpaqueID | None = None
    rule_version: Version | None = None


class DevReadinessBlock(ContractModelV2):
    """Readiness is a distinct concept from completion (per TRD v2 §4.5)."""

    state: Literal["ready", "not_ready", "indeterminate"]
    rule_id: OpaqueID
    rule_version: Version
    translated_user_reasons: list[ShortText] = Field(
        default_factory=list, max_length=20
    )
    blocking_fact_ids: list[OpaqueID] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def validate_reason_disclosure(self) -> Self:
        if self.state == "not_ready" and not self.translated_user_reasons:
            raise ValueError(
                "a not_ready readiness state requires translated user reasons"
            )
        return self


class DevAnswerFact(ContractModelV2):
    fact_id: OpaqueID
    text: ShortText
    kind: Literal["observed", "inferred", "recommendation"]
    evidence_ref_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)
    relationship_path_ids: list[OpaqueID] = Field(default_factory=list, max_length=25)
    confidence: FiniteFloat = Field(ge=0, le=1)


class DevAnswerSection(ContractModelV2):
    section_id: OpaqueID
    title: Label
    fact_ids: list[OpaqueID] = Field(default_factory=list, max_length=200)


class DevComparisonPoint(ContractModelV2):
    label: Label
    current_value: FiniteFloat
    comparison_value: FiniteFloat | None = None
    unit: OpaqueID


class DevFrameConflict(ContractModelV2):
    summary: ShortText
    evidence_ref_ids: list[OpaqueID] = Field(min_length=2, max_length=10)


class DevFrameVersions(ContractModelV2):
    interpreter_version: Version
    plan_id: OpaqueID
    plan_version: Version
    tool_contract_version: Version
    metric_definition_version: Version
    query_version: Version
    prompt_version: Version | None = None
    rule_version: Version | None = None


class DevAnswerFrame(ContractModelV2):
    schema_version: Literal["dev_answer_frame.v1"]
    frame_id: OpaqueID
    run_id: OpaqueID
    generated_at: AwareDatetime
    public_outcome: PublicOutcome
    subject_ref: DevEntityRefV2 | None = None
    subject_set_ref: OpaqueID | None = None
    direct_answer: LongText
    completion: DevCompletionBlock | None = None
    readiness: DevReadinessBlock | None = None
    sections: list[DevAnswerSection] = Field(default_factory=list, max_length=20)
    facts: list[DevAnswerFact] = Field(default_factory=list, max_length=200)
    metrics: list[DevMetricRef] = Field(default_factory=list, max_length=12)
    comparisons: list[DevComparisonPoint] = Field(default_factory=list, max_length=20)
    relationship_paths: list[DevRelationshipPath] = Field(
        default_factory=list, max_length=100
    )
    health_profile_refs: list[OpaqueID] = Field(default_factory=list, max_length=25)
    finding_refs: list[OpaqueID] = Field(default_factory=list, max_length=50)
    deficiency_refs: list[OpaqueID] = Field(default_factory=list, max_length=50)
    conflicts: list[DevFrameConflict] = Field(default_factory=list, max_length=20)
    limitations: list[ShortText] = Field(default_factory=list, max_length=20)
    source_observations: list[DevSourceObservation] = Field(
        default_factory=list, max_length=25
    )
    coverage: DevCoverage
    evidence: list[DevEvidenceRef] = Field(default_factory=list, max_length=25)
    safe_follow_up_questions: list[ShortText] = Field(
        default_factory=list, max_length=10
    )
    versions: DevFrameVersions

    @model_validator(mode="after")
    def validate_frame_semantics(self) -> Self:
        if self.subject_ref is not None and self.subject_set_ref is not None:
            raise ValueError(
                "a frame is either for one subject or a subject set, not both"
            )
        evidence_ids = [item.evidence_ref_id for item in self.evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("evidence reference IDs must be unique")
        relationship_ids = [path.path_id for path in self.relationship_paths]
        if len(relationship_ids) != len(set(relationship_ids)):
            raise ValueError("relationship path IDs must be unique")
        # Structural closure first: the five acceptance-criteria semantic
        # validators below assume a well-formed frame (unique, resolvable
        # fact/section/evidence IDs).
        _validators.validate_structural_closure(self)
        _validators.validate_no_internal_leakage(self)
        _validators.validate_outcome_consistency(self)
        _validators.validate_no_answer_content_leaks(self)
        _validators.validate_completion_denominator(self)
        _validators.validate_relationship_refs_within_frame(self)
        return self
