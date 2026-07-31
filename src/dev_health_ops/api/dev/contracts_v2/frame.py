"""``dev_answer_frame.v1`` — the server-owned canonical answer, Amendment TRD v2 §4.5.

A valid answer frame is independent of whether a provider narrative exists
(CHAOS-3294 guardrail): everything a user needs is representable here
without ``dev_narrative.v1``. The frame's own ``model_validator`` wires in
the five acceptance-criteria semantic validators, plus guardrail (f)
``validate_no_answer_projection`` (a no-content outcome is rebuilt from a
total field allowlist — see the ``validators`` module docstring), via the
``validators`` *module* (not bound-method references) so each can be
disabled independently in a mutation test.

Every collection field is a ``tuple``, not a ``list``. ``frozen=True`` only
rebinds attributes; it leaves a ``list`` field's contents mutable in place,
which adversarial review used to clear a validated ledger's entries and
defeat ``validate_ledger_extends``. Tuples make the whole object immutable
in fact, not just at the attribute boundary — asserted across every v2
model by ``test_contracts_v2``.

That held for v2-native fields but not for the v1 objects this frame embeds:
review round 3 appended to ``frame.coverage.unavailable_required_sources`` on
a fully validated frame and serialized the result. ``coverage``, ``evidence``
and ``metrics`` therefore use the deeply immutable mirrors in
``embedded.py``, and the introspection test now recurses through every
embedded model type rather than accepting a list of acknowledged exceptions.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AwareDatetime, Field, FiniteFloat, model_validator

from . import validators as _validators
from .base import (
    ContractModelV2,
    Label,
    LongText,
    OpaqueID,
    PlatformVersionToken,
    PublicOutcome,
    ShortText,
)
from .embedded import DevCoverageV2, DevEvidenceRefV2, DevMetricRefV2
from .plan import PlanRegistryID
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
    rule_version: PlatformVersionToken | None = None


class DevReadinessBlock(ContractModelV2):
    """Readiness is a distinct concept from completion (per TRD v2 §4.5)."""

    state: Literal["ready", "not_ready", "indeterminate"]
    rule_id: OpaqueID
    rule_version: PlatformVersionToken
    translated_user_reasons: tuple[ShortText, ...] = Field(
        default_factory=tuple, max_length=20
    )
    blocking_fact_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=100
    )

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
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=25)
    relationship_path_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=25
    )
    confidence: FiniteFloat = Field(ge=0, le=1)


class DevAnswerSection(ContractModelV2):
    section_id: OpaqueID
    title: Label
    fact_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=200)


class DevComparisonPoint(ContractModelV2):
    label: Label
    current_value: FiniteFloat
    comparison_value: FiniteFloat | None = None
    unit: OpaqueID


class DevFrameConflict(ContractModelV2):
    summary: ShortText
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(min_length=2, max_length=10)


class DevFrameVersions(ContractModelV2):
    """Platform provenance for one answer frame.

    Every field is a ``PlatformVersionToken`` (a dotted, lowercase,
    version-suffixed token) rather than the free-form ``Version``, so the
    block cannot carry producer-authored copy. A no-answer outcome carries no
    provenance block at all — see ``validators.NO_ANSWER_FRAME_FIELD_POLICY``.
    """

    interpreter_version: PlatformVersionToken
    plan_id: PlanRegistryID
    plan_version: PlatformVersionToken
    tool_contract_version: PlatformVersionToken
    metric_definition_version: PlatformVersionToken
    query_version: PlatformVersionToken
    prompt_version: PlatformVersionToken | None = None
    rule_version: PlatformVersionToken | None = None


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
    sections: tuple[DevAnswerSection, ...] = Field(default_factory=tuple, max_length=20)
    facts: tuple[DevAnswerFact, ...] = Field(default_factory=tuple, max_length=200)
    metrics: tuple[DevMetricRefV2, ...] = Field(default_factory=tuple, max_length=12)
    comparisons: tuple[DevComparisonPoint, ...] = Field(
        default_factory=tuple, max_length=20
    )
    relationship_paths: tuple[DevRelationshipPath, ...] = Field(
        default_factory=tuple, max_length=100
    )
    health_profile_refs: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=25
    )
    finding_refs: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=50)
    deficiency_refs: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=50)
    conflicts: tuple[DevFrameConflict, ...] = Field(
        default_factory=tuple, max_length=20
    )
    limitations: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=20)
    source_observations: tuple[DevSourceObservation, ...] = Field(
        default_factory=tuple, max_length=25
    )
    coverage: DevCoverageV2
    evidence: tuple[DevEvidenceRefV2, ...] = Field(default_factory=tuple, max_length=25)
    safe_follow_up_questions: tuple[ShortText, ...] = Field(
        default_factory=tuple, max_length=10
    )
    # Optional only so a no-answer outcome can omit it entirely; required for
    # every outcome that carries content — see validate_versions_presence.
    versions: DevFrameVersions | None = None

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
        _validators.validate_versions_presence(self)
        _validators.validate_no_answer_projection(self)
        _validators.validate_completion_denominator(self)
        _validators.validate_relationship_refs_within_frame(self)
        return self


# Import-time totality: a field added to DevAnswerFrame without a no-answer
# classification breaks this import rather than opening a silent disclosure
# channel. See the validators module docstring.
_validators.register_no_answer_policy(
    DevAnswerFrame,
    _validators.NO_ANSWER_FRAME_FIELD_POLICY,
    {"direct_answer": _validators.CANONICAL_NO_ANSWER_COPY},
    vocabularies={
        "schema_version": _validators.literal_vocabulary(
            DevAnswerFrame, "schema_version"
        ),
        "public_outcome": _validators.NO_ANSWER_OUTCOMES,
    },
)
