"""``dev_investigation_result.v1`` and ``dev_source_observation.v1``.

Amendment TRD v2 §4.3. The load-bearing invariant here is preserving
"zero versus no-data" semantics (CHAOS-3294 deliverable list): a source
that ran successfully and genuinely found nothing must never be
indistinguishable, on the wire, from a source that was unconfigured,
unavailable, or otherwise never actually measured anything. See
``DevSourceObservation.validate_zero_semantics``.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AwareDatetime, Field, FiniteFloat, model_validator

from .base import (
    ContractModelV2,
    EvidenceHandle,
    Label,
    OpaqueID,
    PlatformVersionToken,
    ServerHandle,
    ShortText,
    SourceClass,
    SourceRequirementState,
    Version,
)
from .embedded import (
    DevCIFactV2,
    DevDeploymentFactV2,
    DevGraphEdgeV2,
    DevIncidentFactV2,
    DevMetricRefV2,
    DevPullRequestFactV2,
    DevRequiredChildFactV2,
    DevStatusFactV2,
)
from .plan import PlanRegistryID

__all__ = [
    "DevInvestigationResult",
    "DevObservedChangeV2",
    "DevRelationshipPath",
    "DevSourceContent",
    "DevSourceObservation",
]

RequirementLevel = Literal["mandatory", "conditional", "optional", "not_applicable"]

#: Observed states for which a source actually ran a query against live or
#: cached data (as opposed to states describing why it could not).
_QUERIED_STATES = frozenset(
    {
        SourceRequirementState.AVAILABLE_CURRENT,
        SourceRequirementState.AVAILABLE_STALE,
        SourceRequirementState.AVAILABLE_UNKNOWN,
    }
)

#: Observed states describing why a source never produced usable data.
_UNMEASURED_STATES = frozenset(
    {
        SourceRequirementState.UNCONFIGURED,
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
        SourceRequirementState.NOT_APPLICABLE,
        SourceRequirementState.TRUNCATED,
    }
)


class DevRelationshipPath(ContractModelV2):
    """One verifiable hop chain from a committed subject to supporting data."""

    path_id: OpaqueID
    source_entity_id: OpaqueID
    relationship: OpaqueID
    target_entity_id: OpaqueID
    provenance: ShortText
    confidence: FiniteFloat = Field(ge=0, le=1)
    observed_at: AwareDatetime
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )


class DevObservedChangeV2(ContractModelV2):
    """One ``change.observed.v1`` fact -- no v1 wire mirror exists to inherit."""

    change_id: OpaqueID
    category: OpaqueID
    entity_type: OpaqueID
    entity_id: OpaqueID
    display_label: Label
    before: ShortText | None = None
    after: ShortText | None = None
    observed_at: AwareDatetime
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )


class DevSourceContent(ContractModelV2):
    """CHAOS-3295: the typed per-step domain content a frame builder consumes.

    Distinct from the observation's own accounting fields
    (``usable_fact_count`` etc.): a count is not a fact. Only the slot(s)
    matching the observation's own ``source_class`` are ever populated by the
    executor -- every other slot stays empty, never omitted, so a builder can
    always address ``content.status_facts`` etc. without a hasattr check.
    Never present when ``observed_state`` is unmeasured -- see
    ``DevSourceObservation.validate_content_semantics``.
    """

    schema_version: Literal["dev_source_content.v1"]
    status_facts: tuple[DevStatusFactV2, ...] = Field(
        default_factory=tuple, max_length=25
    )
    required_children: tuple[DevRequiredChildFactV2, ...] = Field(
        default_factory=tuple, max_length=100
    )
    pull_requests: tuple[DevPullRequestFactV2, ...] = Field(
        default_factory=tuple, max_length=25
    )
    ci_checks: tuple[DevCIFactV2, ...] = Field(default_factory=tuple, max_length=25)
    deployments: tuple[DevDeploymentFactV2, ...] = Field(
        default_factory=tuple, max_length=25
    )
    incidents: tuple[DevIncidentFactV2, ...] = Field(
        default_factory=tuple, max_length=25
    )
    graph_edges: tuple[DevGraphEdgeV2, ...] = Field(
        default_factory=tuple, max_length=25
    )
    observed_changes: tuple[DevObservedChangeV2, ...] = Field(
        default_factory=tuple, max_length=25
    )
    metric_refs: tuple[DevMetricRefV2, ...] = Field(
        default_factory=tuple, max_length=25
    )


class DevSourceObservation(ContractModelV2):
    schema_version: Literal["dev_source_observation.v1"]
    observation_id: ServerHandle
    source_class: SourceClass
    adapter_id: OpaqueID
    requirement_level: RequirementLevel
    observed_state: SourceRequirementState
    data_semantics: Literal["measured_zero", "no_data", "not_measured"]
    watermark: AwareDatetime | None = None
    subject_coverage: FiniteFloat = Field(ge=0, le=1)
    usable_fact_count: int = Field(ge=0, le=100_000)
    sample_count: int | None = Field(default=None, ge=0, le=100_000)
    relationship_paths: tuple[DevRelationshipPath, ...] = Field(
        default_factory=tuple, max_length=25
    )
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )
    limitation: ShortText | None = None
    observed_at: AwareDatetime
    query_version: Version
    #: CHAOS-3295: the typed domain content a frame builder needs.
    #: ``NO_ANSWER_FRAME_FIELD_POLICY`` already sets ``DevAnswerFrame.
    #: source_observations`` to ``ABSENT`` on every no-answer outcome, so
    #: this field is structurally unreachable from a no-answer frame with no
    #: separate policy registration required.
    content: DevSourceContent | None = None

    @model_validator(mode="after")
    def validate_zero_semantics(self) -> Self:
        if self.observed_state in _QUERIED_STATES:
            if self.usable_fact_count == 0 and self.data_semantics == "not_measured":
                raise ValueError(
                    "a queried source with zero facts must report measured_zero "
                    "or no_data, never not_measured"
                )
            if self.usable_fact_count > 0 and self.data_semantics != "measured_zero":
                raise ValueError(
                    "a queried source with usable facts must report measured_zero"
                )
        elif self.observed_state in _UNMEASURED_STATES:
            if self.usable_fact_count != 0:
                raise ValueError(
                    "a source that was never measured cannot report usable facts"
                )
            if self.data_semantics == "measured_zero":
                raise ValueError(
                    "an unconfigured/unavailable/not-applicable/truncated source "
                    "cannot claim a measured zero — that would represent missing "
                    "or unconfigured data as zero"
                )
        if self.observed_state in _UNMEASURED_STATES and self.limitation is None:
            raise ValueError(
                "a source that was not fully measured requires a bounded limitation"
            )
        return self

    @model_validator(mode="after")
    def validate_content_semantics(self) -> Self:
        if self.observed_state in _UNMEASURED_STATES and self.content is not None:
            raise ValueError(
                "a source that was never measured cannot carry domain content"
            )
        return self


class DevInvestigationResult(ContractModelV2):
    schema_version: Literal["dev_investigation_result.v1"]
    result_id: ServerHandle
    plan_id: PlanRegistryID
    plan_version: PlatformVersionToken
    run_id: ServerHandle
    subject_set_fingerprint: OpaqueID | None = None
    subject_entity_id: OpaqueID | None = None
    observations: tuple[DevSourceObservation, ...] = Field(min_length=1, max_length=25)
    completed_steps: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=25)
    skipped_steps: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=25)
    failed_steps: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=25)
    relationship_closure_verified: bool
    completed_at: AwareDatetime

    @model_validator(mode="after")
    def validate_result_invariants(self) -> Self:
        if (
            self.subject_set_fingerprint is not None
            and self.subject_entity_id is not None
        ):
            raise ValueError(
                "a result is either for one subject or a subject set, not both"
            )
        completed = set(self.completed_steps)
        skipped = set(self.skipped_steps)
        failed = set(self.failed_steps)
        if len(completed) != len(self.completed_steps):
            raise ValueError("completed steps must be unique")
        if len(skipped) != len(self.skipped_steps):
            raise ValueError("skipped steps must be unique")
        if len(failed) != len(self.failed_steps):
            raise ValueError("failed steps must be unique")
        if (completed & skipped) or (completed & failed) or (skipped & failed):
            raise ValueError(
                "a step cannot be in more than one of completed/skipped/failed"
            )
        observation_ids = [obs.observation_id for obs in self.observations]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("observation ids must be unique")
        return self
