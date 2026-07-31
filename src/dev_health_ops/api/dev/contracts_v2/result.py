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
    OpaqueID,
    PlatformVersionToken,
    ServerHandle,
    ShortText,
    SourceClass,
    SourceRequirementState,
    Version,
)
from .plan import PlanRegistryID

__all__ = [
    "DevInvestigationResult",
    "DevRelationshipPath",
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
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=25)


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
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=25)
    limitation: ShortText | None = None
    observed_at: AwareDatetime
    query_version: Version

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
