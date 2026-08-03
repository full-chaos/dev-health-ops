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
    DevRelationshipPath,
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
from .deficiency import (
    DeficiencyCategoryStatus,
    DeficiencyFinding,
    assert_full_category_coverage,
    finding_sort_key,
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
from .health_rules import DimensionState, HealthRuleFinding
from .plan import PlanRegistryID

__all__ = [
    "HEALTH_FINDING_SEVERITY_RANK",
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
    #: CHAOS-3297 stack #3: launch-eligible findings only (never
    #: shadow/suppressed -- see ``health_profile_synthesis.HealthProfileResult``)
    #: from ``ProjectHealthService``/``TeamHealthService``/
    #: ``PortfolioStatusService``/``TeamWorkloadService``, ordered
    #: worst-severity-first then ``finding_id`` for stability (mirrors
    #: ``portfolio_status_service._sort_key``'s own severity table) so a
    #: 50-of-N cap always keeps the same 50. ``health_findings_truncated``
    #: is a SEPARATE signal from the cap itself (team-lead amendment,
    #: 2026-08-02: "a capped set without a signal is a false-complete",
    #: the same lesson CHAOS-3297 s2's denominator fix exists for) -- a
    #: bounded tuple alone cannot distinguish "exactly 50 findings exist"
    #: from "51+ findings exist and 1+ were dropped", so the wiring
    #: function that populates this field must set the flag explicitly
    #: whenever the pre-cap count exceeds 50.
    health_findings: tuple[HealthRuleFinding, ...] = Field(
        default_factory=tuple, max_length=50
    )
    health_findings_truncated: bool = False
    #: CHAOS-3297 stack #3: ``OperationalDeficiencyService``'s findings,
    #: same worst-severity-first/``finding_id`` ordering and truncation-
    #: disclosure discipline as ``health_findings`` above.
    deficiency_findings: tuple[DeficiencyFinding, ...] = Field(
        default_factory=tuple, max_length=50
    )
    deficiency_findings_truncated: bool = False
    #: CHAOS-3297 s3 codex full-branch review round 1 (FINDING 2, CONFIRMED
    #: HIGH, 2026-08-02): ``OperationalDeficiencyInventory.category_statuses``
    #: was being discarded entirely on the way into ``DevSourceContent`` --
    #: only ``findings`` was ever copied over. Eight valid UNEVALUATED
    #: categories then produced empty ``deficiency_findings`` with no
    #: distinguishing signal, indistinguishable from eight genuinely
    #: evaluated, genuinely zero-finding categories -- exactly the
    #: evaluated-zero-vs-unevaluated distinction ``DeficiencyCategoryStatus``
    #: exists to preserve (CHAOS-3305), erased one hop downstream. Empty by
    #: default for every source class other than ``DEFICIENCY_INVENTORY``,
    #: which this field does not apply to; when populated, must be exactly
    #: the eight closed categories (``validate_deficiency_category_coverage``
    #: below, via ``deficiency.assert_full_category_coverage`` imported by
    #: reference -- the same check ``OperationalDeficiencyInventory`` itself
    #: enforces, never a second copy that could disagree).
    deficiency_category_statuses: tuple[DeficiencyCategoryStatus, ...] = Field(
        default_factory=tuple, max_length=8
    )
    metric_refs: tuple[DevMetricRefV2, ...] = Field(
        default_factory=tuple, max_length=25
    )

    @model_validator(mode="after")
    def validate_deficiency_category_coverage(self) -> Self:
        if self.deficiency_category_statuses:
            assert_full_category_coverage(self.deficiency_category_statuses)
        return self

    @model_validator(mode="after")
    def validate_finding_order(self) -> Self:
        """``health_findings``/``deficiency_findings`` must already be
        worst-severity-first, then a stable id -- the canonical form,
        enforced here rather than merely documented, the same posture
        ``DevAnswerFact.validate_disclosures_canonical_order`` takes for
        ``FactDisclosure`` (contracts_v2/frame.py). A capped tuple whose
        SURVIVING 50 depend on caller iteration order rather than a
        structural invariant is not reproducible: two evaluations of the
        identical, larger underlying finding set could keep a different
        50 -- this closes that as a construction-time error rather than a
        wiring convention a future call site could silently violate.

        ``deficiency_findings`` reuses ``deficiency.finding_sort_key`` BY
        REFERENCE rather than a second, hand-written severity table:
        ``OperationalDeficiencyInventory.findings`` is already validated
        against that exact key (severity, category, finding_id) at its own
        contract layer, so importing it is the only way this check and
        that one can be PROVEN to agree rather than merely described as
        agreeing (mirrors ``relationship_matrix.py``'s own "imported by
        reference, not duplicated by value" rationale). ``health_findings``
        has no equivalent existing convention to import --
        ``health_profile_synthesis._ordered`` sorts by
        ``(dimension, rule_id)``, not severity -- so this validator defines
        the canonical health order itself, matching
        ``portfolio_status_service._DIMENSION_STATE_SEVERITY``.
        """

        health_keys = [
            (HEALTH_FINDING_SEVERITY_RANK[finding.state], finding.finding_id)
            for finding in self.health_findings
        ]
        if health_keys != sorted(health_keys):
            raise ValueError(
                "health_findings must be ordered worst-severity-first, then finding_id"
            )
        deficiency_keys = [
            finding_sort_key(finding) for finding in self.deficiency_findings
        ]
        if deficiency_keys != sorted(deficiency_keys):
            raise ValueError(
                "deficiency_findings must be ordered per deficiency.finding_sort_key "
                "(severity, category, finding_id)"
            )
        return self


#: Worst-first severity rank for ``health_findings`` ordering -- mirrors
#: ``portfolio_status_service._DIMENSION_STATE_SEVERITY`` exactly (lower
#: sorts first). Import-time-total over ``DimensionState`` below, the same
#: posture as every other closed-vocabulary table in this package.
HEALTH_FINDING_SEVERITY_RANK: dict[DimensionState, int] = {
    DimensionState.CRITICAL: 0,
    DimensionState.AT_RISK: 1,
    DimensionState.WATCH: 2,
    DimensionState.UNKNOWN: 3,
    DimensionState.NOT_APPLICABLE: 4,
    DimensionState.HEALTHY: 5,
}
_missing_health_states = set(DimensionState) - set(HEALTH_FINDING_SEVERITY_RANK)
if _missing_health_states:
    raise RuntimeError(
        f"health_findings severity rank is missing DimensionState member(s): "
        f"{sorted(state.value for state in _missing_health_states)}"
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
