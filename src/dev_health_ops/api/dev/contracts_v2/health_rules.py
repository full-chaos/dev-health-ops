"""``health_rule_definition.v1``, ``dimension_observation.v1``,
``health_rule_finding.v1``, ``team_qualification_result.v1``,
``health_rule_calibration.v1`` -- CHAOS-3302.

The code-owned rule registry and calibration governance layer required
before Ask Dev may describe a project/team as healthy, at risk, needing
attention, overburdened, or operationally deficient (Amendment TRD v2,
CHAOS-3293's parent gate). This module defines the wire shapes; the
registry, evaluation engine, and qualification contract live in
``dev_health_ops.api.dev.health_rule_registry`` (mirrors the plan/tool
registry split: ``contracts_v2.plan`` declares ``PLAN_REGISTRY`` and the
wire shape, ``tool_registry`` owns construction-time validation and
execution).

Two structural decisions carry the ticket's privacy/language and
calibration-discipline requirements at the type level rather than by
detection:

* ``RuleApplicability`` is closed to ``project | team | portfolio``. There
  is no person/individual member, so a person-level rule is not
  representable -- "No person-level rule or output" is a closed vocabulary,
  not a runtime scan.
* ``HealthRuleDefinition.calibration_evidence_ref`` is present if and only
  if ``calibration_state != PROVISIONAL`` (``validate_calibration_evidence``
  below). A provisional rule cannot carry an evidence reference, because
  that would claim a review that never happened; a non-provisional rule
  must, because "product_approved / data_derived / policy_driven" are
  claims about a completed review. This is the type-level enforcement of
  "Do not activate provisional thresholds as canonical rules without
  review" -- a rule cannot even be *constructed* in the contradictory state.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Literal, Self

from pydantic import AwareDatetime, Field, FiniteFloat, model_validator

from .base import (
    ContractModelV2,
    OpaqueID,
    PlatformVersionToken,
    ShortText,
    SourceClass,
    SourceRequirementState,
)

__all__ = [
    "CalibrationRecord",
    "CalibrationState",
    "DimensionObservation",
    "DimensionState",
    "HealthDimension",
    "HealthRuleDefinition",
    "RuleApplicability",
    "RuleDirection",
    "TeamQualificationBasis",
    "TeamQualificationResult",
]

FactKind = Literal["observed", "inferred", "recommendation"]
DataSemantics = Literal["measured_zero", "no_data", "not_measured"]
SuppressedReason = Literal[
    "insufficient_sample",
    "insufficient_coverage",
    "insufficient_cohort",
    "not_sustained",
    "missing_denominator",
    "missing_attribution",
]


class HealthDimension(StrEnum):
    """The nine canonical dimensions named verbatim in CHAOS-3302.

    Wave 3.1 does not implement a universal composite score (explicit
    non-goal); this enum exists so every finding declares exactly one of
    these, never a free-form category.
    """

    EXECUTION_COMPLETION = "execution_completion"
    DELIVERY_FLOW = "delivery_flow"
    RELIABILITY_RELEASE = "reliability_release"
    REVIEW_CI_PRESSURE = "review_ci_pressure"
    CODE_OWNERSHIP_RISK = "code_ownership_risk"
    COGNITIVE_WORKLOAD_PRESSURE = "cognitive_workload_pressure"
    INVESTMENT_BALANCE = "investment_balance"
    DEPENDENCIES_BLOCKERS = "dependencies_blockers"
    DATA_TRUST = "data_trust"


class DimensionState(StrEnum):
    """Per-dimension state. ``unknown``/``not_applicable`` are first-class,

    never collapsed to ``healthy`` (the CHAOS-3302 exit criterion: "Unknown
    and not-applicable states are preserved without being collapsed to
    healthy/zero").
    """

    HEALTHY = "healthy"
    WATCH = "watch"
    AT_RISK = "at_risk"
    CRITICAL = "critical"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


#: States a rule's own condition may resolve to when triggered. A rule can
#: never *author* healthy/unknown/not_applicable -- those are computed
#: structurally by the evaluation engine (absence of data, insufficient
#: sample/coverage/cohort, or a condition that did not fire), never
#: declared by a rule definition.
_TRIGGERABLE_STATES = frozenset(
    {DimensionState.WATCH, DimensionState.AT_RISK, DimensionState.CRITICAL}
)


class CalibrationState(StrEnum):
    """The four-way split named verbatim in CHAOS-3302's calibration section."""

    PROVISIONAL = "provisional"
    PRODUCT_APPROVED = "product_approved"
    DATA_DERIVED = "data_derived"
    POLICY_DRIVEN = "policy_driven"


#: Only a provisional rule may omit a calibration evidence reference; every
#: other state is a claim that a review happened and must cite it.
_REVIEWED_CALIBRATION_STATES = frozenset(
    {
        CalibrationState.PRODUCT_APPROVED,
        CalibrationState.DATA_DERIVED,
        CalibrationState.POLICY_DRIVEN,
    }
)


class RuleApplicability(StrEnum):
    """Closed to project/team/portfolio -- see module docstring."""

    PROJECT = "project"
    TEAM = "team"
    PORTFOLIO = "portfolio"


class RuleDirection(StrEnum):
    """A rule's threshold direction, or ``deterministic`` for a boolean condition."""

    HIGHER_IS_WORSE = "higher_is_worse"
    LOWER_IS_WORSE = "lower_is_worse"
    DETERMINISTIC = "deterministic"


class TeamQualificationBasis(StrEnum):
    MULTI_DIMENSION = "multi_dimension"
    CRITICAL_RULE = "critical_rule"


class HealthRuleDefinition(ContractModelV2):
    """One code-owned, versioned rule. Every field named in CHAOS-3302's

    "Every rule declares" list is represented here.
    """

    schema_version: Literal["health_rule_definition.v1"]
    rule_id: OpaqueID
    rule_version: PlatformVersionToken
    owner: ShortText

    applicability: tuple[RuleApplicability, ...] = Field(min_length=1, max_length=3)
    dimension: HealthDimension

    required_source_classes: tuple[SourceClass, ...] = Field(
        min_length=1, max_length=12
    )
    required_observed_states: tuple[SourceRequirementState, ...] = Field(
        min_length=1, max_length=8
    )

    direction: RuleDirection
    threshold: FiniteFloat | None = Field(default=None)
    comparison_unit: ShortText | None = None

    minimum_sample: int = Field(ge=0, le=1_000_000)
    minimum_coverage: FiniteFloat = Field(ge=0, le=1)
    current_window_days: int = Field(ge=1, le=365)
    comparison_window_days: int | None = Field(default=None, ge=1, le=365)
    sustained_periods_required: int = Field(ge=1, le=12)

    denominator_required: bool
    attribution_required: bool
    minimum_cohort_size: int | None = Field(default=None, ge=1, le=100_000)

    triggered_state: DimensionState
    evidence_source_classes: tuple[SourceClass, ...] = Field(
        min_length=1, max_length=12
    )
    fact_kind: FactKind

    #: A bounded, server-owned remediation template -- never a rendering of
    #: arbitrary producer copy on the wire (mirrors the frame's public-copy
    #: discipline in ``contracts_v2.validators``).
    remediation_template: ShortText

    calibration_state: CalibrationState
    calibration_evidence_ref: OpaqueID | None = None

    @model_validator(mode="after")
    def validate_triggered_state(self) -> Self:
        if self.triggered_state not in _TRIGGERABLE_STATES:
            raise ValueError(
                "triggered_state must be watch, at_risk, or critical -- "
                "healthy/unknown/not_applicable are computed, never authored"
            )
        return self

    @model_validator(mode="after")
    def validate_direction_threshold(self) -> Self:
        if self.direction is RuleDirection.DETERMINISTIC:
            if self.threshold is not None:
                raise ValueError("a deterministic rule cannot carry a threshold")
        elif self.threshold is None:
            raise ValueError("a threshold-direction rule requires a threshold")
        return self

    @model_validator(mode="after")
    def validate_cohort_requirement(self) -> Self:
        needs_cohort = (
            RuleApplicability.TEAM in self.applicability
            or RuleApplicability.PORTFOLIO in self.applicability
        )
        if needs_cohort and self.minimum_cohort_size is None:
            raise ValueError(
                "a rule applicable to team or portfolio requires minimum_cohort_size"
            )
        if not needs_cohort and self.minimum_cohort_size is not None:
            raise ValueError("minimum_cohort_size only applies to team/portfolio rules")
        return self

    @model_validator(mode="after")
    def validate_calibration_evidence(self) -> Self:
        """A provisional rule cannot cite review evidence it never received;

        every reviewed calibration state must cite the review that happened.
        See the module docstring -- this is the type-level enforcement of
        "do not activate provisional thresholds as canonical rules without
        review".
        """

        is_reviewed = self.calibration_state in _REVIEWED_CALIBRATION_STATES
        has_evidence = self.calibration_evidence_ref is not None
        if is_reviewed and not has_evidence:
            raise ValueError(
                f"calibration_state={self.calibration_state.value!r} requires "
                "calibration_evidence_ref"
            )
        if not is_reviewed and has_evidence:
            raise ValueError(
                "a provisional rule cannot carry a calibration_evidence_ref "
                "-- that would claim a review that never happened"
            )
        return self


class DimensionObservation(ContractModelV2):
    """One rule's evaluation input for one subject over one window.

    ``window_index`` orders observations for the sustained-window check:
    ``0`` is the current period, increasing values are further in the past.
    """

    schema_version: Literal["dimension_observation.v1"]
    subject_kind: RuleApplicability
    subject_id: OpaqueID
    cohort_size: int | None = Field(default=None, ge=0, le=100_000)
    observed_states: tuple[SourceRequirementState, ...] = Field(
        min_length=1, max_length=8
    )
    data_semantics: DataSemantics
    sample_count: int | None = Field(default=None, ge=0, le=1_000_000)
    coverage: FiniteFloat = Field(ge=0, le=1)
    current_value: FiniteFloat | None = None
    comparison_value: FiniteFloat | None = None
    denominator_present: bool
    attribution_present: bool
    window_index: int = Field(ge=0, le=52)
    observed_at: AwareDatetime

    @model_validator(mode="after")
    def validate_zero_semantics(self) -> Self:
        """Mirrors ``DevSourceObservation.validate_zero_semantics`` exactly

        (CHAOS-3294's zero-vs-no-data invariant, reused here rather than
        imported, since it is a private module-level frozenset there): a
        source genuinely queried and reporting nothing found must be
        ``measured_zero``, never ``not_measured``; a source that was never
        measured cannot claim a measured zero.
        """

        queried = {
            SourceRequirementState.AVAILABLE_CURRENT,
            SourceRequirementState.AVAILABLE_STALE,
            SourceRequirementState.AVAILABLE_UNKNOWN,
        }
        unmeasured = {
            SourceRequirementState.UNCONFIGURED,
            SourceRequirementState.UNAVAILABLE,
            SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
            SourceRequirementState.NOT_APPLICABLE,
            SourceRequirementState.TRUNCATED,
        }
        states = set(self.observed_states)
        if states <= queried:
            if self.data_semantics == "not_measured":
                raise ValueError(
                    "a queried dimension observation must report measured_zero "
                    "or no_data, never not_measured"
                )
            if (
                self.current_value is not None
                and self.data_semantics != "measured_zero"
            ):
                raise ValueError(
                    "a queried observation with a current value must report "
                    "measured_zero"
                )
        elif states <= unmeasured:
            if self.data_semantics == "measured_zero":
                raise ValueError(
                    "an unmeasured observation cannot claim a measured zero"
                )
            if self.current_value is not None:
                raise ValueError(
                    "an unmeasured observation cannot carry a current_value"
                )
        return self


class HealthRuleFinding(ContractModelV2):
    """One rule's evaluated result for one subject/window.

    ``shadow_only`` findings (from a ``provisional`` rule) are computed for
    calibration review but must never reach a launch surface or the team
    qualification contract -- see
    ``health_rule_registry.evaluate_registry``.
    """

    schema_version: Literal["health_rule_finding.v1"]
    finding_id: OpaqueID
    rule_id: OpaqueID
    rule_version: PlatformVersionToken
    dimension: HealthDimension
    subject_kind: RuleApplicability
    subject_id: OpaqueID
    state: DimensionState
    fact_kind: FactKind
    shadow_only: bool
    evidence_source_classes: tuple[SourceClass, ...] = Field(
        default_factory=tuple, max_length=12
    )
    remediation_template: ShortText
    calibration_state: CalibrationState
    evaluated_at: AwareDatetime
    suppressed_reason: SuppressedReason | None = None

    @model_validator(mode="after")
    def validate_suppression_state(self) -> Self:
        if self.suppressed_reason is not None and self.state not in {
            DimensionState.UNKNOWN,
            DimensionState.NOT_APPLICABLE,
        }:
            raise ValueError(
                "a suppressed finding must report unknown or not_applicable, "
                "never a silent healthy/at_risk/critical"
            )
        return self


class TeamQualificationResult(ContractModelV2):
    """The team-needs-attention qualification contract (CHAOS-3302).

    A team-level finding requires either at least two independent
    applicable dimensions at risk for the sustained window, or one critical
    rule with required evidence and coverage. ``qualifies`` is never True
    without a ``basis``, and the basis's own minimum cardinality is
    enforced structurally below (never trusted from the caller).
    """

    schema_version: Literal["team_qualification_result.v1"]
    team_id: OpaqueID
    qualifies: bool
    basis: TeamQualificationBasis | None = None
    contributing_dimensions: tuple[HealthDimension, ...] = Field(
        default_factory=tuple, max_length=9
    )
    contributing_finding_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=50
    )
    evaluated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_qualification_consistency(self) -> Self:
        if self.qualifies != (self.basis is not None):
            raise ValueError("qualifies must be set if and only if a basis is present")
        if self.basis is TeamQualificationBasis.MULTI_DIMENSION:
            if len(set(self.contributing_dimensions)) < 2:
                raise ValueError(
                    "multi_dimension qualification requires at least two "
                    "independent contributing dimensions"
                )
        if self.basis is TeamQualificationBasis.CRITICAL_RULE:
            if not self.contributing_finding_ids:
                raise ValueError(
                    "critical_rule qualification requires at least one "
                    "contributing finding"
                )
        if self.basis is None and (
            self.contributing_dimensions or self.contributing_finding_ids
        ):
            raise ValueError(
                "a non-qualifying result cannot carry contributing evidence"
            )
        return self


class CalibrationRecord(ContractModelV2):
    """Calibration evidence and owner decision for one rule (CHAOS-3302 deliverable):

    "Record sample sizes, percentiles/distributions, false-positive/negative
    review, small-cohort behavior, and owner approval."
    """

    schema_version: Literal["health_rule_calibration.v1"]
    calibration_id: OpaqueID
    rule_id: OpaqueID
    rule_version: PlatformVersionToken
    calibration_state: CalibrationState
    sample_size: int = Field(ge=0, le=10_000_000)
    distribution_summary: ShortText
    false_positive_review: ShortText
    false_negative_review: ShortText
    small_cohort_behavior: ShortText
    owner: ShortText
    decided_at: date
    evidence_ref: OpaqueID | None = None
    notes: ShortText | None = None

    @model_validator(mode="after")
    def validate_reviewed_state_has_evidence(self) -> Self:
        if (
            self.calibration_state in _REVIEWED_CALIBRATION_STATES
            and self.evidence_ref is None
        ):
            raise ValueError("a reviewed calibration record requires an evidence_ref")
        return self
