"""CHAOS-3302 end-to-end controls for the health rule registry and governance.

Written and run RED (against no implementation) before any ``src/`` edit, per
the Wave 3.1 verification discipline: these assert observable outcomes at the
real seam (the registry's evaluation entry point and team qualification
contract), never internal diagnostics. They are the acceptance evidence for
the exit criteria in CHAOS-3302:

- "Team-needs-attention and workload language obeys the qualification
  contract" (positive controls);
- "no single-signal struggling/overburdened result" (negative control a);
- small-cohort suppression (negative control b);
- "Unknown and not-applicable states are preserved without being collapsed
  to healthy/zero" (negative control c);
- "Provisional thresholds are either calibrated/approved or remain
  shadow-only" (negative control d).

Positive: a calibrated (``product_approved``) rule reaching ``at_risk``/
``critical`` with required evidence and coverage produces exactly one
team-needs-attention qualification, through two independent qualifying
routes (two independent at-risk dimensions; one critical rule).

Negative:
  (a) one metric / one bad week / one provisional threshold alone must NOT
      produce a launch finding or a team qualification;
  (b) a cohort below ``minimum_cohort_size`` suppresses the finding
      (reports ``unknown``, never a silent healthy/zero);
  (c) measured-zero vs no-data must not collapse into the same state;
  (d) a provisional threshold not yet approved remains shadow-only and never
      reaches ``evaluate_registry``'s launch-findings set.

Test-scoped authority (2026-08-01 correction)
----------------------------------------------

A Codex adversarial review confirmed (high severity) that shipping three
``product_approved`` example rules in ``HEALTH_RULE_REGISTRY`` -- with a
same-changeset illustrative calibration record as their only "evidence" --
launch-authorized real findings on review authority that never existed:
any caller of ``evaluate_registry`` would receive them in
``launch_findings``, indistinguishable from a genuinely reviewed rule.
Every rule in the shipped registry is now ``provisional`` (enforced by
``test_health_rule_registry.test_no_shipped_rule_is_launch_authorized``,
a totality check with no exceptions).

The positive controls below therefore prove the "approved launch finding"
and "team qualification" mechanism against ``_AUTHORIZED_TEST_REGISTRY``,
a ``HealthRuleRegistry`` instance defined entirely in this test module.
This is honest test-scoped authority -- these rules are not, and never
were, real reviewed rules -- rather than shipping fake authority in the
production registry to make a positive control easy to write.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceClass, SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationState,
    DimensionObservation,
    DimensionState,
    HealthDimension,
    HealthRuleDefinition,
    RuleApplicability,
    RuleDirection,
)
from dev_health_ops.api.dev.health_rule_registry import (
    HEALTH_RULE_REGISTRY,
    HealthRuleRegistry,
    evaluate_registry,
    qualify_team_needs_attention,
)

_NOW = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)

# ---------------------------------------------------------------------------
# A test-scoped registry of genuinely "reviewed" rules. Never merged into,
# and never read from, HEALTH_RULE_REGISTRY -- see the module docstring.
# ---------------------------------------------------------------------------

_TEST_COMPLETION_STALLED = HealthRuleDefinition(
    schema_version="health_rule_definition.v1",
    rule_id="health_rule.test_completion_stalled.v1",
    rule_version="health_rule.test_completion_stalled.v1",
    owner="test-scoped-fixture",
    applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
    dimension=HealthDimension.EXECUTION_COMPLETION,
    required_source_classes=(SourceClass.STATUS_CHANGE, SourceClass.WORK_ITEM),
    required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
    direction=RuleDirection.HIGHER_IS_WORSE,
    threshold=0.2,
    comparison_unit="stalled_work_item_ratio",
    minimum_sample=10,
    minimum_coverage=0.6,
    current_window_days=14,
    comparison_window_days=None,
    sustained_periods_required=2,
    denominator_required=True,
    attribution_required=False,
    minimum_cohort_size=5,
    triggered_state=DimensionState.AT_RISK,
    evidence_source_classes=(SourceClass.STATUS_CHANGE, SourceClass.WORK_ITEM),
    fact_kind="observed",
    remediation_template="Review stalled work items with the team before the next planning cycle.",
    calibration_state=CalibrationState.PRODUCT_APPROVED,
    calibration_evidence_ref="test.calibration.test_completion_stalled.v1",
)

_TEST_REVIEW_LATENCY_SUSTAINED = HealthRuleDefinition(
    schema_version="health_rule_definition.v1",
    rule_id="health_rule.test_review_latency_sustained.v1",
    rule_version="health_rule.test_review_latency_sustained.v1",
    owner="test-scoped-fixture",
    applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
    dimension=HealthDimension.REVIEW_CI_PRESSURE,
    required_source_classes=(SourceClass.PULL_REQUEST, SourceClass.REVIEW),
    required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
    direction=RuleDirection.HIGHER_IS_WORSE,
    threshold=0.5,
    comparison_unit="p50_review_latency_hours",
    minimum_sample=10,
    minimum_coverage=0.6,
    current_window_days=14,
    comparison_window_days=None,
    sustained_periods_required=2,
    denominator_required=False,
    attribution_required=False,
    minimum_cohort_size=5,
    triggered_state=DimensionState.AT_RISK,
    evidence_source_classes=(SourceClass.PULL_REQUEST, SourceClass.REVIEW),
    fact_kind="observed",
    remediation_template="Review open pull requests aging past the team's usual review latency.",
    calibration_state=CalibrationState.PRODUCT_APPROVED,
    calibration_evidence_ref="test.calibration.test_review_latency_sustained.v1",
)

_TEST_DATA_TRUST_BROKEN = HealthRuleDefinition(
    schema_version="health_rule_definition.v1",
    rule_id="health_rule.test_data_trust_broken.v1",
    rule_version="health_rule.test_data_trust_broken.v1",
    owner="test-scoped-fixture",
    applicability=(
        RuleApplicability.PROJECT,
        RuleApplicability.TEAM,
        RuleApplicability.PORTFOLIO,
    ),
    dimension=HealthDimension.DATA_TRUST,
    required_source_classes=(SourceClass.SOURCE_HEALTH,),
    required_observed_states=(
        SourceRequirementState.AVAILABLE_CURRENT,
        SourceRequirementState.AVAILABLE_STALE,
    ),
    direction=RuleDirection.DETERMINISTIC,
    threshold=None,
    comparison_unit=None,
    minimum_sample=1,
    minimum_coverage=0.0,
    current_window_days=7,
    comparison_window_days=None,
    sustained_periods_required=1,
    denominator_required=False,
    attribution_required=False,
    minimum_cohort_size=1,
    triggered_state=DimensionState.CRITICAL,
    evidence_source_classes=(SourceClass.SOURCE_HEALTH,),
    fact_kind="observed",
    remediation_template="Restore or reconfigure the affected data source before trusting downstream findings.",
    calibration_state=CalibrationState.PRODUCT_APPROVED,
    calibration_evidence_ref="test.calibration.test_data_trust_broken.v1",
)

_AUTHORIZED_TEST_REGISTRY = HealthRuleRegistry(
    (
        _TEST_COMPLETION_STALLED,
        _TEST_REVIEW_LATENCY_SUSTAINED,
        _TEST_DATA_TRUST_BROKEN,
    )
)


def _observation(
    *,
    subject_kind: RuleApplicability = RuleApplicability.TEAM,
    subject_id: str = "team-1",
    cohort_size: int | None = 12,
    observed_states: tuple[SourceRequirementState, ...] = (
        SourceRequirementState.AVAILABLE_CURRENT,
    ),
    data_semantics: str = "measured_zero",
    sample_count: int | None = 40,
    coverage: float = 0.9,
    current_value: float | None = 1.0,
    comparison_value: float | None = None,
    denominator_present: bool = True,
    attribution_present: bool = True,
    window_index: int = 0,
) -> DimensionObservation:
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=observed_states,
        data_semantics=data_semantics,
        sample_count=sample_count,
        coverage=coverage,
        current_value=current_value,
        comparison_value=comparison_value,
        denominator_present=denominator_present,
        attribution_present=attribution_present,
        window_index=window_index,
        observed_at=_NOW,
    )


def _sustained(
    rule_id: str, *, registry: HealthRuleRegistry, periods: int, **kwargs: object
) -> list[DimensionObservation]:
    rule = registry.rule(rule_id)
    return [
        _observation(window_index=index, **kwargs)  # type: ignore[arg-type]
        for index in range(max(periods, rule.sustained_periods_required))
    ]


# ---------------------------------------------------------------------------
# Positive controls (test-scoped authority -- see module docstring)
# ---------------------------------------------------------------------------


def test_positive_two_independent_at_risk_dimensions_qualify_team_needs_attention() -> (
    None
):
    observations = {
        "health_rule.test_completion_stalled.v1": _sustained(
            "health_rule.test_completion_stalled.v1",
            registry=_AUTHORIZED_TEST_REGISTRY,
            periods=2,
        ),
        "health_rule.test_review_latency_sustained.v1": _sustained(
            "health_rule.test_review_latency_sustained.v1",
            registry=_AUTHORIZED_TEST_REGISTRY,
            periods=2,
        ),
    }
    result = evaluate_registry(_AUTHORIZED_TEST_REGISTRY, observations)
    launch_dimensions = {finding.dimension for finding in result.launch_findings}
    assert HealthDimension.EXECUTION_COMPLETION in launch_dimensions
    assert HealthDimension.REVIEW_CI_PRESSURE in launch_dimensions

    qualification = qualify_team_needs_attention(
        result.launch_findings, team_id="team-1"
    )
    assert qualification.qualifies is True
    assert qualification.basis == "multi_dimension"
    assert len(qualification.contributing_dimensions) >= 2


def test_positive_one_critical_rule_with_evidence_and_coverage_qualifies_team() -> None:
    observations = {
        "health_rule.test_data_trust_broken.v1": _sustained(
            "health_rule.test_data_trust_broken.v1",
            registry=_AUTHORIZED_TEST_REGISTRY,
            periods=1,
            coverage=0.95,
            sample_count=100,
        ),
    }
    result = evaluate_registry(_AUTHORIZED_TEST_REGISTRY, observations)
    critical = [f for f in result.launch_findings if f.state == DimensionState.CRITICAL]
    assert len(critical) == 1
    assert critical[0].evidence_source_classes

    qualification = qualify_team_needs_attention(
        result.launch_findings, team_id="team-1"
    )
    assert qualification.qualifies is True
    assert qualification.basis == "critical_rule"


# ---------------------------------------------------------------------------
# Negative controls
# ---------------------------------------------------------------------------


def test_negative_single_provisional_threshold_alone_produces_no_launch_finding() -> (
    None
):
    """(a) + (d) a lone provisional-threshold breach never reaches a launch finding."""

    provisional_ids = [
        rule_id
        for rule_id, rule in HEALTH_RULE_REGISTRY.items()
        if rule.calibration_state == CalibrationState.PROVISIONAL
    ]
    assert provisional_ids, (
        "fixture assumption: at least one provisional rule is registered"
    )
    rule_id = provisional_ids[0]
    observations = {
        rule_id: _sustained(rule_id, registry=HEALTH_RULE_REGISTRY, periods=3)
    }

    result = evaluate_registry(HEALTH_RULE_REGISTRY, observations)
    assert result.launch_findings == ()
    assert len(result.shadow_findings) == 1
    assert result.shadow_findings[0].shadow_only is True

    qualification = qualify_team_needs_attention(
        result.launch_findings, team_id="team-1"
    )
    assert qualification.qualifies is False


def test_negative_single_at_risk_dimension_alone_does_not_qualify_team() -> None:
    """(a) one bad-week signal in one dimension is insufficient by itself,

    even from a genuinely authorized (test-scoped) rule -- this isolates the
    qualification contract's own multi-signal requirement from whether the
    rule is reviewed at all (that is the separate provisional/shadow
    control above).
    """

    observations = {
        "health_rule.test_completion_stalled.v1": _sustained(
            "health_rule.test_completion_stalled.v1",
            registry=_AUTHORIZED_TEST_REGISTRY,
            periods=2,
        ),
    }
    result = evaluate_registry(_AUTHORIZED_TEST_REGISTRY, observations)
    assert len(result.launch_findings) == 1
    assert result.launch_findings[0].state == DimensionState.AT_RISK

    qualification = qualify_team_needs_attention(
        result.launch_findings, team_id="team-1"
    )
    assert qualification.qualifies is False
    assert qualification.basis is None


def test_negative_cohort_below_minimum_suppresses_finding() -> None:
    """(b) a cohort under minimum_cohort_size reports unknown, not a silent finding."""

    rule = _AUTHORIZED_TEST_REGISTRY.rule("health_rule.test_completion_stalled.v1")
    assert rule.minimum_cohort_size is not None
    small_cohort = rule.minimum_cohort_size - 1
    observations = {
        "health_rule.test_completion_stalled.v1": _sustained(
            "health_rule.test_completion_stalled.v1",
            registry=_AUTHORIZED_TEST_REGISTRY,
            periods=2,
            cohort_size=small_cohort,
        ),
    }
    result = evaluate_registry(_AUTHORIZED_TEST_REGISTRY, observations)
    assert result.launch_findings == ()
    assert len(result.suppressed_findings) == 1
    suppressed = result.suppressed_findings[0]
    assert suppressed.suppressed_reason == "insufficient_cohort"
    assert suppressed.state == DimensionState.UNKNOWN


def test_negative_measured_zero_and_no_data_are_not_collapsed() -> None:
    """(c) mirrors DevSourceObservation.validate_zero_semantics: zero != absent.

    Runs against the shipped (provisional/shadow) registry -- this control
    is purely about data-semantics, not about launch authorization, so it
    checks every result bucket rather than requiring test-scoped authority.
    """

    rule_id = next(iter(HEALTH_RULE_REGISTRY))
    measured_zero = _sustained(
        rule_id,
        registry=HEALTH_RULE_REGISTRY,
        periods=2,
        data_semantics="measured_zero",
        current_value=0.0,
    )
    result_zero = evaluate_registry(HEALTH_RULE_REGISTRY, {rule_id: measured_zero})
    zero_finding = (
        result_zero.launch_findings
        + result_zero.shadow_findings
        + result_zero.suppressed_findings
    )[0]
    assert zero_finding.state != DimensionState.UNKNOWN

    no_data = [
        _observation(
            window_index=index,
            data_semantics="no_data",
            observed_states=(SourceRequirementState.UNCONFIGURED,),
            current_value=None,
        )
        for index in range(2)
    ]
    result_no_data = evaluate_registry(HEALTH_RULE_REGISTRY, {rule_id: no_data})
    no_data_finding = (
        result_no_data.launch_findings
        + result_no_data.shadow_findings
        + result_no_data.suppressed_findings
    )[0]
    assert no_data_finding.state == DimensionState.UNKNOWN
    assert no_data_finding.suppressed_reason is None

    assert zero_finding.state != no_data_finding.state


def test_negative_provisional_rule_construction_requires_shadow_and_no_evidence_claim() -> (
    None
):
    """(d) construction-time: a provisional rule cannot masquerade as approved."""

    with pytest.raises(Exception):
        HealthRuleDefinition(
            schema_version="health_rule_definition.v1",
            rule_id="health_rule.unapproved_example.v1",
            rule_version="health_rule.unapproved_example.v1",
            owner="test-owner",
            applicability=(RuleApplicability.TEAM,),
            dimension=HealthDimension.DELIVERY_FLOW,
            required_source_classes=("work_item",),
            required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
            direction="higher_is_worse",
            threshold=1.0,
            comparison_unit="ratio",
            minimum_sample=10,
            minimum_coverage=0.5,
            current_window_days=7,
            comparison_window_days=None,
            sustained_periods_required=1,
            denominator_required=False,
            attribution_required=False,
            minimum_cohort_size=None,
            triggered_state=DimensionState.AT_RISK,
            evidence_source_classes=("work_item",),
            fact_kind="observed",
            remediation_template="Investigate the affected workload.",
            calibration_state=CalibrationState.PROVISIONAL,
            # Provisional rules cannot carry an evidence ref -- that would
            # claim review that never happened. Passing one here must fail.
            calibration_evidence_ref="doc:fake-approval",
        )
