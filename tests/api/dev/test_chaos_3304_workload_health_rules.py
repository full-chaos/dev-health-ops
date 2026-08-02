"""CHAOS-3304 end-to-end controls for the team-workload/investment-balance rules.

Mirrors ``test_chaos_3302_health_rule_e2e_controls.py``'s discipline exactly:
guardrail mechanics (denominator/cohort/coverage suppression, zero-vs-no-data)
are proven against the REAL shipped rules in ``HEALTH_RULE_REGISTRY`` via
``evaluate_rule`` directly -- those checks run identically regardless of
``calibration_state``, so no test-scoped authority is needed to prove them.
Qualification-reaching-launch-findings tests (which DO depend on
``calibration_state != provisional``) use a test-scoped registry of
``product_approved`` copies, exactly like CHAOS-3302's own positive
controls -- honest test-scoped authority, never fake production authority.

Required test cases from the CHAOS-3304 ticket, and where each is proven:

* valid overburdened multi-signal case ->
  ``test_positive_two_independent_workload_dimensions_qualify``
* high raw pressure with no denominator -> not calculable ->
  ``test_negative_review_request_load_without_denominator_is_not_calculable``
* one signal only -> no struggling/overburdened finding ->
  ``test_negative_single_at_risk_dimension_does_not_qualify``
* small cohort suppression ->
  ``test_negative_cohort_below_minimum_suppresses_workload_finding``
* missing team membership but valid own-history baseline ->
  ``test_negative_review_request_load_without_denominator_is_not_calculable``
  (comparison_value asserted separately at the adapter layer, see
  ``test_chaos_3304_workload_observation_adapters.py``)
* team attribution gaps -> ``test_negative_unmeasured_reports_unknown_never_healthy``
* sufficient/insufficient investment coverage ->
  ``test_negative_investment_shift_insufficient_coverage_suppresses``
* high KTLO/security/infra rendered neutrally -> proven at the adapter layer
  (``test_investment_shift_stable_high_ktlo_mix_reports_neutral_measured_zero``)
* missing vs measured zero -> ``test_negative_unmeasured_reports_unknown_never_healthy``
* CHAOS-3331 promotion guard (team-lead ruling, 2026-08-02: legacy
  repo-pattern/identity-map resolver, not canonical primary attribution,
  writes ``user_metrics_daily``/``team_metrics_daily``) ->
  ``test_chaos_3331_blocked_rules_stay_provisional``

Provider-fallback, persisted-replay, and both-surfaces cases are CHAOS-3300
proof-gate scope, not this ticket's service layer -- not reproduced here.
"""

from __future__ import annotations

from datetime import UTC, datetime

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
from dev_health_ops.api.dev.health_rule_calibration_inventory import (
    CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS,
)
from dev_health_ops.api.dev.health_rule_registry import (
    HEALTH_RULE_REGISTRY,
    HealthRuleRegistry,
    _evaluate_with_registry,
    _qualify_team_needs_attention_against_registry,
    evaluate_rule,
)

_NOW = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


def _observation(
    *,
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
        subject_kind=RuleApplicability.TEAM,
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


# ---------------------------------------------------------------------------
# Guardrail mechanics against the REAL shipped rules (no test-scoped
# authority needed -- see module docstring).
# ---------------------------------------------------------------------------


def test_negative_review_request_load_without_denominator_is_not_calculable() -> None:
    """PRD 8.1: a rule with ``denominator_required=True`` and a
    high/measured but un-denominated value must report the burden
    conclusion as ``unknown``/``missing_denominator`` -- never a
    fabricated ``at_risk``.
    """

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_request_load_pressure.v1")
    assert rule.denominator_required is True
    observation = _observation(
        current_value=99.0,  # far above threshold=5.0
        denominator_present=False,
    )
    finding = evaluate_rule(rule, [observation], org_id="org-1")
    assert finding.state is DimensionState.UNKNOWN
    assert finding.suppressed_reason == "missing_denominator"


def test_negative_cohort_below_minimum_suppresses_workload_finding() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.after_hours_pressure_sustained.v1")
    observation = _observation(
        current_value=0.9, cohort_size=1
    )  # minimum_cohort_size=5
    finding = evaluate_rule(rule, [observation], org_id="org-1")
    assert finding.state is DimensionState.UNKNOWN
    assert finding.suppressed_reason == "insufficient_cohort"


def test_negative_investment_shift_insufficient_coverage_suppresses() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.investment_allocation_shift.v1")
    assert rule.minimum_coverage == 0.5
    observation = _observation(current_value=0.9, coverage=0.1)
    finding = evaluate_rule(rule, [observation], org_id="org-1")
    assert finding.state is DimensionState.UNKNOWN
    assert finding.suppressed_reason == "insufficient_coverage"


def test_negative_unmeasured_reports_unknown_never_healthy() -> None:
    """Team attribution gap / missing source: an unmeasured observation
    (``data_semantics="not_measured"``) must report ``unknown``, never a
    fabricated ``healthy`` -- covers both "team attribution gaps" and
    "missing vs measured zero".
    """

    for rule_id in (
        "health_rule.after_hours_pressure_sustained.v1",
        "health_rule.review_request_load_pressure.v1",
        "health_rule.pr_interruption_load_pressure.v1",
        "health_rule.investment_allocation_shift.v1",
    ):
        rule = HEALTH_RULE_REGISTRY.rule(rule_id)
        observation = _observation(
            observed_states=(SourceRequirementState.UNAVAILABLE,),
            data_semantics="not_measured",
            current_value=None,
        )
        finding = evaluate_rule(rule, [observation], org_id="org-1")
        assert finding.state is DimensionState.UNKNOWN
        assert (
            finding.suppressed_reason is None
        )  # honest gap, not a guardrail suppression


def test_negative_measured_zero_is_distinct_from_no_data() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.after_hours_pressure_sustained.v1")
    zero_observation = _observation(current_value=0.0, data_semantics="measured_zero")
    finding = evaluate_rule(rule, [zero_observation], org_id="org-1")
    assert (
        finding.state is DimensionState.HEALTHY
    )  # genuinely measured, below threshold

    no_data_observation = _observation(
        current_value=None,
        data_semantics="no_data",
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
    )
    finding2 = evaluate_rule(rule, [no_data_observation], org_id="org-1")
    assert finding2.state is DimensionState.UNKNOWN


def test_every_new_rule_is_provisional_shadow_only() -> None:
    """Mirrors ``test_health_rule_registry.test_no_shipped_rule_is_launch_authorized``
    scoped to the four CHAOS-3304 rules -- none of these has been through
    the CHAOS-3302 calibration review either.
    """

    for rule_id in (
        "health_rule.after_hours_pressure_sustained.v1",
        "health_rule.review_request_load_pressure.v1",
        "health_rule.pr_interruption_load_pressure.v1",
        "health_rule.investment_allocation_shift.v1",
    ):
        rule = HEALTH_RULE_REGISTRY.rule(rule_id)
        assert rule.calibration_state == CalibrationState.PROVISIONAL
        assert rule.calibration_evidence_ref is None
        observation = _observation(
            current_value=1000.0
        )  # would trigger if not suppressed
        finding = evaluate_rule(rule, [observation], org_id="org-1")
        assert finding.shadow_only is True


# ---------------------------------------------------------------------------
# Qualification-reaching-launch-findings: test-scoped ``product_approved``
# copies, exactly like CHAOS-3302's own positive controls.
# ---------------------------------------------------------------------------

_TEST_AFTER_HOURS_PRESSURE = HealthRuleDefinition(
    schema_version="health_rule_definition.v1",
    rule_id="health_rule.test_after_hours_pressure.v1",
    rule_version="health_rule.test_after_hours_pressure.v1",
    owner="test-scoped-fixture",
    applicability=(RuleApplicability.TEAM,),
    dimension=HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
    required_source_classes=(SourceClass.COGNITIVE_LOAD,),
    required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
    direction=RuleDirection.HIGHER_IS_WORSE,
    threshold=0.25,
    comparison_unit="after_hours_commit_ratio",
    minimum_sample=1,
    minimum_coverage=0.0,
    current_window_days=14,
    comparison_window_days=None,
    sustained_periods_required=1,
    denominator_required=False,
    attribution_required=True,
    minimum_cohort_size=5,
    triggered_state=DimensionState.AT_RISK,
    evidence_source_classes=(SourceClass.COGNITIVE_LOAD,),
    fact_kind="observed",
    remediation_template="Review after-hours commit activity with the team.",
    calibration_state=CalibrationState.PRODUCT_APPROVED,
    calibration_evidence_ref="test.calibration.test_after_hours_pressure.v1",
)

_TEST_INVESTMENT_ALLOCATION_SHIFT = HealthRuleDefinition(
    schema_version="health_rule_definition.v1",
    rule_id="health_rule.test_investment_allocation_shift.v1",
    rule_version="health_rule.test_investment_allocation_shift.v1",
    owner="test-scoped-fixture",
    applicability=(RuleApplicability.TEAM,),
    dimension=HealthDimension.INVESTMENT_BALANCE,
    required_source_classes=(SourceClass.INVESTMENT_ALLOCATION,),
    required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
    direction=RuleDirection.HIGHER_IS_WORSE,
    threshold=0.25,
    comparison_unit="new_value_share_shift",
    minimum_sample=1,
    minimum_coverage=0.5,
    current_window_days=14,
    comparison_window_days=14,
    sustained_periods_required=1,
    denominator_required=True,
    attribution_required=True,
    minimum_cohort_size=5,
    triggered_state=DimensionState.AT_RISK,
    evidence_source_classes=(SourceClass.INVESTMENT_ALLOCATION,),
    fact_kind="observed",
    remediation_template="Review the team's investment mix shift with planning.",
    calibration_state=CalibrationState.PRODUCT_APPROVED,
    calibration_evidence_ref="test.calibration.test_investment_allocation_shift.v1",
)

_AUTHORIZED_TEST_WORKLOAD_REGISTRY = HealthRuleRegistry(
    (_TEST_AFTER_HOURS_PRESSURE, _TEST_INVESTMENT_ALLOCATION_SHIFT)
)


def test_positive_two_independent_workload_dimensions_qualify() -> None:
    """The valid overburdened/needs-attention multi-signal case: two
    INDEPENDENT dimensions (cognitive workload pressure + investment
    balance) both at_risk qualifies the team, through the
    ``multi_dimension`` basis.
    """

    observations = {
        "health_rule.test_after_hours_pressure.v1": [
            _observation(current_value=0.4, coverage=1.0)
        ],
        "health_rule.test_investment_allocation_shift.v1": [
            _observation(current_value=0.5, coverage=0.9)
        ],
    }
    result = _evaluate_with_registry(
        _AUTHORIZED_TEST_WORKLOAD_REGISTRY, observations, org_id="org-1"
    )
    launch_dimensions = {finding.dimension for finding in result.launch_findings}
    assert HealthDimension.COGNITIVE_WORKLOAD_PRESSURE in launch_dimensions
    assert HealthDimension.INVESTMENT_BALANCE in launch_dimensions

    qualification = _qualify_team_needs_attention_against_registry(
        result.launch_findings,
        team_id="team-1",
        registry=_AUTHORIZED_TEST_WORKLOAD_REGISTRY,
    )
    assert qualification.qualifies is True
    assert qualification.basis is not None
    assert qualification.basis.value == "multi_dimension"
    assert set(qualification.contributing_dimensions) == {
        HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        HealthDimension.INVESTMENT_BALANCE,
    }


def test_negative_single_at_risk_dimension_does_not_qualify() -> None:
    """One signal only (after-hours pressure alone) must NOT produce a
    struggling/overburdened qualification -- PRD 6.5's "A single bad week
    ... or one metric is insufficient".
    """

    observations = {
        "health_rule.test_after_hours_pressure.v1": [
            _observation(current_value=0.4, coverage=1.0)
        ],
    }
    result = _evaluate_with_registry(
        _AUTHORIZED_TEST_WORKLOAD_REGISTRY, observations, org_id="org-1"
    )
    qualification = _qualify_team_needs_attention_against_registry(
        result.launch_findings,
        team_id="team-1",
        registry=_AUTHORIZED_TEST_WORKLOAD_REGISTRY,
    )
    assert qualification.qualifies is False
    assert qualification.basis is None


# ---------------------------------------------------------------------------
# CHAOS-3331 promotion guard.
# ---------------------------------------------------------------------------


def test_chaos_3331_blocked_rules_stay_provisional() -> None:
    """Team-lead ruling (2026-08-02, disclose-and-defer): the three
    cognitive-load-sourced rules cannot be promoted out of provisional
    until CHAOS-3331 closes (their sole source table's team_id is a legacy
    repo-pattern/identity-map resolver, not canonical primary attribution).

    This test is deliberately a WEAKER, redundant backstop -- the load-
    bearing guard is ``health_rule_registry.py``'s import-time check, which
    raises ``RuntimeError`` the instant the module loads if any blocked
    rule id's ``calibration_state`` is promoted, so a promotion can never
    reach this test (or any other) in the first place. This test exists so
    a reader auditing test files, not import-time guards, still finds the
    invariant documented and exercised.
    """

    assert CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS == {
        "health_rule.after_hours_pressure_sustained.v1",
        "health_rule.review_request_load_pressure.v1",
        "health_rule.pr_interruption_load_pressure.v1",
    }
    for rule_id in CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS:
        rule = HEALTH_RULE_REGISTRY.rule(rule_id)
        assert rule.calibration_state == CalibrationState.PROVISIONAL
        assert rule.calibration_evidence_ref is None

    # The asymmetry is deliberate: investment_allocation_shift.v1 is NOT
    # blocked -- its source table (investment_metrics_daily) is canonically
    # attributed (metrics/job_work_items.py's resolve_team_attribution +
    # attribution_context), unlike the three above.
    assert (
        "health_rule.investment_allocation_shift.v1"
        not in CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS
    )
