"""Counterexample tests for the 4 CONFIRMED findings from codex's full-branch
review of a37caf322..b0e21a093 (2026-08-02), each written to reproduce the
exact repro shape codex used and prove it is closed. Every test in this file
would have FAILED (or, for finding 1, asserted the wrong value) against the
pre-fix code at b0e21a093:

1. HIGH -- investment exemption failed open ->
   ``test_investment_adapter_fails_closed_not_open`` (see also
   ``test_chaos_3304_workload_observation_adapters.py::
   test_investment_shift_is_also_subject_to_chaos_3331``, the primary proof;
   this file adds the calibration-inventory/registry-side counterpart).
2. HIGH -- promotion guard bypassable (rebound module global + differently
   named rule) -> ``test_codex_exact_repro_rebinding_public_registry_name_
   does_not_redirect_production_evaluation`` and
   ``test_codex_exact_repro_second_registry_with_differently_named_rule_
   is_rejected``.
3. HIGH -- 3331 disclosure preempted by guard order ->
   ``test_attribution_is_the_controlling_guard_before_cohort_and_
   denominator``.
4. MEDIUM -- suppressed leaks into shadow ->
   ``test_suppressed_reason_partitions_before_shadow_only``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import dev_health_ops.api.dev.health_rule_registry as health_rule_registry_module
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
from dev_health_ops.api.dev.dimension_observation_adapters import (
    investment_allocation_shift_observation,
)
from dev_health_ops.api.dev.health_rule_registry import (
    HEALTH_RULE_REGISTRY,
    AttributionProvenanceBlockedError,
    HealthRuleRegistry,
    evaluate_registry,
    evaluate_rule,
)
from dev_health_ops.api.dev.native_team_workload import TeamInvestmentMixResult

_NOW = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


def _observation(
    *,
    cohort_size: int | None = 12,
    sample_count: int | None = 40,
    coverage: float = 0.9,
    current_value: float | None = 1.0,
    denominator_present: bool = True,
    attribution_present: bool = True,
) -> DimensionObservation:
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        cohort_size=cohort_size,
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        data_semantics="measured_zero",
        sample_count=sample_count,
        coverage=coverage,
        current_value=current_value,
        comparison_value=None,
        denominator_present=denominator_present,
        attribution_present=attribution_present,
        window_index=0,
        observed_at=_NOW,
    )


# ---------------------------------------------------------------------------
# 1. Investment exemption failed open.
# ---------------------------------------------------------------------------


def test_investment_adapter_fails_closed_not_open() -> None:
    """Codex's repro: job_work_items.py's attribution_context load can fail
    and continue with attribution_context=None, and resolve_team_attribution
    still writes a row via its legacy fallback chain. There is no field
    anywhere distinguishing that row from a canonically-attributed one, so
    the adapter must report attribution_present=False for EVERY measured
    result, not just the ones that happen to look "risky".
    """

    current = TeamInvestmentMixResult(
        new_value_units=40.0,
        ktlo_units=30.0,
        security_units=10.0,
        infra_units=10.0,
        unclassified_units=10.0,
        total_units=100.0,
        measured=True,
    )
    comparison = TeamInvestmentMixResult(
        new_value_units=20.0,
        ktlo_units=30.0,
        security_units=10.0,
        infra_units=10.0,
        unclassified_units=10.0,
        total_units=80.0,
        measured=True,
    )
    obs = investment_allocation_shift_observation(
        current,
        comparison,
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        cohort_size=25,
        window_index=0,
        observed_at=_NOW,
    )
    assert obs.attribution_present is False

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.investment_allocation_shift.v1")
    finding = evaluate_rule(rule, [obs], org_id="org-1")
    assert finding.state is DimensionState.UNKNOWN
    assert finding.suppressed_reason == "missing_attribution"


# ---------------------------------------------------------------------------
# 2. Promotion guard bypassable.
# ---------------------------------------------------------------------------


def test_codex_exact_repro_second_registry_with_differently_named_rule_is_rejected() -> (
    None
):
    """The exact round-2 repro: construct a SECOND registry (never touching
    the production singleton) with a rule that reads a blocked source class
    under a DIFFERENT, never-shipped rule id, claiming product_approved.
    The old exact-ID, import-once guard never saw this rule id and would
    have let it through; the new construction-time, family-based guard
    rejects it regardless of rule id.
    """

    forged = HealthRuleDefinition(
        schema_version="health_rule_definition.v1",
        rule_id="health_rule.definitely_not_a_shipped_id.v1",
        rule_version="health_rule.definitely_not_a_shipped_id.v1",
        owner="codex-adversarial-repro",
        applicability=(RuleApplicability.TEAM,),
        dimension=HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        required_source_classes=(SourceClass.COGNITIVE_LOAD,),
        required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.1,
        comparison_unit="forged",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=14,
        comparison_window_days=None,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=5,
        triggered_state=DimensionState.CRITICAL,
        evidence_source_classes=(SourceClass.COGNITIVE_LOAD,),
        fact_kind="observed",
        remediation_template="n/a",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        calibration_evidence_ref="codex.repro.definitely_not_a_shipped_id.v1",
    )
    with pytest.raises(AttributionProvenanceBlockedError):
        HealthRuleRegistry((forged,))


def test_codex_exact_repro_rebinding_public_registry_name_does_not_redirect_production_evaluation() -> (
    None
):
    """The exact round-2 repro's second half: even if a forged registry
    somehow existed and the public HEALTH_RULE_REGISTRY name were rebound
    to it, evaluate_registry() must still evaluate against the ORIGINAL
    validated registry -- never the rebound one. This test restores the
    original binding in a try/finally so it cannot leak into other tests.
    """

    # A forged registry using an ordinary, non-blocked rule id/dimension --
    # this test is about the REBIND vector specifically, not the
    # construction-time guard from the test above.
    forged_rule = HealthRuleDefinition(
        schema_version="health_rule_definition.v1",
        rule_id="health_rule.completion_stalled.v1",
        rule_version="health_rule.completion_stalled.v1",
        owner="codex-adversarial-repro",
        applicability=(RuleApplicability.TEAM,),
        dimension=HealthDimension.EXECUTION_COMPLETION,
        required_source_classes=(SourceClass.STATUS_CHANGE,),
        required_observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        direction=RuleDirection.DETERMINISTIC,
        threshold=None,
        comparison_unit=None,
        minimum_sample=0,
        minimum_coverage=0.0,
        current_window_days=1,
        comparison_window_days=None,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.CRITICAL,
        evidence_source_classes=(SourceClass.STATUS_CHANGE,),
        fact_kind="observed",
        remediation_template="n/a",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        calibration_evidence_ref="codex.repro.forged_completion_stalled.v1",
    )
    forged_registry = HealthRuleRegistry((forged_rule,))

    original = health_rule_registry_module.HEALTH_RULE_REGISTRY
    try:
        health_rule_registry_module.HEALTH_RULE_REGISTRY = forged_registry
        assert health_rule_registry_module.HEALTH_RULE_REGISTRY is forged_registry

        # A deterministic condition observation that would trigger CRITICAL
        # under the forged rule's threshold=None/DETERMINISTIC direction.
        observation = _observation(current_value=1.0)
        result = evaluate_registry(
            {"health_rule.completion_stalled.v1": [observation]}, org_id="org-1"
        )
        # If the rebind had redirected evaluate_registry, this would have
        # reached launch_findings with state=CRITICAL (forged rule is
        # product_approved). It must not: evaluate_registry stays bound to
        # the ORIGINAL, real (provisional) completion_stalled.v1 rule.
        assert result.launch_findings == ()
        finding = (result.shadow_findings + result.suppressed_findings)[0]
        assert finding.calibration_state == CalibrationState.PROVISIONAL
    finally:
        health_rule_registry_module.HEALTH_RULE_REGISTRY = original


# ---------------------------------------------------------------------------
# 3. 3331 disclosure preempted by guard order.
# ---------------------------------------------------------------------------


def test_attribution_is_the_controlling_guard_before_cohort_and_denominator() -> None:
    """When BOTH attribution AND another guard (cohort, denominator) would
    independently suppress the same observation, attribution's reason must
    win -- it is checked first.
    """

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_request_load_pressure.v1")
    assert rule.attribution_required is True
    assert rule.denominator_required is True

    # Both cohort AND denominator AND attribution would independently
    # suppress this observation -- attribution must be the reported reason.
    observation = _observation(
        current_value=99.0,
        cohort_size=0,  # < minimum_cohort_size=5
        denominator_present=False,  # denominator_required=True
        attribution_present=False,  # attribution_required=True
    )
    finding = evaluate_rule(rule, [observation], org_id="org-1")
    assert finding.suppressed_reason == "missing_attribution"

    # Isolate: with attribution present, the SAME observation (still
    # cohort- and denominator-insufficient) now reports the next guard in
    # order (cohort, since it is checked before denominator).
    observation_attributed = _observation(
        current_value=99.0,
        cohort_size=0,
        denominator_present=False,
        attribution_present=True,
    )
    finding2 = evaluate_rule(rule, [observation_attributed], org_id="org-1")
    assert finding2.suppressed_reason == "insufficient_cohort"


# ---------------------------------------------------------------------------
# 4. Suppressed leaks into shadow.
# ---------------------------------------------------------------------------


def test_suppressed_reason_partitions_before_shadow_only() -> None:
    """A provisional rule's guardrail-suppressed finding must land in
    suppressed_findings, not shadow_findings -- shadow_findings holds only
    UNSUPPRESSED provisional findings (a genuinely measured, unguarded
    result awaiting review).
    """

    rule_id = "health_rule.after_hours_pressure_sustained.v1"
    rule = HEALTH_RULE_REGISTRY.rule(rule_id)
    assert rule.calibration_state == CalibrationState.PROVISIONAL

    suppressed_observation = _observation(cohort_size=1)  # < minimum_cohort_size=5
    result = evaluate_registry({rule_id: [suppressed_observation]}, org_id="org-1")
    assert result.launch_findings == ()
    assert result.shadow_findings == ()
    assert len(result.suppressed_findings) == 1
    assert result.suppressed_findings[0].suppressed_reason == "insufficient_cohort"
    assert result.suppressed_findings[0].shadow_only is True  # still provisional

    unsuppressed_observation = _observation(current_value=0.0)  # below threshold
    result2 = evaluate_registry({rule_id: [unsuppressed_observation]}, org_id="org-1")
    assert result2.launch_findings == ()
    assert result2.suppressed_findings == ()
    assert len(result2.shadow_findings) == 1
    assert result2.shadow_findings[0].suppressed_reason is None
