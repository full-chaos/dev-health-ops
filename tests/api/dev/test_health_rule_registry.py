"""Construction-time validation and clause-level mutation tests for

``HealthRuleRegistry``/``evaluate_rule``/``qualify_team_needs_attention``
(CHAOS-3302). Each guardrail is tested at its exact boundary value so a
``<`` vs ``<=`` (or similar single-clause) mutation flips the result --
the kill site is named in each test's docstring, matching the mutation
discipline already used by ``contracts_v2.validators``
(disable-one-guard-at-a-time).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationState,
    DimensionObservation,
    DimensionState,
    HealthDimension,
    HealthRuleDefinition,
    HealthRuleFinding,
    RuleApplicability,
    TeamQualificationResult,
)
from dev_health_ops.api.dev.health_rule_registry import (
    HEALTH_RULE_REGISTRY,
    DuplicateRuleError,
    HealthRuleRegistry,
    InvalidRuleIDError,
    evaluate_rule,
    qualify_team_needs_attention,
)

_NOW = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
_RULE = HEALTH_RULE_REGISTRY.rule("health_rule.completion_stalled.v1")
assert _RULE.minimum_cohort_size is not None
assert _RULE.threshold is not None
_MINIMUM_COHORT_SIZE: int = _RULE.minimum_cohort_size
_THRESHOLD: float = _RULE.threshold


def _obs(**overrides: object) -> DimensionObservation:
    base: dict[str, object] = dict(
        schema_version="dimension_observation.v1",
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        cohort_size=_MINIMUM_COHORT_SIZE,
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        data_semantics="measured_zero",
        sample_count=_RULE.minimum_sample,
        coverage=_RULE.minimum_coverage,
        current_value=_THRESHOLD,
        comparison_value=None,
        denominator_present=True,
        attribution_present=True,
        window_index=0,
        observed_at=_NOW,
    )
    base.update(overrides)
    return DimensionObservation(**base)


def _windows(count: int, **overrides: object) -> list[DimensionObservation]:
    return [_obs(window_index=index, **overrides) for index in range(count)]


# ---------------------------------------------------------------------------
# Registry construction
# ---------------------------------------------------------------------------


def test_duplicate_rule_id_rejected_at_construction() -> None:
    with pytest.raises(DuplicateRuleError):
        HealthRuleRegistry((_RULE, _RULE))


def test_malformed_rule_id_rejected_at_construction() -> None:
    bad = _RULE.model_copy(update={"rule_id": "not-a-health-rule-id"})
    with pytest.raises(InvalidRuleIDError):
        HealthRuleRegistry((bad,))


def test_registered_rules_all_match_the_closed_grammar() -> None:
    import re

    from dev_health_ops.api.dev.health_rule_registry import RULE_ID_PATTERN

    for rule_id in HEALTH_RULE_REGISTRY:
        assert re.match(RULE_ID_PATTERN, rule_id)


def test_no_shipped_rule_is_launch_authorized() -> None:
    """Codex finding (high): a ``product_approved``/``data_derived``/

    ``policy_driven`` calibration_state is a claim that a real review
    happened. No rule shipped in ``HEALTH_RULE_REGISTRY`` has had one --
    every rule here is either the real, not-yet-reviewed calibration
    inventory, or an illustrative same-changeset example -- so every
    shipped rule must be ``provisional`` (shadow-only), with no
    exceptions. A rule that has genuinely earned review authority proves
    that in a test-scoped registry instead (see
    ``test_chaos_3302_health_rule_e2e_controls.py``'s positive controls),
    never by shipping fake authority in the production registry.
    """

    non_provisional = [
        rule_id
        for rule_id, rule in HEALTH_RULE_REGISTRY.items()
        if rule.calibration_state != CalibrationState.PROVISIONAL
    ]
    assert non_provisional == []


# ---------------------------------------------------------------------------
# HealthRuleDefinition construction guardrails
# ---------------------------------------------------------------------------


def _rebuild(rule: HealthRuleDefinition, **overrides: object) -> HealthRuleDefinition:
    """Construct a fresh instance through the constructor (not ``model_copy``,

    which bypasses validators on a frozen model) so overridden fields are
    actually re-validated.
    """

    payload = rule.model_dump(mode="json")
    payload.update(overrides)
    return HealthRuleDefinition(**payload)


def test_triggered_state_healthy_rejected() -> None:
    """Kill site: triggered_state whitelist collapsed to 'any DimensionState'."""

    with pytest.raises(ValidationError):
        _rebuild(_RULE, triggered_state=DimensionState.HEALTHY.value)


def test_triggered_state_unknown_rejected() -> None:
    with pytest.raises(ValidationError):
        _rebuild(_RULE, triggered_state=DimensionState.UNKNOWN.value)


def test_team_applicability_without_cohort_size_rejected() -> None:
    """Kill site: cohort requirement gated on the wrong applicability value."""

    with pytest.raises(ValidationError):
        _rebuild(_RULE, minimum_cohort_size=None)


def test_project_only_rule_with_cohort_size_rejected() -> None:
    with pytest.raises(ValidationError):
        _rebuild(
            HEALTH_RULE_REGISTRY.rule("health_rule.data_trust_broken.v1"),
            applicability=[RuleApplicability.PROJECT.value],
        )


# ---------------------------------------------------------------------------
# evaluate_rule -- guardrail boundaries (clause-level)
# ---------------------------------------------------------------------------


def test_cohort_at_exact_minimum_is_not_suppressed() -> None:
    """Kill site: cohort guard `<` mutated to `<=` would suppress at the boundary."""

    finding = evaluate_rule(_RULE, _windows(_RULE.sustained_periods_required))
    assert finding.suppressed_reason != "insufficient_cohort"


def test_cohort_one_below_minimum_is_suppressed() -> None:
    finding = evaluate_rule(
        _RULE,
        _windows(
            _RULE.sustained_periods_required, cohort_size=_MINIMUM_COHORT_SIZE - 1
        ),
    )
    assert finding.suppressed_reason == "insufficient_cohort"
    assert finding.state == DimensionState.UNKNOWN


def test_sample_at_exact_minimum_is_not_suppressed() -> None:
    finding = evaluate_rule(_RULE, _windows(_RULE.sustained_periods_required))
    assert finding.suppressed_reason != "insufficient_sample"


def test_sample_one_below_minimum_is_suppressed() -> None:
    finding = evaluate_rule(
        _RULE,
        _windows(
            _RULE.sustained_periods_required, sample_count=_RULE.minimum_sample - 1
        ),
    )
    assert finding.suppressed_reason == "insufficient_sample"


def test_coverage_at_exact_minimum_is_not_suppressed() -> None:
    finding = evaluate_rule(_RULE, _windows(_RULE.sustained_periods_required))
    assert finding.suppressed_reason != "insufficient_coverage"


def test_coverage_just_below_minimum_is_suppressed() -> None:
    finding = evaluate_rule(
        _RULE,
        _windows(
            _RULE.sustained_periods_required, coverage=_RULE.minimum_coverage - 0.01
        ),
    )
    assert finding.suppressed_reason == "insufficient_coverage"


def test_missing_denominator_suppresses_when_required() -> None:
    finding = evaluate_rule(
        _RULE, _windows(_RULE.sustained_periods_required, denominator_present=False)
    )
    assert finding.suppressed_reason == "missing_denominator"


def test_missing_attribution_suppresses_only_when_required() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_latency_sustained.v1")
    assert rule.attribution_required is False
    finding = evaluate_rule(
        rule,
        [
            _obs(
                subject_kind=RuleApplicability.TEAM,
                cohort_size=rule.minimum_cohort_size,
                sample_count=rule.minimum_sample,
                coverage=rule.minimum_coverage,
                current_value=rule.threshold,
                attribution_present=False,
                window_index=index,
            )
            for index in range(rule.sustained_periods_required)
        ],
    )
    assert finding.suppressed_reason != "missing_attribution"


def test_higher_is_worse_triggers_exactly_at_threshold() -> None:
    """Kill site: >= mutated to > on the higher-is-worse comparison."""

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_latency_sustained.v1")
    finding = evaluate_rule(
        rule,
        [
            _obs(
                cohort_size=rule.minimum_cohort_size,
                sample_count=rule.minimum_sample,
                coverage=rule.minimum_coverage,
                current_value=rule.threshold,
                window_index=index,
            )
            for index in range(rule.sustained_periods_required)
        ],
    )
    assert finding.state == rule.triggered_state


def test_higher_is_worse_does_not_trigger_just_below_threshold() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_latency_sustained.v1")
    assert rule.threshold is not None
    below_threshold = rule.threshold - 0.001
    finding = evaluate_rule(
        rule,
        [
            _obs(
                cohort_size=rule.minimum_cohort_size,
                sample_count=rule.minimum_sample,
                coverage=rule.minimum_coverage,
                current_value=below_threshold,
                window_index=index,
            )
            for index in range(rule.sustained_periods_required)
        ],
    )
    assert finding.state == DimensionState.HEALTHY


def test_deterministic_zero_value_does_not_trigger() -> None:
    """Kill site: `!= 0` mutated to a tautology on the deterministic condition."""

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.data_trust_broken.v1")
    finding = evaluate_rule(
        rule,
        [
            _obs(
                subject_kind=RuleApplicability.TEAM,
                cohort_size=1,
                sample_count=1,
                coverage=0.0,
                current_value=0.0,
                window_index=0,
            )
        ],
    )
    assert finding.state == DimensionState.HEALTHY


def test_deterministic_nonzero_value_triggers_critical() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.data_trust_broken.v1")
    finding = evaluate_rule(
        rule,
        [
            _obs(
                subject_kind=RuleApplicability.TEAM,
                cohort_size=1,
                sample_count=1,
                coverage=0.0,
                current_value=1.0,
                window_index=0,
            )
        ],
    )
    assert finding.state == DimensionState.CRITICAL


def test_sustained_window_requires_every_period_to_trigger() -> None:
    """Kill site: `all(...)` mutated to `any(...)` on the sustained-window check."""

    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_latency_sustained.v1")
    assert rule.threshold is not None
    threshold = rule.threshold
    windows = [
        _obs(
            cohort_size=rule.minimum_cohort_size,
            sample_count=rule.minimum_sample,
            coverage=rule.minimum_coverage,
            current_value=threshold if index == 0 else threshold - 10,
            window_index=index,
        )
        for index in range(rule.sustained_periods_required)
    ]
    finding = evaluate_rule(rule, windows)
    assert finding.suppressed_reason == "not_sustained"
    assert finding.state == DimensionState.UNKNOWN


def test_no_data_short_circuits_before_cohort_guard() -> None:
    """Kill site: guard ordering swapped so cohort check runs before no-data check."""

    finding = evaluate_rule(
        _RULE,
        _windows(
            _RULE.sustained_periods_required,
            data_semantics="no_data",
            observed_states=(SourceRequirementState.UNCONFIGURED,),
            current_value=None,
            cohort_size=0,
        ),
    )
    assert finding.state == DimensionState.UNKNOWN
    assert finding.suppressed_reason is None


def test_not_measured_short_circuits_before_cohort_guard() -> None:
    """Codex finding (medium): ``not_measured`` is a second valid spelling of

    "genuinely never measured" (``DimensionObservation`` accepts either
    ``no_data`` or ``not_measured`` for an unmeasured ``observed_states``
    set -- see its ``validate_zero_semantics``). It must short-circuit
    exactly like ``no_data``, before the cohort/sample/coverage guards,
    so a source that was never measured is never misreported as
    ``insufficient_cohort``.
    """

    finding = evaluate_rule(
        _RULE,
        _windows(
            _RULE.sustained_periods_required,
            data_semantics="not_measured",
            observed_states=(SourceRequirementState.UNCONFIGURED,),
            current_value=None,
            cohort_size=0,
        ),
    )
    assert finding.state == DimensionState.UNKNOWN
    assert finding.suppressed_reason is None


# ---------------------------------------------------------------------------
# qualify_team_needs_attention -- clause-level
# ---------------------------------------------------------------------------


def _finding(**overrides: object) -> HealthRuleFinding:
    base = dict(
        schema_version="health_rule_finding.v1",
        finding_id="00000000-0000-0000-0000-000000000000",
        rule_id=_RULE.rule_id,
        rule_version=_RULE.rule_version,
        dimension=HealthDimension.EXECUTION_COMPLETION,
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        state=DimensionState.AT_RISK,
        fact_kind="observed",
        shadow_only=False,
        evidence_source_classes=("work_item",),
        remediation_template="x",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        evaluated_at=_NOW,
        suppressed_reason=None,
    )
    base.update(overrides)
    return HealthRuleFinding(**base)


def test_two_distinct_at_risk_dimensions_qualify() -> None:
    findings = (
        _finding(dimension=HealthDimension.EXECUTION_COMPLETION),
        _finding(dimension=HealthDimension.REVIEW_CI_PRESSURE),
    )
    result = qualify_team_needs_attention(findings, team_id="t")
    assert result.qualifies is True


def test_two_findings_same_dimension_do_not_qualify() -> None:
    """Kill site: dimension counted per-finding instead of per-unique-dimension."""

    findings = (
        _finding(dimension=HealthDimension.EXECUTION_COMPLETION),
        _finding(dimension=HealthDimension.EXECUTION_COMPLETION, subject_id="team-2"),
    )
    result = qualify_team_needs_attention(findings, team_id="t")
    assert result.qualifies is False


def test_shadow_only_finding_excluded_from_qualification() -> None:
    """Kill site: shadow_only filter dropped before qualification."""

    findings = (
        _finding(dimension=HealthDimension.EXECUTION_COMPLETION, shadow_only=True),
        _finding(dimension=HealthDimension.REVIEW_CI_PRESSURE, shadow_only=True),
    )
    result = qualify_team_needs_attention(findings, team_id="t")
    assert result.qualifies is False


def test_suppressed_finding_cannot_be_constructed_as_critical() -> None:
    """A suppressed finding is structurally confined to unknown/not_applicable

    (``HealthRuleFinding.validate_suppression_state``), so a "suppressed
    critical" finding -- the scenario ``qualify_team_needs_attention``'s
    ``suppressed_reason is None`` guard defends against -- cannot even be
    constructed. This is the contract-level half of that defense: kill
    site for a mutation that widened ``validate_suppression_state`` to
    allow ``critical``/``at_risk`` alongside a ``suppressed_reason``.
    """

    with pytest.raises(ValidationError):
        _finding(
            state=DimensionState.CRITICAL, suppressed_reason="insufficient_coverage"
        )


def test_critical_finding_without_evidence_does_not_qualify() -> None:
    """Kill site: evidence_source_classes non-empty check dropped."""

    findings = (_finding(state=DimensionState.CRITICAL, evidence_source_classes=()),)
    result = qualify_team_needs_attention(findings, team_id="t")
    assert result.qualifies is False


def test_qualification_result_rejects_qualifies_true_without_basis() -> None:
    with pytest.raises(ValidationError):
        TeamQualificationResult(
            schema_version="team_qualification_result.v1",
            team_id="t",
            qualifies=True,
            basis=None,
            evaluated_at=_NOW,
        )
