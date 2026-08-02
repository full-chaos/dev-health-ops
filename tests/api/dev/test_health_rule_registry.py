"""Construction-time validation and clause-level mutation tests for

``HealthRuleRegistry``/``evaluate_rule``/``qualify_team_needs_attention``
(CHAOS-3302). Each guardrail is tested at its exact boundary value so a
``<`` vs ``<=`` (or similar single-clause) mutation flips the result --
the kill site is named in each test's docstring, matching the mutation
discipline already used by ``contracts_v2.validators``
(disable-one-guard-at-a-time).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta, timezone
from types import MappingProxyType

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts_v2.base import SourceClass, SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationRecord,
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
    InvalidCalibrationEvidenceError,
    InvalidRuleIDError,
    _qualify_team_needs_attention_against_registry,
    evaluate_registry,
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


#: A fixed org scope for guardrail/boundary tests that don't exercise
#: tenant-scoping itself (that is ``test_finding_id_scoped_to_org`` and
#: ``test_finding_id_scoped_to_cohort_size`` below) -- ``org_id`` is
#: still required on every call, per the Codex-confirmed cross-tenant
#: collision finding, but its exact value is irrelevant to these tests.
_ORG_ID = "org-test-1"


def _eval(
    rule: HealthRuleDefinition, observations: list[DimensionObservation]
) -> HealthRuleFinding:
    return evaluate_rule(rule, observations, org_id=_ORG_ID)


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


def test_registry_rules_mapping_cannot_be_mutated_into_launch_authority() -> None:
    """Codex finding (high): the registry's rule mapping must not be

    replaceable in place. Before the fix, ``HealthRuleRegistry`` stored
    rules in a plain mutable dict; a caller could replace a shipped
    provisional rule with a ``model_copy``-forged ``product_approved`` rule
    (bypassing every constructor validator) and ``evaluate_registry`` would
    then treat it as launch authority. The rule mapping is now a read-only
    view, so item assignment must raise, and the shipped rule must remain
    exactly what construction validated.
    """

    rule_id = next(iter(HEALTH_RULE_REGISTRY))
    forged = HEALTH_RULE_REGISTRY.rule(rule_id).model_copy(
        update={
            "calibration_state": CalibrationState.PRODUCT_APPROVED,
            "calibration_evidence_ref": "forged-evidence-ref",
        }
    )
    with pytest.raises(TypeError):
        HEALTH_RULE_REGISTRY._rules[rule_id] = forged  # type: ignore[index]
    assert (
        HEALTH_RULE_REGISTRY.rule(rule_id).calibration_state
        == CalibrationState.PROVISIONAL
    )


def test_registry_construction_rejects_model_copy_invalid_rule() -> None:
    """Codex finding (high): construction must reject a rule whose fields

    were set through ``model_copy`` (which bypasses every validator on a
    frozen model) into a combination the constructor itself would never
    allow -- here, ``calibration_state=product_approved`` with no
    ``calibration_evidence_ref``, which ``validate_calibration_evidence``
    forbids.
    """

    forged = _RULE.model_copy(
        update={
            "rule_id": "health_rule.forged_authority.v1",
            "rule_version": "health_rule.forged_authority.v1",
            "calibration_state": CalibrationState.PRODUCT_APPROVED,
        }
    )
    # model_copy bypassed validate_calibration_evidence entirely -- confirm
    # the forged instance really is in the state __init__ forbids.
    assert forged.calibration_state == CalibrationState.PRODUCT_APPROVED
    assert forged.calibration_evidence_ref is None
    with pytest.raises(ValidationError):
        HealthRuleRegistry((forged,))


def test_evaluate_registry_does_not_accept_a_registry_argument() -> None:
    """Codex finding (high, round 3): ``evaluate_registry`` previously took

    an arbitrary caller-supplied ``HealthRuleRegistry`` -- exactly the
    authority-forging vector the round-3 repro exploited (a second
    registry, normally constructed, whose rules assert reviewed authority).
    The production seam must be hard-bound to ``HEALTH_RULE_REGISTRY`` and
    therefore must not accept a registry positional argument at all; a
    caller that passes one gets a ``TypeError`` from Python's own call
    binding, not a chance to substitute the registry.
    """

    with pytest.raises(TypeError):
        evaluate_registry(HEALTH_RULE_REGISTRY, {}, org_id="org-1")  # type: ignore[misc,arg-type]


def test_registry_rules_attribute_cannot_be_rebound() -> None:
    """Codex finding (high, round 3): ``MappingProxyType`` blocks item

    assignment on ``self._rules`` but not *rebinding* the attribute itself
    to a whole new mapping -- ``HEALTH_RULE_REGISTRY._rules =
    MappingProxyType({...})`` previously succeeded and replaced a shipped
    provisional rule with a forged product_approved one. The registry's own
    ``__setattr__`` must reject any attribute assignment once construction
    has finished.
    """

    rule_id = next(iter(HEALTH_RULE_REGISTRY))
    forged = HEALTH_RULE_REGISTRY.rule(rule_id).model_copy(
        update={
            "calibration_state": CalibrationState.PRODUCT_APPROVED,
            "calibration_evidence_ref": "forged-evidence-ref",
        }
    )
    with pytest.raises(AttributeError):
        HEALTH_RULE_REGISTRY._rules = MappingProxyType({rule_id: forged})
    assert (
        HEALTH_RULE_REGISTRY.rule(rule_id).calibration_state
        == CalibrationState.PROVISIONAL
    )


def test_registry_construction_rejects_reviewed_rule_without_matching_calibration_record() -> (
    None
):
    """Codex finding (high, round 3): a structurally-valid non-empty

    ``calibration_evidence_ref`` is not the same claim as "this rule was
    actually reviewed" -- a caller previously could construct a second
    registry asserting ``product_approved`` with any string as evidence,
    and it passed construction (``evaluate_registry`` then emitted a
    launch finding on that authority). When a calibration inventory is
    supplied, a reviewed rule's evidence must resolve against it, not
    merely be present.
    """

    claimed_reviewed = _rebuild(
        _RULE,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        calibration_evidence_ref="not-a-real-calibration-record",
    )
    with pytest.raises(InvalidCalibrationEvidenceError):
        HealthRuleRegistry((claimed_reviewed,), calibration_records={})


def test_registry_construction_accepts_reviewed_rule_with_matching_calibration_record() -> (
    None
):
    """The positive case: a rule whose evidence_ref resolves to a genuinely

    reviewed record (itself non-provisional, with its own evidence_ref) in
    the supplied inventory constructs cleanly.
    """

    claimed_reviewed = _rebuild(
        _RULE,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        calibration_evidence_ref="health_rule_calibration.test_fixture.v1",
    )
    record = CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.test_fixture.v1",
        rule_id=claimed_reviewed.rule_id,
        rule_version=claimed_reviewed.rule_version,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        sample_size=500,
        distribution_summary="test fixture",
        false_positive_review="test fixture",
        false_negative_review="test fixture",
        small_cohort_behavior="test fixture",
        owner="test-owner",
        decided_at=date(2026, 8, 1),
        evidence_ref="doc:real-review",
        notes=None,
    )
    registry = HealthRuleRegistry(
        (claimed_reviewed,), calibration_records={record.calibration_id: record}
    )
    assert (
        registry.rule(claimed_reviewed.rule_id).calibration_state
        == CalibrationState.PRODUCT_APPROVED
    )


def _matching_calibration_fixture() -> tuple[HealthRuleDefinition, CalibrationRecord]:
    """A rule and calibration record that resolve against each other cleanly.

    Each mismatch test below mutates exactly one field of ``record`` away
    from this baseline to prove that field, specifically, is checked.
    """

    rule = _rebuild(
        _RULE,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        calibration_evidence_ref="health_rule_calibration.mismatch_fixture.v1",
    )
    record = CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.mismatch_fixture.v1",
        rule_id=rule.rule_id,
        rule_version=rule.rule_version,
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        sample_size=500,
        distribution_summary="test fixture",
        false_positive_review="test fixture",
        false_negative_review="test fixture",
        small_cohort_behavior="test fixture",
        owner="test-owner",
        decided_at=date(2026, 8, 1),
        evidence_ref="doc:real-review",
        notes=None,
    )
    return rule, record


def test_registry_construction_rejects_calibration_id_mismatch() -> None:
    """Codex finding (high, round 4): the mapping key is not proof the

    record's own ``calibration_id`` actually matches -- resolution checks
    the record's own field, never trusting however the caller happened to
    key the ``calibration_records`` mapping.
    """

    rule, record = _matching_calibration_fixture()
    mismatched_record = record.model_copy(
        update={"calibration_id": "health_rule_calibration.a_different_id.v1"}
    )
    with pytest.raises(InvalidCalibrationEvidenceError):
        HealthRuleRegistry(
            (rule,), calibration_records={record.calibration_id: mismatched_record}
        )


def test_registry_construction_rejects_rule_id_mismatch() -> None:
    """A record naming a different rule cannot authorize this one."""

    rule, record = _matching_calibration_fixture()
    mismatched_record = record.model_copy(
        update={"rule_id": "health_rule.review_latency_sustained.v1"}
    )
    with pytest.raises(InvalidCalibrationEvidenceError):
        HealthRuleRegistry(
            (rule,), calibration_records={record.calibration_id: mismatched_record}
        )


def test_registry_construction_rejects_rule_version_mismatch() -> None:
    """Codex finding (high, round 4): the exact repro shape -- a

    ``product_approved`` v1 rule must not resolve against a record for a
    *different version* of the same rule (e.g. v99). A changed threshold
    bumps the version; a record for the stale version must not authorize
    the new one.
    """

    rule, record = _matching_calibration_fixture()
    mismatched_record = record.model_copy(
        update={"rule_version": "health_rule.completion_stalled.v99"}
    )
    with pytest.raises(InvalidCalibrationEvidenceError):
        HealthRuleRegistry(
            (rule,), calibration_records={record.calibration_id: mismatched_record}
        )


def test_registry_construction_rejects_calibration_state_mismatch() -> None:
    """Codex finding (high, round 4): a record documenting a DIFFERENT

    reviewed state than the rule claims cannot authorize it -- a
    ``data_derived`` review does not make a ``product_approved`` claim
    true, and vice versa.
    """

    rule, record = _matching_calibration_fixture()
    mismatched_record = record.model_copy(
        update={"calibration_state": CalibrationState.DATA_DERIVED}
    )
    with pytest.raises(InvalidCalibrationEvidenceError):
        HealthRuleRegistry(
            (rule,), calibration_records={record.calibration_id: mismatched_record}
        )


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

    finding = _eval(_RULE, _windows(_RULE.sustained_periods_required))
    assert finding.suppressed_reason != "insufficient_cohort"


def test_cohort_one_below_minimum_is_suppressed() -> None:
    finding = _eval(
        _RULE,
        _windows(
            _RULE.sustained_periods_required, cohort_size=_MINIMUM_COHORT_SIZE - 1
        ),
    )
    assert finding.suppressed_reason == "insufficient_cohort"
    assert finding.state == DimensionState.UNKNOWN


def test_sample_at_exact_minimum_is_not_suppressed() -> None:
    finding = _eval(_RULE, _windows(_RULE.sustained_periods_required))
    assert finding.suppressed_reason != "insufficient_sample"


def test_sample_one_below_minimum_is_suppressed() -> None:
    finding = _eval(
        _RULE,
        _windows(
            _RULE.sustained_periods_required, sample_count=_RULE.minimum_sample - 1
        ),
    )
    assert finding.suppressed_reason == "insufficient_sample"


def test_coverage_at_exact_minimum_is_not_suppressed() -> None:
    finding = _eval(_RULE, _windows(_RULE.sustained_periods_required))
    assert finding.suppressed_reason != "insufficient_coverage"


def test_coverage_just_below_minimum_is_suppressed() -> None:
    finding = _eval(
        _RULE,
        _windows(
            _RULE.sustained_periods_required, coverage=_RULE.minimum_coverage - 0.01
        ),
    )
    assert finding.suppressed_reason == "insufficient_coverage"


def test_missing_denominator_suppresses_when_required() -> None:
    finding = _eval(
        _RULE, _windows(_RULE.sustained_periods_required, denominator_present=False)
    )
    assert finding.suppressed_reason == "missing_denominator"


def test_missing_attribution_suppresses_only_when_required() -> None:
    rule = HEALTH_RULE_REGISTRY.rule("health_rule.review_latency_sustained.v1")
    assert rule.attribution_required is False
    finding = _eval(
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
    finding = _eval(
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
    finding = _eval(
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
    finding = _eval(
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
    finding = _eval(
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
    finding = _eval(rule, windows)
    assert finding.suppressed_reason == "not_sustained"
    assert finding.state == DimensionState.UNKNOWN


def test_no_data_short_circuits_before_cohort_guard() -> None:
    """Kill site: guard ordering swapped so cohort check runs before no-data check."""

    finding = _eval(
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

    finding = _eval(
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


def test_finding_id_scoped_to_org() -> None:
    """Codex finding (medium): ``subject_id`` is a provider-scoped key, not

    globally unique. Two different organizations reusing the same subject
    id at the same timestamp must not mint the same ``finding_id``, or
    findings could collide/dedupe/join across a tenant boundary.
    """

    windows = _windows(_RULE.sustained_periods_required)
    finding_org_a = evaluate_rule(_RULE, windows, org_id="org-a")
    finding_org_b = evaluate_rule(_RULE, windows, org_id="org-b")
    assert finding_org_a.finding_id != finding_org_b.finding_id


def test_finding_id_scoped_to_cohort_size() -> None:
    """Codex finding (medium): the exact repro shape -- two evaluations of the

    same subject at the same timestamp but over different cohort sizes
    (1 vs. 99) previously minted the identical finding_id because
    cohort_size was absent from the mint payload.
    """

    small_cohort = evaluate_rule(
        _RULE,
        _windows(_RULE.sustained_periods_required, cohort_size=1),
        org_id=_ORG_ID,
    )
    large_cohort = evaluate_rule(
        _RULE,
        _windows(_RULE.sustained_periods_required, cohort_size=99),
        org_id=_ORG_ID,
    )
    assert small_cohort.finding_id != large_cohort.finding_id


def test_finding_id_deterministic_across_equivalent_utc_offsets() -> None:
    """Codex finding (medium, round 3): ``AwareDatetime.isoformat()``

    preserves the caller's own offset, so two equal instants expressed
    with different offsets (UTC vs. -07:00) previously minted two
    different finding ids -- a genuine identity failure, since the
    contract promises the same rule/subject/window always mints the same
    id. Normalizing to UTC before minting fixes this.
    """

    utc_time = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    offset_time = datetime(2026, 8, 1, 5, 0, tzinfo=timezone(timedelta(hours=-7)))
    assert utc_time == offset_time  # same instant, different representation

    finding_utc = evaluate_rule(
        _RULE,
        _windows(_RULE.sustained_periods_required, observed_at=utc_time),
        org_id=_ORG_ID,
    )
    finding_offset = evaluate_rule(
        _RULE,
        _windows(_RULE.sustained_periods_required, observed_at=offset_time),
        org_id=_ORG_ID,
    )
    assert finding_utc.finding_id == finding_offset.finding_id


# ---------------------------------------------------------------------------
# qualify_team_needs_attention -- clause-level
# ---------------------------------------------------------------------------

#: Codex finding (high, round 4): qualification now derives launch
#: eligibility from the REGISTRY's own record of a finding's rule_id/
#: rule_version, never from what the finding itself claims -- so these
#: clause-level tests (dimension counting, evidence-presence, etc.) need a
#: registry in which ``_RULE.rule_id`` genuinely resolves to a reviewed
#: rule; the real ``HEALTH_RULE_REGISTRY`` cannot serve that purpose since
#: every shipped rule is provisional. This test-scoped registry mirrors
#: ``_AUTHORIZED_TEST_REGISTRY`` in ``test_chaos_3302_health_rule_e2e_
#: controls.py``: honest test-scoped authority, never merged into or read
#: from the production singleton.
_QUALIFY_TEST_RULE = _rebuild(
    _RULE,
    calibration_state=CalibrationState.PRODUCT_APPROVED,
    calibration_evidence_ref="test.calibration.qualify_fixture.v1",
)
_QUALIFY_TEST_REGISTRY = HealthRuleRegistry((_QUALIFY_TEST_RULE,))


def _qualify(
    findings: Sequence[HealthRuleFinding], *, team_id: str
) -> TeamQualificationResult:
    return _qualify_team_needs_attention_against_registry(
        findings, team_id=team_id, registry=_QUALIFY_TEST_REGISTRY
    )


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
    result = _qualify(findings, team_id="t")
    assert result.qualifies is True


def test_two_findings_same_dimension_do_not_qualify() -> None:
    """Kill site: dimension counted per-finding instead of per-unique-dimension."""

    findings = (
        _finding(dimension=HealthDimension.EXECUTION_COMPLETION),
        _finding(dimension=HealthDimension.EXECUTION_COMPLETION, subject_id="team-2"),
    )
    result = _qualify(findings, team_id="t")
    assert result.qualifies is False


def test_shadow_only_finding_excluded_from_qualification() -> None:
    """Kill site (round 3 shape, still a valid negative): these findings

    claim ``shadow_only=True, calibration_state=provisional``, but what
    actually excludes them (round 4) is that their rule_id resolves, in
    the real ``HEALTH_RULE_REGISTRY``, to a genuinely provisional rule --
    not their own claimed fields, which qualification no longer reads.
    Uses the production seam deliberately (not ``_qualify``/
    ``_QUALIFY_TEST_REGISTRY``) since the point is that the real registry
    excludes them.
    """

    findings = (
        _finding(
            dimension=HealthDimension.EXECUTION_COMPLETION,
            shadow_only=True,
            calibration_state=CalibrationState.PROVISIONAL,
        ),
        _finding(
            dimension=HealthDimension.REVIEW_CI_PRESSURE,
            shadow_only=True,
            calibration_state=CalibrationState.PROVISIONAL,
        ),
    )
    result = qualify_team_needs_attention(findings, team_id="t")
    assert result.qualifies is False


def test_shadow_only_must_match_calibration_state() -> None:
    """Codex finding (high): ``shadow_only`` is not an independent caller-set

    flag -- it must equal ``calibration_state == provisional``. Before the
    fix, a finding claiming ``calibration_state=provisional`` while
    declaring ``shadow_only=False`` (or the reverse -- a reviewed
    calibration_state declaring ``shadow_only=True``) constructed without
    error and would be treated as launch authority it never earned.
    """

    with pytest.raises(ValidationError):
        _finding(
            calibration_state=CalibrationState.PROVISIONAL,
            shadow_only=False,
        )
    with pytest.raises(ValidationError):
        _finding(
            calibration_state=CalibrationState.PRODUCT_APPROVED,
            shadow_only=True,
        )


def test_qualify_team_evaluated_at_excludes_shadow_findings() -> None:
    """Codex finding (high): ``evaluated_at`` must be derived strictly from

    launch-only evidence. Before the round-3 fix, ``max()`` was computed
    over the unfiltered findings list, so a fresher shadow/provisional
    finding could make stale launch evidence appear current. Uses
    ``_QUALIFY_TEST_REGISTRY`` (round 4: eligibility is registry-derived)
    for the launch finding; the "shadow" finding's rule_id
    (``review_latency_sustained``) is deliberately absent from that
    registry, so it is excluded by registry resolution regardless of its
    own claimed ``shadow_only``/``calibration_state``.
    """

    launch_only_at = _NOW
    later_shadow_at = _NOW + timedelta(days=30)

    launch_finding = _finding(
        dimension=HealthDimension.EXECUTION_COMPLETION,
        evaluated_at=launch_only_at,
    )
    shadow_finding = _finding(
        rule_id="health_rule.review_latency_sustained.v1",
        rule_version="health_rule.review_latency_sustained.v1",
        dimension=HealthDimension.REVIEW_CI_PRESSURE,
        shadow_only=True,
        calibration_state=CalibrationState.PROVISIONAL,
        evaluated_at=later_shadow_at,
    )
    result = _qualify((launch_finding, shadow_finding), team_id="t")
    assert result.evaluated_at == launch_only_at
    assert result.evaluated_at != later_shadow_at


def test_qualify_team_ignores_forged_shadow_only_via_model_copy() -> None:
    """Codex finding (high, round 3): starting from a genuine provisional

    shadow finding, ``model_copy(update={"shadow_only": False})`` bypasses
    every validator and produces a finding reporting
    ``calibration_state=provisional, shadow_only=False``. Qualification
    must not be fooled by the forged flag. (Round 4 strengthens this
    further: qualification no longer even reads ``calibration_state`` off
    the finding -- it resolves ``_RULE.rule_id`` against the real
    ``HEALTH_RULE_REGISTRY``, itself genuinely provisional, via the
    production seam.)
    """

    genuine_shadow = _finding(
        dimension=HealthDimension.EXECUTION_COMPLETION,
        state=DimensionState.CRITICAL,
        shadow_only=True,
        calibration_state=CalibrationState.PROVISIONAL,
    )
    forged = genuine_shadow.model_copy(update={"shadow_only": False})
    assert forged.shadow_only is False
    assert forged.calibration_state == CalibrationState.PROVISIONAL

    result = qualify_team_needs_attention((forged,), team_id="t")
    assert result.qualifies is False


def test_qualify_team_ignores_forged_shadow_only_via_model_construct() -> None:
    """Codex finding (high, round 3): ``model_construct`` bypasses every

    validator even more directly than ``model_copy`` (it does not require
    starting from a valid instance at all) and can produce
    ``calibration_state=provisional, shadow_only=False`` from scratch.
    (Round 4: same strengthening note as the ``model_copy`` variant above.)
    """

    forged = HealthRuleFinding.model_construct(
        schema_version="health_rule_finding.v1",
        finding_id="00000000-0000-0000-0000-000000000000",
        rule_id=_RULE.rule_id,
        rule_version=_RULE.rule_version,
        dimension=HealthDimension.EXECUTION_COMPLETION,
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-1",
        state=DimensionState.CRITICAL,
        fact_kind="observed",
        shadow_only=False,
        evidence_source_classes=(SourceClass.WORK_ITEM,),
        remediation_template="x",
        calibration_state=CalibrationState.PROVISIONAL,
        evaluated_at=_NOW,
        suppressed_reason=None,
    )
    assert forged.shadow_only is False
    assert forged.calibration_state == CalibrationState.PROVISIONAL

    result = qualify_team_needs_attention((forged,), team_id="t")
    assert result.qualifies is False


def test_qualify_team_ignores_ordinary_finding_claiming_unearned_authority() -> None:
    """Codex finding (high, round 4): qualification previously trusted

    ``finding.calibration_state`` directly. An entirely ORDINARY
    ``HealthRuleFinding`` construction -- no ``model_copy``, no
    ``model_construct``, no guard bypass of any kind -- claiming
    ``calibration_state=product_approved`` for a rule that is
    ``provisional`` in the canonical registry (every shipped rule is,
    today) qualified a team through the critical-rule path. Launch
    eligibility must be re-derived from the registry's own record of the
    rule; the finding's own ``calibration_state`` is read nowhere in that
    decision. Uses the production ``qualify_team_needs_attention`` seam
    deliberately -- this is exactly the seam the finding names.
    """

    forged_authority = _finding(
        dimension=HealthDimension.EXECUTION_COMPLETION,
        state=DimensionState.CRITICAL,
    )
    # An entirely ordinary construction. rule_id/rule_version are the real,
    # canonical -- and genuinely provisional -- health_rule.completion_
    # stalled.v1; calibration_state/shadow_only/evidence_source_classes are
    # _finding()'s ordinary defaults (product_approved, False, non-empty).
    assert forged_authority.rule_id == _RULE.rule_id
    assert forged_authority.calibration_state == CalibrationState.PRODUCT_APPROVED
    assert _RULE.calibration_state == CalibrationState.PROVISIONAL

    result = qualify_team_needs_attention((forged_authority,), team_id="t")
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
    """Kill site: evidence_source_classes non-empty check dropped.

    Uses ``_qualify``/``_QUALIFY_TEST_REGISTRY`` (round 4: eligibility is
    registry-derived) so the finding is registry-eligible and this
    exercises the evidence-presence clause specifically, not registry
    ineligibility.
    """

    findings = (_finding(state=DimensionState.CRITICAL, evidence_source_classes=()),)
    result = _qualify(findings, team_id="t")
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
