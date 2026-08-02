"""Tests for CHAOS-3303's ``health_profile_synthesis`` shared rule-application engine.

Covers the required-implementation test list from the CHAOS-3303 issue that
applies at this layer: complete/mixed/unknown/not-applicable dimensions,
measured-zero vs. absent, deterministic ordering, and no composite score.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dev_health_ops.api.dev.contracts_v2.health_rules import (
    DimensionState,
    RuleApplicability,
)
from dev_health_ops.api.dev.data_health_service import (
    DataHealthResult,
    DataHealthSource,
    DataHealthState,
)
from dev_health_ops.api.dev.health_profile_synthesis import (
    HealthEvaluationSources,
    synthesize_health_profile,
)
from dev_health_ops.api.dev.health_rule_registry import HEALTH_RULE_REGISTRY
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    CompletionState,
    IncidentFact,
    StatusResultState,
    StatusSnapshotResult,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_ORG_ID = "org-1"


def _actual_completion() -> ActualCompletion:
    return ActualCompletion(
        state=CompletionState.READY,
        rule_id="actual-completion",
        rule_version="actual-completion.v4",
        reason_codes=(),
        required_children=(),
        required_child_total=0,
        required_child_complete=0,
        display_truncated=False,
        conflicts=(),
        source_ref_ids=(),
        evidence_ref_ids=(),
    )


def _snapshot(
    *, state: StatusResultState, incidents: tuple[IncidentFact, ...] = ()
) -> StatusSnapshotResult:
    return StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=state,
        scope=None,  # type: ignore[arg-type]
        as_of=_NOW,
        declared=None,
        actual=_actual_completion(),
        children=(),
        blockers=(),
        pull_requests=(),
        ci=(),
        deployments=(),
        incidents=incidents,
        source_refs=(),
        warnings=(),
    )


def _incident(entity_id: str) -> IncidentFact:
    return IncidentFact(
        entity_id=entity_id,
        display_label=entity_id,
        status="open",
        active=True,
        blocking=False,
        observed_at=_NOW,
        source_ref_id="ref-1",
        evidence_ref_ids=(),
    )


def _data_health(*, complete_eligible: bool) -> DataHealthResult:
    return DataHealthResult(
        sources=(
            DataHealthSource(
                source_system="work_items",
                state=DataHealthState.COMPLETE,
                required=True,
                last_successful_at=_NOW,
                watermark=_NOW,
                missing_repository_ids=(),
                missing_entity_ids=(),
                coverage=1.0,
                confidence_impact=None,
                freshness_policy_version="v1",
            ),
        ),
        complete_eligible=complete_eligible,
    )


def test_registry_totality_covers_every_rule() -> None:
    """The module-level totality check must already have passed at import time
    for every rule currently shipped in HEALTH_RULE_REGISTRY -- this test
    just re-derives that same set to catch a future rule silently excluded
    from the check itself (not just from the binding table).
    """

    from dev_health_ops.api.dev import health_profile_synthesis as synthesis

    covered = synthesis._BOUND_RULE_IDS | frozenset(synthesis._UNBOUND_RULE_LIMITATIONS)
    assert frozenset(HEALTH_RULE_REGISTRY) <= covered


def test_project_profile_has_no_composite_score_field() -> None:
    profile = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=HealthEvaluationSources(),
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    assert not hasattr(profile, "score")
    assert not hasattr(profile, "composite_score")


def test_project_profile_covers_every_project_applicable_rule() -> None:
    profile = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=HealthEvaluationSources(),
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    expected_rule_ids = {
        rule.rule_id
        for rule in HEALTH_RULE_REGISTRY.values()
        if RuleApplicability.PROJECT in rule.applicability
    }
    # Every shipped rule is provisional (test_every_shipped_finding_is_
    # shadow_only_today), so every finding lands in shadow_findings today.
    assert profile.launch_findings == ()
    assert profile.suppressed_findings == ()
    assert {finding.rule_id for finding in profile.shadow_findings} == expected_rule_ids


def test_mixed_profile_reports_complete_unknown_and_not_applicable_dimensions() -> None:
    """Complete (data_trust: measured), unknown (change_failure_rate: never
    queried because sources is None), not_applicable (unbound rules like
    completion_stalled reported as UNAVAILABLE -> UNKNOWN) all coexist in one
    profile, independently -- no dimension's state leaks into another's.
    """

    sources = HealthEvaluationSources(
        data_health=_data_health(complete_eligible=True),
        status_snapshot=_snapshot(
            state=StatusResultState.COMPLETE,
            incidents=(_incident("inc-1"),),
        ),
        change_failure_rate_metric=None,
        change_failure_rate_not_applicable=False,
    )
    profile = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=sources,
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    # Every rule is provisional today, so every finding is shadow -- see
    # test_every_shipped_finding_is_shadow_only_today.
    findings_by_rule = {finding.rule_id: finding for finding in profile.shadow_findings}

    # data_trust_broken: measured healthy (complete_eligible=True -> current_value=0.0).
    assert findings_by_rule["health_rule.data_trust_broken.v1"].state == (
        DimensionState.HEALTHY
    )
    # incident_load: 1 incident, sample_count=1 clears minimum_sample=1,
    # value 1.0 < threshold 10.0 -> healthy.
    assert findings_by_rule["health_rule.incident_load.v1"].state == (
        DimensionState.HEALTHY
    )
    # change_failure_rate: metric never supplied -> unknown, never zero/healthy.
    assert findings_by_rule["health_rule.change_failure_rate.v1"].state == (
        DimensionState.UNKNOWN
    )
    # An unbound rule (no canonical source wired) is honestly unknown too.
    assert findings_by_rule["health_rule.completion_stalled.v1"].state == (
        DimensionState.UNKNOWN
    )


def test_change_failure_rate_not_applicable_flag_is_honored() -> None:
    sources = HealthEvaluationSources(
        change_failure_rate_metric=None, change_failure_rate_not_applicable=True
    )
    profile = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=sources,
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    finding = next(
        f
        for f in profile.shadow_findings
        if f.rule_id == "health_rule.change_failure_rate.v1"
    )
    assert finding.state == DimensionState.UNKNOWN


def test_team_profile_without_cohort_suppresses_every_applicable_rule() -> None:
    """A team subject with no resolved attribution (cohort_size=None) must
    never surface a healthy/at-risk finding -- every applicable rule with
    real (though cohort-insufficient) input suppresses as
    insufficient_cohort -> unknown; every rule with no canonical source
    bound yet (health_profile_synthesis's own _UNBOUND_RULE_LIMITATIONS)
    short-circuits at the earlier no-data check instead, before the cohort
    guard even runs, and reports plain unknown with no suppressed_reason.

    Bucket split (health_rule_registry._evaluate_with_registry partitions
    suppressed_reason BEFORE shadow_only, Codex-confirmed finding, round 2,
    2026-08-02): a genuinely suppressed provisional finding now lands in
    ``suppressed_findings``, not ``shadow_findings`` -- the invariant this
    test actually cares about (no healthy/at-risk finding) must therefore
    be checked across BOTH buckets, never just one.
    """

    sources = HealthEvaluationSources(
        data_health=_data_health(complete_eligible=True),
        status_snapshot=_snapshot(state=StatusResultState.COMPLETE, incidents=()),
        change_failure_rate_metric=None,
        change_failure_rate_not_applicable=True,
    )
    profile = synthesize_health_profile(
        applicability=RuleApplicability.TEAM,
        subject_id="team-1",
        cohort_size=None,
        sources=sources,
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    assert profile.launch_findings == ()
    all_findings = profile.shadow_findings + profile.suppressed_findings
    assert all_findings
    for finding in all_findings:
        assert finding.state in (DimensionState.UNKNOWN, DimensionState.NOT_APPLICABLE)
    # At least one rule (a source genuinely present, cohort genuinely
    # insufficient) reaches the specific insufficient_cohort suppression.
    assert profile.suppressed_findings
    assert all(
        finding.suppressed_reason == "insufficient_cohort"
        for finding in profile.suppressed_findings
    )


def test_findings_are_deterministically_ordered() -> None:
    sources = HealthEvaluationSources(
        data_health=_data_health(complete_eligible=True),
        status_snapshot=_snapshot(state=StatusResultState.COMPLETE, incidents=()),
    )
    profile_a = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=sources,
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    profile_b = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=sources,
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    ordering_a = [f.rule_id for f in profile_a.shadow_findings]
    ordering_b = [f.rule_id for f in profile_b.shadow_findings]
    assert ordering_a == ordering_b

    expected = sorted(
        profile_a.shadow_findings, key=lambda f: (f.dimension.value, f.rule_id)
    )
    assert list(profile_a.shadow_findings) == expected


def test_every_shipped_finding_is_shadow_only_today() -> None:
    """Every HEALTH_RULE_REGISTRY rule is provisional (CHAOS-3302's own
    totality test) -- ``launch_findings`` (which requires a non-provisional
    rule per ``health_rule_registry._evaluate_with_registry``) must stay
    empty, and EVERY finding this synthesis produces today -- whether it
    lands in ``shadow_findings`` or ``suppressed_findings`` -- must itself
    report ``shadow_only=True``.

    ``suppressed_findings`` is no longer necessarily empty (Codex-confirmed
    finding, round 2, 2026-08-02): ``_evaluate_with_registry`` now
    partitions ``suppressed_reason`` before ``shadow_only``, so a
    provisional rule that genuinely triggers a guardrail (e.g.
    ``incident_load.v1``'s ``insufficient_sample`` here, since 0 incidents
    is below its ``minimum_sample``) correctly lands in
    ``suppressed_findings`` -- ``shadow_only=True`` still holds for it,
    because it is still a provisional-rule finding; it is just no longer
    conflated with the OTHER provisional findings that were never
    suppressed by any guardrail at all (this synthesis's six
    canonically-unbound rules, which short-circuit at the earlier no-data
    check with no suppressed_reason).
    """

    sources = HealthEvaluationSources(
        data_health=_data_health(complete_eligible=True),
        status_snapshot=_snapshot(state=StatusResultState.COMPLETE, incidents=()),
    )
    profile = synthesize_health_profile(
        applicability=RuleApplicability.PROJECT,
        subject_id="proj-1",
        cohort_size=None,
        sources=sources,
        org_id=_ORG_ID,
        observed_at=_NOW,
    )
    assert profile.launch_findings == ()
    all_findings = profile.shadow_findings + profile.suppressed_findings
    assert all_findings
    assert all(finding.shadow_only for finding in all_findings)
    assert profile.suppressed_findings
    assert all(
        finding.suppressed_reason is not None for finding in profile.suppressed_findings
    )
    assert all(finding.suppressed_reason is None for finding in profile.shadow_findings)
