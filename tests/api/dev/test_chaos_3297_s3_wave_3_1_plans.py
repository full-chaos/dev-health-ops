"""Tests for CHAOS-3297 stack #3's wave-3.1 plan wiring: the four new
DevInvestigationPlan documents (health.project.v1/health.team.v1/
balance.team_workload.v1/deficiency.operational.v1) and their step
registrations against CHAOS-3303/3304/3305's own services.

Every step here is source-anchored against fake service DOUBLES that
implement this module's own narrow Protocols (never the full concrete
service classes -- those have their own dedicated test suites), proving
the wiring layer itself: a service result's launch_findings/findings reach
DevSourceContent in the correct, capped, truncation-disclosed, canonically
ordered shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, TypedDict

import pytest

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.contracts_v2.base import (
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DEFICIENCY_CATEGORIES,
    DeficiencyCategory,
    DeficiencyEvidenceClassification,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
    OperationalDeficiencyInventory,
    finding_sort_key,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    CalibrationState,
    DimensionState,
    HealthDimension,
    HealthRuleFinding,
    RuleApplicability,
)
from dev_health_ops.api.dev.health_profile_synthesis import HealthProfileResult
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.investigation_plans.registry_validation import (
    validate_registry,
)
from dev_health_ops.api.dev.investigation_plans.steps import StepContext, StepRegistry
from dev_health_ops.api.dev.investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
    WAVE_3_1_QUESTION_INTENT_IDS,
    _deficiency_inventory_content,
    build_registry_with_wave_3_1,
    register_wave_3_1_steps,
)
from tests._chaos_3295_plan_executor import FakePlanExecutorRuntime

_ORG_ID = "org_fullchaos"
_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def _time_range() -> DevTimeRange:
    return DevTimeRange(
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 31, tzinfo=UTC),
        timezone="UTC",
    )


def _project_scope(entity_id: str = "project-1") -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": entity_id,
                "display_label": "Project",
                "repository_id": None,
            }
        ],
        time_range=_time_range(),
    )


def _team_scope(team_id: str = "team-1") -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.TEAM,
        entity_refs=[
            {
                "entity_type": "team",
                "entity_id": team_id,
                "display_label": "Team",
                "repository_id": None,
            }
        ],
        team_ids=[team_id],
        time_range=_time_range(),
    )


def _step_context(scope: DevScope) -> StepContext:
    return StepContext(
        org_id=_ORG_ID,
        permission_fingerprint="fingerprint",
        scope=scope,
        run_id="run-1",
        now=_NOW,
    )


def _handle(suffix: str) -> str:
    return f"00000000-0000-0000-0000-{suffix:0>12}"


def _health_finding(
    *, finding_id: str, state: DimensionState = DimensionState.AT_RISK
) -> HealthRuleFinding:
    return HealthRuleFinding(
        schema_version="health_rule_finding.v1",
        finding_id=_handle(finding_id),
        rule_id="health_rule.test_rule.v1",
        rule_version="health_rule.test_rule.v1.1",
        dimension=HealthDimension.EXECUTION_COMPLETION,
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        state=state,
        fact_kind="observed",
        shadow_only=False,
        evidence_source_classes=(SourceClass.STATUS_CHANGE,),
        remediation_template="Investigate.",
        calibration_state=CalibrationState.PRODUCT_APPROVED,
        evaluated_at=_NOW,
        suppressed_reason=None,
    )


def _health_profile_result(
    launch_findings: tuple[HealthRuleFinding, ...] = (),
) -> HealthProfileResult:
    return HealthProfileResult(
        subject_kind=RuleApplicability.PROJECT,
        subject_id="proj-1",
        observations=(),
        launch_findings=launch_findings,
        shadow_findings=(),
        suppressed_findings=(),
        observations_by_rule={},
    )


def _deficiency_finding(
    *,
    finding_id: str,
    severity: DeficiencySeverity = DeficiencySeverity.AT_RISK,
    subject_kind: RuleApplicability = RuleApplicability.PROJECT,
    subject_id: str = "proj-1",
) -> DeficiencyFinding:
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_handle(finding_id),
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id="deficiency_rule.test_rule.v1",
        rule_version="deficiency_rule.test_rule.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        severity=severity,
        fact_kind="observed",
        observed_state=SourceRequirementState.UNCONFIGURED,
        data_semantics="not_measured",
        sample_count=None,
        coverage=0.0,
        current_window_days=1,
        comparison_window_days=None,
        evidence_ref_ids=(),
        evidence_classification=DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE,
        blast_radius="Required source is unconfigured.",
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template="Investigate.",
            verification_condition="Resolves once re-evaluated healthy.",
        ),
        limitations=(),
        evaluated_at=_NOW,
    )


def _deficiency_category_statuses(
    findings: tuple[DeficiencyFinding, ...],
    *,
    unevaluated: frozenset[DeficiencyCategory] = frozenset(),
) -> tuple[Any, ...]:
    from dev_health_ops.api.dev.contracts_v2.deficiency import (
        DEFICIENCY_CATEGORIES,
        DeficiencyCategoryStatus,
    )

    counts: dict[DeficiencyCategory, int] = {}
    for finding in findings:
        counts[finding.category] = counts.get(finding.category, 0) + 1
    return tuple(
        DeficiencyCategoryStatus(
            schema_version="deficiency_category_status.v1",
            category=category,
            evaluated=category not in unevaluated,
            finding_count=0 if category in unevaluated else counts.get(category, 0),
            applicability_states_observed=(),
            limitation=(
                f"category_{category.value}_not_yet_calibrated"
                if category in unevaluated
                else None
            ),
        )
        for category in DEFICIENCY_CATEGORIES
    )


def _deficiency_inventory(
    *,
    subject_kind: RuleApplicability = RuleApplicability.PROJECT,
    subject_id: str = "proj-1",
    findings: tuple[DeficiencyFinding, ...] = (),
    unevaluated: frozenset[DeficiencyCategory] = frozenset(),
) -> OperationalDeficiencyInventory:
    ordered = tuple(sorted(findings, key=finding_sort_key))
    return OperationalDeficiencyInventory(
        schema_version="deficiency_operational_inventory.v1",
        inventory_id=_handle("f"),
        subject_kind=subject_kind,
        subject_id=subject_id,
        findings=ordered,
        category_statuses=_deficiency_category_statuses(
            ordered, unevaluated=unevaluated
        ),
        evaluated_at=_NOW,
    )


class _FakeProjectHealth:
    def __init__(self, result: HealthProfileResult) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

    async def evaluate_project(self, *, org_id, permission_fingerprint, scope, now):
        self.calls.append(
            {
                "org_id": org_id,
                "permission_fingerprint": permission_fingerprint,
                "scope": scope,
                "now": now,
            }
        )
        return self._result


class _FakeTeamHealth:
    def __init__(self, result: HealthProfileResult) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

    async def evaluate_team(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        self.calls.append(
            {
                "org_id": org_id,
                "permission_fingerprint": permission_fingerprint,
                "scope": scope,
                "team_id": team_id,
                "now": now,
            }
        )
        return self._result


class _FakeTeamWorkload:
    def __init__(self, result: HealthProfileResult) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

    async def evaluate_workload(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        self.calls.append(
            {
                "org_id": org_id,
                "permission_fingerprint": permission_fingerprint,
                "scope": scope,
                "team_id": team_id,
                "now": now,
            }
        )
        return self._result


class _FakeOperationalDeficiency:
    def __init__(
        self,
        *,
        project_result: OperationalDeficiencyInventory | None = None,
        team_result: OperationalDeficiencyInventory | None = None,
    ) -> None:
        self._project_result = project_result
        self._team_result = team_result
        self.project_calls: list[dict[str, Any]] = []
        self.team_calls: list[dict[str, Any]] = []

    async def evaluate_project(self, *, org_id, permission_fingerprint, scope, now):
        self.project_calls.append({"scope": scope})
        assert self._project_result is not None
        return self._project_result

    async def evaluate_team(
        self, *, org_id, permission_fingerprint, scope, team_id, now
    ):
        self.team_calls.append({"scope": scope, "team_id": team_id})
        assert self._team_result is not None
        return self._team_result


def _registry(
    *,
    project_health: _FakeProjectHealth,
    team_health: _FakeTeamHealth,
    team_workload: _FakeTeamWorkload,
    operational_deficiency: _FakeOperationalDeficiency,
) -> StepRegistry:
    registry = StepRegistry()
    register_wave_3_1_steps(
        registry,
        project_health=project_health,
        team_health=team_health,
        team_workload=team_workload,
        operational_deficiency=operational_deficiency,
    )
    return registry


class _Doubles(TypedDict):
    project_health: _FakeProjectHealth
    team_health: _FakeTeamHealth
    team_workload: _FakeTeamWorkload
    operational_deficiency: _FakeOperationalDeficiency


def _empty_doubles() -> _Doubles:
    return {
        "project_health": _FakeProjectHealth(_health_profile_result()),
        "team_health": _FakeTeamHealth(_health_profile_result()),
        "team_workload": _FakeTeamWorkload(_health_profile_result()),
        "operational_deficiency": _FakeOperationalDeficiency(
            project_result=_deficiency_inventory(), team_result=_deficiency_inventory()
        ),
    }


def test_wave_3_1_plans_cover_exactly_the_four_intents():
    assert WAVE_3_1_QUESTION_INTENT_IDS == frozenset(WAVE_3_1_PLANS_BY_INTENT)
    assert WAVE_3_1_QUESTION_INTENT_IDS == {
        QuestionIntentID.PROJECT_HEALTH,
        QuestionIntentID.TEAM_HEALTH,
        QuestionIntentID.TEAM_WORKLOAD_BALANCE,
        QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
    }


def test_wave_3_1_registry_validates_against_the_real_plan_documents():
    """RED-first: if a plan declares a step this module never registers
    (or vice versa), this must fail construction, not surface at run time.
    """

    registry = _registry(**_empty_doubles())
    validate_registry(
        plans_by_intent=WAVE_3_1_PLANS_BY_INTENT,
        steps=registry,
        core_intents=WAVE_3_1_QUESTION_INTENT_IDS,
    )


@pytest.mark.asyncio
async def test_health_project_step_wires_launch_findings_into_content():
    findings = (
        _health_finding(finding_id="a", state=DimensionState.CRITICAL),
        _health_finding(finding_id="b", state=DimensionState.WATCH),
    )
    project_health = _FakeProjectHealth(_health_profile_result(findings))
    registry = _registry(
        project_health=project_health,
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=_FakeOperationalDeficiency(),
    )
    step = registry.get("health.project.v1", "health_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.observed_state == SourceRequirementState.AVAILABLE_CURRENT
    assert outcome.usable_fact_count == 2
    assert outcome.content is not None
    assert [f.finding_id for f in outcome.content.health_findings] == [
        f.finding_id for f in findings
    ]
    assert outcome.content.health_findings_truncated is False
    assert len(project_health.calls) == 1
    assert project_health.calls[0]["scope"].entity_refs[0].entity_id == "project-1"


@pytest.mark.asyncio
async def test_health_project_step_reports_zero_findings_honestly():
    registry = _registry(**_empty_doubles())
    step = registry.get("health.project.v1", "health_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.observed_state == SourceRequirementState.AVAILABLE_CURRENT
    assert outcome.usable_fact_count == 0
    assert outcome.content is not None
    assert outcome.content.health_findings == ()


@pytest.mark.asyncio
async def test_health_team_step_passes_the_scopes_own_team_id():
    team_health = _FakeTeamHealth(_health_profile_result())
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=team_health,
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=_FakeOperationalDeficiency(),
    )
    step = registry.get("health.team.v1", "health_evaluation")
    await step.run(_step_context(_team_scope("team-9")))

    assert team_health.calls[0]["team_id"] == "team-9"


@pytest.mark.asyncio
async def test_workload_evaluation_step_passes_the_scopes_own_team_id():
    team_workload = _FakeTeamWorkload(_health_profile_result())
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=team_workload,
        operational_deficiency=_FakeOperationalDeficiency(),
    )
    step = registry.get("balance.team_workload.v1", "workload_evaluation")
    await step.run(_step_context(_team_scope("team-9")))

    assert team_workload.calls[0]["team_id"] == "team-9"


@pytest.mark.asyncio
async def test_deficiency_evaluation_step_routes_project_scope_to_evaluate_project():
    inventory = _deficiency_inventory(findings=(_deficiency_finding(finding_id="a"),))
    operational_deficiency = _FakeOperationalDeficiency(project_result=inventory)
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=operational_deficiency,
    )
    step = registry.get("deficiency.operational.v1", "deficiency_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert len(operational_deficiency.project_calls) == 1
    assert len(operational_deficiency.team_calls) == 0
    assert outcome.content is not None
    assert len(outcome.content.deficiency_findings) == 1


@pytest.mark.asyncio
async def test_deficiency_evaluation_step_routes_team_scope_to_evaluate_team():
    inventory = _deficiency_inventory(
        subject_kind=RuleApplicability.TEAM,
        subject_id="team-9",
        findings=(
            _deficiency_finding(
                finding_id="a",
                subject_kind=RuleApplicability.TEAM,
                subject_id="team-9",
            ),
        ),
    )
    operational_deficiency = _FakeOperationalDeficiency(team_result=inventory)
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=operational_deficiency,
    )
    step = registry.get("deficiency.operational.v1", "deficiency_evaluation")
    outcome = await step.run(_step_context(_team_scope("team-9")))

    assert len(operational_deficiency.team_calls) == 1
    assert len(operational_deficiency.project_calls) == 0
    assert operational_deficiency.team_calls[0]["team_id"] == "team-9"
    assert outcome.content is not None
    assert len(outcome.content.deficiency_findings) == 1


@pytest.mark.asyncio
async def test_health_findings_beyond_the_cap_are_truncated_and_disclosed():
    """Kill site: a service returning 51 findings must never silently ship
    only 50 with no signal -- team-lead amendment (c).
    """

    findings = tuple(
        _health_finding(finding_id=f"{i:x}", state=DimensionState.WATCH)
        for i in range(51)
    )
    project_health = _FakeProjectHealth(_health_profile_result(findings))
    registry = _registry(
        project_health=project_health,
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=_FakeOperationalDeficiency(),
    )
    step = registry.get("health.project.v1", "health_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.content is not None
    assert len(outcome.content.health_findings) == 50
    assert outcome.content.health_findings_truncated is True


@pytest.mark.asyncio
async def test_health_findings_within_the_cap_are_not_marked_truncated():
    findings = tuple(
        _health_finding(finding_id=f"{i:x}", state=DimensionState.WATCH)
        for i in range(50)
    )
    project_health = _FakeProjectHealth(_health_profile_result(findings))
    registry = _registry(
        project_health=project_health,
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=_FakeOperationalDeficiency(),
    )
    step = registry.get("health.project.v1", "health_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.content is not None
    assert len(outcome.content.health_findings) == 50
    assert outcome.content.health_findings_truncated is False


def test_build_registry_with_wave_3_1_validates_all_ten_plans_together():
    """The real production entry point: the six core plans PLUS this
    module's four, one shared registry, one combined totality check --
    proves a plan_id collision or cross-registration mismatch between the
    two groups fails construction, not the first request that reaches it.
    """

    registry = build_registry_with_wave_3_1(
        FakePlanExecutorRuntime(),
        **_empty_doubles(),
    )
    for plan in {**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT}.values():
        registered = registry.for_plan(plan.plan_id)
        for step_id in (*plan.mandatory_steps, *plan.conditional_steps):
            assert step_id in registered


# ---------------------------------------------------------------------------
# CHAOS-3297 s3 codex full-branch review round 1 (FINDING 2, CONFIRMED HIGH,
# 2026-08-02): OperationalDeficiencyInventory.category_statuses was being
# discarded entirely on the way into DevSourceContent -- eight valid
# UNEVALUATED categories produced content indistinguishable from eight
# genuinely-evaluated zero-finding categories. Proves: (a) the coverage
# block survives into content, (b) the step's own observed_state/limitation
# reflect which categories (if any) are unevaluated, (c) evaluated-zero
# stays distinguishable from unevaluated at both layers.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fully_evaluated_inventory_carries_coverage_and_stays_current():
    inventory = _deficiency_inventory(findings=(_deficiency_finding(finding_id="a"),))
    operational_deficiency = _FakeOperationalDeficiency(project_result=inventory)
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=operational_deficiency,
    )
    step = registry.get("deficiency.operational.v1", "deficiency_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.content is not None
    assert len(outcome.content.deficiency_category_statuses) == 8
    assert all(
        status.evaluated for status in outcome.content.deficiency_category_statuses
    )
    assert outcome.observed_state is SourceRequirementState.AVAILABLE_CURRENT
    assert outcome.limitation is None


@pytest.mark.asyncio
async def test_partially_unevaluated_inventory_carries_coverage_and_downgrades():
    """Plant the defect codex reproduced: eight category_statuses with two
    genuinely UNEVALUATED must not be silently dropped, and the step must
    not claim AVAILABLE_CURRENT/no-limitation as if every category ran.
    """

    unevaluated = frozenset(
        {DeficiencyCategory.INVESTMENT_BALANCE, DeficiencyCategory.OWNERSHIP_CODE_RISK}
    )
    inventory = _deficiency_inventory(
        findings=(_deficiency_finding(finding_id="a"),), unevaluated=unevaluated
    )
    operational_deficiency = _FakeOperationalDeficiency(project_result=inventory)
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=operational_deficiency,
    )
    step = registry.get("deficiency.operational.v1", "deficiency_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.content is not None
    # The bug: this used to be empty -- category_statuses was dropped
    # entirely on the way into DevSourceContent.
    assert len(outcome.content.deficiency_category_statuses) == 8
    statuses_by_category = {
        status.category: status
        for status in outcome.content.deficiency_category_statuses
    }
    for category in unevaluated:
        assert statuses_by_category[category].evaluated is False
        assert statuses_by_category[category].limitation is not None
    for category in DEFICIENCY_CATEGORIES:
        if category not in unevaluated:
            assert statuses_by_category[category].evaluated is True
    # The bug: this used to unconditionally be AVAILABLE_CURRENT/None
    # regardless of category_statuses.
    assert outcome.observed_state is SourceRequirementState.AVAILABLE_STALE
    assert outcome.limitation is not None
    assert "investment_balance" in outcome.limitation
    assert "ownership_code_risk" in outcome.limitation
    # Findings from the categories that DID evaluate are still real content
    # -- a partial answer is not the same as no answer.
    assert len(outcome.content.deficiency_findings) == 1


@pytest.mark.asyncio
async def test_fully_unevaluated_inventory_still_carries_coverage_never_unmeasured():
    """Deliberately never reports an UNMEASURED-family observed_state, even
    when every category is unevaluated: OperationalDeficiencyService
    genuinely ran and returned a real, disclosed inventory (each
    category_status IS a measured fact). Reporting an unmeasured state
    would violate DevSourceObservation.validate_content_semantics (content
    forbidden on an unmeasured observation) and silently discard the very
    coverage block this fix exists to preserve.
    """

    inventory = _deficiency_inventory(
        findings=(), unevaluated=frozenset(DEFICIENCY_CATEGORIES)
    )
    operational_deficiency = _FakeOperationalDeficiency(project_result=inventory)
    registry = _registry(
        project_health=_FakeProjectHealth(_health_profile_result()),
        team_health=_FakeTeamHealth(_health_profile_result()),
        team_workload=_FakeTeamWorkload(_health_profile_result()),
        operational_deficiency=operational_deficiency,
    )
    step = registry.get("deficiency.operational.v1", "deficiency_evaluation")
    outcome = await step.run(_step_context(_project_scope()))

    assert outcome.content is not None
    assert len(outcome.content.deficiency_category_statuses) == 8
    assert all(
        not status.evaluated for status in outcome.content.deficiency_category_statuses
    )
    assert outcome.observed_state is SourceRequirementState.AVAILABLE_STALE
    assert outcome.limitation is not None
    assert outcome.content.deficiency_findings == ()


def test_evaluated_zero_is_distinguishable_from_unevaluated_in_content():
    """Direct proof of the exact distinction codex named: a category with
    evaluated=True/finding_count=0 (a genuine, measured "no deficiencies")
    is a structurally different record from evaluated=False (never
    checked) -- both survive into DevSourceContent, never collapsed to the
    same "empty" shape.
    """

    zero_findings_inventory = _deficiency_inventory(findings=())
    unevaluated_inventory = _deficiency_inventory(
        findings=(), unevaluated=frozenset(DEFICIENCY_CATEGORIES)
    )

    zero_content = _deficiency_inventory_content(zero_findings_inventory)
    unevaluated_content = _deficiency_inventory_content(unevaluated_inventory)

    assert all(
        status.evaluated and status.finding_count == 0
        for status in zero_content.deficiency_category_statuses
    )
    assert all(
        not status.evaluated
        for status in unevaluated_content.deficiency_category_statuses
    )
    assert zero_content.deficiency_findings == ()
    assert unevaluated_content.deficiency_findings == ()
    # Both are empty on `deficiency_findings`, but NOT the same content --
    # the coverage block is what tells them apart.
    assert zero_content.deficiency_category_statuses != (
        unevaluated_content.deficiency_category_statuses
    )
