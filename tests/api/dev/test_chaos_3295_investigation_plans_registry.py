"""Registry-construction validation for CHAOS-3295's plan/step registry.

Every negative case here proves the specific guard named in Amendment TRD
v2 §4.3 fires -- not merely that *some* exception is raised.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevPlanStepDependency,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.investigation_plans import (
    CORE_PLANS_BY_INTENT,
    CORE_QUESTION_INTENT_IDS,
    PlanStepDefinition,
    StepOutcome,
    StepRegistry,
    build_default_registry,
    plan_registry_manifest,
    register_builtin_steps,
    validate_registry,
)
from dev_health_ops.api.dev.investigation_plans.registry_validation import (
    DependencyCycleError,
    MissingCorePlanError,
    MissingStepImplementationError,
)
from dev_health_ops.api.dev.investigation_plans.steps import DuplicateStepError
from tests._chaos_3295_plan_executor import FakePlanExecutorRuntime


def test_build_default_registry_succeeds_for_the_real_six_core_plans():
    registry = build_default_registry(FakePlanExecutorRuntime())
    for plan in CORE_PLANS_BY_INTENT.values():
        registered = registry.for_plan(plan.plan_id)
        for step_id in (*plan.mandatory_steps, *plan.conditional_steps):
            assert step_id in registered


def test_totality_every_core_intent_has_a_registered_plan():
    assert CORE_QUESTION_INTENT_IDS == frozenset(CORE_PLANS_BY_INTENT)
    for intent_id in QuestionIntentID:
        if intent_id in {
            QuestionIntentID.PROJECT_HEALTH,
            QuestionIntentID.TEAM_HEALTH,
            QuestionIntentID.TEAM_WORKLOAD_BALANCE,
            QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
            QuestionIntentID.PORTFOLIO_STATUS,
            QuestionIntentID.BOUNDED_INVESTIGATION,
        }:
            # Explicitly out of CHAOS-3295 scope (3303/3304/3305, or never
            # plan-governed at all for BOUNDED_INVESTIGATION).
            continue
        assert intent_id in CORE_PLANS_BY_INTENT, f"{intent_id} has no core plan"


def test_missing_core_plan_is_rejected():
    plans = dict(CORE_PLANS_BY_INTENT)
    del plans[QuestionIntentID.DATA_TRUST]
    registry = StepRegistry()
    register_builtin_steps(registry, FakePlanExecutorRuntime())
    with pytest.raises(MissingCorePlanError):
        validate_registry(
            plans_by_intent=plans, steps=registry, core_intents=CORE_QUESTION_INTENT_IDS
        )


def test_step_declared_but_never_registered_is_rejected():
    registry = StepRegistry()  # deliberately empty -- no builtin steps registered
    with pytest.raises(MissingStepImplementationError):
        validate_registry(
            plans_by_intent=CORE_PLANS_BY_INTENT,
            steps=registry,
            core_intents=CORE_QUESTION_INTENT_IDS,
        )


def test_registering_the_same_step_twice_is_rejected():
    registry = StepRegistry()

    async def run(_ctx):
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="no_data",
            usable_fact_count=0,
        )

    definition = PlanStepDefinition(
        step_id="status_snapshot",
        plan_id="status.entity.v2",
        source_class=SourceClass.STATUS_CHANGE,
        adapter_id="status_change_service.status_snapshot.v1",
        requirement_level="mandatory",
        run=run,
    )
    registry.register(definition)
    with pytest.raises(DuplicateStepError):
        registry.register(definition)


def _minimal_plan(
    *, step_dependencies: tuple[DevPlanStepDependency, ...]
) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("a", "b"),
        conditional_steps=(),
        step_dependencies=step_dependencies,
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.a.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.WORK_ITEM,
                adapter_id="test.b.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )


def test_multi_node_dependency_cycle_is_rejected():
    """A -> B -> A is invisible to the per-plan self-loop check alone."""

    plan = _minimal_plan(
        step_dependencies=(
            DevPlanStepDependency(step_id="a", depends_on=("b",)),
            DevPlanStepDependency(step_id="b", depends_on=("a",)),
        )
    )
    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    for step_id, adapter_id in (("a", "test.a.v1"), ("b", "test.b.v1")):
        registry.register(
            PlanStepDefinition(
                step_id=step_id,
                plan_id=plan.plan_id,
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id=adapter_id,
                requirement_level="mandatory",
                run=run,
            )
        )
    with pytest.raises(DependencyCycleError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )


def test_plan_registry_manifest_matches_plan_registry_membership():
    """Drift test: the generated manifest can only ever describe real plans."""

    manifest = plan_registry_manifest()
    manifest_ids = {row["plan_id"] for row in manifest}
    assert manifest_ids == {plan.plan_id for plan in CORE_PLANS_BY_INTENT.values()}
    for row in manifest:
        assert int(row["mandatory_step_count"]) >= 1  # type: ignore[call-overload]
        assert int(row["source_requirement_count"]) >= 1  # type: ignore[call-overload]
