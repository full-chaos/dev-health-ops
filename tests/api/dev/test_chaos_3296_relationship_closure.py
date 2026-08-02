"""Relationship-closed evidence controls (CHAOS-3296).

The executor comment left by CHAOS-3295 (``investigation_plans/executor.py``)
names this issue's job precisely: "3296 populates DevSourceObservation.
relationship_paths and owns the actual closure check." These controls prove
that job at the layer it actually runs -- the executor's own
``DevSourceObservation``/``DevInvestigationResult`` construction -- using
hand-built ``StepOutcome.content`` the same way
``test_chaos_3295_investigation_plans_executor.py`` hand-builds outcomes,
never a full orchestrator/frame stack that does not exist yet.

Positive: every content fact from a committed single-subject scope mints a
verified path back to that subject, and the result-level
``relationship_closure_verified`` flag turns on.

Negative (issue acceptance criteria, verbatim): cross-tenant/forged entity
ID, stale (low-confidence) link, duplicate edge, and path-cycle inputs must
each fail *closed* -- no relationship path minted for the offending fact,
``relationship_closure_verified`` stays ``False`` -- never a silent path
fabricated from an unrelated or self-referential edge, and never an
unhandled exception.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import DevGraphEdgeV2
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.contracts_v2.result import DevSourceContent
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    MIN_RELATIONSHIP_CONFIDENCE,
)

ORG_ID = "org_fullchaos"
ROOT_ENTITY_ID = "project-1"
OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


def _now() -> datetime:
    return OBSERVED_AT


def _scope(*, direct_scope: DirectScope = DirectScope.PROJECT) -> DevScope:
    time_range = DevTimeRange(
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 31, tzinfo=UTC),
        timezone="UTC",
    )
    if direct_scope is DirectScope.ORGANIZATION:
        return DevScope(
            schema_version="dev_scope.v1",
            organization_id=ORG_ID,
            direct_scope=DirectScope.ORGANIZATION,
            time_range=time_range,
        )
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=direct_scope,
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": ROOT_ENTITY_ID,
                "display_label": "Project One",
                "repository_id": None,
            }
        ],
        time_range=time_range,
    )


def _context(*, direct_scope: DirectScope = DirectScope.PROJECT) -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_scope(direct_scope=direct_scope),
        run_id="run-1",
        now=_now(),
    )


def _plan(source_class: SourceClass) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.1",
        intent_id=QuestionIntentID.ENTITY_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
        mandatory_steps=("one",),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=source_class,
                adapter_id="test.one.v1",
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


def _edge(
    *,
    source_entity_id: str,
    target_entity_id: str,
    relationship: str = "references",
    confidence: float = 1.0,
    evidence_ref_ids: tuple[str, ...] = ("ev1_" + "a" * 40,),
) -> DevGraphEdgeV2:
    return DevGraphEdgeV2(
        source_entity_id=source_entity_id,
        relationship=relationship,
        target_entity_id=target_entity_id,
        provenance="work_graph",
        confidence=confidence,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=evidence_ref_ids,
    )


def _content(edges: tuple[DevGraphEdgeV2, ...]) -> DevSourceContent:
    return DevSourceContent(schema_version="dev_source_content.v1", graph_edges=edges)


async def _run_single_step(
    *,
    source_class: SourceClass,
    outcome: StepOutcome,
    direct_scope: DirectScope = DirectScope.PROJECT,
) -> tuple:
    plan = _plan(source_class)
    registry = StepRegistry()

    async def run(_ctx: StepContext) -> StepOutcome:
        return outcome

    registry.register(
        PlanStepDefinition(
            step_id="one",
            plan_id=plan.plan_id,
            source_class=source_class,
            adapter_id="test.one.v1",
            requirement_level="mandatory",
            run=run,
        )
    )
    executor = PlanExecutor(registry=registry, now=_now)
    result = await executor.run(
        plan=plan,
        context=_context(direct_scope=direct_scope),
        run_id="run-1",
        subject_entity_id=ROOT_ENTITY_ID
        if direct_scope is not DirectScope.ORGANIZATION
        else None,
    )
    assert len(result.observations) == 1
    return result, result.observations[0]


def _queried_outcome(content: DevSourceContent) -> StepOutcome:
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        usable_fact_count=1,
        content=content,
    )


@pytest.mark.asyncio
async def test_a_real_edge_touching_the_subject_mints_a_verified_relationship_path():
    outcome = _queried_outcome(
        _content((_edge(source_entity_id=ROOT_ENTITY_ID, target_entity_id="pr-9"),))
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert len(observation.relationship_paths) == 1
    path = observation.relationship_paths[0]
    assert path.source_entity_id == ROOT_ENTITY_ID
    assert path.target_entity_id == "pr-9"
    assert path.relationship == "references"
    assert path.evidence_ref_ids == ("ev1_" + "a" * 40,)
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_an_edge_oriented_target_to_root_is_normalized_to_source_equals_root():
    outcome = _queried_outcome(
        _content((_edge(source_entity_id="pr-9", target_entity_id=ROOT_ENTITY_ID),))
    )
    _result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert len(observation.relationship_paths) == 1
    path = observation.relationship_paths[0]
    assert path.source_entity_id == ROOT_ENTITY_ID
    assert path.target_entity_id == "pr-9"


@pytest.mark.asyncio
async def test_organization_wide_scope_has_no_root_and_is_vacuously_closed():
    """No committed single subject -- broad facts are legitimately unfiltered
    (PRD v2 §3.2/§7), so there is nothing to enforce closure against."""

    outcome = _queried_outcome(
        _content((_edge(source_entity_id="pr-1", target_entity_id="pr-2"),))
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH,
        outcome=outcome,
        direct_scope=DirectScope.ORGANIZATION,
    )

    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_an_edge_unrelated_to_the_committed_subject_fails_closed():
    """Cross-tenant/forged-ID acceptance criterion: neither end of the edge
    is the committed subject -- must never mint a path claiming otherwise."""

    outcome = _queried_outcome(
        _content(
            (_edge(source_entity_id="unrelated-a", target_entity_id="unrelated-b"),)
        )
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_a_self_loop_edge_is_rejected_as_a_path_cycle():
    outcome = _queried_outcome(
        _content(
            (_edge(source_entity_id=ROOT_ENTITY_ID, target_entity_id=ROOT_ENTITY_ID),)
        )
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_a_low_confidence_edge_is_rejected_as_a_stale_link():
    below_floor = MIN_RELATIONSHIP_CONFIDENCE - 0.01
    assert below_floor >= 0.0
    outcome = _queried_outcome(
        _content(
            (
                _edge(
                    source_entity_id=ROOT_ENTITY_ID,
                    target_entity_id="pr-9",
                    confidence=below_floor,
                ),
            )
        )
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_an_unapproved_relationship_type_is_rejected_never_forged():
    """A relationship token outside the closed matrix vocabulary must never
    reach the wire, even if every other field looks legitimate."""

    outcome = _queried_outcome(
        _content(
            (
                _edge(
                    source_entity_id=ROOT_ENTITY_ID,
                    target_entity_id="pr-9",
                    relationship="fabricated_relationship_type",
                ),
            )
        )
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert observation.relationship_paths == ()
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_duplicate_edges_mint_exactly_one_path_never_double_counted():
    outcome = _queried_outcome(
        _content(
            (
                _edge(source_entity_id=ROOT_ENTITY_ID, target_entity_id="pr-9"),
                _edge(
                    source_entity_id=ROOT_ENTITY_ID,
                    target_entity_id="pr-9",
                    evidence_ref_ids=("ev1_" + "b" * 40,),
                ),
            )
        )
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert len(observation.relationship_paths) == 1
    assert result.relationship_closure_verified is True


@pytest.mark.asyncio
async def test_a_mixed_batch_with_one_bad_edge_marks_closure_unverified_but_never_raises():
    outcome = _queried_outcome(
        _content(
            (
                _edge(source_entity_id=ROOT_ENTITY_ID, target_entity_id="pr-9"),
                _edge(source_entity_id="unrelated-a", target_entity_id="unrelated-b"),
            )
        )
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert len(observation.relationship_paths) == 1
    assert observation.relationship_paths[0].target_entity_id == "pr-9"
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_unmeasured_observation_has_no_content_and_no_relationship_paths():
    outcome = StepOutcome(
        observed_state=SourceRequirementState.UNAVAILABLE,
        data_semantics="not_measured",
        usable_fact_count=0,
        limitation="step_execution_failed",
    )
    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, outcome=outcome
    )

    assert observation.content is None
    assert observation.relationship_paths == ()
    # An unmeasured source contributes nothing to close over -- closure
    # verification is about facts that *were* found, not sources that never
    # ran (that gap is disclosed separately via source coverage).
    assert result.relationship_closure_verified is True
