"""CHAOS-3393 codex HIGH-1: ``PlanExecutor.run`` must verify a PLURAL_COHORT/
ORGANIZATION_WIDE step's ``StepContext.subject_set_scopes`` batch against the
caller's own authorized ``subject_set_fingerprint`` receipt BEFORE any step
runs -- never trust an unverified batch just because it rode in on
``StepContext``.

Constructs a ``StepContext``/``PlanExecutor`` directly (mirrors
``test_chaos_3295_investigation_plans_executor.py``'s own house style) --
this is a property of the executor's own trust boundary, independent of how
a subject set got committed upstream.
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
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    PlanRegistryError,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)
from dev_health_ops.api.dev.scope_service import (
    EntityKind as ScopeEntityKind,
)
from dev_health_ops.api.dev.scope_service import (
    subject_set_fingerprint,
)

ORG_ID = "org_fullchaos"


def _now() -> datetime:
    return datetime(2026, 8, 5, 12, 0, 0, tzinfo=UTC)


def _time_range() -> DevTimeRange:
    return DevTimeRange(
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 31, tzinfo=UTC),
        timezone="UTC",
    )


def _org_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ORGANIZATION,
        entity_refs=[],
        time_range=_time_range(),
    )


def _project_scope(
    project_id: str, *, direct_scope: DirectScope = DirectScope.PROJECT
) -> DevScope:
    entity_type = "project" if direct_scope is DirectScope.PROJECT else "team"
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=direct_scope,
        entity_refs=[
            {
                "entity_type": entity_type,
                "entity_id": project_id,
                "display_label": project_id,
                "repository_id": None,
            }
        ],
        team_ids=[project_id] if direct_scope is DirectScope.TEAM else [],
        time_range=_time_range(),
    )


def _plan() -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="status.portfolio.v1",
        plan_version="status.portfolio.v1.0",
        intent_id=QuestionIntentID.PORTFOLIO_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(
            Cardinality.PLURAL_COHORT,
            Cardinality.ORGANIZATION_WIDE,
        ),
        mandatory_steps=("portfolio_status_evaluation",),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.HEALTH_PROFILE,
                adapter_id="test.portfolio.v1",
                requirement_level="mandatory",
                freshness_policy="health_profile_freshness.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="batched_fan_out",
        per_step_timeout_seconds=120,
        max_rows_per_step=200,
        max_bytes_per_step=131_072,
        enrichment_allowed=True,
        completion_rule_id="test.no_completion_concept",
        completion_rule_version="1",
    )


def _registry(step_ran: list[int]) -> StepRegistry:
    async def run(_ctx: StepContext) -> StepOutcome:
        step_ran.append(1)
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="no_data",
            usable_fact_count=0,
        )

    registry = StepRegistry()
    registry.register(
        PlanStepDefinition(
            step_id="portfolio_status_evaluation",
            plan_id="status.portfolio.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="test.portfolio.v1",
            requirement_level="mandatory",
            run=run,
        )
    )
    return registry


def _context(*, subject_set_scopes: tuple[DevScope, ...]) -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_org_scope(),
        run_id="run-1",
        now=_now(),
        subject_set_scopes=subject_set_scopes,
    )


@pytest.mark.asyncio
async def test_a_batch_matching_its_own_fingerprint_executes() -> None:
    """Positive control: the real, legitimate shape (subject_set_scopes and
    subject_set_fingerprint minted from the SAME committed set) executes
    the step normally -- proves the check does not false-positive on the
    happy path every other CHAOS-3393 test already exercises."""

    step_ran: list[int] = []
    scopes = (_project_scope("project-a"), _project_scope("project-b"))
    fingerprint = subject_set_fingerprint(
        ScopeEntityKind.PROJECT, ["project-a", "project-b"]
    )
    executor = PlanExecutor(registry=_registry(step_ran), now=_now)

    result = await executor.run(
        plan=_plan(),
        context=_context(subject_set_scopes=scopes),
        run_id="run-1",
        subject_set_fingerprint=fingerprint,
    )

    assert step_ran == [1]
    assert "portfolio_status_evaluation" in result.completed_steps


@pytest.mark.asyncio
async def test_a_foreign_project_smuggled_into_the_batch_fails_closed() -> None:
    """The adversarial case: subject_set_scopes carries an EXTRA project
    (``project-forbidden``) the caller's own fingerprint receipt never
    authorized (the fingerprint was minted over only project-a/project-b).
    The executor must refuse the WHOLE run -- never silently drop the
    foreign project and proceed, never evaluate it."""

    step_ran: list[int] = []
    scopes = (
        _project_scope("project-a"),
        _project_scope("project-b"),
        _project_scope("project-forbidden"),
    )
    # The receipt only ever authorized project-a/project-b.
    fingerprint = subject_set_fingerprint(
        ScopeEntityKind.PROJECT, ["project-a", "project-b"]
    )
    executor = PlanExecutor(registry=_registry(step_ran), now=_now)

    with pytest.raises(PlanRegistryError, match="subject_set_fingerprint"):
        await executor.run(
            plan=_plan(),
            context=_context(subject_set_scopes=scopes),
            run_id="run-1",
            subject_set_fingerprint=fingerprint,
        )

    assert step_ran == [], (
        "no step may run once the batch fails the authorization check"
    )


@pytest.mark.asyncio
async def test_a_batch_with_no_fingerprint_at_all_fails_closed() -> None:
    """A caller that hands the executor scopes to batch over but no receipt
    to verify them against must never be trusted by default -- there is
    nothing here to distinguish "forgot to pass it" from "deliberately
    smuggled an unverified batch", so both fail exactly the same way."""

    step_ran: list[int] = []
    scopes = (_project_scope("project-a"),)
    executor = PlanExecutor(registry=_registry(step_ran), now=_now)

    with pytest.raises(PlanRegistryError, match="subject_set_fingerprint"):
        await executor.run(
            plan=_plan(),
            context=_context(subject_set_scopes=scopes),
            run_id="run-1",
            subject_set_fingerprint=None,
        )

    assert step_ran == []


@pytest.mark.asyncio
async def test_a_stale_fingerprint_from_a_different_batch_fails_closed() -> None:
    """A fingerprint minted for a genuinely different (but equally real)
    committed set must not verify a DIFFERENT batch, even if both are
    individually well-formed -- proves this is a real cross-check, not a
    non-empty-string sanity check."""

    step_ran: list[int] = []
    scopes = (_project_scope("project-a"), _project_scope("project-b"))
    unrelated_fingerprint = subject_set_fingerprint(
        ScopeEntityKind.PROJECT, ["project-x", "project-y"]
    )
    executor = PlanExecutor(registry=_registry(step_ran), now=_now)

    with pytest.raises(PlanRegistryError, match="subject_set_fingerprint"):
        await executor.run(
            plan=_plan(),
            context=_context(subject_set_scopes=scopes),
            run_id="run-1",
            subject_set_fingerprint=unrelated_fingerprint,
        )

    assert step_ran == []


@pytest.mark.asyncio
async def test_a_non_homogeneous_kind_batch_fails_closed() -> None:
    """subject_set_scopes mixing PROJECT and TEAM direct scopes has no
    single fingerprint kind to verify against -- refused outright rather
    than guessing which kind the caller meant."""

    step_ran: list[int] = []
    scopes = (
        _project_scope("project-a"),
        _project_scope("team-a", direct_scope=DirectScope.TEAM),
    )
    fingerprint = subject_set_fingerprint(
        ScopeEntityKind.PROJECT, ["project-a", "team-a"]
    )
    executor = PlanExecutor(registry=_registry(step_ran), now=_now)

    with pytest.raises(PlanRegistryError, match="homogeneous"):
        await executor.run(
            plan=_plan(),
            context=_context(subject_set_scopes=scopes),
            run_id="run-1",
            subject_set_fingerprint=fingerprint,
        )

    assert step_ran == []
