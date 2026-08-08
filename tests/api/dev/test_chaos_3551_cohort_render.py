"""RED-first coverage for CHAOS-3551: a committed cohort answers instead of
refusing.

The defect, in the terms CHAOS-3534 already established:

A question naming a bounded, homogeneous set of real subjects resolves every
member exactly and commits a real ``dev_subject_set.v1`` -- CHAOS-3534's own
``committed_cohort_resolution_for`` even builds a faithful multi-repository
``DevScopeResolution`` for it. But until this ticket, ``subject_preflight``
only ever used that resolution to describe an honest TERMINATE
(``committed_cohort_v1_only``, ``feature_not_enabled``) -- the render half of
D1 was never wired. The corpus case ``scope.bounded-subject-set`` measures
exactly this shape and stays red on ``public_outcome_in`` because of it.

THE FIX, and why it is scoped to REPOSITORY only: v1's ``DevScope`` has no
multi-entity representation for any other kind (``DevScope.
validate_direct_scope`` requires exactly one ``entity_ref`` for project/
team/work-unit/issue/pull-request, while ``repositories`` is a list) --
``committed_cohort_resolution_for`` itself refuses any other kind, so a
project or team cohort still terminates unsupported exactly as before. This
is also why the fix can never open person-level output: there is no
``EntityKind.PERSON`` anywhere in this catalog (``EntityKind`` is
organization/repository/project/work_unit/issue/pull_request/team) -- a
committed subject is never a person, cohort or not, so there is no
"person-level" branch to weaken here at all. The closest analog, a TEAM
cohort, is pinned below to keep refusing.

Driving the REAL ``DevOrchestrator.run()`` end to end (not just
``SubjectPreflight`` in isolation) for the primary case, because "does this
answer" is an orchestration-level claim, not just a preflight decision --
``committed_resolution`` alone does not prove the legacy model round loop
actually executes a subject-bearing tool against the committed scope and
returns an answer.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import ToolID
from dev_health_ops.api.dev.investigation_plans.executor import PlanExecutor
from dev_health_ops.api.dev.investigation_plans.steps import StepRegistry
from dev_health_ops.api.dev.investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
    register_wave_3_1_steps,
)
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import (
    SUBJECT_BEARING_TOOLS,
    PreflightDecision,
    SubjectPreflight,
)
from tests._chaos_3292_preflight import (
    ORG_ID,
    PLATFORM_TEAM,
    SeededCatalog,
    fixed_now,
    request_for,
    run_preflight_orchestrator,
    sequential_ids,
    versions,
)

#: CHAOS-3534's own corpus case, verbatim from
#: ``case-scope.bounded-subject-set.json``: both real, both individually
#: resolvable, named together in one question.
WEB_APP = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/web-app", "meridian/web-app"
)
API_GATEWAY = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/api-gateway", "meridian/api-gateway"
)
COHORT_QUESTION = (
    'What\'s the status of repo "meridian/web-app" and repo "meridian/api-gateway"?'
)

#: A second TEAM, distinct-labelled from PLATFORM_TEAM, for the still-refuses
#: control below -- two same-label teams would resolve ambiguously instead of
#: committing a cohort at all, proving nothing about the kind gate.
ROCKET_TEAM = AuthorizedEntity(EntityKind.TEAM, "team-rocket", "Rocket")


def _preflight(entities, **catalog_kwargs) -> SubjectPreflight:
    mint = sequential_ids()
    return SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog(entities, **catalog_kwargs), cache=ScopeRequestCache()
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )


async def _run(preflight: SubjectPreflight, request, **kwargs):
    return await preflight.run(
        request=request,
        org_id=kwargs.pop("org_id", ORG_ID),
        permission_fingerprint="permissions_01",
        authorized_scope=kwargs.pop("authorized_scope", request.scope),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        **kwargs,
    )


@pytest.mark.asyncio
async def test_a_committed_repository_cohort_answers_instead_of_refusing() -> None:
    """The defect, in its exact corpus shape, driven end to end.

    Before this fix: ``output.result.error.code == "feature_not_enabled"``,
    ``output.result.answer is None``, and the model round loop never runs --
    ``output.calls == []``. That is the RED this test replaces: a fully-
    resolved, two-repository cohort refuses to say anything about either
    repository. Confirmed by reverting the ``subject_preflight.py`` hunk
    locally and re-running this test, which fails exactly that way.

    After: the run PROCEEDs, the model executes ``status_snapshot.v1``
    against the committed multi-repository scope, and the run completes with
    a real answer -- the render half of D1.
    """

    output = await run_preflight_orchestrator(
        question=COHORT_QUESTION,
        entities=[(ORG_ID, WEB_APP), (ORG_ID, API_GATEWAY)],
        script_id="chaos-3551-cohort-render",
    )

    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [
        ("committed_cohort_v1_render", None)
    ], (
        "setup control: this must be the new render branch, not some other "
        "path that happens to also produce an answer"
    )

    # The claim CHAOS-3551 exists to close: a committed cohort ANSWERS.
    assert output.result.state is RunState.COMPLETED
    assert output.result.error is None, (
        f"a committed, fully-resolved cohort must not refuse -- got "
        f"{output.result.error!r}"
    )
    assert output.result.answer is not None

    # Not merely "no error" -- the model round loop actually ran a
    # subject-bearing tool against the committed scope, rather than the
    # commit being decorative.
    assert [call.tool_id for call in output.calls] == [ToolID.STATUS_SNAPSHOT]

    # The scope the run answered under names exactly the two committed
    # repositories -- never a widened or narrowed substitute.
    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome.value == "exact"
    assert sorted(resolution.authorized_repository_ids) == [
        "meridian/api-gateway",
        "meridian/web-app",
    ]
    assert resolution.resolved_scope is not None
    assert sorted(resolution.resolved_scope.repositories) == [
        "meridian/api-gateway",
        "meridian/web-app",
    ]
    executed_scope = output.calls[0].scope
    assert sorted(executed_scope.repositories) == [
        "meridian/api-gateway",
        "meridian/web-app",
    ]


@pytest.mark.asyncio
async def test_a_committed_repository_cohort_withholds_resolve_scope() -> None:
    """Mirrors the singular commit's own rationale: nothing is left for the
    model to resolve, so ``resolve_scope.v1`` is withheld -- the same leak
    channel that branch's own comment closes.
    """

    result = await _run(
        _preflight([(ORG_ID, WEB_APP), (ORG_ID, API_GATEWAY)]),
        request_for(COHORT_QUESTION),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "committed_cohort_v1_render"
    assert result.committed_resolution is not None
    assert result.allowed_tools == frozenset(ToolID) - {ToolID.RESOLVE_SCOPE}
    assert SUBJECT_BEARING_TOOLS <= result.allowed_tools
    assert result.all_subjects_committed is True


@pytest.mark.asyncio
async def test_a_team_cohort_still_refuses_after_the_render_fix() -> None:
    """Pin: the fix is REPOSITORY-only. A team cohort -- the closest thing
    to a "who" cohort this catalog has, since there is no ``EntityKind.
    PERSON`` at all -- must keep refusing exactly as before CHAOS-3551.

    Not a person-level regression by construction (no person-kind subject
    exists to leak), but this is the one kind whose committed members are
    named individuals rather than code artifacts, so it is the sharpest
    available proxy for "did bounded-cohort answering quietly widen past
    what the PRD allows" -- and it must not have.
    """

    result = await _run(
        _preflight([(ORG_ID, PLATFORM_TEAM), (ORG_ID, ROCKET_TEAM)]),
        request_for("What is the status of team Platform and team Rocket?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.diagnostic == "committed_cohort_v1_only"
    assert result.committed_resolution is None
    assert result.subject_set is not None
    assert result.subject_set.cohort_complete is True
    committed_ids = {ref.entity_id for ref in result.subject_set.committed_entity_refs}
    assert committed_ids == {PLATFORM_TEAM.canonical_id, ROCKET_TEAM.canonical_id}


@pytest.mark.asyncio
async def test_a_partial_repository_cohort_still_refuses() -> None:
    """Pin: the render fix is gated on ``cohort_complete``. A repository
    cohort that omitted a named member did not resolve everything the
    question named, so it must keep terminating unsupported rather than
    rendering an answer over a silently-narrowed set.

    ``meridian/never-seeded`` is never seeded, so it is omitted; the other
    two repositories resolve exactly. Phrased as "Compare ... and ... and
    ..." rather than "What's the status of" deliberately: the latter
    classifies as ``PORTFOLIO_STATUS``, whose own D2 carve-out (CHAOS-3393)
    terminates on the FIRST unresolved mention rather than committing a
    partial cohort at all -- a real, different-diagnosis termination this
    test is not about. "Compare" classifies as ``BOUNDED_INVESTIGATION``,
    where D2 lets >=2 distinct exact matches commit a partial cohort, which
    is the shape this test needs to pin ``cohort_complete`` against.
    """

    result = await _run(
        _preflight([(ORG_ID, WEB_APP), (ORG_ID, API_GATEWAY)]),
        request_for(
            'Compare repo "meridian/web-app" and repo "meridian/api-gateway" '
            'and repo "meridian/never-seeded"'
        ),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.diagnostic == "committed_cohort_v1_only"
    assert result.committed_resolution is None
    assert result.subject_set is not None
    assert result.subject_set.cohort_complete is False


class _NeverCalled:
    """A stand-in for every wave_3_1 evaluator this suite must not touch."""

    async def evaluate_project(self, **_kwargs: Any) -> Any:
        raise AssertionError("no wave_3_1 evaluator should run for a repository cohort")

    async def evaluate_team(self, **_kwargs: Any) -> Any:
        raise AssertionError("no wave_3_1 evaluator should run for a repository cohort")

    async def evaluate_workload(self, **_kwargs: Any) -> Any:
        raise AssertionError("no wave_3_1 evaluator should run for a repository cohort")

    async def evaluate_portfolio(self, **_kwargs: Any) -> Any:
        raise AssertionError(
            "status.portfolio.v1 must never run for a REPOSITORY subject set -- "
            "it only ever supports PROJECT (WAVE_3_1_PLANS_BY_INTENT's own "
            "supported_subject_kinds), and _project_scope_from_ref would "
            "misrepresent each repository as a same-named PROJECT scope"
        )


def _real_wave_3_1_plan_executor() -> PlanExecutor:
    registry = StepRegistry()
    register_wave_3_1_steps(
        registry,
        project_health=_NeverCalled(),
        team_health=_NeverCalled(),
        team_workload=_NeverCalled(),
        operational_deficiency=_NeverCalled(),
        portfolio_status=_NeverCalled(),
    )
    return PlanExecutor(registry=registry, now=fixed_now)


@pytest.mark.asyncio
async def test_a_repository_cohort_never_reaches_the_portfolio_plan() -> None:
    """Regression pin for a defect CHAOS-3551 found while wiring the render
    fix, not the render fix itself.

    ``COHORT_QUESTION`` classifies as ``PORTFOLIO_STATUS`` (its vocabulary
    is kind-blind -- see ``subject_preflight``'s own PORTFOLIO_STATUS
    comment), exactly like the project-cohort case ``status.portfolio.v1``
    is built for. With a REAL ``plan_registry``/``plan_executor`` wired (the
    production shape this suite's other tests use only a fake ``Recorder``
    for), the orchestrator's PLURAL_COHORT plan-eligibility gate used to
    check only "is there a plan for this intent, and a committed subject
    set" -- never the committed subject set's OWN kind against the plan's
    declared ``supported_subject_kinds``. A REPOSITORY cohort would have
    satisfied both existing checks and been handed to ``status.
    portfolio.v1``'s step, which builds each batch entry via
    ``_project_scope_from_ref`` -- documented as trusting ITS CALLER to
    restrict entries to PROJECT -- misrepresenting a repository as a
    same-named project.

    This proves the fix instead: the plan-eligibility gate now also checks
    kind, so ``evaluate_portfolio`` (the ``_NeverCalled`` fake above) is
    never invoked, and the run still answers -- through the ordinary
    status-snapshot path, exactly like the isolated ``SubjectPreflight``
    tests above already prove for the fake-recorder shape.
    """

    output = await run_preflight_orchestrator(
        question=COHORT_QUESTION,
        entities=[(ORG_ID, WEB_APP), (ORG_ID, API_GATEWAY)],
        script_id="chaos-3551-cohort-render-plan-gate",
        plan_registry=WAVE_3_1_PLANS_BY_INTENT,
        plan_executor=_real_wave_3_1_plan_executor(),
    )

    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [
        ("committed_cohort_v1_render", None)
    ]
    assert output.result.error is None
    assert output.result.answer is not None
    assert [call.tool_id for call in output.calls] == [ToolID.STATUS_SNAPSHOT]
