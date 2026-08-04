"""CHAOS-3300 proof-gate control: what actually happens today for an intent
outside ``CORE_PLANS_BY_INTENT``.

CHAOS-3303/3304/3305 built ProjectHealthService/TeamHealthService/
PortfolioStatusService/TeamWorkloadService/OperationalDeficiencyService, but
their DevInvestigationPlan/StepRegistry wiring is a ratified, sequenced
deferral to the CHAOS-3297 stack-3 lane (see wave31_manifest.py's
``_blocking_matrix_blocked`` for the full citation). Before this test, the
manifest's honest answer for "what happens when someone asks a project-health
question today" was "presumably UNSUPPORTED" -- an unverified guess. Traced
here instead: ``DevOrchestrator.run`` (``orchestrator.py:967-969``) looks up
``self._plan_registry.get(intent.intent_id)``, gets ``None`` for
``PROJECT_HEALTH`` (absent from the 6-entry ``CORE_PLANS_BY_INTENT``), sets
``plan_eligible = False``, and skips the whole plan-governed investigation
block -- falling through to the **legacy pre-CHAOS-3295 model-tool-choice
loop** below it, not to any dedicated "not supported yet" outcome.

That loop is still safe (CHAOS-3289's grounding/insufficient-evidence guard,
the closed 9-tool registry, the canonical no-answer copy table all still
apply to it) -- but it answers with whatever a generic tool call like
``status_snapshot.v1`` returns, never with a real project-health profile. A
user asking "what's this project's health" gets a generic status-shaped
answer, not an honest "not supported yet" and not a fabricated health
verdict either. This test pins that exact, traced shape so nobody has to
guess again, and so a future change that makes it worse (e.g. silently
fabricating a health finding) fails loudly here.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    run_preflight_orchestrator,
)
from tests._chaos_3295_plan_executor import (
    FakePlanExecutorRuntime,
    InvestigationRecorder,
    executor_for,
)


@pytest.mark.asyncio
async def test_project_health_question_falls_through_to_the_legacy_loop_not_a_plan() -> (
    None
):
    """Traced run: a project-health question against the real (6-entry)
    ``CORE_PLANS_BY_INTENT`` never reaches the plan-governed investigation
    path, and instead completes through the legacy model-tool-choice loop.
    """

    runtime = FakePlanExecutorRuntime()
    output = await run_preflight_orchestrator(
        question="What's the health of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3300-unwired-project-health",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    # The interpreter still correctly classifies the question -- this is not
    # an interpretation gap, only an execution one (checked below via
    # CORE_PLANS_BY_INTENT membership, the same registry orchestrator.py
    # actually consults).
    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)

    # Plan-governed investigation never ran: no health-specific step calls,
    # no investigation result recorded.
    assert runtime.status_snapshot_calls == 0
    assert runtime.data_health_calls == 0
    assert recorder.results == []

    # It did NOT silently fail or crash -- the legacy loop reached a real
    # terminal state via the generic status tool the default script drives,
    # never a health-specific one (none exists in the 9-tool registry).
    assert recorder.terminals, "run never reached a terminal state"
    assert output.calls, "legacy loop made no tool calls at all"
    called_tool_ids = {call.tool_id for call in output.calls}
    assert called_tool_ids == {"status_snapshot.v1"}, (
        f"expected only the generic status tool via the legacy loop, got "
        f"{called_tool_ids}"
    )

    # Structural guardrail: prove the premise stays true -- if a future PR
    # adds PROJECT_HEALTH to CORE_PLANS_BY_INTENT (i.e. stack-3 lands), this
    # assertion is what must be updated, not silently left describing a
    # fallback that no longer happens.
    assert QuestionIntentID.PROJECT_HEALTH not in CORE_PLANS_BY_INTENT, (
        "PROJECT_HEALTH is now wired -- this test's fallback premise is "
        "stale; update wave31_manifest.py's blocked-item evidence and "
        "flip the corresponding rows to proven"
    )


@pytest.mark.asyncio
async def test_team_health_question_also_falls_through_when_subject_resolves() -> None:
    """Same trace for TEAM_HEALTH, confirming this is not PROJECT_HEALTH-only.

    Uses an unresolvable team name deliberately: the point is not to re-prove
    CHAOS-3301's team resolution (covered elsewhere), only that whatever
    intent the interpreter reaches for a *team* health question is also
    outside ``CORE_PLANS_BY_INTENT`` -- checked via the ledger's resolution
    outcome, not an inferred side effect.
    """

    runtime = FakePlanExecutorRuntime()
    output = await run_preflight_orchestrator(
        question="How healthy is the Platform team?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos3300-unwired-team-health",
        recorder_factory=InvestigationRecorder,
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(runtime),
    )

    recorder = output.recorder
    assert isinstance(recorder, InvestigationRecorder)
    assert recorder.results == []
    # No authorized team named "Platform" is seeded, so this resolves
    # not_found -- itself a safe, correct outcome (proven elsewhere by the
    # CHAOS-3292 preflight suite). What this test adds: even had it resolved,
    # TEAM_HEALTH is equally absent from CORE_PLANS_BY_INTENT.
    assert QuestionIntentID.TEAM_HEALTH not in CORE_PLANS_BY_INTENT
    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
