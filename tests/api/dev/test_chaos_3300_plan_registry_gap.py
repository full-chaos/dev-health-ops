"""CHAOS-3300 finding (2026-08-02): an intent whose plan_registry entry is
missing at runtime silently falls back to the legacy model-tool-choice loop
-- a genuine capability downgrade (a generic status_snapshot.v1 answer
instead of a governed evaluation) with no operational signal. This tests the
fix: orchestrator.run() now distinguishes "designed to be legacy"
(QuestionIntentID.BOUNDED_INVESTIGATION, per
preflight_outcomes.LEGACY_ONLY_QUESTION_INTENTS) from "should be
plan-governed but this runtime's registry doesn't carry it" (everything
else) -- only the latter is loud (a structured WARNING log record plus
ASK_DEV_PLAN_REGISTRY_GAP_TOTAL).

Team-lead ratification (2026-08-02, superseding an earlier, reverted
attempt at an honest "feature_not_enabled" early termination here): the
legacy fallback stays the TERMINAL behavior for both cases -- terminating
unsupported for a recognized-but-unwired intent (PORTFOLIO_STATUS today)
would regress live free-form traffic that previously degraded to
BOUNDED_INVESTIGATION and got a real, if ungoverned, answer. That
behavioral cliff is exactly what the epic's own §g sequencing defers to
the stack-5 guard cutover, once frames are proven. One rule until then: a
recognized-but-unwired intent falls back loudly, never terminally.

Driven through the real orchestrator seam (``run_preflight_orchestrator``),
never a diagnostic that inspects internals without exercising the seam --
matches the CHAOS-3295 acceptance suite's own house rule.
"""

from __future__ import annotations

import logging

import pytest

from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.preflight_outcomes import LEGACY_ONLY_QUESTION_INTENTS
from dev_health_ops.metrics.prometheus import ASK_DEV_PLAN_REGISTRY_GAP_TOTAL
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    run_preflight_orchestrator,
)
from tests._chaos_3295_plan_executor import FakePlanExecutorRuntime, executor_for


def test_bounded_investigation_is_the_only_legacy_only_intent():
    """Regression fence: adding a real plan for an intent means removing it
    from this set is a deliberate, reviewed edit (see that constant's own
    docstring) -- this pins today's membership so a silent addition/removal
    is caught.
    """

    assert LEGACY_ONLY_QUESTION_INTENTS == frozenset(
        {QuestionIntentID.BOUNDED_INVESTIGATION}
    )


@pytest.mark.asyncio
async def test_plan_registry_gap_is_loud_for_a_normally_plan_governed_intent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Simulates the exact CHAOS-3300 scenario directly: ENTITY_STATUS is a
    real, wired core plan, but this run's own ``plan_registry`` is built
    with it deliberately removed (standing in for "not wired on this
    runtime yet", the same shape PORTFOLIO_STATUS is in today). The run
    must still complete via the legacy fallback (ratified: no terminal
    behavior change before the stack-5 guard cutover) but must also emit
    the WARNING log record and increment the counter -- silence is the bug
    this closes.
    """

    gapped_registry = {
        intent: plan
        for intent, plan in CORE_PLANS_BY_INTENT.items()
        if intent != QuestionIntentID.ENTITY_STATUS
    }
    before = ASK_DEV_PLAN_REGISTRY_GAP_TOTAL.labels(intent="entity_status")._value.get()

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.api.dev.orchestrator"):
        output = await run_preflight_orchestrator(
            question="What's the status of the Ask Dev project?",
            entities=[(ORG_ID, ASK_DEV_PROJECT)],
            script_id="chaos3300-gap",
            plan_registry=gapped_registry,
            plan_executor=executor_for(FakePlanExecutorRuntime()),
        )

    # Behavior is unchanged: the run still completes with a real answer via
    # the legacy fallback -- only the operational signal is new.
    assert output.result.answer is not None
    assert any(
        record.levelno == logging.WARNING and "plan_registry_gap" in record.message
        for record in caplog.records
    ), "an intent with no registered plan must log a WARNING, not fail silently"

    after = ASK_DEV_PLAN_REGISTRY_GAP_TOTAL.labels(intent="entity_status")._value.get()
    assert after == before + 1, (
        "a missing plan for a normally plan-governed intent must increment "
        "ASK_DEV_PLAN_REGISTRY_GAP_TOTAL"
    )


@pytest.mark.asyncio
async def test_bounded_investigation_never_triggers_the_gap_signal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The designed fallthrough (no anchor matched -> BOUNDED_INVESTIGATION,
    question_interpreter.py's own "a miss degrades to this") must stay
    silent -- it was never a capability downgrade, it is the intended
    behavior for an unrecognized question.
    """

    before = ASK_DEV_PLAN_REGISTRY_GAP_TOTAL.labels(
        intent="bounded_investigation"
    )._value.get()

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.api.dev.orchestrator"):
        await run_preflight_orchestrator(
            question="Tell me something interesting",
            entities=[(ORG_ID, ASK_DEV_PROJECT)],
            script_id="chaos3300-bounded",
            plan_registry=CORE_PLANS_BY_INTENT,
            plan_executor=executor_for(FakePlanExecutorRuntime()),
        )

    assert not any(
        record.levelno == logging.WARNING and "plan_registry_gap" in record.message
        for record in caplog.records
    ), (
        "BOUNDED_INVESTIGATION's designed legacy fallthrough must never log the gap warning"
    )

    after = ASK_DEV_PLAN_REGISTRY_GAP_TOTAL.labels(
        intent="bounded_investigation"
    )._value.get()
    assert after == before, (
        "BOUNDED_INVESTIGATION's designed legacy fallthrough must never "
        "increment ASK_DEV_PLAN_REGISTRY_GAP_TOTAL"
    )
