"""CHAOS-3421: a model that leaks a raw internal outcome token (e.g.
``forbidden_or_not_found``, what ``resolve_scope.v1`` returns for a named
subject the catalog does not confirm) on an organization-wide-fallback run
must never fail closed to the generic ``internal_error`` -- it must land on
the SAME graceful, honest terminal the legacy narration guard already ships
for this exact scenario class (``resolve_scope_not_found``).

Live incident: a prior turn's clarification named "Dev Health Agent Context
Runtime (Context Fabric)"; the user asked again with that exact, unquoted
label. The catalog-matching gap (CHAOS-3407, currently paused -- see that
ticket) is untouched here and this run still MISSES the subject -- the
question this file answers is only "does a miss on this specific path fail
SAFELY", not "does it resolve".

Two closely related but distinct failure classes exist on this exact
preflight branch (``subject_preflight.py``'s ``unresolved_untyped`` ->
``proceeded_unresolved_bare_name``), and both are covered:

1. The model's answer NARRATES the unresolved bare name in its own prose --
   already covered, unchanged, by the legacy backstop's
   ``narrated_unresolved_entity`` guard (test_chaos_3292_review_findings.py).
2. The model calls ``resolve_scope.v1`` (still in ``allowed_tools`` on this
   branch before this fix), gets back the raw ``forbidden_or_not_found``
   enum value, and echoes it into its own summary/claim text WITHOUT
   narrating the bare name itself -- the narration guard's own text-
   similarity check never fires (nothing about "Nightfall" appears in the
   leaked text), so the run reaches the unconditional completion path,
   where the internal-token leak scan is the ONLY thing left to catch it --
   and used to fail closed generically. This file's own defect.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.llm.agent.contracts import AgentFinalAnswer
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    CHAOS_3388_ACR_PROJECT,
    ORG_ID,
    grounded_answer_payload,
    run_preflight_orchestrator,
    scope_dict,
    status_then_answer,
)

#: The exact internal outcome token this incident leaked -- a real member of
#: ``ScopeResolutionOutcome``, never invented for this test.
_LEAKED_TOKEN = "forbidden_or_not_found"


def _leaking_but_not_narrating(script_id: str) -> list[ScriptedStep]:
    """A scripted final answer that echoes the raw resolve_scope.v1 outcome
    token, but never mentions the bare name itself -- so the legacy guard's
    own ``narrated_unresolved_entity`` text-similarity check has nothing to
    catch, and only the internal-token leak scan is left standing between
    this and the wire.
    """

    steps = status_then_answer(script_id)
    steps[-1] = ScriptedStep(
        decision=AgentFinalAnswer(
            grounded_answer_payload(
                script_id=script_id,
                summary=(
                    f"The scope resolution tool returned {_LEAKED_TOKEN} for "
                    "the requested subject."
                ),
                validity_scope=scope_dict(),
            )
        )
    )
    return steps


@pytest.mark.asyncio
async def test_a_leaked_resolution_outcome_on_a_bare_name_fallback_is_graceful() -> (
    None
):
    output = await run_preflight_orchestrator(
        question="How is Nightfall doing?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script=_leaking_but_not_narrating,
        script_id="leak-not-narrate",
    )

    # The setup control: this run really did take the unresolved-bare-name
    # organization-wide-fallback branch, and the narration guard really did
    # NOT fire (proving the leak scan, not the guard, is what this test
    # exercises).
    assert output.preflight_outcomes() == ("proceeded_unresolved_bare_name",)
    assert output.guard_reasons() == ()

    # The graceful terminal, never the generic fail-closed one.
    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.state is not RunState.FAILED
    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert output.result.error.code != "internal_error"
    assert output.result.error.safe_message == "The requested scope was not found."

    # The load-bearing negative: the raw token itself never reaches
    # anything persisted or returned -- not the discarded answer (there
    # must not even BE one), not the error, not the run's own error code.
    assert output.result.answer is None
    assert _LEAKED_TOKEN not in output.result.error.safe_message
    assert _LEAKED_TOKEN not in output.result.error.code


@pytest.mark.asyncio
async def test_an_unrelated_leak_off_this_branch_still_fails_closed() -> None:
    """Anti-vacuity: the graceful reroute is scoped to the ONE preflight
    branch that sets ``legacy_guard_required`` (``proceeded_unresolved_
    bare_name``) -- a leak on an ordinary, fully-resolved run must still
    fail closed exactly as before. Claims dropped entirely (rather than
    reusing the stock fixture's own claim) so this stays focused on the
    leak-routing decision, never an unrelated grounding-floor/validity-
    scope mismatch on a committed PROJECT scope.
    """

    def leaking_on_a_resolved_run(script_id: str) -> list[ScriptedStep]:
        steps = status_then_answer(script_id)
        payload = grounded_answer_payload(
            script_id=script_id,
            summary=f"Evaluation returned {_LEAKED_TOKEN} unexpectedly.",
            validity_scope=scope_dict(),
        )
        payload["claims"] = []
        steps[-1] = ScriptedStep(decision=AgentFinalAnswer(payload))
        return steps

    output = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script=leaking_on_a_resolved_run,
        script_id="leak-resolved-run",
    )

    assert output.preflight_outcomes() == ("proceeded_committed_subject",)
    assert output.result.state is RunState.FAILED
    assert output.result.error is not None
    assert output.result.error.code == "internal_error"


# ---------------------------------------------------------------------------
# The kill shot: the live incident, replicated end to end. Matching itself
# (CHAOS-3407) is PAUSED by explicit user directive -- this run still MISSES
# the subject, on purpose, exactly as team-lead's revised scope describes.
# What must be true regardless: the miss lands on a graceful, disclosed
# organization-wide fallback, never a crash.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_live_incident_question_never_crashes_even_though_it_still_misses() -> (
    None
):
    """A prior turn's clarification named the real CHAOS-3388 fixture entity
    ("Dev Health Agent Context Runtime (Context Fabric)"); the user asks
    again with that exact, unquoted label + "project". Preflight runs
    unconditionally every turn (CHAOS-3407's own diagnosis), so a single-
    turn run exercises the identical preflight/orchestrator code path a
    multi-turn conversation would -- no conversation history is required to
    prove this.

    With matching paused, the parenthetical still breaks kind-noun
    adjacency (unchanged), the truncated bare-name span still fuzzy-matches
    the one real project as a SOLE, non-exact candidate (proven empirically
    against the real ScopeResolutionService/SeededCatalog: this lands on
    ``proceeded_unresolved_bare_name``, the same organization-wide-fallback
    branch as the "Nightfall" cases above), and the model still calls
    resolve_scope.v1 in the pre-CHAOS-3421 world and leaks its raw outcome.
    Post-3421: RESOLVE_SCOPE is withheld on this branch (nothing to leak
    from that source), and even a leak reaching the wire some other way
    routes to the graceful ``scope_not_found`` terminal, never
    ``internal_error``.
    """

    label = CHAOS_3388_ACR_PROJECT.label

    def leaking_on_the_real_incident_shape(script_id: str) -> list[ScriptedStep]:
        steps = status_then_answer(script_id)
        steps[-1] = ScriptedStep(
            decision=AgentFinalAnswer(
                grounded_answer_payload(
                    script_id=script_id,
                    summary=(
                        f"The scope resolution tool returned {_LEAKED_TOKEN} "
                        "for the requested subject."
                    ),
                    validity_scope=scope_dict(),
                )
            )
        )
        return steps

    output = await run_preflight_orchestrator(
        question=f"What is the status of the {label} project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, CHAOS_3388_ACR_PROJECT)],
        script=leaking_on_the_real_incident_shape,
        script_id="killshot-live-incident",
    )

    # The miss, by design and disclosed: an organization-wide fallback, not
    # a clarification or a resolved subject -- CHAOS-3407 (matching) stays
    # untouched and unfixed here on purpose.
    assert output.preflight_outcomes() == ("proceeded_unresolved_bare_name",)

    # The graceful landing CHAOS-3421 guarantees, never a crash and never
    # the raw enum anywhere on the wire.
    assert output.result.state is not RunState.FAILED
    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.error is not None
    assert output.result.error.code != "internal_error"
    assert output.result.error.code == "scope_not_found"
    assert _LEAKED_TOKEN not in output.result.error.safe_message
    assert _LEAKED_TOKEN not in output.result.error.code
    assert output.result.answer is None
