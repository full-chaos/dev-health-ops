"""CHAOS-3497: a run's scope decision must be observable on EVERY terminal.

Two defects in one family, both about a scope decision that was made and then
not disclosed.

1. ``scope.resolved`` was emitted only inside ``if result.answer is not None``.
   A run that resolved scope and then terminated without an answer
   (insufficient_evidence, a refusal, a not-found) published nothing about
   what it resolved -- so an auditor reading the wire could not tell a failed
   run that resolved to an exact subject from one that silently widened to
   organization scope. That is the whole point of the no-silent-widening
   audit family, and the widen-then-fail run is exactly the shape it exists
   to catch.

2. When the preflight widens to organization scope because a named bare
   subject went unresolved, the widening was recorded machine-readably
   (``fallbacks == ["organization"]``) but said nowhere a person reading the
   answer would see it.

Every test here observes the state the system exists to reach -- the frame on
the wire, the sentence in the rendered answer -- never that a code path ran.
Negative controls sit beside the positives so a change that discloses
*everything* (which would be its own defect: an ordinary organization-wide
question was never widened away from anything) fails too.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevError,
    DevScope,
    DevScopeResolution,
    DevToolResult,
    DirectScope,
    ScopeResolutionOutcome,
    StreamEventType,
    ToolID,
)
from dev_health_ops.api.dev.contracts_v2 import PublicOutcome
from dev_health_ops.api.dev.no_match_terminal import (
    SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE,
    disclose_scope_widening,
)
from dev_health_ops.api.dev.orchestrator import (
    OrchestratorEvent,
    OrchestratorResult,
    RunState,
)
from dev_health_ops.api.dev.streaming import (
    encode_sse,
    stream_orchestrator,
    validate_completed_stream,
)
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent.contracts import (
    AgentFinalAnswer,
    AgentToolRequest,
    AgentUsage,
)
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    ORG_ID,
    run_preflight_orchestrator,
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _scope() -> DevScope:
    return DevScope.model_validate(positive_fixtures()["dev_scope.v1"])


def _resolution(outcome: ScopeResolutionOutcome) -> DevScopeResolution:
    """A resolution in ``outcome``, valid for that outcome's own payload rules."""

    scope = _scope()
    resolved: DevScope | None = None
    if outcome in {
        ScopeResolutionOutcome.EXACT,
        ScopeResolutionOutcome.FILTERED,
        ScopeResolutionOutcome.INHERITED,
    }:
        resolved = scope
    elif outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK:
        resolved = DevScope(
            schema_version="dev_scope.v1",
            organization_id=scope.organization_id,
            direct_scope=DirectScope.ORGANIZATION,
            repositories=[],
            entity_refs=[],
            team_ids=[],
            time_range=scope.time_range,
            comparison_range=scope.comparison_range,
            surface_context=None,
        )
    return DevScopeResolution(
        schema_version="dev_scope_resolution.v1",
        requested_scope=scope,
        resolved_scope=resolved,
        outcome=outcome,
        fallbacks=(
            ["organization"]
            if outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK
            else []
        ),
        resolved_at=datetime(2026, 8, 6, 12, 0, tzinfo=UTC),
    )


def _error_result(*, scope_resolution: DevScopeResolution | None) -> OrchestratorResult:
    error = DevError(
        schema_version="dev_error.v1",
        request_id="request_01",
        code="insufficient_evidence",
        safe_message="There was not enough evidence to answer.",
        retryable=False,
    )
    return OrchestratorResult(
        run_id="run_01",
        state=RunState.INSUFFICIENT_EVIDENCE,
        answer=None,
        error=error,
        events=(OrchestratorEvent(RunState.INSUFFICIENT_EVIDENCE, error.code),),
        usage=AgentUsage(),
        tool_call_count=0,
        provider_fingerprint=None,
        model_fingerprint=None,
        scope_resolution=scope_resolution,
    )


def _answer_result() -> OrchestratorResult:
    answer = DevAnswer.model_validate(positive_fixtures()["dev_answer.v1"])
    return OrchestratorResult(
        run_id="run_01",
        state=RunState.COMPLETED,
        answer=answer,
        error=None,
        events=(OrchestratorEvent(RunState.COMPLETED),),
        usage=AgentUsage(),
        tool_call_count=1,
        provider_fingerprint="provider_01",
        model_fingerprint="model_01",
        scope_resolution=answer.resolved_scope,
    )


def _bare_answer(script_id: str) -> dict:
    """The shared answer fixture, model-fingerprinted for ``script_id``."""

    from tests.api.dev.test_orchestrator import _answer

    return _answer(script_id=script_id)


async def _stream(result: OrchestratorResult, states: tuple[RunState, ...]):
    async def run(sink):
        for state in states:
            await sink(OrchestratorEvent(state))
        return result

    return [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=run, cancellation=asyncio.Event()
        )
    ]


# ---------------------------------------------------------------------------
# 1. the wire
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_answer_terminal_publishes_the_scope_it_actually_resolved() -> None:
    """The defect, stated as the state the stream must reach.

    An ``unresolved`` outcome is the one the corpus's own
    ``scope_resolution_outcome_in`` cases declare and could never observe: it
    only ever occurs on a run that terminates WITHOUT an answer.
    """

    events = await _stream(
        _error_result(scope_resolution=_resolution(ScopeResolutionOutcome.UNRESOLVED)),
        (RunState.ACCEPTED, RunState.RESOLVING_SCOPE, RunState.INSUFFICIENT_EVIDENCE),
    )

    validate_completed_stream(events)
    resolved = [
        event for event in events if event.event is StreamEventType.SCOPE_RESOLVED
    ]
    assert len(resolved) == 1
    assert resolved[0].scope_resolution is not None
    assert resolved[0].scope_resolution.outcome is ScopeResolutionOutcome.UNRESOLVED
    # It is the RUN's own outcome on the wire, not a placeholder.
    assert b'"outcome":"unresolved"' in encode_sse(resolved[0])


@pytest.mark.asyncio
async def test_scope_resolved_precedes_the_error_terminal() -> None:
    """Ordering, which every consumer of this stream enforces positionally.

    ``contracts.validate_stream`` and web's ``client.ts`` both require the
    terminal frame to be immediately followed by ``done``; a ``scope.resolved``
    emitted after ``error`` is a hard client-side failure, not a cosmetic one.
    """

    events = await _stream(
        _error_result(
            scope_resolution=_resolution(ScopeResolutionOutcome.ORGANIZATION_FALLBACK)
        ),
        (RunState.ACCEPTED, RunState.RESOLVING_SCOPE, RunState.FAILED),
    )

    kinds = [event.event for event in events]
    assert kinds[-2:] == [StreamEventType.ERROR, StreamEventType.DONE]
    assert kinds.index(StreamEventType.SCOPE_RESOLVED) < kinds.index(
        StreamEventType.ERROR
    )
    assert [event.sequence for event in events] == list(range(len(events)))


@pytest.mark.asyncio
async def test_a_run_that_never_resolved_scope_emits_no_scope_frame() -> None:
    """Negative control: absent is honest when nothing was ever resolved.

    A run cancelled or faulted before ``resolve`` returned has no scope
    decision to publish, and inventing one would be worse than the gap this
    ticket closes.
    """

    events = await _stream(
        _error_result(scope_resolution=None),
        (RunState.ACCEPTED, RunState.CANCELLED),
    )

    validate_completed_stream(events)
    assert not [
        event for event in events if event.event is StreamEventType.SCOPE_RESOLVED
    ]


@pytest.mark.asyncio
async def test_answer_path_wire_shape_is_unchanged() -> None:
    """Control on the path that already worked -- it must not move."""

    result = _answer_result()
    assert result.answer is not None
    events = await _stream(
        result,
        (RunState.ACCEPTED, RunState.RESOLVING_SCOPE, RunState.COMPLETED),
    )

    validate_completed_stream(events)
    kinds = [event.event for event in events]
    assert kinds[0] is StreamEventType.RUN_STARTED
    assert kinds[-2:] == [StreamEventType.ANSWER_COMPLETED, StreamEventType.DONE]
    resolved = [
        event for event in events if event.event is StreamEventType.SCOPE_RESOLVED
    ]
    assert len(resolved) == 1
    # Still the answer's own embedded resolution, byte for byte.
    assert resolved[0].scope_resolution == result.answer.resolved_scope
    assert kinds.index(StreamEventType.SCOPE_RESOLVED) < kinds.index(
        StreamEventType.ANSWER_COMPLETED
    )


# ---------------------------------------------------------------------------
# 2. the orchestrator actually carries it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orchestrator_carries_its_resolution_on_a_no_answer_terminal() -> None:
    """End to end through the real orchestrator, not a hand-built result.

    A resolver that returns ``unresolved`` drives the run to the
    ``scope_not_found`` terminal -- the exact production shape the corpus's
    ``scope.outcome.unresolved`` case declares -- and the resolution must
    survive onto the result and out onto the wire.
    """

    from tests.api.dev.test_orchestrator import _orchestrator, _run

    async def resolve(**_values) -> DevScopeResolution:
        return _resolution(ScopeResolutionOutcome.UNRESOLVED)

    orchestrator = _orchestrator([], script_id="chaos-3497", scope_resolver=resolve)
    result = await _run(orchestrator)

    assert result.state is RunState.INSUFFICIENT_EVIDENCE
    assert result.answer is None
    assert result.error is not None and result.error.code == "scope_not_found"
    assert result.scope_resolution is not None
    assert result.scope_resolution.outcome is ScopeResolutionOutcome.UNRESOLVED

    async def run_with_events(sink):
        del sink
        return result

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01",
            run_with_events=run_with_events,
            cancellation=asyncio.Event(),
        )
    ]
    validate_completed_stream(events)
    assert [
        event.scope_resolution.outcome
        for event in events
        if event.event is StreamEventType.SCOPE_RESOLVED
        and event.scope_resolution is not None
    ] == [ScopeResolutionOutcome.UNRESOLVED]


@pytest.mark.asyncio
async def test_the_deferred_corpus_invariants_become_satisfiable() -> None:
    """The claim the ticket actually makes, executed rather than argued.

    ~15 corpus cases are blocked because their invariants read
    ``scope.resolved`` off the stream and their expected terminal is a
    non-answer. Both blocked checkers are run here against a REAL stream
    produced by the real orchestrator and the real SSE projection, shaped
    exactly as ``test_wave4_corpus_runner_live._post_sse`` shapes it -- so
    this measures the same bytes the corpus runner measures, not a
    hand-authored fixture of what they might look like.

    ``no_unauthorized_candidate_surfaces`` is the security one: it is
    designed to FAIL, not skip, when nothing was observed. That is exactly
    what it did on every one of these cases.
    """

    from scripts.acceptance.corpus.invariants import CHECKS, InvariantContext
    from tests.api.dev.test_orchestrator import _orchestrator, _run

    async def resolve(**_values) -> DevScopeResolution:
        return _resolution(ScopeResolutionOutcome.UNRESOLVED)

    result = await _run(
        _orchestrator([], script_id="chaos-3497-corpus", scope_resolver=resolve)
    )
    assert result.answer is None, "a non-answer terminal is the whole point"

    frames = [
        event
        async for event in stream_orchestrator(
            run_id="run_01",
            run_with_events=_replay(result),
            cancellation=asyncio.Event(),
        )
    ]
    validate_completed_stream(frames)
    events = [
        {"event": frame.event.value, "data": frame.model_dump(mode="json")}
        for frame in frames
    ]
    context = InvariantContext(
        resolution_path=None,
        public_outcome=None,
        events=events,
        expectations={},
    )

    outcome_result = CHECKS["scope_resolution_outcome_in"](
        {"allowed": ["unresolved"]}, context
    )
    assert outcome_result.passed, outcome_result.detail

    candidate_result = CHECKS["no_unauthorized_candidate_surfaces"](
        {"authorized_entity_ids": ["entity_01"]}, context
    )
    assert candidate_result.passed, candidate_result.detail
    assert "was not measured" not in candidate_result.detail


# ---------------------------------------------------------------------------
# 2b. the preflight TERMINATE path -- the shape MOST of the blocked corpus
#     cases actually take, and the one a first cut of this fix got wrong
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_preflight_not_found_publishes_not_found_not_the_stale_scope() -> None:
    """The run's OWN outcome for the subject the question named.

    Reproduced before the fix: this published ``inherited`` -- the run's
    original top-level resolve, which by construction can only ever be
    healthy, because every unhealthy outcome already returned earlier. That
    put "scope resolved: inherited" one frame ahead of an error saying the
    named subject could not be found: the exact juxtaposition
    ``no_match_terminal``'s module docstring says the PRD prohibits.
    """

    output = await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos-3497-not-found",
    )

    assert output.preflight_outcomes() == ("unresolved_no_authorized_match",)
    assert output.result.answer is None
    published = output.result.scope_resolution
    assert published is not None
    assert published.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert published.resolved_scope is None


@pytest.mark.asyncio
async def test_an_ambiguous_preflight_publishes_the_candidates_it_showed() -> None:
    """The security invariant gets a real measurement, not a vacuous one.

    ``no_unauthorized_candidate_surfaces`` reads
    ``scope_resolution.candidates``. Before the fix this terminal published
    the stale healthy resolution, which v1 forbids from carrying candidates
    at all -- so the check saw "one scope.resolved event, zero candidates"
    and PASSED, having measured nothing. That is worse than the "not
    measured" failure it replaced, and is the vacuity CHAOS-3219 removed.

    The negative half matters as much as the positive: an authorized set
    that excludes these candidates must FAIL, or "passed" means nothing.
    """

    from scripts.acceptance.corpus.invariants import CHECKS, InvariantContext

    output = await run_preflight_orchestrator(
        question="What's the status of the Atlas project?",
        entities=[(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)],
        script_id="chaos-3497-ambiguous",
    )

    assert output.preflight_outcomes() == ("unresolved_ambiguous_candidates",)
    published = output.result.scope_resolution
    assert published is not None
    assert published.outcome is ScopeResolutionOutcome.AMBIGUOUS
    surfaced = sorted(
        candidate.entity_ref.entity_id for candidate in published.candidates
    )
    assert surfaced == sorted(
        [ATLAS_PROJECT_ONE.canonical_id, ATLAS_PROJECT_TWO.canonical_id]
    ), "the candidates on the wire must be the ones the user was shown"

    frames = [
        event
        async for event in stream_orchestrator(
            run_id="run_01",
            run_with_events=_replay(output.result),
            cancellation=asyncio.Event(),
        )
    ]
    validate_completed_stream(frames)
    context = InvariantContext(
        resolution_path=None,
        public_outcome=None,
        events=[
            {"event": frame.event.value, "data": frame.model_dump(mode="json")}
            for frame in frames
        ],
        expectations={},
    )

    authorized = CHECKS["no_unauthorized_candidate_surfaces"](
        {"authorized_entity_ids": surfaced}, context
    )
    assert authorized.passed, authorized.detail
    assert "was not measured" not in authorized.detail

    # The discriminating half: if the check could not see these candidates
    # it would pass here too, and would be worthless.
    leaked = CHECKS["no_unauthorized_candidate_surfaces"](
        {"authorized_entity_ids": ["some-other-entity"]}, context
    )
    assert not leaked.passed
    assert "unauthorized candidate id(s) surfaced" in leaked.detail


@pytest.mark.asyncio
async def test_a_later_failed_lookup_does_not_overwrite_a_real_commit() -> None:
    """A miss for one name must not be reported as the run's scope decision.

    ``resolve_scope.v1`` records EVERY attempt that produced an outcome, but
    only a successful one re-commits the run's scope. A run that committed a
    real project and then failed to look up some other name still executed
    under that project -- publishing the later miss would invent a
    resolution failure that never happened, the mirror image of the defect
    this ticket closes. Mirrors the diversion guard the answer path already
    applies (organization scope only).
    """

    from tests.api.dev.test_orchestrator import (
        _answer,
        _not_found_resolution,
        _orchestrator,
        _organization_resolution,
        _project_resolution,
        _run,
    )

    async def resolve(**_values) -> DevScopeResolution:
        return _organization_resolution()

    committed = _project_resolution()
    missed = _not_found_resolution()
    results = [committed, missed]

    async def execute(_context, request):
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        if request.tool_id is ToolID.RESOLVE_SCOPE:
            payload["scope_resolution"] = results.pop(0).model_dump(mode="json")
        return DevToolResult.model_validate(payload)

    script_id = "chaos-3497-stale-miss"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "Ask Dev project", "limit": 25},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="resolve_scope.v1",
                        arguments={"query": "Nightfall project", "limit": 25},
                        call_id="tool_call_02",
                    )
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer(script_id=script_id, invalid_schema=True)
                    )
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer(script_id=script_id, invalid_schema=True)
                    )
                ),
            ],
            script_id=script_id,
            registry=AskDevToolRegistry({tool_id: execute for tool_id in ToolID}),
            scope_resolver=resolve,
        )
    )

    assert result.answer is None, "this run must reach a no-answer terminal"
    assert result.scope_resolution is not None
    assert result.scope_resolution.outcome is ScopeResolutionOutcome.EXACT, (
        "the run executed under the committed project, not the later miss"
    )
    assert result.scope_resolution.model_dump(mode="json") == committed.model_dump(
        mode="json"
    )


# ---------------------------------------------------------------------------
# 3. the prose disclosure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_widened_run_says_so_in_user_facing_prose() -> None:
    """A named bare subject that went unresolved must be disclosed in words.

    Asserted on all three surfaces a reader can actually reach: the persisted
    answer's warnings, the persisted frame's ``limitations`` (what the web
    answer renders), and the ``warning`` frames on the wire.
    """

    project_ref = {
        "entity_type": "project",
        "entity_id": ASK_DEV_PROJECT.canonical_id,
        "display_label": ASK_DEV_PROJECT.label,
        "repository_id": None,
    }
    output = await run_preflight_orchestrator(
        question="How is Nightfall doing?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        scope_overrides={"direct_scope": "project", "entity_refs": [project_ref]},
        script_id="chaos-3497-widened",
    )

    assert output.preflight_outcomes() == ("proceeded_unresolved_bare_name",)
    answer = output.result.answer
    assert answer is not None, "this case answers organization-wide today"
    assert answer.resolved_scope.outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK
    assert answer.resolved_scope.fallbacks == ["organization"]

    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE in answer.warnings

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01",
            run_with_events=_replay(output.result),
            cancellation=asyncio.Event(),
        )
    ]
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE in [
        event.warning for event in events if event.event is StreamEventType.WARNING
    ]


@pytest.mark.asyncio
async def test_the_widening_marker_alone_does_not_trigger_the_disclosure() -> None:
    """The test that actually pins WHICH field the trigger reads.

    The sibling organization-wide control does not discriminate this: that
    scenario resolves ``inherited`` with empty ``fallbacks``, so a predicate
    swapped to ``fallbacks`` would not fire there either and the control
    would stay green while the copy became wrong. Measured, not assumed --
    the swap was applied and every test still passed.

    This one puts a run in the one state that separates the two predicates:
    ``ORGANIZATION_FALLBACK`` with ``fallbacks == ["organization"]``, and NO
    preflight (so ``legacy_guard_required`` is false because no bare name
    went unresolved -- nothing was widened away from anything). Keyed on the
    marker, this run would be told its subject was missed. It must not be.
    """

    from tests.api.dev.test_orchestrator import (
        _answer_with_no_claims,
        _orchestrator,
        _run,
    )

    async def resolve(**_values) -> DevScopeResolution:
        return _resolution(ScopeResolutionOutcome.ORGANIZATION_FALLBACK)

    script_id = "chaos-3497-marker-only"
    result = await _run(
        _orchestrator(
            [
                ScriptedStep(
                    decision=AgentToolRequest(
                        tool_id="status_snapshot.v1",
                        arguments={"limit": 25},
                        call_id="tool_call_01",
                    )
                ),
                ScriptedStep(
                    decision=AgentFinalAnswer(
                        _answer_with_no_claims(script_id=script_id)
                    )
                ),
            ],
            script_id=script_id,
            scope_resolver=resolve,
        )
    )

    assert result.answer is not None
    assert (
        result.answer.resolved_scope.outcome
        is ScopeResolutionOutcome.ORGANIZATION_FALLBACK
    )
    assert result.answer.resolved_scope.fallbacks == ["organization"]
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE not in result.answer.warnings, (
        "the disclosure trigger moved from the preflight's "
        "`legacy_guard_required` to the `fallbacks` marker. This run named no "
        "subject, so nothing was widened away from anything -- telling this "
        "reader their subject could not be matched states something false. "
        "Fix the predicate in orchestrator.finish(), not this test."
    )


def test_the_disclosure_reaches_the_frame_a_reader_actually_sees() -> None:
    """``warnings`` is the channel because the frame's ``limitations`` mirror it.

    Asserted here on a frame that really builds rather than through
    ``run_preflight_orchestrator``: that harness's answer is the shared
    ``dev_answer.v1`` fixture, whose unsigned evidence refs make
    ``wrap_legacy_answer_as_frame`` fall back to an error frame -- a
    pre-existing fixture gap documented in ``test_orchestrator.
    _answer_with_signed_evidence``, not a property of this change. Asserting
    through it would have been a test that cannot fail for the right reason.
    """

    from dev_health_ops.api.dev import terminal_frames
    from tests.api.dev.test_orchestrator import _answer_with_signed_evidence

    answer = DevAnswer.model_validate(
        _answer_with_signed_evidence(script_id="chaos-3497-frame")
    )
    disclosed = disclose_scope_widening(answer)

    frame = terminal_frames.wrap_legacy_answer_as_frame(disclosed, run_id="run_01")
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE in frame.limitations
    assert frame.public_outcome is PublicOutcome.ANSWERED_WITH_GAPS


def test_the_disclosure_is_added_once_and_keeps_every_warning_it_can() -> None:
    """Idempotent, and it costs a producer warning only at the bound.

    CHAOS-3531 CORRECTED this test, and the correction is the point. It used
    to assert ``disclose_scope_widening(full).warnings == full.warnings`` --
    that at the twenty-warning bound the answer came back untouched. That
    pinned the DEFECT as intended behaviour: a run that widened to
    organization scope could answer organization-wide with no prose
    disclosure at all, while this ticket's own write-up claimed the widening
    is "said out loud".

    The half worth keeping is here unchanged (added once, idempotent, nothing
    else disturbed). The half that encoded the bug is replaced by its
    opposite: at the bound the disclosure displaces the last producer warning
    rather than yielding to it. See ``test_chaos_3531_disclosure_bound.py``
    for the full statement of that rule and why the trade is the right one.
    """

    # A REAL producer warning, not the fixture's empty list: asserting
    # `set(answer.warnings) <= set(once.warnings)` against `[]` is vacuously
    # true and would pass even if the helper discarded everything below the
    # bound (adversarial review, LOW).
    answer = DevAnswer.model_validate(positive_fixtures()["dev_answer.v1"]).model_copy(
        update={"warnings": ["a warning the producer chose"]}
    )
    once = disclose_scope_widening(answer)
    twice = disclose_scope_widening(once)
    assert once.warnings == twice.warnings
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE in once.warnings
    assert "a warning the producer chose" in once.warnings, (
        "below the bound nothing a producer wrote may be lost"
    )
    assert len(once.warnings) == 2

    full = answer.model_copy(update={"warnings": [f"w{index}" for index in range(20)]})
    disclosed = disclose_scope_widening(full)
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE in disclosed.warnings, (
        "the disclosure must survive a saturated warning list -- an "
        "undisclosed widening is the defect CHAOS-3531 closed"
    )
    assert len(disclosed.warnings) == 20


@pytest.mark.asyncio
async def test_an_ordinary_organization_wide_run_is_not_told_it_was_widened() -> None:
    """Negative control: a run that widened nothing says nothing."""

    output = await run_preflight_orchestrator(
        question="How are we doing on delivery this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos-3497-orgwide",
    )

    assert output.preflight_outcomes() == ("proceeded_organization_wide",)
    answer = output.result.answer
    assert answer is not None
    assert SCOPE_WIDENED_TO_ORGANIZATION_SENTENCE not in answer.warnings


@pytest.mark.asyncio
async def test_the_widening_marker_alone_would_be_the_wrong_predicate() -> None:
    """Why the disclosure is keyed on the preflight, not on ``fallbacks``.

    ``fallbacks == ["organization"]`` has two producers. The second one --
    ``scope_service.resolve``'s ``used_org_fallback`` -- fires for a request
    that named NO subject at all and was allowed to default to the
    organization; nothing was widened away from anything there, so keying
    user-facing copy on the marker would state something false.

    That producer is unreachable from Ask Dev today, and this pins BOTH halves
    of that claim: the marker really is produced subject-free, and production
    really does close the door. Without the second assertion the first is just
    a fact about a module nobody calls that way.
    """

    from dev_health_ops.api.dev.production_runtime import _scope_request
    from dev_health_ops.api.dev.scope_service import (
        ScopeResolutionService,
        ScopeResolveRequest,
    )
    from tests.api.dev.test_scope_service import FakeCatalog

    service = ScopeResolutionService(FakeCatalog([]))
    subject_free = await service.resolve(
        org_id="org-a",
        permission_fingerprint="perm-a",
        request=ScopeResolveRequest(allow_organization_fallback=True),
    )
    assert subject_free.outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK, (
        "HALF ONE MOVED: scope_service no longer produces the widening marker "
        "for a subject-free request. If that producer is genuinely gone, the "
        "marker has one producer again and keying the disclosure on it would "
        "become safe -- re-derive the trigger deliberately, do not delete this."
    )
    assert "organization" in subject_free.fallbacks

    assert _scope_request(_scope()).allow_organization_fallback is False, (
        "HALF TWO MOVED: production now ASKS for the organization fallback, so "
        "the second producer is reachable from Ask Dev and an ordinary "
        "org-wide answer can carry fallbacks==['organization'] with no subject "
        "ever missed. The disclosure must stay keyed on the preflight's "
        "`legacy_guard_required`; anything reading the marker now lies."
    )


def _replay(result: OrchestratorResult):
    async def run_with_events(sink):
        del sink
        return result

    return run_with_events
