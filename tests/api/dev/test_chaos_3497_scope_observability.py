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
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevError,
    DevScope,
    DevScopeResolution,
    DirectScope,
    ScopeResolutionOutcome,
    StreamEventType,
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
from dev_health_ops.llm.agent.contracts import AgentUsage
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
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


def test_the_disclosure_is_appended_once_and_never_evicts_a_warning() -> None:
    """Idempotent, and it refuses to spend a warning slot it does not have."""

    answer = DevAnswer.model_validate(positive_fixtures()["dev_answer.v1"])
    once = disclose_scope_widening(answer)
    twice = disclose_scope_widening(once)
    assert once.warnings == twice.warnings
    assert once.warnings[:-1] == list(answer.warnings)

    full = answer.model_copy(update={"warnings": [f"w{index}" for index in range(20)]})
    assert disclose_scope_widening(full).warnings == full.warnings


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
    assert subject_free.outcome is ScopeResolutionOutcome.ORGANIZATION_FALLBACK
    assert "organization" in subject_free.fallbacks

    assert _scope_request(_scope()).allow_organization_fallback is False


def _replay(result: OrchestratorResult):
    async def run_with_events(sink):
        del sink
        return result

    return run_with_events
