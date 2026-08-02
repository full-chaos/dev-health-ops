"""RED-first reachability proof for CHAOS-3297 C0.

CHAOS-3299's v2 replay branch (``router._replayed_result`` /
``router.py`` ``contract_generation == "v2"`` gate) can only ever be
exercised by a run that actually persisted a ``dev_answer_frame.v1`` row via
``PersistenceRunRecorder.record_frame``. Today the subject preflight (CHAOS-
3292) builds a fully validated ``DevAnswerV2`` no-answer frame in
``preflight_outcomes.build_preflight_answer`` and the orchestrator's
TERMINATE branch (``orchestrator.py`` around the ``project_preflight_error``
call) discards it -- only the projected v1 ``DevError`` is used. No
production code path ever calls ``record_frame`` for a preflight
termination, so ``dev_runs.contract_generation`` never becomes ``'v2'`` and
the v2 replay branch is unreachable dead code.

This module drives the *real* ``DevOrchestrator`` + real ``SubjectPreflight``
through the real ``/api/v1/dev/conversations/{id}/messages`` endpoint (not
the hand-rolled ``PreflightNoAnswerRuntime`` fake in ``test_router.py``,
which manually calls ``recorder.record_frame`` and therefore cannot catch
this gap) and asserts the frame the preflight already built lands in
Postgres.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dev import router as dev_router_module
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    no_answer_answer_fixture,
    positive_fixtures,
)
from dev_health_ops.api.dev.contracts import (
    DevContractVersions,
    DevScope,
    DevScopeResolution,
    ScopeResolutionOutcome,
)
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame as FrameContract
from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.runtime import BoundedDevRuntime
from dev_health_ops.api.dev.scope_service import (
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
from dev_health_ops.llm.agent.scripted import ScriptedAgentProvider
from dev_health_ops.models.dev_persistence import DevAnswerFrame, DevRun
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    SeededCatalog,
    fixed_now,
    recording_registry,
    sequential_ids,
)
from tests.api.dev.test_router import (  # noqa: F401
    _parse_sse_events,
    _scope_payload,
    dev_api_context,
)

pytestmark = pytest.mark.asyncio


def _test_versions() -> DevContractVersions:
    return DevContractVersions(
        prompt_version="ask_dev_prompt.v1",
        tool_contract_version="ask_dev_tools.v1",
        metric_definition_version="ask_dev_metrics.v1",
        query_version="ask_dev_queries.v1",
    )


async def assert_frame_persisted(session: AsyncSession, run_id: uuid.UUID) -> None:
    """N0: prove one terminal run actually reached the CHAOS-3299 v2 replay path.

    A run that only streamed a correct v1 error is not enough -- the frame
    row, the run's ``contract_generation`` tag, and the run's own
    ``public_outcome`` must all agree, and the persisted payload must still
    validate as a ``dev_answer_frame.v1``. Any one of those missing means
    ``router._replayed_result``'s ``== "v2"`` branch stays unreachable for
    this run, silently falling back to the "did not complete" replay shape.
    """

    frame = await session.scalar(
        select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
    )
    assert frame is not None, f"no dev_answer_frames row was persisted for run {run_id}"

    run = await session.get(DevRun, run_id)
    assert run is not None, f"no dev_runs row for run {run_id}"
    assert run.contract_generation == "v2", (
        f"dev_runs.contract_generation was {run.contract_generation!r}, expected 'v2' "
        "-- record_frame was never called, or was called without the "
        "run.contract_generation write-through"
    )
    assert run.public_outcome == frame.payload.get("public_outcome"), (
        "dev_runs.public_outcome must match the persisted frame's own "
        f"public_outcome; got run={run.public_outcome!r} "
        f"frame={frame.payload.get('public_outcome')!r}"
    )
    # Round-trips the exact bytes written to Postgres back through the
    # contract model -- a stored payload that fails validation would still
    # satisfy every assertion above while being useless to a v2 replay.
    FrameContract.model_validate(frame.payload)


def _preflight_runtime(*, org_id: uuid.UUID) -> BoundedDevRuntime:
    """The production runtime seam, wired with a real preflight and catalog.

    Mirrors ``tests/_chaos_3292_preflight.py``'s ``run_preflight_orchestrator``
    construction, but returns the ``BoundedDevRuntime`` the router's
    ``get_dev_execution_runtime`` dependency normally builds -- so the test
    drives the same orchestrator code path production does, through the real
    HTTP endpoint, instead of calling ``DevOrchestrator`` directly.
    """

    catalog = SeededCatalog([(str(org_id), ASK_DEV_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    mint = sequential_ids()
    preflight = SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=scope_service,
        versions=_test_versions(),
        mint_id=mint,
        now=fixed_now,
    )

    async def scope_resolver(
        *, org_id: str, user_id: str, requested_scope: DevScope
    ) -> DevScopeResolution:
        del user_id
        assert requested_scope.organization_id == org_id
        return DevScopeResolution(
            schema_version="dev_scope_resolution.v1",
            outcome=ScopeResolutionOutcome.EXACT,
            requested_scope=requested_scope,
            resolved_scope=requested_scope,
            authorized_repository_ids=list(requested_scope.repositories),
            authorized_entity_ids=[
                item.entity_id for item in requested_scope.entity_refs
            ],
            candidates=[],
            fallbacks=[],
            warnings=[],
            resolved_at=fixed_now(),
        )

    return BoundedDevRuntime(
        # Never called: the preflight terminates before the first model
        # round, and an empty script raises loudly (AgentProviderError) if
        # it ever were -- a silent no-op provider would let a broken test
        # setup masquerade as a passing one.
        provider=cast(Any, ScriptedAgentProvider([], script_id="chaos_3297_c0")),
        provider_source="platform",
        provider_family="scripted",
        registry=recording_registry([]),
        scope_resolver=scope_resolver,
        versions=_test_versions(),
        preflight=preflight,
    )


def _preflight_terminating_payload(
    conversation_id: str, org_id: uuid.UUID
) -> dict[str, Any]:
    return {
        "schema_version": "dev_message_request.v1",
        "request_id": "request_chaos_3297_c0",
        "client_message_id": "client_chaos_3297_c0",
        "conversation_id": conversation_id,
        "question": "What's the status of the Nightfall project?",
        "question_class": "status",
        "scope": _scope_payload(org_id),
    }


async def test_assert_frame_persisted_fails_loudly_when_frame_missing(
    dev_api_context,  # noqa: F811 -- pytest fixture imported from test_router
) -> None:
    """N0 self-proof: the helper must fail, not silently pass, on a frameless run.

    Drives a normal completed run through the stock ``FakeBoundedRuntime``
    fixture (which records an answer, never a frame) and asserts
    ``assert_frame_persisted`` raises rather than passing vacuously -- proving
    the helper is actually load-bearing before C0 relies on it.
    """

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(dev_api_context.org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = {
        "schema_version": "dev_message_request.v1",
        "request_id": "request_chaos_3297_n0",
        "client_message_id": "client_chaos_3297_n0",
        "conversation_id": conversation_id,
        "question": "What changed?",
        "question_class": "observed_change",
        "scope": _scope_payload(dev_api_context.org_id),
    }
    response = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert response.status_code == 200

    async with dev_api_context.maker() as session:
        run = await session.scalar(
            select(DevRun).where(DevRun.conversation_id == uuid.UUID(conversation_id))
        )
        assert run is not None
        with pytest.raises(AssertionError, match="no dev_answer_frames row"):
            await assert_frame_persisted(session, run.id)


async def test_preflight_termination_persists_frame_and_replays(
    dev_api_context,  # noqa: F811 -- pytest fixture imported from test_router
) -> None:
    """C0: a real preflight-terminated run must persist its frame and replay it.

    Drives a question naming a project absent from the authorized catalog
    through the real ``DevOrchestrator`` + real ``SubjectPreflight`` (not a
    fake runtime), so the orchestrator's TERMINATE branch runs exactly as it
    does in production. Before the fix this is RED: the frame the preflight
    built is discarded, so ``assert_frame_persisted`` fails on
    ``dev_runs.contract_generation`` never becoming 'v2'.
    """

    org_id = dev_api_context.org_id
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(
        runtime=_preflight_runtime(org_id=org_id)
    )

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = _preflight_terminating_payload(conversation_id, org_id)

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200
    live_events = dict(_parse_sse_events(live.text))
    assert "answer.completed" not in live_events, (
        "a not-found subject must not fabricate an answer"
    )
    assert "error" in live_events

    async with dev_api_context.maker() as session:
        run = await session.scalar(
            select(DevRun).where(DevRun.conversation_id == uuid.UUID(conversation_id))
        )
        assert run is not None
        await assert_frame_persisted(session, run.id)

    # Replay reachability: the same client_message_id must now take the v2
    # frame-reconstruction branch in router._replayed_result and stream the
    # identical terminal error -- proving the persisted frame is not just
    # written but actually consumable.
    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert replay.status_code == 200
    replay_events = dict(_parse_sse_events(replay.text))
    assert "answer.completed" not in replay_events
    assert "error" in replay_events

    def _comparable(error: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in error.items() if k != "request_id"}

    assert _comparable(live_events["error"]["error"]) == _comparable(
        replay_events["error"]["error"]
    )

    async with dev_api_context.maker() as session:
        runs = (
            await session.scalars(
                select(DevRun).where(
                    DevRun.conversation_id == uuid.UUID(conversation_id)
                )
            )
        ).all()
        assert len(runs) == 1, "the replay must not have created a second run"
        assert runs[0].contract_generation == "v2"


async def test_preflight_frame_flush_failure_reaches_terminal_state_not_stranded(
    dev_api_context,  # noqa: F811 -- pytest fixture imported from test_router
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex review HIGH: a record_frame flush failure must not strand the run.

    Before the fix, a database-layer failure while flushing the preflight's
    frame write (a constraint violation, a dropped connection) marks the
    request's session rollback-only; the terminal() write that follows then
    raises PendingRollbackError instead of completing, so the run is never
    written past 'accepted'/'v1'. Every retry with the same
    client_message_id then hits router.py's not-created branch, sees a
    non-terminal state, and 409s forever.

    Poisons the session with a genuine CHECK-constraint violation on
    record_frame -- an ``IntegrityError`` raised by the real flush, not a
    bare Python raise -- so the session is actually marked rollback-only the
    way the Codex repro's SQLAlchemy script demonstrated, and asserts the
    run still reaches a terminal state with a safe error, and that a
    duplicate POST replays (200) instead of 409ing.
    """

    org_id = dev_api_context.org_id

    async def poisoned_record_frame(self: PersistenceRunRecorder, frame: Any) -> None:
        del frame
        # A real DB-layer failure: violates dev_answer_frames' public_outcome
        # CHECK constraint at flush time, exactly the failure class the
        # review confirmed (not a synthetic raise -- the session must
        # actually become rollback-only).
        bad = DevAnswerFrame(
            run_id=self._run_id,
            org_id=self._org_id,
            user_id=self._user_id,
            frame_id=uuid.uuid4(),
            public_outcome="not_a_real_public_outcome",
            payload={},
        )
        self._service.session.add(bad)
        await self._service.session.flush()

    monkeypatch.setattr(PersistenceRunRecorder, "record_frame", poisoned_record_frame)

    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(
        runtime=_preflight_runtime(org_id=org_id)
    )

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = _preflight_terminating_payload(conversation_id, org_id)

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200, live.text
    live_events = dict(_parse_sse_events(live.text))
    assert "answer.completed" not in live_events
    assert "error" in live_events, (
        "a poisoned frame write must still reach a terminal error, not hang or 500"
    )

    async with dev_api_context.maker() as session:
        run = await session.scalar(
            select(DevRun).where(DevRun.conversation_id == uuid.UUID(conversation_id))
        )
        assert run is not None
        assert run.state != "accepted", (
            "the run must not be stranded nonterminal after the frame flush failed"
        )
        assert run.contract_generation == "v1", (
            "the poisoned frame write must have been rolled back, not left "
            "half-tagged v2 with no frame to back it"
        )
        frame_row = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run.id)
        )
        assert frame_row is None, "the poisoned frame insert must have been rolled back"
        # Codex review MEDIUM #2: record_preflight()'s diagnostic flush
        # happens before record_frame() on this same session/transaction, so
        # the session.rollback() above -- necessary to clear the poisoned
        # frame's rollback-only state -- would also erase it unless the
        # orchestrator re-persists it. A run that terminated for a reason
        # must not silently lose the reason.
        assert run.preflight_outcome == "unresolved_no_authorized_match", (
            "the record_preflight() diagnostic must survive the post-poison "
            "rollback, not be silently erased along with the bad frame write "
            f"(got {run.preflight_outcome!r})"
        )

    # The duplicate POST must replay, not 409 -- proving the run actually
    # reached a terminal state the router's not-created branch can see.
    duplicate = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert duplicate.status_code == 200, duplicate.text
    duplicate_events = dict(_parse_sse_events(duplicate.text))
    assert "error" in duplicate_events


async def test_replay_with_corrupted_frame_payload_falls_back_safely(
    dev_api_context,  # noqa: F811 -- pytest fixture imported from test_router
) -> None:
    """Codex review MEDIUM, superseded by CHAOS-3297 (0079): a corrupted v2
    frame payload must not 500 every replay -- and, since
    ``terminal_error_payload`` was added, must not even need the frame at all.

    Persists a real preflight-terminated run, then corrupts the stored
    *frame* payload directly (simulating a damaged row or a since-changed
    schema). Before CHAOS-3297 Codex review HIGH #1's fix, the frame was the
    only source ``_replayed_result`` had for a no-answer-payload run, so a
    corrupted frame degraded the replay to the generic "did not complete"
    shape (still safe, but not the live run's own message). Now
    ``PersistenceRunRecorder.terminal`` also persists the exact live
    ``DevError`` on ``dev_runs.terminal_error_payload``, independent of the
    frame -- so the replay reads that column first and never reaches the
    (corrupted) frame at all, serving the *real* live message even with a
    damaged frame row.
    """

    org_id = dev_api_context.org_id
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(
        runtime=_preflight_runtime(org_id=org_id)
    )

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = _preflight_terminating_payload(conversation_id, org_id)

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200
    live_events = dict(_parse_sse_events(live.text))
    live_message = live_events["error"]["error"]["safe_message"]

    async with dev_api_context.maker() as session:
        run = await session.scalar(
            select(DevRun).where(DevRun.conversation_id == uuid.UUID(conversation_id))
        )
        assert run is not None
        frame_row = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run.id)
        )
        assert frame_row is not None
        # Missing every field DevAnswerFrame.model_validate requires beyond
        # schema_version -- a damaged/legacy row, not a well-formed one.
        frame_row.payload = {"schema_version": "dev_answer_frame.v1"}
        await session.commit()

    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert replay.status_code == 200, (
        f"a corrupted frame payload must degrade to a safe error, not 500: "
        f"{replay.status_code} {replay.text}"
    )
    replay_events = dict(_parse_sse_events(replay.text))
    assert "answer.completed" not in replay_events
    assert "error" in replay_events
    fallback_message = "The prior Ask Dev request did not complete with an answer."
    # The live run's own preflight-specific copy must differ from the
    # generic fallback -- otherwise this test could not distinguish "used
    # the real terminal_error_payload" from "always falls back".
    assert live_message != fallback_message
    assert replay_events["error"]["error"]["safe_message"] == live_message, (
        "terminal_error_payload is independent of the frame, so a corrupted "
        "frame must have no effect on replay fidelity at all"
    )


async def test_replay_with_mismatched_frame_outcome_falls_back_safely(
    dev_api_context,  # noqa: F811 -- pytest fixture imported from test_router
) -> None:
    """Codex re-review MEDIUM, superseded by CHAOS-3297 (0079): a
    self-consistent but wrong-outcome frame must not project its own
    semantics over the run's persisted outcome -- and, since
    ``terminal_error_payload`` was added, the frame is not even consulted.

    Persists a real preflight-terminated ``not_found`` frame, then swaps the
    stored payload for a *different*, independently valid no-answer frame (a
    ``denied`` fixture built by the real fixture producer --
    ``DevAnswerFrame.model_validate`` accepts it on its own). Before this fix
    ``_replayed_result`` trusted the frame's own ``public_outcome`` with no
    cross-check against ``dev_runs.public_outcome``, so a stale or corrupted
    row could replay a ``forbidden``/"You do not have access" error for a run
    that actually terminated ``not_found`` -- false public semantics. Now
    ``dev_runs.terminal_error_payload`` (independent of the frame) is read
    first, so this run replays its own real ``not_found`` error exactly,
    never reaching the frame cross-check at all.
    """

    org_id = dev_api_context.org_id
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(
        runtime=_preflight_runtime(org_id=org_id)
    )

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = _preflight_terminating_payload(conversation_id, org_id)

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200
    live_events = dict(_parse_sse_events(live.text))
    live_error = live_events["error"]["error"]

    async with dev_api_context.maker() as session:
        run = await session.scalar(
            select(DevRun).where(DevRun.conversation_id == uuid.UUID(conversation_id))
        )
        assert run is not None
        assert run.public_outcome == "not_found"
        frame_row = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run.id)
        )
        assert frame_row is not None
        # A fully valid frame, produced by the real fixture builder -- just
        # for a different outcome than the run itself recorded.
        denied_frame = no_answer_answer_fixture("denied")["frame"]
        # The frame's own run_id must match the real run's id -- DevAnswerV2
        # independently enforces that consistency, and this test's target
        # (the run/frame *outcome* cross-check) must fail on its own merits,
        # not be masked by an unrelated run_id mismatch.
        denied_frame["run_id"] = str(run.id)
        FrameContract.model_validate(denied_frame)  # sanity: valid on its own
        frame_row.payload = denied_frame
        await session.commit()

    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert replay.status_code == 200, (
        f"a mismatched-but-valid frame outcome must degrade to a safe "
        f"error, not project the frame's own outcome: "
        f"{replay.status_code} {replay.text}"
    )
    replay_events = dict(_parse_sse_events(replay.text))
    assert "answer.completed" not in replay_events
    assert "error" in replay_events
    replay_error = replay_events["error"]["error"]
    # "forbidden" is the code the (wrong) denied frame would have projected
    # -- proving the replay did not adopt the mismatched frame's semantics.
    assert replay_error["code"] != "forbidden"
    assert replay_error["safe_message"] == live_error["safe_message"], (
        "terminal_error_payload is independent of the frame, so a "
        "mismatched frame outcome must have no effect on replay fidelity "
        "at all -- the run replays its own real error, not a fallback"
    )
    assert replay_error["code"] == live_error["code"]


async def test_replay_with_answered_frame_falls_back_safely(
    dev_api_context,  # noqa: F811 -- pytest fixture imported from test_router
) -> None:
    """Codex re-review MEDIUM, superseded by CHAOS-3297 (0079): an
    out-of-vocabulary ``answered`` frame must not crash the replay -- and,
    since ``terminal_error_payload`` was added, is not even reached.

    ``project_preflight_error`` is only total over the no-answer outcomes
    plus ``needs_clarification`` (CHAOS-3292's ratified preflight-
    termination vocabulary) -- a preflight itself never terminates with
    ``answered``. Swaps the stored payload for the canonical ``answered``
    positive fixture, which validates cleanly as a ``DevAnswerFrame`` but
    hits ``project_preflight_error``'s own defensive
    ``isinstance(projected, DevError)`` assertion. Before this fix that
    raised an uncaught ``RuntimeError``, turning every future replay of the
    idempotency key into a 500. Now ``dev_runs.terminal_error_payload`` is
    read first and this run's own real no-answer error replays verbatim,
    never reaching the (corrupted) frame or ``project_preflight_error`` at
    all.
    """

    org_id = dev_api_context.org_id
    dev_api_context.app.dependency_overrides[
        dev_router_module.get_dev_execution_runtime
    ] = lambda: dev_router_module.DevExecutionRuntimeResolution(
        runtime=_preflight_runtime(org_id=org_id)
    )

    client = dev_api_context.client
    created = await client.post(
        "/api/v1/dev/conversations",
        json={"current_scope": _scope_payload(org_id)},
    )
    conversation_id = created.json()["conversation_id"]
    payload = _preflight_terminating_payload(conversation_id, org_id)

    live = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert live.status_code == 200
    live_events = dict(_parse_sse_events(live.text))
    live_message = live_events["error"]["error"]["safe_message"]

    async with dev_api_context.maker() as session:
        run = await session.scalar(
            select(DevRun).where(DevRun.conversation_id == uuid.UUID(conversation_id))
        )
        assert run is not None
        frame_row = await session.scalar(
            select(DevAnswerFrame).where(DevAnswerFrame.run_id == run.id)
        )
        assert frame_row is not None
        # positive_fixtures()'s canonical "answered" frame: fully valid,
        # produced by the real fixture builder, not a shape this run ever
        # actually persisted.
        answered_frame = positive_fixtures()["dev_answer_frame.v1"]
        # See the mismatched-outcome test above: the frame's run_id must
        # match the real run so this test's target (the answered-outcome
        # totality gap) fails on its own merits.
        answered_frame["run_id"] = str(run.id)
        FrameContract.model_validate(answered_frame)  # sanity: valid on its own
        frame_row.payload = answered_frame
        await session.commit()

    replay = await client.post(
        f"/api/v1/dev/conversations/{conversation_id}/messages", json=payload
    )
    assert replay.status_code == 200, (
        f"an answered-outcome frame on a preflight-terminated run must "
        f"degrade to a safe error, not 500: {replay.status_code} {replay.text}"
    )
    replay_events = dict(_parse_sse_events(replay.text))
    assert "answer.completed" not in replay_events
    assert "error" in replay_events
    assert replay_events["error"]["error"]["safe_message"] == live_message, (
        "terminal_error_payload is independent of the frame, so a bogus "
        "answered-outcome frame must have no effect on replay fidelity at "
        "all -- the run replays its own real error, not a fallback"
    )
