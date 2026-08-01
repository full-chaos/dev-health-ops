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
from dev_health_ops.api.dev.contracts import (
    DevContractVersions,
    DevScope,
    DevScopeResolution,
    ScopeResolutionOutcome,
)
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame as FrameContract
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
