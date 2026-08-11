from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevAnswerGraphAssistance,
    DevError,
    GraphAssistedAvailability,
    StreamEventType,
)
from dev_health_ops.api.dev.orchestrator import (
    OrchestratorEvent,
    OrchestratorResult,
    RunState,
)
from dev_health_ops.api.dev.streaming import (
    encode_sse,
    encoded_persisted_sse_stream,
    stream_orchestrator,
    validate_completed_stream,
    validate_persisted_resume_events,
)
from dev_health_ops.llm.agent.contracts import AgentUsage


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
    )


def _error_result() -> OrchestratorResult:
    error = DevError(
        schema_version="dev_error.v1",
        request_id="request_01",
        code="provider_unavailable",
        safe_message="The provider is temporarily unavailable.",
        retryable=True,
    )
    return OrchestratorResult(
        run_id="run_01",
        state=RunState.FAILED,
        answer=None,
        error=error,
        events=(OrchestratorEvent(RunState.FAILED, error.code),),
        usage=AgentUsage(),
        tool_call_count=0,
        provider_fingerprint=None,
        model_fingerprint=None,
    )


def _graph_state(
    state: GraphAssistedAvailability = GraphAssistedAvailability.UNAVAILABLE,
) -> DevAnswerGraphAssistance:
    return DevAnswerGraphAssistance(
        schema_version="dev_answer_graph_assistance.v1",
        state=state,
        as_of=datetime(2026, 8, 10, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_answer_stream_contains_only_controlled_progress_and_contracts() -> None:
    async def run(sink):
        for state in (
            RunState.ACCEPTED,
            RunState.RESOLVING_SCOPE,
            RunState.MODEL_DECISION,
            RunState.TOOL_VALIDATION,
            RunState.TOOL_EXECUTION,
            RunState.ANSWER_VALIDATION,
            RunState.COMPLETED,
        ):
            await sink(OrchestratorEvent(state))
        return _answer_result()

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=run, cancellation=asyncio.Event()
        )
    ]
    validate_completed_stream(events)
    assert events[0].event is StreamEventType.RUN_STARTED
    assert events[-2].event is StreamEventType.ANSWER_COMPLETED
    assert events[-1].terminal_kind == "answer"
    encoded = b"".join(encode_sse(event) for event in events)
    assert b"raw_prompt" not in encoded
    assert b"provider_response" not in encoded
    assert b"chain_of_thought" not in encoded


@pytest.mark.asyncio
async def test_error_stream_has_one_error_then_done() -> None:
    async def run(sink):
        await sink(OrchestratorEvent(RunState.ACCEPTED))
        await sink(OrchestratorEvent(RunState.FAILED, "provider_unavailable"))
        return _error_result()

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=run, cancellation=asyncio.Event()
        )
    ]
    validate_completed_stream(events)
    assert [event.event for event in events[-2:]] == [
        StreamEventType.ERROR,
        StreamEventType.DONE,
    ]


@pytest.mark.asyncio
async def test_graph_state_is_projected_as_a_safe_interior_event() -> None:
    graph_state = _graph_state()

    async def run(sink):
        await sink(OrchestratorEvent(RunState.TOOL_EXECUTION, graph_state=graph_state))
        return _answer_result()

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=run, cancellation=asyncio.Event()
        )
    ]

    validate_completed_stream(events)
    graph_events = [
        event for event in events if event.event is StreamEventType.GRAPH_STATE
    ]
    assert len(graph_events) == 1
    assert graph_events[0].graph_state == graph_state
    assert b"Graphiti" not in encode_sse(graph_events[0])


@pytest.mark.asyncio
async def test_unexpected_runner_failure_still_has_one_safe_terminal_pair() -> None:
    async def run(sink):
        await sink(OrchestratorEvent(RunState.ACCEPTED))
        raise RuntimeError("private provider failure details")

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=run, cancellation=asyncio.Event()
        )
    ]

    validate_completed_stream(events)
    assert [event.event for event in events[-2:]] == [
        StreamEventType.ERROR,
        StreamEventType.DONE,
    ]
    assert events[-2].error is not None
    assert events[-2].error.code == "internal_error"
    assert "private provider failure details" not in events[-2].error.safe_message


@pytest.mark.asyncio
async def test_closing_stream_signals_cancellation_and_waits_for_safe_terminal() -> (
    None
):
    cancellation = asyncio.Event()
    terminal_written = asyncio.Event()

    async def run(sink):
        await sink(OrchestratorEvent(RunState.ACCEPTED))
        await cancellation.wait()
        terminal_written.set()
        return _error_result()

    stream = stream_orchestrator(
        run_id="run_01", run_with_events=run, cancellation=cancellation
    )
    first = await anext(stream)
    assert first.event is StreamEventType.RUN_STARTED
    await stream.aclose()
    assert cancellation.is_set()
    assert terminal_written.is_set()


@pytest.mark.parametrize(
    ("result_factory", "expected"),
    [
        (
            _answer_result,
            ["run.started", "answer.completed", "done"],
        ),
        (_error_result, ["run.started", "error", "done"]),
    ],
)
@pytest.mark.asyncio
async def test_disconnect_persists_completion_once_before_generator_close(
    result_factory: Callable[[], OrchestratorResult],
    expected: list[str],
) -> None:
    persisted: list[Mapping[str, Any]] = []
    cancellation = asyncio.Event()

    async def run(_sink):
        await cancellation.wait()
        return result_factory()

    async def persist(event: Mapping[str, Any]) -> None:
        persisted.append(event)

    stream = stream_orchestrator(
        run_id="run_01",
        run_with_events=run,
        cancellation=cancellation,
        persist_event=persist,
    )
    first = await anext(stream)
    assert first.event is StreamEventType.RUN_STARTED
    await stream.aclose()
    assert [event["event"] for event in persisted] == expected
    replayed = validate_persisted_resume_events(
        run_id="run_01", after_sequence=0, persisted_events=persisted[1:]
    )
    assert [event.event for event in replayed] == [
        StreamEventType.ANSWER_COMPLETED
        if result_factory is _answer_result
        else StreamEventType.ERROR,
        StreamEventType.DONE,
    ]


@pytest.mark.asyncio
async def test_graph_state_persists_and_replays_once_after_cursor() -> None:
    graph_state = _graph_state()
    answer = _answer_result().answer
    assert answer is not None
    result = OrchestratorResult(
        run_id="run_01",
        state=RunState.COMPLETED,
        answer=answer.model_copy(update={"graph_assisted": graph_state}),
        error=None,
        events=(OrchestratorEvent(RunState.COMPLETED),),
        usage=AgentUsage(),
        tool_call_count=1,
        provider_fingerprint="provider_01",
        model_fingerprint="model_01",
    )
    persisted: list[Mapping[str, Any]] = []

    async def run(sink):
        await sink(
            OrchestratorEvent(
                RunState.TOOL_EXECUTION,
                graph_state=graph_state,
            )
        )
        return result

    async def persist(event: Mapping[str, Any]) -> None:
        persisted.append(event)

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01",
            run_with_events=run,
            cancellation=asyncio.Event(),
            persist_event=persist,
        )
    ]
    validate_completed_stream(events)
    persisted_graph = [
        event
        for event in persisted
        if event["event"] == StreamEventType.GRAPH_STATE.value
    ]
    assert len(persisted_graph) == 1

    replayed = validate_persisted_resume_events(
        run_id="run_01",
        after_sequence=0,
        persisted_events=persisted[1:],
    )
    replayed_graph = [
        event for event in replayed if event.event is StreamEventType.GRAPH_STATE
    ]
    assert len(replayed_graph) == 1
    assert replayed_graph[0].graph_state == graph_state

    graph_sequence = persisted_graph[0]["sequence"]
    suffix = validate_persisted_resume_events(
        run_id="run_01",
        after_sequence=graph_sequence,
        persisted_events=persisted[graph_sequence + 1 :],
    )
    assert not any(event.event is StreamEventType.GRAPH_STATE for event in suffix)


@pytest.mark.asyncio
async def test_resume_replays_only_contiguous_persisted_events_after_cursor() -> None:
    async def run(_sink):
        return _answer_result()

    events = [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=run, cancellation=asyncio.Event()
        )
    ]
    raw: list[dict[str, Any]] = [event.model_dump(mode="json") for event in events]
    chunks = [
        chunk
        async for chunk in encoded_persisted_sse_stream(
            run_id="run_01", after_sequence=0, persisted_events=raw[1:]
        )
    ]
    assert len(chunks) == len(events) - 1
    assert b'"sequence":1' in chunks[0]
    with pytest.raises(ValueError, match="contiguous"):
        _ = [
            chunk
            async for chunk in encoded_persisted_sse_stream(
                run_id="run_01", after_sequence=0, persisted_events=raw[2:]
            )
        ]


def test_resume_rejects_terminal_without_following_done() -> None:
    answer = _answer_result().answer
    assert answer is not None
    raw = [
        {
            "schema_version": "dev_stream_event.v1",
            "run_id": "run_01",
            "sequence": 0,
            "event": "run.started",
            "occurred_at": "2026-01-01T00:00:00Z",
        },
        {
            "schema_version": "dev_stream_event.v1",
            "run_id": "run_01",
            "sequence": 1,
            "event": "answer.completed",
            "occurred_at": "2026-01-01T00:00:00Z",
            "answer": answer.model_dump(mode="json"),
        },
    ]
    with pytest.raises(ValueError, match="no done"):
        validate_persisted_resume_events(
            run_id="run_01", after_sequence=-1, persisted_events=raw
        )


def test_resume_rejects_event_after_terminal_before_done() -> None:
    error = _error_result().error
    assert error is not None
    raw = [
        {
            "schema_version": "dev_stream_event.v1",
            "run_id": "run_01",
            "sequence": 0,
            "event": "run.started",
            "occurred_at": "2026-01-01T00:00:00Z",
        },
        {
            "schema_version": "dev_stream_event.v1",
            "run_id": "run_01",
            "sequence": 1,
            "event": "error",
            "occurred_at": "2026-01-01T00:00:00Z",
            "error": error.model_dump(mode="json"),
        },
        {
            "schema_version": "dev_stream_event.v1",
            "run_id": "run_01",
            "sequence": 2,
            "event": "progress",
            "occurred_at": "2026-01-01T00:00:01Z",
            "progress": "preparing_answer",
        },
    ]
    with pytest.raises(ValueError, match="not followed by done"):
        validate_persisted_resume_events(
            run_id="run_01", after_sequence=-1, persisted_events=raw
        )


def test_resume_rejects_lone_or_duplicate_done_and_missing_start() -> None:
    done = {
        "schema_version": "dev_stream_event.v1",
        "run_id": "run_01",
        "sequence": 0,
        "event": "done",
        "occurred_at": "2026-01-01T00:00:00Z",
        "terminal_kind": "error",
    }
    with pytest.raises(ValueError, match="invalid done"):
        validate_persisted_resume_events(
            run_id="run_01",
            after_sequence=-1,
            persisted_events=[{**done, "sequence": 0}],
        )

    started = {
        "schema_version": "dev_stream_event.v1",
        "run_id": "run_01",
        "sequence": 0,
        "event": "run.started",
        "occurred_at": "2026-01-01T00:00:00Z",
    }
    with pytest.raises(ValueError, match="missing run.started"):
        validate_persisted_resume_events(
            run_id="run_01",
            after_sequence=-1,
            persisted_events=[
                {
                    **started,
                    "event": "progress",
                    "progress": "preparing_answer",
                }
            ],
        )

    answer = _answer_result().answer
    assert answer is not None
    terminal = {
        **started,
        "sequence": 1,
        "event": "answer.completed",
        "answer": answer.model_dump(mode="json"),
    }
    terminal_done = {
        **started,
        "sequence": 2,
        "event": "done",
        "terminal_kind": "answer",
    }
    with pytest.raises(ValueError, match="duplicate done"):
        validate_persisted_resume_events(
            run_id="run_01",
            after_sequence=-1,
            persisted_events=[
                started,
                terminal,
                terminal_done,
                terminal_done | {"sequence": 3},
            ],
        )


def test_resume_accepts_done_only_suffix_after_terminal_cursor() -> None:
    done = {
        "schema_version": "dev_stream_event.v1",
        "run_id": "run_01",
        "sequence": 2,
        "event": "done",
        "occurred_at": "2026-01-01T00:00:00Z",
        "terminal_kind": "answer",
    }
    replayed = validate_persisted_resume_events(
        run_id="run_01",
        after_sequence=1,
        persisted_events=[done],
    )
    assert [event.event for event in replayed] == [StreamEventType.DONE]
    with pytest.raises(ValueError, match="invalid done"):
        validate_persisted_resume_events(
            run_id="run_01",
            after_sequence=-1,
            persisted_events=[{**done, "sequence": 0}],
        )


@pytest.mark.parametrize(
    "code",
    ["resume_scope_mismatch", "resume_unavailable", "resume_stream_invalid"],
)
def test_resume_http_error_codes_are_wire_contract_values(code: str) -> None:
    error = DevError(
        schema_version="dev_error.v1",
        request_id="request_01",
        code=code,
        safe_message="Resume request cannot be completed safely.",
        retryable=True,
    )
    assert error.code == code
