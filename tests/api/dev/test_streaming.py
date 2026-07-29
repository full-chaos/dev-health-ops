from __future__ import annotations

import asyncio

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevAnswer, DevError, StreamEventType
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
