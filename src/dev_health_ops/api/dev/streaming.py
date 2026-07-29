"""Safe SSE projection for bounded Ask Dev orchestrator runs."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from .contracts import (
    DevError,
    DevStreamEvent,
    ProgressState,
    StreamEventType,
    validate_stream,
)
from .orchestrator import (
    EventSink,
    OrchestratorEvent,
    OrchestratorResult,
    RunState,
)

RunWithEvents = Callable[[EventSink], Awaitable[OrchestratorResult]]

_PROGRESS_BY_STATE = {
    RunState.RESOLVING_SCOPE: ProgressState.RESOLVING_SCOPE,
    RunState.MODEL_DECISION: ProgressState.PREPARING_ANSWER,
    RunState.TOOL_VALIDATION: ProgressState.CHECKING_EVIDENCE,
    RunState.TOOL_EXECUTION: ProgressState.CHECKING_EVIDENCE,
    RunState.ANSWER_VALIDATION: ProgressState.PREPARING_ANSWER,
}


def encode_sse(event: DevStreamEvent) -> bytes:
    """Serialize one canonical event without comments or hidden payloads."""

    data = json.dumps(
        event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )
    return f"event: {event.event.value}\ndata: {data}\n\n".encode()


async def stream_orchestrator(
    *,
    run_id: str,
    run_with_events: RunWithEvents,
    cancellation: asyncio.Event,
) -> AsyncGenerator[DevStreamEvent, None]:
    """Run the orchestrator and expose only the approved public event vocabulary."""

    queue: asyncio.Queue[OrchestratorEvent] = asyncio.Queue(maxsize=128)
    sequence = 0
    last_progress: ProgressState | None = None

    def public_event(event: StreamEventType, **payload) -> DevStreamEvent:
        nonlocal sequence
        value = DevStreamEvent(
            schema_version="dev_stream_event.v1",
            run_id=run_id,
            sequence=sequence,
            event=event,
            occurred_at=datetime.now(UTC),
            **payload,
        )
        sequence += 1
        return value

    async def sink(event: OrchestratorEvent) -> None:
        await queue.put(event)

    run_task: asyncio.Future[OrchestratorResult] = asyncio.ensure_future(
        run_with_events(sink)
    )
    try:
        yield public_event(StreamEventType.RUN_STARTED)
        while not run_task.done():
            next_internal = asyncio.create_task(queue.get())
            wait_set: set[asyncio.Future[Any]] = {run_task, next_internal}
            done, _ = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
            if next_internal in done:
                internal = next_internal.result()
                progress = _PROGRESS_BY_STATE.get(internal.state)
                if progress is not None and progress is not last_progress:
                    last_progress = progress
                    yield public_event(StreamEventType.PROGRESS, progress=progress)
            else:
                next_internal.cancel()
                await asyncio.gather(next_internal, return_exceptions=True)

        while not queue.empty():
            internal = queue.get_nowait()
            progress = _PROGRESS_BY_STATE.get(internal.state)
            if progress is not None and progress is not last_progress:
                last_progress = progress
                yield public_event(StreamEventType.PROGRESS, progress=progress)

        try:
            result = await run_task
        except Exception:
            error = DevError(
                schema_version="dev_error.v1",
                request_id=run_id,
                code="internal_error",
                safe_message="The request could not be completed.",
                retryable=True,
            )
            yield public_event(StreamEventType.ERROR, error=error)
            yield public_event(StreamEventType.DONE, terminal_kind="error")
            return
        if result.answer is not None:
            yield public_event(
                StreamEventType.SCOPE_RESOLVED,
                scope_resolution=result.answer.resolved_scope,
            )
            for warning in result.answer.warnings:
                yield public_event(StreamEventType.WARNING, warning=warning)
            yield public_event(
                StreamEventType.ANSWER_COMPLETED,
                answer=result.answer,
            )
            yield public_event(StreamEventType.DONE, terminal_kind="answer")
        else:
            assert result.error is not None
            yield public_event(StreamEventType.ERROR, error=result.error)
            yield public_event(StreamEventType.DONE, terminal_kind="error")
    finally:
        if not run_task.done():
            cancellation.set()
            try:
                await asyncio.shield(run_task)
            except Exception:
                pass


async def encoded_sse_stream(
    *,
    run_id: str,
    run_with_events: RunWithEvents,
    cancellation: asyncio.Event,
) -> AsyncGenerator[bytes, None]:
    async for event in stream_orchestrator(
        run_id=run_id,
        run_with_events=run_with_events,
        cancellation=cancellation,
    ):
        yield encode_sse(event)


def validate_completed_stream(events: list[DevStreamEvent]) -> None:
    """Public verification seam used by API and acceptance fixtures."""

    validate_stream(events)


__all__ = [
    "RunWithEvents",
    "encode_sse",
    "encoded_sse_stream",
    "stream_orchestrator",
    "validate_completed_stream",
]
