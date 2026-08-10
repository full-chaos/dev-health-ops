"""Safe SSE projection for bounded Ask Dev orchestrator runs."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
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
PersistStreamEvent = Callable[[Mapping[str, Any]], Awaitable[None]]

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
    persist_event: PersistStreamEvent | None = None,
) -> AsyncGenerator[DevStreamEvent, None]:
    """Run the orchestrator and expose only the approved public event vocabulary."""

    queue: asyncio.Queue[OrchestratorEvent] = asyncio.Queue(maxsize=128)
    sequence = 0
    last_progress: ProgressState | None = None

    async def public_event(event: StreamEventType, **payload) -> DevStreamEvent:
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
        if persist_event is not None:
            await persist_event(value.model_dump(mode="json"))
        return value

    async def sink(event: OrchestratorEvent) -> None:
        await queue.put(event)

    run_task: asyncio.Future[OrchestratorResult] = asyncio.ensure_future(
        run_with_events(sink)
    )
    completion_emitted = False

    async def terminal_events(
        result: OrchestratorResult,
    ) -> list[DevStreamEvent]:
        events: list[DevStreamEvent] = []
        if result.scope_resolution is not None:
            events.append(
                await public_event(
                    StreamEventType.SCOPE_RESOLVED,
                    scope_resolution=result.scope_resolution,
                )
            )
        if result.answer is not None:
            for warning in result.answer.warnings:
                events.append(
                    await public_event(StreamEventType.WARNING, warning=warning)
                )
            events.append(
                await public_event(
                    StreamEventType.ANSWER_COMPLETED,
                    answer=result.answer,
                )
            )
            events.append(
                await public_event(StreamEventType.DONE, terminal_kind="answer")
            )
        else:
            assert result.error is not None
            events.append(await public_event(StreamEventType.ERROR, error=result.error))
            events.append(
                await public_event(StreamEventType.DONE, terminal_kind="error")
            )
        return events

    async def persist_completed_run() -> None:
        """Finish persistence after a client closes the SSE generator."""

        nonlocal completion_emitted, last_progress
        if completion_emitted:
            return
        while not queue.empty():
            internal = queue.get_nowait()
            progress = _PROGRESS_BY_STATE.get(internal.state)
            if progress is not None and progress is not last_progress:
                last_progress = progress
                await public_event(StreamEventType.PROGRESS, progress=progress)
        try:
            result = run_task.result()
        except Exception:
            error = DevError(
                schema_version="dev_error.v1",
                request_id=run_id,
                code="internal_error",
                safe_message="The request could not be completed.",
                retryable=True,
            )
            await public_event(StreamEventType.ERROR, error=error)
            await public_event(StreamEventType.DONE, terminal_kind="error")
        else:
            await terminal_events(result)
        completion_emitted = True

    try:
        yield await public_event(StreamEventType.RUN_STARTED)
        while not run_task.done():
            next_internal = asyncio.create_task(queue.get())
            wait_set: set[asyncio.Future[Any]] = {run_task, next_internal}
            done, _ = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
            if next_internal in done:
                internal = next_internal.result()
                progress = _PROGRESS_BY_STATE.get(internal.state)
                if progress is not None and progress is not last_progress:
                    last_progress = progress
                    yield await public_event(
                        StreamEventType.PROGRESS, progress=progress
                    )
            else:
                next_internal.cancel()
                await asyncio.gather(next_internal, return_exceptions=True)

        while not queue.empty():
            internal = queue.get_nowait()
            progress = _PROGRESS_BY_STATE.get(internal.state)
            if progress is not None and progress is not last_progress:
                last_progress = progress
                yield await public_event(StreamEventType.PROGRESS, progress=progress)

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
            terminal = [
                await public_event(StreamEventType.ERROR, error=error),
                await public_event(StreamEventType.DONE, terminal_kind="error"),
            ]
            completion_emitted = True
            for event in terminal:
                yield event
            return
        # CHAOS-3497: emitted for EVERY terminal whose run got as far as
        # completing scope resolution -- not only the answering ones.
        #
        # This frame used to be built from ``result.answer.resolved_scope``
        # inside the answer branch below, so a run that resolved scope and
        # then terminated without an answer (insufficient_evidence, a
        # refusal, a not-found) published no scope decision at all. An
        # auditor reading the wire could not tell a failed run that resolved
        # to an exact subject from one that silently widened to organization
        # scope -- and a run that widens and then fails to ground is exactly
        # the shape the no-silent-widening audit family exists to catch.
        #
        # Ordering is unchanged for the answer path (scope.resolved, then
        # warnings, then the terminal) and mirrors it for the error path
        # (scope.resolved, then the terminal), so ``validate_stream``'s
        # "terminal result immediately followed by done" rule still holds.
        terminal = await terminal_events(result)
        completion_emitted = True
        for event in terminal:
            yield event
    finally:
        if not run_task.done():
            cancellation.set()
            try:
                await asyncio.shield(run_task)
            except Exception:
                pass
        if run_task.done():
            await persist_completed_run()


async def encoded_sse_stream(
    *,
    run_id: str,
    run_with_events: RunWithEvents,
    cancellation: asyncio.Event,
    persist_event: PersistStreamEvent | None = None,
) -> AsyncGenerator[bytes, None]:
    async for event in stream_orchestrator(
        run_id=run_id,
        run_with_events=run_with_events,
        cancellation=cancellation,
        persist_event=persist_event,
    ):
        yield encode_sse(event)


async def encoded_persisted_sse_stream(
    *,
    run_id: str,
    after_sequence: int,
    persisted_events: Sequence[Mapping[str, Any]],
) -> AsyncGenerator[bytes, None]:
    """Replay only durable events after a client-provided cursor."""

    for event in validate_persisted_resume_events(
        run_id=run_id,
        after_sequence=after_sequence,
        persisted_events=persisted_events,
    ):
        yield encode_sse(event)


def validate_persisted_resume_events(
    *,
    run_id: str,
    after_sequence: int,
    persisted_events: Sequence[Mapping[str, Any]],
) -> list[DevStreamEvent]:
    """Validate a replay suffix before returning an HTTP 200 response."""

    if after_sequence < -1:
        raise ValueError("invalid stream cursor")
    expected = after_sequence + 1
    parsed: list[DevStreamEvent] = []
    terminal_seen = False
    done_seen = False
    for raw in persisted_events:
        event = DevStreamEvent.model_validate(raw)
        if event.run_id != run_id or event.sequence != expected:
            raise ValueError("persisted stream is not contiguous for resume")
        if after_sequence == -1 and not parsed:
            if event.event is StreamEventType.DONE:
                raise ValueError("persisted stream contains invalid done")
            if event.event is not StreamEventType.RUN_STARTED:
                raise ValueError("persisted stream is missing run.started")
        if event.event is StreamEventType.DONE:
            if done_seen:
                raise ValueError("persisted stream contains duplicate done")
            if after_sequence == -1 and not terminal_seen:
                raise ValueError("persisted stream contains invalid done")
            done_seen = True
        elif terminal_seen:
            raise ValueError("persisted terminal event is not followed by done")
        if event.event in {
            StreamEventType.ANSWER_COMPLETED,
            StreamEventType.ERROR,
        }:
            terminal_seen = True
        parsed.append(event)
        expected += 1
    if after_sequence == -1 and not parsed:
        raise ValueError("persisted stream is missing run.started")
    if terminal_seen and not done_seen:
        raise ValueError("persisted terminal event has no done")
    return parsed


def validate_completed_stream(events: list[DevStreamEvent]) -> None:
    """Public verification seam used by API and acceptance fixtures."""

    validate_stream(events)


__all__ = [
    "RunWithEvents",
    "encode_sse",
    "encoded_sse_stream",
    "encoded_persisted_sse_stream",
    "validate_persisted_resume_events",
    "stream_orchestrator",
    "validate_completed_stream",
]
