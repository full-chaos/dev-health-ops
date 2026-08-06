"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: pure SSE-frame parsing for the corpus
runner's live HTTP driver.

Deliberately decoupled from ``dev_health_ops.api.dev.contracts.DevStreamEvent``
-- this module returns plain ``dict`` frames (``{"event": str, "data": dict}``)
so its parsing logic (frame splitting, ``event:``/``data:`` line handling,
JSON decoding) is unit-testable without constructing real contract payloads.
The live corpus runner validates each returned frame's ``data`` through
``DevStreamEvent.model_validate`` itself, exactly like every existing smoke
script (``smoke_ask_dev_exact_commit.py``'s ``_sse_request``, which this
module's parsing mirrors) -- that validation belongs at the call site, which
knows it is talking to the real Ask Dev wire contract, not in a
contract-agnostic parser.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

__all__ = ["SseFrame", "SseParseError", "parse_sse_events"]


class SseParseError(Exception):
    """A frame is missing its event name or its data payload.

    Raised rather than silently dropping the malformed frame: a corpus
    case's assertions read the event sequence positionally and by kind
    (e.g. "the scope.resolved event", "the last event"); a silently-dropped
    frame would shift or hide exactly the evidence an assertion depends on.
    """


@dataclass(frozen=True, slots=True)
class SseFrame:
    event: str
    data: dict[str, Any]


def parse_sse_events(body: str) -> list[SseFrame]:
    """Split a full SSE response body into its ``event``/``data`` frames.

    Frames are separated by a blank line (``\\n\\n``); a frame may carry
    multiple ``data: `` lines, which are joined before JSON-decoding (the
    SSE spec's own multi-line data convention). Trailing/leading blank
    frames (a body ending in a stray ``\\n\\n``) are skipped, not reported
    as malformed.
    """

    frames: list[SseFrame] = []
    for raw_frame in body.split("\n\n"):
        if not raw_frame.strip():
            continue
        event_name: str | None = None
        data_lines: list[str] = []
        for line in raw_frame.splitlines():
            if line.startswith("event: "):
                event_name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data_lines.append(line.removeprefix("data: "))
        if event_name is None:
            raise SseParseError(f"SSE frame omitted an event name: {raw_frame!r}")
        if not data_lines:
            raise SseParseError(f"SSE {event_name!r} frame omitted data: {raw_frame!r}")
        try:
            data = json.loads("\n".join(data_lines))
        except json.JSONDecodeError as exc:
            raise SseParseError(
                f"SSE {event_name!r} frame data is not valid JSON: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise SseParseError(
                f"SSE {event_name!r} frame data must decode to an object, "
                f"got {type(data).__name__}"
            )
        frames.append(SseFrame(event=event_name, data=data))
    return frames
