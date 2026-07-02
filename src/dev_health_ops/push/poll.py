"""Shared polling loop for `push batch --poll` / `push status --poll`
(CHAOS-2700 brief decision 7 -- one implementation, two call sites).

Terminal set pinned by master-spec CC12/CC29: ``{completed, partial,
failed}``. ``stream_unavailable`` is a special-cased non-terminal-but-give-up
state (master-spec CC29): the batch was durably accepted in Postgres but
never reached the stream, so polling it for the full timeout would just hang
-- the caller should print the "re-run push batch" hint and exit 3
immediately instead.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

from .http_client import IngestClientConfig, get_batch_status

#: Master-spec CC12: terminal outcomes for a batch (mirrors
#: models.external_ingest.TERMINAL_STATUSES, duplicated here as plain
#: strings since the CLI has no reason to import the Postgres-model enum).
TERMINAL_STATUSES = frozenset({"completed", "partial", "failed"})

DEFAULT_POLL_INTERVAL_SECONDS = 5.0
#: Brief Risks section: 300s is long enough for typical bounded recompute,
#: short enough not to eat a customer's CI minutes budget on a stuck job.
DEFAULT_POLL_TIMEOUT_SECONDS = 300.0


class PollTimeoutError(Exception):
    """Raised when ``--poll-timeout`` elapses before a terminal status."""

    def __init__(self, last_status: dict[str, Any]) -> None:
        super().__init__(f"poll timed out; last status: {last_status.get('status')!r}")
        self.last_status = last_status


class StreamUnavailableResult(Exception):
    """Raised when the batch status is ``stream_unavailable`` -- durably
    accepted but never reached the stream (master-spec CC29); not worth
    polling out the full timeout for."""

    def __init__(self, status_body: dict[str, Any]) -> None:
        super().__init__("batch status is stream_unavailable")
        self.status_body = status_body


@dataclass(frozen=True)
class _Clock:
    """Injectable time source so tests can drive the loop deterministically
    without real sleeps."""

    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    now: Callable[[], float] = time.monotonic


async def poll_until_terminal(
    client: httpx.AsyncClient,
    config: IngestClientConfig,
    ingestion_id: str,
    *,
    interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    clock: _Clock = _Clock(),
) -> dict[str, Any]:
    """Poll ``GET /batches/{id}`` until a terminal status, ``stream_
    unavailable``, or ``timeout_seconds`` elapses.

    Returns the last status body on reaching a terminal status. Raises
    ``StreamUnavailableResult``/``PollTimeoutError`` otherwise -- both carry
    the last-seen status body so the caller can render it.
    """
    deadline = clock.now() + timeout_seconds
    body = await get_batch_status(client, config, ingestion_id)
    while True:
        status = body.get("status")
        if status in TERMINAL_STATUSES:
            return body
        if status == "stream_unavailable":
            raise StreamUnavailableResult(body)
        if clock.now() >= deadline:
            raise PollTimeoutError(body)
        remaining = deadline - clock.now()
        await clock.sleep(min(interval_seconds, max(remaining, 0.0)))
        body = await get_batch_status(client, config, ingestion_id)


__all__ = [
    "TERMINAL_STATUSES",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_POLL_TIMEOUT_SECONDS",
    "PollTimeoutError",
    "StreamUnavailableResult",
    "poll_until_terminal",
]
