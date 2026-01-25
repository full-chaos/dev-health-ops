"""Queue-based rate limiting helpers.

These utilities provide a simple, shared backoff gate that can be used by
multiple workers (threads or asyncio tasks) to coordinate pauses when a
rate-limit response is encountered.

The goal is to avoid a stampede where many workers repeatedly hit the API
at the same time.
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RateLimitConfig:
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 300.0
    backoff_factor: float = 2.0


class RateLimitGate:
    """Thread-safe, event-loop-friendly shared backoff gate."""

    def __init__(self, config: Optional[RateLimitConfig] = None) -> None:
        self._config = config or RateLimitConfig()
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0
        self._current_backoff = self._config.initial_backoff_seconds

    def reset(self) -> None:
        with self._lock:
            self._current_backoff = self._config.initial_backoff_seconds

    def penalize(self, delay_seconds: Optional[float] = None) -> float:
        """Push the next allowed time into the future.

        If delay_seconds is not provided, uses exponential backoff.
        Returns the applied delay.
        """
        with self._lock:
            if delay_seconds is None:
                delay_seconds = min(
                    self._current_backoff,
                    self._config.max_backoff_seconds,
                )
                self._current_backoff = min(
                    self._current_backoff * self._config.backoff_factor,
                    self._config.max_backoff_seconds,
                )
            else:
                # If we get an explicit server reset delay, keep exponential
                # backoff state but still honor the explicit delay.
                delay_seconds = max(0.0, float(delay_seconds))

            now = time.time()
            self._next_allowed_at = max(
                self._next_allowed_at,
                now + delay_seconds,
            )
            return delay_seconds

    def _sleep_seconds(self) -> float:
        with self._lock:
            return max(0.0, self._next_allowed_at - time.time())

    def wait_sync(self) -> None:
        seconds = self._sleep_seconds()
        if seconds > 0:
            time.sleep(seconds)

    async def wait_async(self) -> None:
        seconds = self._sleep_seconds()
        if seconds > 0:
            await asyncio.sleep(seconds)
