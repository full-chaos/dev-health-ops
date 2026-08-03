"""Automatic re-certification of the platform-owned Ask Dev provider.

CHAOS-3358. The platform provider's stored certification is invalidated by
construction on every ``READINESS_VERSION`` bump and every change to the
readiness-fingerprint formula, and also whenever the operator rotates a
credential, model, organization, project, or custom header. Before this,
recovering from any of those required a superadmin to open Platform Admin and
press the preflight button; until they did, the readiness badge sat on
"stale". Requiring a human button-press for a condition the system creates
for itself is the bug.

So the system re-certifies itself. When a production resolution observes that
the platform record has gone stale (or was never written), it schedules a
background re-certification that runs the SAME code path the button runs and
writes the SAME record, so the badge self-heals to ready -- or to a real
failure state with a reason -- with no human action.

Three properties this module exists to guarantee:

* **It never blocks a run.** Certification performs several live provider
  round-trips with their own timeouts. Doing that inline would turn "the
  fingerprint changed" into a multi-second stall on somebody's question, and
  a broken operator endpoint into a stall for its full timeout -- a softer
  version of exactly the block CHAOS-3358 removes. The work is scheduled onto
  a background task and the caller returns immediately. Failure is logged and
  left in the record; it is never raised at the caller.

* **It never stampedes the provider.** ``schedule`` is single-flight (at most
  one attempt in flight) AND throttled (at most one attempt per
  ``_MIN_ATTEMPT_INTERVAL_SECONDS``). Both checks run synchronously with no
  ``await`` between the test and the set, so concurrent resolutions on one
  event loop cannot interleave past them. The throttle -- not just the
  single-flight -- is what bounds a FAILING endpoint: without it, every
  request after a failed attempt would immediately start another one.

* **It never writes a lying record.** It does not reuse a caller's candidate
  or fingerprint. ``certify_platform_provider`` re-resolves the platform
  provider itself and writes the fingerprint of the resolution it actually
  probed.

Scope bound, stated because it is easy to over-read: the single-flight and
throttle are per process. In a multi-worker deployment the bound is one
attempt per worker per interval, not one globally. That is deliberate -- a
global bound would need a distributed lock -- and it is sized so the
worst case (a fleet restarting into a fingerprint change) is a handful of
certifications, not a per-request flood.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.db import get_postgres_session

logger = logging.getLogger(__name__)

# How long to wait before another automatic attempt. Applies to successes and
# failures alike: a successful attempt makes the record current so nothing
# re-schedules anyway, while a failing one is exactly the case that needs the
# ceiling.
_MIN_ATTEMPT_INTERVAL_SECONDS = 300.0

SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]


class PlatformAutoCertifier:
    """Single-flight, throttled scheduler for platform re-certification."""

    def __init__(
        self,
        *,
        session_factory: SessionFactory | None = None,
        min_interval_seconds: float = _MIN_ATTEMPT_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session_factory = session_factory
        self._min_interval_seconds = min_interval_seconds
        self._clock = clock
        self._in_flight: asyncio.Task[None] | None = None
        self._last_attempt_at: float | None = None

    def schedule(self) -> asyncio.Task[None] | None:
        """Start a re-certification attempt unless one is suppressed.

        Returns the task so callers that need determinism (tests, a startup
        hook) can await it. Returns ``None`` when single-flight or the
        throttle suppressed this call -- ``None`` is a normal outcome, not an
        error.

        Deliberately NOT a coroutine: every decision below is a plain
        attribute read/write with no ``await`` in between, which is what makes
        the check-then-set atomic with respect to the event loop. Making this
        ``async`` would introduce the very interleaving the single-flight
        exists to prevent.
        """

        if self._in_flight is not None and not self._in_flight.done():
            return None
        now = self._clock()
        if (
            self._last_attempt_at is not None
            and now - self._last_attempt_at < self._min_interval_seconds
        ):
            return None
        self._last_attempt_at = now
        task = asyncio.create_task(self._attempt())
        self._in_flight = task
        return task

    async def _attempt(self) -> None:
        # Its own session: the caller's request session is committed and
        # closed by the request's dependency well before this task finishes,
        # and writing through a closed session would lose the record (or
        # poison the caller's transaction on the way out).
        try:
            async with self._open_session() as session:
                from .production_runtime import certify_platform_provider

                await certify_platform_provider(session)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Certification's own failures are already persisted as a FAILED
            # record by AgentReadinessService.certify. Reaching here means
            # something around it broke (database unavailable, provider
            # construction). There is nothing to escalate to: the run this was
            # scheduled from has long since returned, and by ruling a
            # certification failure must not affect it.
            logger.warning(
                "Automatic platform Ask Dev re-certification failed",
                exc_info=True,
            )

    @contextlib.asynccontextmanager
    async def _open_session(self) -> AsyncIterator[AsyncSession]:
        factory = self._session_factory
        if factory is None:
            async with get_postgres_session() as session:
                yield session
            return
        async with factory() as session:
            yield session


_certifier = PlatformAutoCertifier()


def schedule_platform_recertification() -> asyncio.Task[None] | None:
    """Schedule automatic platform re-certification on the process certifier."""

    return _certifier.schedule()


def set_platform_auto_certifier(certifier: PlatformAutoCertifier) -> None:
    """Replace the process certifier. For tests and for a startup hook that
    needs its own throttle/clock; production code uses the module default."""

    global _certifier
    _certifier = certifier


def reset_platform_auto_certifier() -> None:
    """Restore a pristine process certifier (test isolation)."""

    set_platform_auto_certifier(PlatformAutoCertifier())


__all__ = [
    "PlatformAutoCertifier",
    "reset_platform_auto_certifier",
    "schedule_platform_recertification",
    "set_platform_auto_certifier",
]
