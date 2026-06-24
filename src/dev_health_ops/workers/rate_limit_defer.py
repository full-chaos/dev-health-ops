"""Rate-limit deferral helpers for Celery sync tasks.

Provider rate-limit signals (HTTP 429 / ``Retry-After``) are treated as
*deferred work*, not task failures. Instead of consuming the genuine-failure
retry budget (Celery's single ``self.request.retries`` counter) and stamping
the run ``FAILED``, the sync tasks re-enqueue a fresh invocation with the
server-provided delay and explicit rate-limit budget metadata.

Two budgets bound the deferral so a permanently rate-limited provider still
eventually surfaces as a real failure:

* a **count** budget (:data:`RATE_LIMIT_MAX_DEFERRALS`), incremented once per
  provider 429, and
* a **wall-clock** budget (:data:`RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS`) measured
  from the first deferral.

Long server delays (e.g. GitHub primary-limit resets up to ~1h) are *chunked*:
a single Celery countdown is capped at :data:`RATE_LIMIT_MAX_COUNTDOWN_SECONDS`
and an absolute ``not_before`` timestamp is carried forward so the task
re-defers **without calling the provider again** until the window elapses.
Chunk re-defers do not count against the count budget.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.exceptions import RateLimitException

logger = logging.getLogger(__name__)

# Bound total deferral by count AND wall-clock so a permanently rate-limited
# provider eventually becomes a real failure rather than looping forever.
RATE_LIMIT_MAX_DEFERRALS = 10
RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS = 2 * 60 * 60  # 2 hours

# A single Celery countdown chunk. Kept at/below the distributed gate's Redis
# TTL (``RateLimitConfig.max_backoff_seconds * 2`` == 600s) so a deferral never
# outlives the shared cooldown key. Longer server delays are chunked via
# ``not_before`` and re-deferred without re-calling the provider.
RATE_LIMIT_MAX_COUNTDOWN_SECONDS = 600.0

# Used when the provider signalled a rate limit without a usable delay value.
RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS = 60.0

# Additive jitter (0..N seconds) de-correlates many orgs/tasks waking against
# the same provider at once (thundering-herd mitigation at the re-enqueue
# layer, keeping the frozen ``connectors`` gate untouched).
RATE_LIMIT_JITTER_SECONDS = 5.0


@dataclass(frozen=True)
class RateLimitDeferral:
    """A planned re-enqueue of a rate-limited sync task."""

    countdown: float
    """Seconds to pass to ``apply_async(countdown=...)`` (jittered, chunked)."""

    attempts: int
    """Updated deferral count to carry forward in task kwargs."""

    first_seen_at: str
    """ISO-8601 timestamp of the first deferral, carried forward unchanged."""

    not_before: str
    """ISO-8601 absolute time before which the provider must not be called."""


def _now(now: datetime | None) -> datetime:
    return now or datetime.now(timezone.utc)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _jittered(seconds: float) -> float:
    # Not security-sensitive: jitter only de-correlates retry timing.
    return max(0.0, seconds + random.uniform(0.0, RATE_LIMIT_JITTER_SECONDS))  # noqa: S311


def plan_rate_limit_deferral(
    *,
    retry_after_seconds: float | None,
    attempts: int,
    first_seen_at: str | None,
    now: datetime | None = None,
) -> RateLimitDeferral | None:
    """Plan the next deferral for a provider 429, or ``None`` when exhausted.

    ``None`` means the count/wall-clock budget is spent: callers must fall
    through to normal failure handling (stamp the run failed, etc.).
    """
    current = _now(now)
    first_seen = _parse_iso(first_seen_at) or current
    elapsed = (current - first_seen).total_seconds()

    if attempts >= RATE_LIMIT_MAX_DEFERRALS:
        return None
    if elapsed >= RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS:
        return None

    delay = retry_after_seconds
    if delay is None or delay <= 0:
        delay = RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS

    # Never schedule a wait that runs past the wall-clock deadline.
    remaining_budget = RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS - elapsed
    delay = min(float(delay), remaining_budget)

    not_before_ts = current.timestamp() + delay
    countdown = _jittered(min(delay, RATE_LIMIT_MAX_COUNTDOWN_SECONDS))

    return RateLimitDeferral(
        countdown=countdown,
        attempts=attempts + 1,
        first_seen_at=first_seen.isoformat(),
        not_before=datetime.fromtimestamp(not_before_ts, tz=timezone.utc).isoformat(),
    )


def plan_not_before_wait(
    not_before: str | None,
    *,
    now: datetime | None = None,
) -> float | None:
    """Countdown to re-defer a chunked wait, or ``None`` to proceed now.

    Called at the top of a sync task: if a carried ``not_before`` is still in
    the future, return the (chunked, jittered) countdown so the task
    re-enqueues itself **without calling the provider**; otherwise ``None``.
    """
    target = _parse_iso(not_before)
    if target is None:
        return None
    remaining = (target - _now(now)).total_seconds()
    if remaining <= 0:
        return None
    return _jittered(min(remaining, RATE_LIMIT_MAX_COUNTDOWN_SECONDS))


def rate_limit_metadata(deferral: RateLimitDeferral) -> dict[str, object]:
    """Build the ``_rate_limit_*`` kwargs to carry into the re-enqueued task."""
    return {
        "_rate_limit_attempts": deferral.attempts,
        "_rate_limit_first_seen_at": deferral.first_seen_at,
        "_rate_limit_not_before": deferral.not_before,
    }


def maybe_plan_rate_limit_deferral(
    exc: BaseException,
    *,
    attempts: int,
    first_seen_at: str | None,
    now: datetime | None = None,
) -> RateLimitDeferral | None:
    """Plan a deferral for a caught exception, or ``None``.

    Returns ``None`` when ``exc`` is not a rate-limit error OR the deferral
    budget is exhausted; in both cases the caller must run its normal failure
    handling. Returns a :class:`RateLimitDeferral` when the task should be
    re-enqueued instead of failed.
    """
    if not isinstance(exc, RateLimitException):
        return None
    return plan_rate_limit_deferral(
        retry_after_seconds=getattr(exc, "retry_after_seconds", None),
        attempts=attempts,
        first_seen_at=first_seen_at,
        now=now,
    )


def reenqueue_after_rate_limit(task: Any, deferral: RateLimitDeferral) -> None:
    """Re-enqueue a fresh run of ``task`` carrying updated deferral metadata."""
    request = task.request
    task.apply_async(
        args=list(request.args or []),
        kwargs={**(request.kwargs or {}), **rate_limit_metadata(deferral)},
        countdown=deferral.countdown,
    )


def reenqueue_rate_limit_chunk(task: Any, countdown: float) -> None:
    """Re-enqueue ``task`` unchanged to wait out a chunked ``not_before`` window.

    Used when a carried ``not_before`` is still in the future: the provider is
    NOT called; the task simply re-defers itself. Deferral metadata is
    preserved as-is (the count budget is not consumed by chunk waits).
    """
    request = task.request
    task.apply_async(
        args=list(request.args or []),
        kwargs=dict(request.kwargs or {}),
        countdown=countdown,
    )
