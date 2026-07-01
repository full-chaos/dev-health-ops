"""Rate-limit deferral planning for the unit sync worker.

Provider rate-limit signals (HTTP 429 / ``Retry-After``) are treated as
*deferred work*, not task failures. When a unit hits a provider rate limit,
:func:`plan_rate_limit_deferral` computes the next deferral -- a jittered,
chunked countdown plus the ``attempts`` / ``first_seen_at`` / ``not_before``
bookkeeping the worker (``workers/sync_units.py``) stamps onto the unit so it
re-runs as ``RETRYING`` instead of consuming the genuine-failure retry budget.

Two budgets bound the deferral so a permanently rate-limited provider still
eventually surfaces as a real failure:

* a **count** budget (:data:`RATE_LIMIT_MAX_DEFERRALS`), incremented once per
  provider 429, and
* a **wall-clock** budget (:data:`RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS`) measured
  from the first deferral.

Long server delays (e.g. GitHub primary-limit resets up to ~1h) are *chunked*:
a single countdown is capped at :data:`RATE_LIMIT_MAX_COUNTDOWN_SECONDS` and an
absolute ``not_before`` timestamp is carried forward so the unit re-defers
**without calling the provider again** until the window elapses.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import datetime, timezone

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
