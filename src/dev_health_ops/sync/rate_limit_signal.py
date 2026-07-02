"""Provider-neutral rate-limit signal (CHAOS-2742 / CHAOS-2753).

One structured value crosses every provider ``client`` -> ``worker`` boundary so
a rate-limit observation can be attributed to a budget bucket and (in a later
slice) persisted as a shared cooldown. Provider clients populate the fields they
already hold at the classification site -- ``provider``, ``host``, ``dimension``,
``reason``, ``retry_after_seconds``, ``reset_at`` and ``request_id``. The
``integration_id`` and ``route_family`` fields are enriched at the worker
boundary (ws-d), so clients leave them ``None``.

``dimension`` reuses :class:`~dev_health_ops.sync.budget_types.BudgetDimension`
so signals join the same buckets the budget estimators use.

``reset_at`` is always normalized to a timezone-aware UTC :class:`datetime`
(or ``None``). Providers report reset windows in different units -- Linear uses
epoch **milliseconds**, GitHub and GitLab use epoch **seconds** -- so convert at
the classification site with :meth:`RateLimitSignal.reset_at_from_epoch_seconds`
or :meth:`RateLimitSignal.reset_at_from_epoch_millis` before constructing the
signal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from dev_health_ops.sync.budget_types import BudgetDimension


def _coerce_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class RateLimitSignal:
    """A provider-neutral, structured rate-limit observation."""

    provider: str
    host: str | None = None
    integration_id: str | None = None
    route_family: str | None = None
    dimension: BudgetDimension | None = None
    retry_after_seconds: float | None = None
    reset_at: datetime | None = None
    reason: str | None = None
    request_id: str | None = None

    def __post_init__(self) -> None:
        # All reset windows are absolute UTC instants; normalize tz-naive
        # datetimes to UTC and convert tz-aware ones to UTC. Frozen dataclass,
        # so mutate through ``object.__setattr__``.
        reset = self.reset_at
        if isinstance(reset, datetime):
            if reset.tzinfo is None:
                object.__setattr__(self, "reset_at", reset.replace(tzinfo=timezone.utc))
            else:
                object.__setattr__(self, "reset_at", reset.astimezone(timezone.utc))

    @staticmethod
    def reset_at_from_epoch_seconds(value: object) -> datetime | None:
        """Build a UTC ``reset_at`` from an epoch-**seconds** value (GitHub/GitLab)."""
        epoch = _coerce_float(value)
        if epoch is None or epoch <= 0:
            return None
        return datetime.fromtimestamp(epoch, tz=timezone.utc)

    @staticmethod
    def reset_at_from_epoch_millis(value: object) -> datetime | None:
        """Build a UTC ``reset_at`` from an epoch-**milliseconds** value (Linear)."""
        epoch_ms = _coerce_float(value)
        if epoch_ms is None or epoch_ms <= 0:
            return None
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)

    @staticmethod
    def reset_at_from_iso8601(value: object) -> datetime | None:
        """Build a UTC ``reset_at`` from an ISO 8601 timestamp string (Jira).

        Verified against Atlassian's Jira Cloud rate-limiting docs
        (developer.atlassian.com/cloud/jira/platform/rate-limiting/):
        ``X-RateLimit-Reset`` is documented as "ISO 8601 timestamp when the
        current window resets" (e.g. ``2025-10-08T15:00:00Z``) -- an absolute
        UTC instant, NOT an epoch-seconds/-millis integer like GitHub/GitLab
        (``Retry-After`` remains the authoritative delay for Jira; this is
        only a supplementary hint, and parsing failures degrade to ``None``
        gracefully rather than raising).
        """
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None

    def to_dict(self) -> dict[str, object | None]:
        return {
            "provider": self.provider,
            "host": self.host,
            "integration_id": self.integration_id,
            "route_family": self.route_family,
            "dimension": self.dimension.value if self.dimension is not None else None,
            "retry_after_seconds": self.retry_after_seconds,
            "reset_at": self.reset_at.isoformat()
            if self.reset_at is not None
            else None,
            "reason": self.reason,
            "request_id": self.request_id,
        }
