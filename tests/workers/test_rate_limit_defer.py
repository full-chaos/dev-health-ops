"""Tests for workers.rate_limit_defer deferral planning."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dev_health_ops.workers.rate_limit_defer import (
    RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS,
    RATE_LIMIT_JITTER_SECONDS,
    RATE_LIMIT_MAX_COUNTDOWN_SECONDS,
    RATE_LIMIT_MAX_DEFERRALS,
    RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    plan_rate_limit_deferral,
)

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _within_jitter(value: float, base: float) -> bool:
    return base <= value <= base + RATE_LIMIT_JITTER_SECONDS


class TestPlanRateLimitDeferral:
    def test_basic_uses_server_delay(self) -> None:
        d = plan_rate_limit_deferral(
            retry_after_seconds=42.0, attempts=0, first_seen_at=None, now=NOW
        )
        assert d is not None
        assert _within_jitter(d.countdown, 42.0)
        assert d.attempts == 1
        # first_seen_at defaults to now on the first deferral.
        assert d.first_seen_at == NOW.isoformat()
        # not_before is the absolute time the provider may be called again.
        expected_not_before = (NOW + timedelta(seconds=42.0)).isoformat()
        assert d.not_before == expected_not_before

    def test_default_countdown_when_no_server_delay(self) -> None:
        d = plan_rate_limit_deferral(
            retry_after_seconds=None, attempts=0, first_seen_at=None, now=NOW
        )
        assert d is not None
        assert _within_jitter(d.countdown, RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS)

    def test_zero_or_negative_delay_uses_default(self) -> None:
        d = plan_rate_limit_deferral(
            retry_after_seconds=0.0, attempts=0, first_seen_at=None, now=NOW
        )
        assert d is not None
        assert _within_jitter(d.countdown, RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS)

    def test_count_budget_exhausted_returns_none(self) -> None:
        assert (
            plan_rate_limit_deferral(
                retry_after_seconds=10.0,
                attempts=RATE_LIMIT_MAX_DEFERRALS,
                first_seen_at=NOW.isoformat(),
                now=NOW,
            )
            is None
        )

    def test_wall_clock_budget_exhausted_returns_none(self) -> None:
        first_seen = NOW - timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + 1)
        assert (
            plan_rate_limit_deferral(
                retry_after_seconds=10.0,
                attempts=1,
                first_seen_at=first_seen.isoformat(),
                now=NOW,
            )
            is None
        )

    def test_long_delay_is_chunked_but_not_before_is_full(self) -> None:
        # A 1-hour server delay: countdown is capped to one chunk, while
        # not_before carries the full window so the task re-defers without
        # re-calling the provider.
        d = plan_rate_limit_deferral(
            retry_after_seconds=3600.0, attempts=0, first_seen_at=None, now=NOW
        )
        assert d is not None
        assert _within_jitter(d.countdown, RATE_LIMIT_MAX_COUNTDOWN_SECONDS)
        assert d.not_before == (NOW + timedelta(seconds=3600.0)).isoformat()

    def test_first_seen_at_is_preserved(self) -> None:
        first_seen = (NOW - timedelta(seconds=120)).isoformat()
        d = plan_rate_limit_deferral(
            retry_after_seconds=10.0, attempts=2, first_seen_at=first_seen, now=NOW
        )
        assert d is not None
        assert d.first_seen_at == first_seen
        assert d.attempts == 3

    def test_delay_clamped_to_remaining_wall_clock_budget(self) -> None:
        # Only 30s of wall-clock budget remains; a 5-min server delay must not
        # schedule a wait past the deadline.
        first_seen = NOW - timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS - 30)
        d = plan_rate_limit_deferral(
            retry_after_seconds=300.0,
            attempts=1,
            first_seen_at=first_seen.isoformat(),
            now=NOW,
        )
        assert d is not None
        # not_before may not exceed first_seen + total budget.
        deadline = first_seen + timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS)
        assert datetime.fromisoformat(d.not_before) <= deadline
