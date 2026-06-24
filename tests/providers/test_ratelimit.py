"""Tests for providers._ratelimit.gate_call context manager."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)
from dev_health_ops.providers._ratelimit import (
    gate_call,
    parse_retry_after_header,
)


class TestGateCall:
    def test_success_calls_wait_then_reset(self) -> None:
        gate = MagicMock(spec=RateLimitGate)
        with gate_call(gate):
            pass
        gate.wait_sync.assert_called_once()
        gate.reset.assert_called_once()
        gate.penalize.assert_not_called()

    def test_failure_calls_penalize_and_reraises(self) -> None:
        gate = MagicMock(spec=RateLimitGate)

        with pytest.raises(RuntimeError, match="boom"):
            with gate_call(gate):
                raise RuntimeError("boom")

        gate.wait_sync.assert_called_once()
        gate.reset.assert_not_called()
        gate.penalize.assert_called_once_with(None)

    def test_failure_with_explicit_retry_after(self) -> None:
        gate = MagicMock(spec=RateLimitGate)

        with pytest.raises(ValueError):
            with gate_call(gate, retry_after=12.5):
                raise ValueError("throttled")

        gate.penalize.assert_called_once_with(12.5)

    def test_swallow_flag_suppresses_exception(self) -> None:
        gate = MagicMock(spec=RateLimitGate)
        with gate_call(gate, swallow=True):
            raise RuntimeError("swallow me")

        gate.penalize.assert_called_once_with(None)

    def test_real_gate_integration_success(self) -> None:
        gate = RateLimitGate(RateLimitConfig(initial_backoff_seconds=0.01))
        # Use the real gate to confirm wait/reset don't raise
        with gate_call(gate):
            value = 42
        assert value == 42


class TestParseRetryAfterHeader:
    def test_delta_seconds(self) -> None:
        assert parse_retry_after_header({"Retry-After": "120"}) == 120.0

    def test_delta_seconds_float(self) -> None:
        assert parse_retry_after_header({"Retry-After": "12.5"}) == 12.5

    def test_negative_seconds_clamped_to_zero(self) -> None:
        assert parse_retry_after_header({"Retry-After": "-5"}) == 0.0

    def test_missing_header_returns_none(self) -> None:
        assert parse_retry_after_header({}) is None

    def test_none_headers_returns_none(self) -> None:
        assert parse_retry_after_header(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert parse_retry_after_header({"Retry-After": "   "}) is None

    def test_garbage_returns_none(self) -> None:
        assert parse_retry_after_header({"Retry-After": "soon"}) is None

    def test_http_date_future(self) -> None:
        from datetime import datetime, timedelta, timezone
        from email.utils import format_datetime

        future = datetime.now(timezone.utc) + timedelta(seconds=300)
        result = parse_retry_after_header({"Retry-After": format_datetime(future)})
        assert result is not None
        # Allow scheduling slack; should be close to 300s and strictly positive.
        assert 280.0 <= result <= 300.0

    def test_http_date_past_clamped_to_zero(self) -> None:
        from datetime import datetime, timedelta, timezone
        from email.utils import format_datetime

        past = datetime.now(timezone.utc) - timedelta(seconds=300)
        assert parse_retry_after_header({"Retry-After": format_datetime(past)}) == 0.0
