"""Tests for providers._ratelimit.gate_call context manager."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)
from dev_health_ops.providers._ratelimit import gate_call


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
