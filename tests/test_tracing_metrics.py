from __future__ import annotations

from unittest.mock import Mock

import pytest

from dev_health_ops import tracing


def test_shutdown_metrics_flushes_before_closing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = Mock()
    provider.force_flush.return_value = True
    monkeypatch.setattr(tracing, "_meter_provider", provider)

    assert tracing.shutdown_metrics() is True

    provider.force_flush.assert_called_once_with(timeout_millis=10_000)
    provider.shutdown.assert_called_once_with(timeout_millis=30_000)
