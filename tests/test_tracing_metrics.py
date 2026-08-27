from __future__ import annotations

from unittest.mock import Mock, call

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


# CHAOS-4317: init_tracing used to swallow every non-ImportError failure with
# a bare logger.warning and never retry -- a transient pthread_create
# failure (the exact failure OTel hit on 2026-08-26, one line before the
# metrics-bridge subprocess hang) left tracing permanently off for the rest
# of the process with no telemetry anywhere. These pin the fixed behavior:
# retry once, count both attempts, never raise.


def test_init_tracing_retries_once_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tracing, "_initialized", False)
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setattr(tracing.time, "sleep", lambda _seconds: None)

    attempts: list[int] = []

    def fake_try_init_tracing(
        *, configure_metrics: bool
    ) -> tuple[bool, Exception | None]:
        attempts.append(1)
        if len(attempts) == 1:
            return False, RuntimeError(
                "pthread_create failed: Resource temporarily unavailable"
            )
        return True, None

    monkeypatch.setattr(tracing, "_try_init_tracing", fake_try_init_tracing)
    counter = Mock()
    monkeypatch.setattr(
        "dev_health_ops.metrics.prometheus.DEV_HEALTH_OTEL_INIT_FAILURES_TOTAL",
        counter,
    )

    # Falsifier: before this ticket, the first (and only) failed attempt
    # would have returned False here with no retry ever attempted.
    assert tracing.init_tracing(configure_metrics=False) is True
    assert len(attempts) == 2
    counter.labels.assert_called_once_with(attempt="initial")
    counter.labels.return_value.inc.assert_called_once()


def test_init_tracing_exhausted_retry_is_non_fatal_and_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tracing, "_initialized", False)
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setattr(tracing.time, "sleep", lambda _seconds: None)

    def fake_try_init_tracing(
        *, configure_metrics: bool
    ) -> tuple[bool, Exception | None]:
        return False, RuntimeError(
            "pthread_create failed: Resource temporarily unavailable"
        )

    monkeypatch.setattr(tracing, "_try_init_tracing", fake_try_init_tracing)
    counter = Mock()
    monkeypatch.setattr(
        "dev_health_ops.metrics.prometheus.DEV_HEALTH_OTEL_INIT_FAILURES_TOTAL",
        counter,
    )

    # Falsifier: a version that re-raises instead of returning False would
    # crash the caller (api/main.py's module-level init_tracing() call) --
    # this must return, never raise.
    assert tracing.init_tracing(configure_metrics=False) is False
    assert counter.labels.call_args_list == [
        call(attempt="initial"),
        call(attempt="final"),
    ]
    assert counter.labels.return_value.inc.call_count == 2


def test_init_tracing_import_error_does_not_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tracing, "_initialized", False)
    monkeypatch.setenv("OTEL_ENABLED", "true")

    attempts: list[int] = []

    def fake_try_init_tracing(
        *, configure_metrics: bool
    ) -> tuple[bool, Exception | None]:
        attempts.append(1)
        return False, None

    monkeypatch.setattr(tracing, "_try_init_tracing", fake_try_init_tracing)
    counter = Mock()
    monkeypatch.setattr(
        "dev_health_ops.metrics.prometheus.DEV_HEALTH_OTEL_INIT_FAILURES_TOTAL",
        counter,
    )

    # An ImportError means the opentelemetry package is not installed --
    # retrying cannot help, so this must return immediately, uncounted.
    assert tracing.init_tracing(configure_metrics=False) is False
    assert len(attempts) == 1
    counter.labels.assert_not_called()
