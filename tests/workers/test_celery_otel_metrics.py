from __future__ import annotations

from unittest.mock import Mock

import pytest

from dev_health_ops.workers import celery_app


def test_worker_process_initializes_otel_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_metrics = Mock(return_value=True)
    monkeypatch.setattr(celery_app, "init_metrics", init_metrics)

    celery_app._init_worker_metrics()

    init_metrics.assert_called_once_with(shutdown_on_exit=False)


def test_worker_process_flushes_otel_metrics_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shutdown_metrics = Mock(return_value=True)
    monkeypatch.setattr(celery_app, "shutdown_metrics", shutdown_metrics)

    celery_app._shutdown_worker_metrics()

    shutdown_metrics.assert_called_once_with()
