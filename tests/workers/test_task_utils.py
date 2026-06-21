from __future__ import annotations

import pytest

from dev_health_ops.workers.task_utils import (
    _get_db_url,
    _validate_worker_clickhouse_uri,
)


def test_worker_db_url_uses_clickhouse_uri_not_database_fallback(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://ch:ch@clickhouse:8123/default")
    monkeypatch.setenv("DATABASE_URI", "clickhouse://fake:8123/test")
    monkeypatch.setenv("DATABASE_URL", "clickhouse://fake:8123/test")

    assert _get_db_url() == "clickhouse://ch:ch@clickhouse:8123/default"


def test_worker_db_url_preserves_database_uri_fallback(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.setenv("DATABASE_URI", "postgresql://db.example/dev_health")
    monkeypatch.setenv("DATABASE_URL", "clickhouse://fake:8123/test")

    assert _get_db_url() == "postgresql://db.example/dev_health"


def test_worker_clickhouse_uri_requires_configured_uri():
    with pytest.raises(RuntimeError, match="CLICKHOUSE_URI"):
        _validate_worker_clickhouse_uri("")


def test_worker_clickhouse_uri_rejects_placeholder_host_outside_tests(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("DEV_HEALTH_ALLOW_PLACEHOLDER_CLICKHOUSE_URI", raising=False)

    with pytest.raises(RuntimeError, match="placeholder ClickHouse URI host 'fake'"):
        _validate_worker_clickhouse_uri("clickhouse://fake:8123/test")


def test_worker_clickhouse_uri_allows_placeholder_when_enabled(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_PLACEHOLDER_CLICKHOUSE_URI", "true")

    assert (
        _validate_worker_clickhouse_uri("clickhouse://fake:8123/test")
        == "clickhouse://fake:8123/test"
    )
