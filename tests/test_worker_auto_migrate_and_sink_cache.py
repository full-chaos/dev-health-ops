"""Tests for CHAOS-2252: worker auto-migrate gate and process-local sink cache.

Issue 4: _run_migrations_on_startup must be gated behind
DEV_HEALTH_WORKER_AUTO_MIGRATE (default OFF).

Issue 5: get_process_sink returns a cached instance per (pid, dsn);
create_sink still returns fresh owned instances; reset_process_sinks
closes and clears the cache.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Issue 4 — auto-migrate gate
# ---------------------------------------------------------------------------


def _invoke_run_migrations(**env_overrides: str) -> None:
    """Import and call _run_migrations_on_startup with a patched environment."""
    # Re-import to get a fresh reference each time (module is already loaded,
    # but we patch os.environ so the function reads the patched value).
    from dev_health_ops.workers.celery_app import _run_migrations_on_startup

    _run_migrations_on_startup()


@patch("dev_health_ops.workers.celery_app.os.environ.get")
def test_auto_migrate_disabled_by_default(mock_env_get) -> None:
    """When DEV_HEALTH_WORKER_AUTO_MIGRATE is unset, upgrade must NOT be called."""
    # Simulate env var absent (returns "")
    mock_env_get.return_value = ""

    with patch("alembic.command.upgrade") as mock_upgrade:
        from dev_health_ops.workers.celery_app import _run_migrations_on_startup

        _run_migrations_on_startup()

    mock_upgrade.assert_not_called()


@patch.dict(os.environ, {"DEV_HEALTH_WORKER_AUTO_MIGRATE": ""}, clear=False)
def test_auto_migrate_disabled_when_empty_string() -> None:
    """Empty string is treated as disabled."""
    with patch("alembic.command.upgrade") as mock_upgrade:
        from dev_health_ops.workers.celery_app import _run_migrations_on_startup

        _run_migrations_on_startup()

    mock_upgrade.assert_not_called()


@patch.dict(os.environ, {"DEV_HEALTH_WORKER_AUTO_MIGRATE": "0"}, clear=False)
def test_auto_migrate_disabled_when_zero() -> None:
    """'0' is treated as disabled (only '1' and 'true' enable)."""
    with patch("alembic.command.upgrade") as mock_upgrade:
        from dev_health_ops.workers.celery_app import _run_migrations_on_startup

        _run_migrations_on_startup()

    mock_upgrade.assert_not_called()


@patch.dict(os.environ, {"DEV_HEALTH_WORKER_AUTO_MIGRATE": "1"}, clear=False)
def test_auto_migrate_enabled_when_one() -> None:
    """'1' enables auto-migrate; alembic.command.upgrade must be called."""
    fake_cfg = MagicMock()
    with (
        patch("dev_health_ops.migrate._make_alembic_config", return_value=fake_cfg),
        patch("alembic.command.upgrade") as mock_upgrade,
    ):
        from dev_health_ops.workers.celery_app import _run_migrations_on_startup

        _run_migrations_on_startup()

    mock_upgrade.assert_called_once_with(fake_cfg, "head")


@patch.dict(os.environ, {"DEV_HEALTH_WORKER_AUTO_MIGRATE": "true"}, clear=False)
def test_auto_migrate_enabled_when_true() -> None:
    """'true' (case-insensitive) enables auto-migrate."""
    fake_cfg = MagicMock()
    with (
        patch("dev_health_ops.migrate._make_alembic_config", return_value=fake_cfg),
        patch("alembic.command.upgrade") as mock_upgrade,
    ):
        from dev_health_ops.workers.celery_app import _run_migrations_on_startup

        _run_migrations_on_startup()

    mock_upgrade.assert_called_once_with(fake_cfg, "head")


@patch.dict(os.environ, {"DEV_HEALTH_WORKER_AUTO_MIGRATE": "TRUE"}, clear=False)
def test_auto_migrate_enabled_case_insensitive() -> None:
    """'TRUE' (uppercase) is also accepted."""
    fake_cfg = MagicMock()
    with (
        patch("dev_health_ops.migrate._make_alembic_config", return_value=fake_cfg),
        patch("alembic.command.upgrade") as mock_upgrade,
    ):
        from dev_health_ops.workers.celery_app import _run_migrations_on_startup

        _run_migrations_on_startup()

    mock_upgrade.assert_called_once_with(fake_cfg, "head")


# ---------------------------------------------------------------------------
# Issue 5 — process-local sink cache
# ---------------------------------------------------------------------------

DSN = "clickhouse://localhost:8123/default"
DSN2 = "clickhouse://localhost:8123/other"


@pytest.fixture(autouse=True)
def _clear_process_sinks():
    """Ensure the process-sink cache is empty before and after each test."""
    from dev_health_ops.metrics.sinks import factory

    factory._process_sinks.clear()
    yield
    factory._process_sinks.clear()


def _make_mock_sink():
    sink = MagicMock()
    sink.close = MagicMock()
    return sink


def test_get_process_sink_returns_same_instance_for_same_dsn() -> None:
    """get_process_sink returns the identical object on repeated calls."""
    mock_sink = _make_mock_sink()
    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        return_value=mock_sink,
    ):
        from dev_health_ops.metrics.sinks.factory import get_process_sink

        s1 = get_process_sink(DSN)
        s2 = get_process_sink(DSN)

    assert s1 is s2


def test_get_process_sink_different_dsn_returns_different_instances() -> None:
    """Different DSNs produce different cached sinks."""
    mock_sink_a = _make_mock_sink()
    mock_sink_b = _make_mock_sink()
    side_effects = [mock_sink_a, mock_sink_b]

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        side_effect=side_effects,
    ):
        from dev_health_ops.metrics.sinks.factory import get_process_sink

        sa = get_process_sink(DSN)
        sb = get_process_sink(DSN2)

    assert sa is not sb
    assert sa is mock_sink_a
    assert sb is mock_sink_b


def test_create_sink_returns_fresh_instance_each_call() -> None:
    """create_sink always returns a new owned instance (no caching)."""
    mock_a = _make_mock_sink()
    mock_b = _make_mock_sink()

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        side_effect=[mock_a, mock_b],
    ):
        from dev_health_ops.metrics.sinks.factory import create_sink

        s1 = create_sink(DSN)
        s2 = create_sink(DSN)

    assert s1 is not s2
    assert s1 is mock_a
    assert s2 is mock_b


def test_reset_process_sinks_closes_and_clears() -> None:
    """reset_process_sinks closes every cached sink and empties the cache."""
    mock_sink = _make_mock_sink()

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        return_value=mock_sink,
    ):
        from dev_health_ops.metrics.sinks.factory import (
            _process_sinks,
            get_process_sink,
            reset_process_sinks,
        )

        get_process_sink(DSN)
        assert len(_process_sinks) == 1

        reset_process_sinks()

    mock_sink.close.assert_called_once()
    assert len(_process_sinks) == 0


def test_reset_process_sinks_is_idempotent() -> None:
    """Calling reset_process_sinks on an empty cache does not raise."""
    from dev_health_ops.metrics.sinks.factory import reset_process_sinks

    reset_process_sinks()
    reset_process_sinks()  # second call must not raise


def test_reset_process_sinks_tolerates_close_error() -> None:
    """reset_process_sinks continues even if a sink.close() raises."""
    mock_sink = _make_mock_sink()
    mock_sink.close.side_effect = RuntimeError("boom")

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        return_value=mock_sink,
    ):
        from dev_health_ops.metrics.sinks.factory import (
            _process_sinks,
            get_process_sink,
            reset_process_sinks,
        )

        get_process_sink(DSN)

    # Should not raise even though close() raises
    reset_process_sinks()
    assert len(_process_sinks) == 0


def test_get_process_sink_reads_env_dsn(monkeypatch) -> None:
    """get_process_sink resolves DSN from CLICKHOUSE_URI when not passed."""
    monkeypatch.setenv("CLICKHOUSE_URI", DSN)
    mock_sink = _make_mock_sink()

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        return_value=mock_sink,
    ):
        from dev_health_ops.metrics.sinks.factory import get_process_sink

        s = get_process_sink()

    assert s is mock_sink


def test_get_process_sink_raises_without_dsn(monkeypatch) -> None:
    """get_process_sink raises ValueError when no DSN is available."""
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DEV_HEALTH_SINK", raising=False)

    from dev_health_ops.metrics.sinks.factory import get_process_sink

    with pytest.raises(ValueError, match="No sink DSN provided"):
        get_process_sink()


def test_hot_path_reuses_process_sink_across_invocations() -> None:
    """Simulates two sequential task invocations: get_process_sink must return
    the same instance both times (no re-create, no churn).
    """
    mock_sink = _make_mock_sink()
    call_count = 0

    def _factory(dsn: str):
        nonlocal call_count
        call_count += 1
        return mock_sink

    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink",
        side_effect=_factory,
    ):
        from dev_health_ops.metrics.sinks.factory import get_process_sink

        # First task invocation
        s1 = get_process_sink(DSN)
        # Second task invocation (same process, same DSN)
        s2 = get_process_sink(DSN)

    assert s1 is s2, "process sink must be reused, not re-created"
    assert call_count == 1, (
        f"ClickHouseMetricsSink constructed {call_count} times; expected 1"
    )
    # Sink must NOT have been closed between invocations
    mock_sink.close.assert_not_called()
