"""Tests for CHAOS-2252 / CHAOS-2268: worker migration hook removal.

Issue 4: Workers must NEVER run migrations. The @worker_init migration hook
has been removed entirely from workers/celery_app.py. Migrations are a
deploy/init-step concern (dev-hops migrate postgres|clickhouse).

CHAOS-2268: the ClickHouse sink's ambient ``ensure_tables()`` calls (reached
from Celery tasks via run_work_items_sync_job and friends) also ran SQL
migrations. ``ensure_schema()`` now honours AUTO_RUN_MIGRATIONS=false (set on
worker/beat/api in compose, which gate on the one-shot ``migrate`` service);
the CLI bypasses the flag with ``force=True``.
"""

from __future__ import annotations

from unittest import mock

import pytest

import dev_health_ops.workers.celery_app as celery_module
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


def test_worker_module_has_no_migration_on_startup_hook() -> None:
    """The celery_app module must not register any worker_init handler
    that calls alembic command.upgrade.

    Verifies that _run_migrations_on_startup (or any equivalent) does not
    exist as a module-level attribute, and that no worker_init receiver
    in the module body calls command.upgrade.
    """
    # The function must be gone entirely
    assert not hasattr(celery_module, "_run_migrations_on_startup"), (
        "_run_migrations_on_startup must be removed from celery_app"
    )


def test_worker_module_source_has_no_upgrade_call() -> None:
    """No code path in celery_app.py calls alembic command.upgrade.

    Reads the module source to confirm the string 'command.upgrade' is
    absent — the only place it should appear is in migrate.py (CLI).
    """
    import inspect

    source = inspect.getsource(celery_module)
    assert "command.upgrade" not in source, (
        "celery_app must not call command.upgrade; "
        "migrations belong in the deploy/init step (dev-hops migrate)"
    )


def test_worker_module_has_no_worker_init_migration_receiver() -> None:
    """No worker_init signal receiver in celery_app imports or calls alembic."""
    import inspect

    from celery.signals import worker_init

    # Collect all receivers registered on worker_init
    receivers = [
        func
        for _, func in worker_init.receivers
        if inspect.getmodule(func) is celery_module
    ]

    for func in receivers:
        src = inspect.getsource(func)
        assert "alembic" not in src, (
            f"worker_init receiver {func.__name__!r} must not reference alembic; "
            "migrations belong in the deploy/init step"
        )
        assert "command.upgrade" not in src, (
            f"worker_init receiver {func.__name__!r} must not call command.upgrade"
        )


def _sink_with_fake_client() -> ClickHouseMetricsSink:
    return ClickHouseMetricsSink(
        dsn="clickhouse://ch:ch@localhost:8123/default", client=mock.MagicMock()
    )


def test_ensure_schema_skips_migrations_when_auto_run_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AUTO_RUN_MIGRATIONS=false turns ambient ensure_schema() into a no-op."""
    monkeypatch.setenv("AUTO_RUN_MIGRATIONS", "false")
    sink = _sink_with_fake_client()
    with mock.patch.object(sink, "_apply_sql_migrations") as apply:
        sink.ensure_schema()
        sink.ensure_tables()  # backward-compat alias must honour the flag too
    apply.assert_not_called()


def test_ensure_schema_force_bypasses_auto_run_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """force=True (the dev-hops migrate CLI path) always runs migrations."""
    monkeypatch.setenv("AUTO_RUN_MIGRATIONS", "false")
    sink = _sink_with_fake_client()
    with mock.patch.object(sink, "_apply_sql_migrations") as apply:
        sink.ensure_schema(force=True)
    apply.assert_called_once()


def test_ensure_schema_runs_migrations_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the env flag, behaviour is unchanged (deploy stacks without a
    one-shot migrate service still rely on ambient auto-migration)."""
    monkeypatch.delenv("AUTO_RUN_MIGRATIONS", raising=False)
    sink = _sink_with_fake_client()
    with mock.patch.object(sink, "_apply_sql_migrations") as apply:
        sink.ensure_schema()
    apply.assert_called_once()
