"""Tests for CHAOS-2252 / CHAOS-2268: worker migration hook removal.

Issue 4: Workers must NEVER run migrations. The @worker_init migration hook
has been removed with the Celery app. Migrations are a
deploy/init-step concern (dev-hops migrate postgres|clickhouse).

CHAOS-2268: the ClickHouse sink's ambient ``ensure_tables()`` calls (reached
from Celery tasks -- at the time, via run_work_items_sync_job and friends;
CHAOS-5351 later deleted that function, native provider-sync is the only
work-items ingest path now -- and other Celery-dispatched tasks) also ran
SQL migrations. ``ensure_schema()`` now honours AUTO_RUN_MIGRATIONS=false (set on
worker/beat/api in compose, which gate on the one-shot ``migrate`` service);
the CLI bypasses the flag with ``force=True``.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


def test_worker_package_never_runs_alembic_migrations() -> None:
    """Workers must not migrate: no module under workers/ may call alembic's
    command.upgrade or register a migration-on-startup hook.

    (This used to assert the same about workers/celery_app.py; that module is
    deleted with the Celery app, so the guard now covers the whole package.)
    """
    workers = Path(__file__).resolve().parents[1] / "src" / "dev_health_ops" / "workers"
    sources = sorted(workers.glob("*.py"))
    assert sources, "workers/ has no modules: this test would prove nothing"
    for path in sources:
        source = path.read_text(encoding="utf-8")
        assert "command.upgrade" not in source, (
            f"{path.name} must not call command.upgrade; "
            "migrations belong in the deploy/init step (dev-hops migrate)"
        )
        assert "_run_migrations_on_startup" not in source, path.name


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
