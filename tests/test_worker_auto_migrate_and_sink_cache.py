"""Tests for CHAOS-2252: worker migration hook removal.

Issue 4: Workers must NEVER run migrations. The @worker_init migration hook
has been removed entirely from workers/celery_app.py. Migrations are a
deploy/init-step concern (dev-hops migrate postgres|clickhouse).
"""

from __future__ import annotations

import dev_health_ops.workers.celery_app as celery_module


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
