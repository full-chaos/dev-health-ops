"""Celery application factory and instance."""

import logging
import os
import time
from typing import Any

from celery import Celery
from celery.signals import (
    task_postrun,
    task_prerun,
    worker_init,
    worker_process_init,
    worker_process_shutdown,
)

from dev_health_ops.logging_config import configure_logging
from dev_health_ops.sentry import init_sentry
from dev_health_ops.tracing import init_tracing, instrument_celery

# Configure logging, Sentry, and OpenTelemetry for Celery workers
configure_logging()
init_sentry()
init_tracing()
instrument_celery()

# Per-task start-time registry for duration tracking
_task_start: dict[str, float] = {}


@task_prerun.connect
def _task_started(task_id: str, task: Any, **kwargs: Any) -> None:
    _task_start[task_id] = time.perf_counter()


@task_postrun.connect
def _task_finished(
    task_id: str,
    task: Any,
    state: str = "SUCCESS",
    **kwargs: Any,
) -> None:
    start = _task_start.pop(task_id, None)
    duration = (time.perf_counter() - start) if start is not None else 0.0
    try:
        from dev_health_ops.metrics.prometheus import record_celery_task

        record_celery_task(
            task_name=task.name,
            state=state.lower(),
            duration_seconds=duration,
        )
    except (ImportError, AttributeError, RuntimeError, TypeError, ValueError):
        logging.getLogger(__name__).debug(
            "Celery task metrics recording failed (non-fatal)",
            exc_info=True,
        )


@worker_init.connect
def _run_migrations_on_startup(**kwargs: Any) -> None:
    """Apply pending Alembic migrations when the worker process starts.

    Gated behind DEV_HEALTH_WORKER_AUTO_MIGRATE env var (default OFF).
    Migrations belong in a deploy/init step; auto-migrating from workers
    races Alembic across workers and gives workers schema write-authority.
    """
    _logger = logging.getLogger(__name__)
    auto_migrate = os.environ.get("DEV_HEALTH_WORKER_AUTO_MIGRATE", "").strip().lower()
    if auto_migrate not in ("1", "true"):
        _logger.info(
            "worker auto-migrate disabled; run 'dev-hops migrate' as a deploy step"
        )
        return
    try:
        from alembic import command

        from dev_health_ops.migrate import _make_alembic_config

        cfg = _make_alembic_config()
        command.upgrade(cfg, "head")
        _logger.info("Alembic migrations applied (upgrade to head)")
    except (ImportError, RuntimeError, OSError):
        _logger.exception("Auto-migration on worker startup failed (non-fatal)")


@worker_process_init.connect
def _worker_process_init(**kwargs: Any) -> None:
    """Reset process-local sink cache on worker process fork."""
    from dev_health_ops.metrics.sinks import factory

    factory.reset_process_sinks()


@worker_process_shutdown.connect
def _worker_process_shutdown(**kwargs: Any) -> None:
    """Close and clear process-local sink cache on worker process shutdown."""
    from dev_health_ops.metrics.sinks import factory

    factory.reset_process_sinks()


def create_celery_app() -> Celery:
    """Create and configure the Celery application."""
    app = Celery("dev_health_ops")

    # Load configuration from dev_health_ops.config module
    app.config_from_object("dev_health_ops.workers.config")

    # Auto-discover tasks in the workers.tasks module
    app.autodiscover_tasks(["dev_health_ops.workers"])

    return app


# Global Celery application instance
celery_app = create_celery_app()


# Optional: Expose app for celery CLI
app = celery_app
