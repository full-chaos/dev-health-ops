"""Celery application factory and instance."""

import time

from celery import Celery
from celery.signals import task_prerun, task_postrun

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
def _task_started(task_id: str, task, **kwargs) -> None:  # type: ignore[no-untyped-def]
    _task_start[task_id] = time.perf_counter()


@task_postrun.connect
def _task_finished(task_id: str, task, state: str = "SUCCESS", **kwargs) -> None:  # type: ignore[no-untyped-def]
    start = _task_start.pop(task_id, None)
    duration = (time.perf_counter() - start) if start is not None else 0.0
    try:
        from dev_health_ops.metrics.prometheus import record_celery_task

        record_celery_task(
            task_name=task.name,
            state=state.lower(),
            duration_seconds=duration,
        )
    except Exception:
        pass  # Metrics recording is best-effort; never fail a task over it


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
