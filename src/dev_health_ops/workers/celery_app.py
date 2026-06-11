"""Celery application factory and instance."""

import logging
import time
from datetime import datetime, timezone
from typing import Any

from celery import Celery
from celery.signals import before_task_publish, task_postrun, task_prerun

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


@before_task_publish.connect
def _stamp_enqueued_at(headers: Any = None, **kwargs: Any) -> None:
    """Stamp publish time into the message headers (CHAOS-2299).

    Celery's task protocol carries no enqueue timestamp, so the queue-depth
    monitor (workers.queue_monitor) reads this header off the oldest queued
    message to report per-queue backlog age.
    """
    if isinstance(headers, dict) and "enqueued_at" not in headers:
        headers["enqueued_at"] = datetime.now(timezone.utc).isoformat()


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
