"""Celery application factory and instance."""

from celery import Celery
from dev_health_ops.logging_config import configure_logging
from dev_health_ops.sentry import init_sentry

# Configure logging and Sentry for Celery workers
configure_logging()
init_sentry()


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
