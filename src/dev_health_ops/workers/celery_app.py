"""Celery application factory and instance."""

from celery import Celery


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
