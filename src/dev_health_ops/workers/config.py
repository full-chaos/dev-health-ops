"""Celery configuration from environment variables."""

import os

from celery.schedules import crontab

# Broker and backend (Redis)
broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# Serialization
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]

# Timezone
timezone = "UTC"
enable_utc = True

# Task settings
task_track_started = True
task_time_limit = 3600  # 1 hour max per task
task_soft_time_limit = 3300  # Soft limit at 55 minutes

# Retry settings
task_default_retry_delay = 60  # 1 minute between retries
task_max_retries = 3

# Queue settings
task_default_queue = "default"
task_queues = {
    "default": {},
    "metrics": {},
    "sync": {},
    "webhooks": {},
    "ingest": {},
}

# Beat schedule (periodic tasks)
beat_schedule = {
    "dispatch-scheduled-syncs": {
        "task": "dev_health_ops.workers.tasks.dispatch_scheduled_syncs",
        "schedule": 300.0,
        "options": {"queue": "default"},
    },
    "dispatch-scheduled-metrics": {
        "task": "dev_health_ops.workers.tasks.dispatch_scheduled_metrics",
        "schedule": 300.0,
        "options": {"queue": "default"},
    },
    "run-daily-metrics": {
        "task": "dev_health_ops.workers.tasks.dispatch_daily_metrics_partitioned",
        "schedule": crontab(hour=1, minute=0),
        "options": {"queue": "default"},
    },
    "sync-team-drift": {
        "task": "dev_health_ops.workers.tasks.sync_team_drift",
        "schedule": crontab(hour=2, minute=30),
        "options": {"queue": "sync"},
    },
    "reconcile-team-members": {
        "task": "dev_health_ops.workers.tasks.reconcile_team_members",
        "schedule": crontab(hour=3, minute=0),
        "options": {"queue": "sync"},
    },
    "run-capacity-forecast": {
        "task": "dev_health_ops.workers.tasks.run_capacity_forecast_job",
        "schedule": crontab(hour=4, minute=0, day_of_week="monday"),
        "kwargs": {"all_teams": True},
        "options": {"queue": "metrics"},
    },
    "process-ingest-streams": {
        "task": "dev_health_ops.workers.tasks.run_ingest_consumer",
        "schedule": 30.0,
        "kwargs": {"max_iterations": 50},
        "options": {"queue": "ingest"},
    },
    "phone-home-heartbeat": {
        "task": "dev_health_ops.workers.tasks.phone_home_heartbeat",
        "schedule": crontab(hour=0, minute=0),
        "options": {"queue": "default"},
    },
}

# Result settings
result_expires = 86400  # Results expire after 24 hours
