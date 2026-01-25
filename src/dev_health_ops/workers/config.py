"""Celery configuration from environment variables."""

import os

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
    "metrics": {},  # Long-running metrics jobs
    "sync": {},  # Data sync tasks
}

# Result settings
result_expires = 86400  # Results expire after 24 hours
