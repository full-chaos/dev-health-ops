"""Core business logic module — provider/framework agnostic.

Contains pure functions that are shared across the API, Celery workers, and
analytics/metrics layers. Nothing here should import from dev_health_ops.api.*
or have any I/O dependency (no DB, no HTTP, no filesystem).
"""
