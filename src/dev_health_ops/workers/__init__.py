"""Celery worker package for background job processing."""

# CHAOS-4026 (2026-08-21): metrics_tasks.py, product_tasks.py, and
# report_scheduler.py were deleted -- Go now owns the periodic cadences
# they dispatched and Celery Beat has not scheduled them since the
# 2026-08-19 stop. See tests/workers/test_celery_dead_code_contract.py.
from dev_health_ops.workers import (
    system_tasks,
    work_graph_tasks,
)

__all__ = [
    "system_tasks",
    "work_graph_tasks",
]
