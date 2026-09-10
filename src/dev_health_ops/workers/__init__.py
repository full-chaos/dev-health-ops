"""Celery worker package for background job processing."""

# CHAOS-4026 (2026-08-21): metrics_tasks.py, product_tasks.py, and
# report_scheduler.py were deleted -- Go now owns the periodic cadences
# they dispatched and Celery Beat has not scheduled them since the
# 2026-08-19 stop. See tests/workers/test_celery_dead_code_contract.py.
# CHAOS-3093: work_graph_tasks.py itself was deleted -- both its Celery
# tasks (run_investment_materialize_chunk/finalize_investment_materialize_
# partitioned/run_membership_backfill) were already dead (no consumer since
# 2026-08-19) and dispatch_investment_materialize_partitioned was dead-into-
# the-void the same way (investment.materialize is river-only,
# rollback_route=none).
from dev_health_ops.workers import system_tasks

__all__ = [
    "system_tasks",
]
