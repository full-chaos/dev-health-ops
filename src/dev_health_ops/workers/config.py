"""Celery configuration from environment variables."""

import os
from typing import Any


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# Broker and backend (Valkey, using redis:// wire protocol)
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
task_acks_late = False
task_reject_on_worker_lost = False

late_ack_excluded_tasks: tuple[str, ...] = (
    # CHAOS-4026: the daily-metrics partitioned dispatch chain,
    # dispatch_capacity_forecast, dispatch_complexity_job,
    # dispatch_release_impact, dispatch_membership_backfill, and
    # dispatch_scheduled_reports were deleted -- Go now owns these cadences
    # and Celery Beat has not scheduled them since the 2026-08-19 stop.
    #
    # CHAOS-3093: dispatch_investment_materialize_partitioned (workers/
    # work_graph_tasks.py) and flush_external_ingest_recompute (workers/
    # external_ingest_recompute.py) were both deleted outright -- dead-into-
    # the-void, no Celery consumer since 2026-08-19 -- so neither registers
    # with the Celery app any more and both were removed from this tuple.
    # PR2a' deleted dispatch_scheduled_syncs (workers/sync_scheduler.py) the
    # same way -- internal/scheduler/sync's coordinator/loop owns this
    # cadence natively now.
    #
    # PR2b dropped phone_home_heartbeat's own `@celery_app.task` decorator
    # (Celery has had zero consumers since CHAOS-4026) -- it no longer
    # registers with the Celery app, so its exclusion entry is removed too.
    # This tuple is now empty.
)
task_annotations = {
    task_name: {"acks_late": False, "reject_on_worker_lost": False}
    for task_name in late_ack_excluded_tasks
}

# Worker settings
# Long-running tasks (sync, stream consumers) make prefetching dangerous:
# with the default multiplier (4) a 2-slot worker reserves up to 8 messages,
# and once those reservations fill with slow-queue messages the QoS window
# never opens — newer messages on other queues (e.g. Sync Now on `sync`)
# are never fetched until a restart releases the unacked reservations.
# One-at-a-time fetching keeps cross-queue round-robin fair (CHAOS-2277).
worker_prefetch_multiplier = 1

worker_disable_prefetch = True

stream_consumer_schedule_seconds = 30.0
stream_consumer_max_iterations = 5
stream_consumer_expires_seconds = 30

# Retry settings
task_default_retry_delay = 60  # 1 minute between retries
task_max_retries = 3
sync_unit_expired_lease_max_retries = max(
    0,
    _int_env("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", 1),
)
sync_unit_expired_lease_retry_backoff_seconds = max(
    0,
    _int_env("SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS", 60),
)

# Queue settings
task_default_queue = "default"
task_queues: dict[str, dict[str, Any]] = {
    "default": {},
    "metrics": {},
    # Shared sync queue: fallback for unknown providers plus any messages
    # already in flight at deploy time. Per-provider queues (CHAOS-2299)
    # make queue depth answer "is <provider> stuck?" with one LLEN and let
    # operators purge a single provider. Routing lives in
    # workers.queues.sync_queue_for_provider.
    "sync": {},
    "sync.github": {},
    "sync.gitlab": {},
    "sync.linear": {},
    "sync.jira": {},
    "sync.launchdarkly": {},
    # Cost-class sub-queues (CHAOS-2517). Gated by SYNC_COST_CLASS_QUEUES flag.
    # Deploy these queue entries first (consumers), then flip the flag on
    # producers. Routing lives in workers.queues / sync.dispatch_policy.
    "sync.github.light": {},
    "sync.github.medium": {},
    "sync.github.heavy": {},
    "sync.gitlab.light": {},
    "sync.gitlab.medium": {},
    "sync.gitlab.heavy": {},
    "sync.jira.medium": {},
    "sync.linear.medium": {},
    "backfill": {},
    "webhooks": {},
    "ingest": {},
    # Dedicated queue (CHAOS-2693 D8), not the shared `ingest` queue:
    # external-ingest is customer-facing, potentially spiky/large-batch, and
    # must not have its processing throughput hostage to an unrelated
    # internal consumer backlog (nor vice versa). Consumed by the dedicated
    # `worker-external-ingest` container (compose.yml), single replica at
    # --concurrency=1 (master-spec CC11 deployment invariant).
    "external-ingest": {},
    "reports": {},
    "scheduler": {},
    # Dedicated telemetry queue: monitor_queue_depths must not share a queue
    # with floodable work — if `default` backs up, queue-depth telemetry would
    # die exactly when it is needed. Consumed by BOTH `worker` and
    # `worker-heavy` in compose.yml for redundancy.
    "monitoring": {},
}

# Beat schedule (periodic tasks)
#
# CHAOS-4026 (2026-08-21): Celery is retired -- zero Python celery services
# run in prod since the 2026-08-19 stop. Go now owns the periodic cadences
# formerly dispatched from here (run-daily-metrics, run-complexity-daily,
# run-recommendations, run-release-impact-daily, run-capacity-forecast,
# process-*-streams, external-ingest-stream-health, phone-home-heartbeat,
# dispatch-scheduled-reports, run-membership-backfill-daily, ask-dev-
# retention-sweep, and the never-live consume-pending-scheduled-sync-
# occurrences seam); their Python task implementations were deleted with
# this cleanup and are pinned absent by
# tests/workers/test_celery_dead_code_contract.py. See CHAOS-4056's beat
# inventory comment for the full per-entry Go-successor mapping.
#
# CHAOS-3093 (2026-09-09, PR2a') retired the three entries this file used to
# keep -- dispatch-scheduled-syncs, reconcile-sync-dispatch, prune-rate-
# limit-observations -- outright. This is the reviewed pass PR2a deferred:
# internal/scheduler/sync's coordinator/loop (live, already the real
# OwnerRef in internal/scheduler/fixed/inventory.go's LegacyBeatInventory
# before this change) owns sync dispatch; internal/syncreconciler (live,
# SweepModeActive) owns reconciliation; internal/jobs/system's
# RateLimitObservationStore + internal/scheduler/fixed's RetentionProducer
# own the rate-limit retention sweep. Every deleted Python invariant test's
# Go-side pin is mapped in this PR's TEST-EVIDENCE, not restated here.
# workers/sync_scheduler.py and workers/sync_reconciler.py are deleted
# outright. All three entries are pinned absent by
# tests/workers/test_celery_dead_code_contract.py.
beat_schedule: dict[str, Any] = {}

# Result settings
result_expires = 86400  # Results expire after 24 hours
