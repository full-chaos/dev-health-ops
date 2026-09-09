"""Celery configuration from environment variables."""

import os
from typing import Any

from celery.schedules import crontab


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

late_ack_excluded_tasks = (
    # CHAOS-4026: the daily-metrics partitioned dispatch chain,
    # dispatch_capacity_forecast, dispatch_complexity_job,
    # dispatch_release_impact, dispatch_membership_backfill, and
    # dispatch_scheduled_reports were deleted -- Go now owns these cadences
    # and Celery Beat has not scheduled them since the 2026-08-19 stop.
    # dispatch_scheduled_syncs stays (see beat_schedule's header comment).
    "dev_health_ops.workers.tasks.dispatch_scheduled_syncs",
    "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned",
    "dev_health_ops.workers.tasks.phone_home_heartbeat",
    # CHAOS-2699's debounced recompute flush task. Valkey's SETNX debounce
    # guard is the durability/dedup layer here, not Celery's acks-late
    # redelivery -- reuses the existing `default` queue, no task_queues/compose
    # change needed.
    "dev_health_ops.workers.tasks.flush_external_ingest_recompute",
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
# The entries below survive deliberately -- flagged to team-lead rather
# than deleted, per-entry reasons:
#   * dispatch-scheduled-syncs, reconcile-sync-dispatch -- each has a very
#     large test surface (canonical-incident-feature gating, outbox relay,
#     backfill-orphan cleanup, unreclaimable-dispatching sweep) beyond what
#     CHAOS-4056's inventory sweep verified 1:1 Go parity for. Removing
#     either needs its own reviewed pass, not a drive-by deletion here --
#     that pass is CHAOS-3093's PR2a' (tracked separately from this PR,
#     which retired the other three): its TEST-EVIDENCE must carry a
#     one-row-per-deleted-test mapping from each Python invariant test onto
#     the Go test (internal/syncreconciler / internal/scheduler/sync) that
#     pins the same invariant, or a filed gap ticket -- no third option.
#     (CHAOS-4054 step 4 has since deleted the Celery-transport fallback
#     these two used to reach -- provider_unit_transport.py is gone -- so
#     that particular reason for keeping them no longer applies; the test
#     surface one still does.)
#   * prune-rate-limit-observations -- lives in the same sync_reconciler.py
#     module as reconcile-sync-dispatch above, so it is kept for the same
#     reviewed-pass reason, not because it still needs a real Celery fleet
#     (CHAOS-4065 already replaced the ask-dev-acceptance fleet's coverage of
#     this cadence with a Go-native probe -- that replacement *did* let
#     CHAOS-3093 retire monitor-queue-depths and prune-external-ingest-
#     batches outright, just not this one, see below).
#
# CHAOS-3093 (2026-09-09) retired monitor-queue-depths and prune-external-
# ingest-batches outright: CHAOS-4065 replaced the ask-dev-acceptance
# release-blocking gate's real Celery worker+beat fleet (these two entries'
# last reason to survive) with a Go-native probe (cmd/ask-dev-jobs-probe)
# that re-executes the same production Go code directly (queueHealthMonitor,
# RetentionProducer's prune_external_ingest_batches schedule). Their Python
# implementations (workers/queue_monitor.py, external_ingest_reconciler.py)
# are deleted. Both retired entries are pinned absent by
# tests/workers/test_celery_dead_code_contract.py; the three still-flagged
# entries above are pinned present by the same file's
# test_flagged_entries_were_not_silently_dropped.
beat_schedule = {
    "dispatch-scheduled-syncs": {
        "task": "dev_health_ops.workers.tasks.dispatch_scheduled_syncs",
        "schedule": 300.0,
        "options": {"queue": "scheduler"},
    },
    "reconcile-sync-dispatch": {
        "task": "dev_health_ops.workers.tasks.reconcile_sync_dispatch",
        "schedule": 60.0,
        "options": {"queue": "sync"},
    },
    # Retention for the durable rate-limit observation store (CHAOS-2758).
    # Env-tunable via SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS (default 14,
    # see workers/sync_reconciler.py). Scheduled off-peak, clear of the other
    # nightly jobs (1:00 metrics, 1:30 release-impact, 2:00 recommendations,
    # 3:30 membership backfill).
    "prune-rate-limit-observations": {
        "task": "dev_health_ops.workers.tasks.prune_rate_limit_observations",
        "schedule": crontab(hour=5, minute=0),
        "options": {"queue": "sync"},
    },
}

# Result settings
result_expires = 86400  # Results expire after 24 hours
