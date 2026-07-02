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
    "dev_health_ops.workers.tasks.dispatch_scheduled_syncs",
    "dev_health_ops.workers.tasks.dispatch_scheduled_metrics",
    "dev_health_ops.workers.tasks.dispatch_daily_metrics_partitioned",
    "dev_health_ops.workers.tasks.dispatch_daily_metrics_for_all_orgs",
    "dev_health_ops.workers.tasks.dispatch_complexity_job",
    "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned",
    "dev_health_ops.workers.tasks.dispatch_release_impact",
    "dev_health_ops.workers.tasks.dispatch_membership_backfill",
    "dev_health_ops.workers.tasks.dispatch_scheduled_reports",
    "dev_health_ops.workers.tasks.phone_home_heartbeat",
    "dev_health_ops.workers.system_ops.send_billing_notification",
    "dev_health_ops.workers.tasks.run_ingest_consumer",
    "dev_health_ops.workers.tasks.run_product_telemetry_consumer",
    "dev_health_ops.workers.tasks.run_external_ingest_consumer",
    # CHAOS-2699's debounced recompute flush task (master-spec CC20; see the
    # INTEGRATOR TODO atop workers/external_ingest_recompute.py). Valkey's
    # SETNX debounce guard is the durability/dedup layer here, not Celery's
    # acks-late redelivery -- reuses the existing `default` queue, no
    # task_queues/compose change needed.
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
beat_schedule = {
    "dispatch-scheduled-syncs": {
        "task": "dev_health_ops.workers.tasks.dispatch_scheduled_syncs",
        "schedule": 300.0,
        "options": {"queue": "scheduler"},
    },
    "dispatch-scheduled-metrics": {
        "task": "dev_health_ops.workers.tasks.dispatch_scheduled_metrics",
        "schedule": 300.0,
        "options": {"queue": "default"},
    },
    # Fans out per active organization (CHAOS-2849): discover_repos (job_daily.py)
    # scopes the repos query by org_id, so a single blank-org run would never
    # match a real (UUID-scoped) tenant's rows and repo_metrics_daily would never
    # be populated. dispatch_daily_metrics_for_all_orgs enumerates active orgs and
    # enqueues one dispatch_daily_metrics_partitioned per org_id.
    "run-daily-metrics": {
        "task": "dev_health_ops.workers.tasks.dispatch_daily_metrics_for_all_orgs",
        "schedule": crontab(hour=1, minute=0),
        "options": {"queue": "default"},
    },
    # Complexity daily floor cadence (CHAOS-2850): run_complexity_job previously
    # only ran chained after a git sync, so an org with infrequent syncs left
    # repo_complexity_daily stale, and complexity_delta's trailing 30-day window
    # (compounding_risk.py) read a flat trend. This dispatcher fans out one
    # run_complexity_job per active org daily, independent of sync activity.
    # Scheduled before run-daily-metrics (01:00) so the daily hotspot/risk compute
    # reads a freshly-refreshed complexity snapshot for the day.
    "run-complexity-daily": {
        "task": "dev_health_ops.workers.tasks.dispatch_complexity_job",
        "schedule": crontab(hour=0, minute=45),
        "options": {"queue": "default"},
    },
    # Daily safety net for recommendations_daily (CHAOS-2373). The primary
    # trigger is completion-gated: run_daily_metrics_finalize_task chains
    # run_recommendations_job once each (org, day) finalize completes. This beat
    # entry is a backstop in case a finalize callback was lost; the task itself
    # skips any org whose daily_finalize checkpoint is still in flight, so it
    # never reads partial metric tables. Scheduled at 02:00, after the 01:00
    # run-daily-metrics dispatch.
    "run-recommendations": {
        "task": "dev_health_ops.workers.tasks.run_recommendations_job",
        "schedule": crontab(hour=2, minute=0),
        "options": {"queue": "metrics"},
    },
    # Release-impact daily compute (CHAOS-2381): materializes
    # release_impact_daily from telemetry_signal_bucket + deployments, read by
    # the /feature-flags release-reliability cards. The dispatcher fans out one
    # per-org compute — the compute is org-scoped, so a single blank-org run
    # would match zero rows for real (UUID-scoped) tenants. Runs after
    # run-daily-metrics so the deployments it joins against are materialized.
    "run-release-impact-daily": {
        "task": "dev_health_ops.workers.tasks.dispatch_release_impact",
        "schedule": crontab(hour=1, minute=30),
        "options": {"queue": "default"},
    },
    # The Postgres team-drift / identity-reconcile beat entries were removed in
    # CHAOS-2600 CS5; their tasks + services were deleted in CS6. ClickHouse is
    # the sole team/identity system of record, so no periodic Postgres-mapping
    # writer remains.
    "reconcile-sync-dispatch": {
        "task": "dev_health_ops.workers.tasks.reconcile_sync_dispatch",
        "schedule": 60.0,
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
        "schedule": stream_consumer_schedule_seconds,
        "kwargs": {"max_iterations": stream_consumer_max_iterations},
        "options": {"queue": "ingest", "expires": stream_consumer_expires_seconds},
    },
    "process-product-telemetry-streams": {
        "task": "dev_health_ops.workers.tasks.run_product_telemetry_consumer",
        "schedule": stream_consumer_schedule_seconds,
        "kwargs": {"max_iterations": stream_consumer_max_iterations},
        "options": {"queue": "ingest", "expires": stream_consumer_expires_seconds},
    },
    "process-external-ingest-streams": {
        "task": "dev_health_ops.workers.tasks.run_external_ingest_consumer",
        "schedule": stream_consumer_schedule_seconds,
        "kwargs": {"max_iterations": stream_consumer_max_iterations},
        "options": {
            "queue": "external-ingest",
            "expires": stream_consumer_expires_seconds,
        },
    },
    "external-ingest-stream-health": {
        "task": "dev_health_ops.workers.tasks.external_ingest_stream_health",
        "schedule": 60.0,
        # Dedicated `monitoring` queue (matches monitor-queue-depths):
        # telemetry must keep flowing even when `external-ingest` itself
        # backs up -- that is exactly when it is needed.
        "options": {"queue": "monitoring"},
    },
    "phone-home-heartbeat": {
        "task": "dev_health_ops.workers.tasks.phone_home_heartbeat",
        "schedule": crontab(hour=0, minute=0),
        "options": {"queue": "default"},
    },
    "dispatch-scheduled-reports": {
        "task": "dev_health_ops.workers.tasks.dispatch_scheduled_reports",
        "schedule": 300.0,
        "options": {"queue": "default"},
    },
    "monitor-queue-depths": {
        "task": "dev_health_ops.workers.tasks.monitor_queue_depths",
        "schedule": 60.0,
        # Dedicated `monitoring` queue: telemetry must keep flowing even when
        # `default` floods (that is precisely when it is needed).
        "options": {"queue": "monitoring"},
    },
    # Daily safety net for work_unit_membership (CHAOS-2439/2433). The primary
    # trigger is event-driven: post-sync build -> LLM materialize chain. This
    # beat entry fans out a cheap no-LLM backfill (build -> project membership)
    # per active org once per day so idle orgs and the post-deploy window are
    # always covered. The backfill uses the run_id / completion-marker protocol
    # (CHAOS-2433), so it coexists safely with the event-driven materializer.
    # Scheduled at 03:30 UTC, after daily metrics (01:00) and recommendations
    # (02:00), to avoid competing with the heaviest nightly jobs.
    "run-membership-backfill-daily": {
        "task": "dev_health_ops.workers.tasks.dispatch_membership_backfill",
        "schedule": crontab(hour=3, minute=30),
        "options": {"queue": "default"},
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
    # Retention for the external-ingest status store (CHAOS-2694). Env-tunable
    # via EXTERNAL_INGEST_STATUS_RETENTION_DAYS (default 90, see
    # workers/external_ingest_reconciler.py). Scheduled immediately after
    # prune-rate-limit-observations (5:00), clear of the other nightly jobs.
    "prune-external-ingest-batches": {
        "task": "dev_health_ops.workers.tasks.prune_external_ingest_batches",
        "schedule": crontab(hour=5, minute=15),
        "options": {"queue": "sync"},
    },
}

# Result settings
result_expires = 86400  # Results expire after 24 hours
