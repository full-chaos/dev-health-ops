# Workers & Celery

Background job processing for dev-health-ops, powered by Celery with Redis as broker and result backend.

---

## Triggering operations via Celery jobs (interim workaround for CHAOS-2475)

Until the CLI argument-enforcement gaps tracked in Linear CHAOS-2475 are fixed, operators must trigger affected operations through the Celery worker and job system rather than running the bare `dev-hops` CLI directly.

### Why this workaround is required

The bare CLI runs jobs inline within the caller's process. Preflight checks in the CLI only model database URIs and organization parameters, failing to enforce required credentials or inputs like provider tokens, LLM API keys, or Stripe keys. Because of this, inline CLI commands often fail deep in the execution path or fail silently.

In contrast, Celery-triggered jobs run inside the worker process, which is started via `dev-hops workers start-worker` or `dev-hops workers start-scheduler`. The worker process is fully configured with the message broker and the worker-side environment and credentials. Routing operations through a Celery job request ensures the task runs with the worker's configured credentials.

Key implementation details:
- **Broker and Result Backend**: Configured in `workers/config.py:8-64` and initialized in `workers/celery_app.py:66-84`. The default broker and result backend URL is `redis://localhost:6379/0` (controlled by `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`).
- **Queue Routing**: Defined in `workers/queues.py:7-62`. The `PROVIDER_SYNC_QUEUES_ENABLED` flag gates provider-specific queues; `SYNC_COST_CLASS_QUEUES` further routes eligible units to light/medium/heavy sub-queues.
- **Sync Budget Guard**: Defined in `sync/budget_guard.py`. `SYNC_BUDGET_BUCKET_LIMITS` enables enforced GitHub budget deferrals; dry-run limits record observations without deferring work.

### On-Demand Trigger Paths

Operators can trigger background operations on-demand through three primary API paths. These paths bypass the periodic scheduler and enqueue tasks directly onto the Celery broker. For detailed API specifications, refer to the [GraphQL API Overview](../api/graphql-overview.md) and the [AI Reports Architecture](../architecture/ai-reports-architecture.md).

1. **Data Sync Trigger**
   * **Endpoint**: `POST /api/v1/admin/sync-configs/{config_id}/trigger` (defined in `api/admin/routers/sync.py:997-1164`)
   * **Flow**: Builds a `SyncPlanRequest` from the config's migrated integration, plans a `SyncRun` (one `SyncRunUnit` per source/dataset/window), and enqueues `dispatch_sync_run` onto the `sync` queue. The integration planner is the only routing path — the legacy `run_sync_config` / `dispatch_batch_sync` in-process workers were removed in CHAOS-2647.

2. **Historical Backfill Trigger**
   * **Endpoint**: `POST /api/v1/admin/sync-configs/{config_id}/backfill` (defined in `api/admin/routers/sync.py:1167-1233`)
   * **Flow**: Creates a `BackfillJob` record, plans a backfill-mode `SyncRun`, and enqueues `dispatch_sync_run` onto the `sync` queue (fan-out). The `BackfillJob` is linked to its run via a `sync_run:<id>` marker on `celery_task_id`, so `finalize_sync_run` updates its status and chunk counts.

3. **Report Execution Trigger**
   * **Trigger**: GraphQL `triggerReport` mutation or the "Run Now" button in the Report Center UI.
   * **Flow**: Creates a `ReportRun` record and enqueues `execute_saved_report` onto the `reports` queue.

### Affected Operations Quick Reference

The following table maps bare CLI commands to their Celery task equivalents, trigger paths, and required worker environment variables.

| CLI Command (Inline, May Fail) | Celery Task Equivalent | Trigger Path | Required Worker Env |
|---|---|---|---|
| `dev-hops sync git/prs/cicd/deployments/incidents/security/tests` | `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run` | `POST /api/v1/admin/sync-configs/{config_id}/trigger` | `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_API_TOKEN`, `JIRA_EMAIL` |
| `dev-hops sync work-items` | `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run` | `POST /api/v1/admin/sync-configs/{config_id}/trigger` | `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_API_TOKEN`, `JIRA_EMAIL` |
| `dev-hops metrics daily` | `run_daily_metrics` / `dispatch_daily_metrics_partitioned` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI` |
| `dev-hops recommendations compute` | `run_recommendations_job` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI` |
| `dev-hops work-graph build` | `run_work_graph_build` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI` |
| `dev-hops investment materialize` | `run_investment_materialize` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI`, `OPENAI_API_KEY` (or other LLM keys) |
| `dev-hops backfill run` | `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run` (backfill-mode `SyncRun`) | `POST /api/v1/admin/sync-configs/{config_id}/backfill` | `CLICKHOUSE_URI`, `DATABASE_URI`, provider tokens |

Triggering `run_investment_materialize` via the worker ensures the worker-side LLM API key applies. This avoids the silent `MockProvider` fallback that the bare CLI hits when no key is present in the caller's environment (tracked in Linear CHAOS-2476).

### Observability Caveat

Tasks like `run_work_graph_build` and `run_investment_materialize` don't persist a `JobRun` row in the database. Their execution status and progress can only be tracked through worker logs and distributed traces. For more details on sync observability, see the [Platform Sync Observability](../architecture/platform-sync-observability.md) documentation.

---

## Architecture

Workers execute long-running tasks (syncs, metrics computation, webhooks) asynchronously. The system consists of:

- **Celery workers**: consume tasks from Redis queues
- **Celery beat**: scheduler that dispatches periodic tasks on cron/interval schedules
- **Redis**: message broker and result backend

On startup, workers automatically apply pending Alembic migrations and initialize logging, Sentry, and OpenTelemetry tracing.
See also: [Worker horizontal-scaling readiness](../architecture/worker-scaling-readiness.md)


---

## Starting Workers

### Via CLI

```bash
# Start a worker (default queues: default, metrics, sync)
dev-hops workers start-worker

# Specify queues and concurrency
dev-hops workers start-worker --queues default metrics sync webhooks ingest --concurrency 4

# Start the beat scheduler (periodic tasks)
dev-hops workers start-scheduler
```

### Via Celery directly

```bash
celery -A dev_health_ops.workers.celery_app worker --loglevel=INFO --queues=default,metrics,sync
celery -A dev_health_ops.workers.celery_app beat --loglevel=INFO
```

---

## Queues

| Queue | Purpose |
|-------|---------|
| `default` | Scheduling dispatchers, health checks, heartbeat |
| `metrics` | Daily metrics, complexity, DORA, capacity forecast, investment |
| `sync` | Unitized sync dispatch/execution/post-sync relay (git, PR, work-item) + team auto-import |
| `sync.<provider>` | Provider-specific sync queues (e.g., `sync.github`, `sync.gitlab`, `sync.linear`, `sync.jira`, `sync.launchdarkly`) |
| `sync.<provider>.<class>` | Cost-class sub-queues (CHAOS-2517): `light`/`medium` on `worker`, `heavy` on `worker-heavy`. Always declared in `task_queues`; routing gated by `SYNC_COST_CLASS_QUEUES`. **Two-phase rollout: expand consumer `-Q` lists first, then flip the flag on producers.** |
| `backfill` | Legacy/reserved — unused after CHAOS-2647 (API backfill now plans a backfill-mode `SyncRun` and dispatches on `sync`) |
| `reports` | AI report execution (SavedReport to ReportRun) |
| `webhooks` | Webhook event processing, billing notifications |
| `ingest` | Stream ingestion consumer |
| `monitoring` | Telemetry and queue depth monitoring |

### Routing and budget environment

The bundled Docker Compose, Kubernetes, and Helm deployments already declare the provider and cost-class queues in their worker `-Q` lists, so they can enable routing by default. Custom deployments must update worker consumers before enabling producer-side routing; otherwise, routed messages can sit on unconsumed queues.

| Variable | Default in deploy templates | Effect |
|---|---:|---|
| `PROVIDER_SYNC_QUEUES_ENABLED` | `true` | Routes known providers from `sync` to `sync.<provider>`. |
| `SYNC_COST_CLASS_QUEUES` | `true` | Routes eligible GitHub/GitLab/Jira/Linear units to cost-class sub-queues after provider routing is enabled. |
| `HIDE_MIGRATED_CHILD_CONFIGS` | `true` | Hides migrated child sync configs from operator-facing config lists. |
| `SYNC_RUN_MAX_UNITS` | `1000` | Caps unit count for one planned sync run. |
| `SYNC_UNIT_CONCURRENCY_PER_BUCKET` | `8` | Caps concurrently dispatchable units per org/provider/cost-class bucket. |
| `SYNC_UNIT_DISPATCH_STALE_SECONDS` | `900` | Reclaims stale `DISPATCHING` units after this age. |
| `SYNC_UNIT_RUNNING_STALE_SECONDS` | `3600` | Treats long-running units as stale for reconciliation/reporting. |
| `SYNC_DISPATCH_REDISPATCH_COUNTDOWN` | `60` | Delay used when redispatching sync-run work. |
| `SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS` | `300` | Dispatch outbox claim lease duration. |
| `SYNC_WATERMARK_OVERLAP` | `0` | Subtracts this many seconds from incremental watermark reads to intentionally re-read a lookback margin. |

GitHub budget limits are abstract reservation units derived from the estimated shape of a sync unit. They are not GitHub's raw hourly request counters. Leaving `SYNC_BUDGET_BUCKET_LIMITS` unset disables enforcement; setting it enables deferrals when the reservation would exceed a configured bucket. LaunchDarkly provider budgeting is planned in [LaunchDarkly sync budgeting](../architecture/launchdarkly-sync-budgeting.md); when that provider's raw sync path ships, it must use the same JSON maps with `launchdarkly:*` bucket keys before fetch code is accepted.

| Variable | Default in deploy templates | Effect |
|---|---:|---|
| `SYNC_BUDGET_BUCKET_LIMITS` | `{"github:rest_core":250,"github:graphql_cost":500,"github:contents_blob":100,"github:secondary_abuse_risk":25}` | Enforced per-bucket reservation limits. |
| `SYNC_BUDGET_DEFAULT_LIMIT` | `1000000` | Fallback enforced limit for buckets not named in the JSON map. |
| `SYNC_BUDGET_DEFERRAL_SECONDS` | `60` | Base countdown when enforcement defers a unit. |
| `SYNC_BUDGET_DEFERRAL_JITTER_SECONDS` | `5` | Random jitter added to enforced deferrals. |
| `SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS` | unset | Observation-only limits; records estimates without deferring work. |
| `SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT` | `1000000` | Fallback dry-run limit. |
| `SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS` | `60` | Observation-only deferral estimate. |

Planned LaunchDarkly bucket examples for dry-run rollout: `launchdarkly:rest_core`, `launchdarkly:rest_core:flags`, `launchdarkly:rest_core:projects`, `launchdarkly:secondary_abuse_risk:audit_log`, and `launchdarkly:secondary_abuse_risk:code_refs`.
---

## Task Registry

The system registers Celery tasks under the `workers/` directory. The primary registered tasks, their wrapped CLI-equivalent operations, and their target queues are listed in the table below.

| Task Name | Wrapped CLI Operation | Queue | Description |
|---|---|---|---|
| `dispatch_sync_run` | None | `sync` | Authorizes a planned `SyncRun`, routes each pending unit, and fans out `run_sync_unit` tasks. Source: `sync_units.py:113-190`. |
| `run_sync_unit` | None | `sync` (or `sync.<provider>` / cost-class queue) | Executes exactly one planned source/dataset/window unit and updates its status. Source: `sync_units.py:193-293`. |
| `finalize_sync_run` | None | `sync` | Aggregates unit statuses and dispatches post-sync work once all units are terminal. Source: `sync_units.py:295-513`. |
| `reconcile_sync_dispatch` | None | `sync` (beat, 60s) | Sole durable relay for the dispatch outbox: expires dead-lease RUNNING units, then materializes + relays `dispatch_sync_run`/`finalize_sync_run`/`post_sync` wakeups for stranded runs. See [Dispatch Outbox](../architecture/dispatch-outbox.md) (CHAOS-2581). Source: `sync_reconciler.py`. |
| ~~`run_sync_config` / `dispatch_batch_sync` / `_batch_sync_callback` / `_run_sync_for_repo` / `run_work_items_sync`~~ | — | — | **Removed in CHAOS-2647.** The legacy in-process sync workers (`sync_runtime.py`, `sync_batch.py`, `sync_misc.py`) are deleted; all sync now flows through the unitized `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run` planner path. |
| `run_post_sync_team_autoimport` | None | `sync` | Refreshes team/project/member attribution after a successful `SyncRun` whose canonical config has `auto_import_teams` enabled. Dispatched once per terminal run by the post-sync relay; resolves credentials from the run's `Integration.credential_id`. Source: `team_autoimport.py`. |
| ~~`sync_team_drift` / `reconcile_team_members`~~ | — | — | **Deleted in CHAOS-2600 CS6 (CHAOS-2607).** Both Celery tasks and `workers/sync_team.py` are removed (they were fail-closed no-ops in CS5). ClickHouse is the team/identity system of record; the Postgres drift engine + member-reconcile no longer exist. |
| `run_daily_metrics` | `metrics daily` | `metrics` | Computes daily repository and user metrics. Source: `metrics_daily.py`. |
| `dispatch_daily_metrics_partitioned` | `metrics daily` (partitioned) | `default` | Partitions daily metrics across organizations and fans out. Source: `metrics_partitioned.py`. |
| `run_daily_metrics_batch` | None | `metrics` | Processes a batch of repositories for daily metrics. Source: `metrics_partitioned.py`. |
| `run_daily_metrics_finalize_task` | None | `default` | Finalizes daily metrics computation. Source: `metrics_partitioned.py`. |
| `run_complexity_job` | None | `metrics` | Analyzes code complexity for repositories. Source: `metrics_extra.py:16-319`. |
| `run_dora_metrics` | None | `metrics` | Computes DORA metrics. Source: `metrics_extra.py:16-319`. |
| `run_release_impact_job` | None | `metrics` | Computes release impact metrics. Source: `metrics_extra.py:16-319`. |
| `dispatch_release_impact` | None | `default` | Fans out release impact computation. Source: `metrics_extra.py`. |
| `run_work_graph_build` | `work-graph build` | `metrics` | Builds work graph edges linking issues, PRs, and commits. Source: `work_graph_tasks.py:17-334`. |
| `run_investment_materialize` | `investment materialize` | `metrics` | Classifies work units into investment categories via LLM. Source: `work_graph_tasks.py`. |
| `run_membership_backfill` | None | `metrics` | Backfills work unit membership. Source: `work_graph_tasks.py`. |
| `dispatch_membership_backfill` | None | `default` | Fans out membership backfill. Source: `work_graph_tasks.py`. |
| `run_capacity_forecast_job` | None | `metrics` | Computes weekly capacity forecasting. Source: `product_tasks.py`. |
| `run_recommendations_job` | `recommendations compute` | `metrics` | Computes recommendations for organizations. Source: `recommendations_tasks.py:249-333`. |
| `process_webhook_event` | None | `webhooks` | Processes inbound webhook events. Source: `system_webhooks.py`. |
| `send_billing_notification` | None | `webhooks` | Sends billing-related email notifications. Source: `system_ops.py`. |
| `run_ingest_consumer` | None | `ingest` | Consumes from ingest streams. Source: `system_ops.py`. |
| `run_product_telemetry_consumer` | None | `ingest` | Consumes product telemetry streams. Source: `system_ops.py`. |
| `health_check` | None | `default` | Worker health check. Source: `system_ops.py`. |
| `phone_home_heartbeat` | None | `default` | Daily heartbeat for deployment telemetry. Source: `system_ops.py`. |
| `execute_saved_report` | None | `reports` | Executes a SavedReport plan and persists markdown. Source: `report_task.py`. |
| `dispatch_scheduled_reports` | None | `default` | Fans out scheduled report executions. Source: `report_scheduler.py`. |
| ~~`run_backfill`~~ | `backfill run` | — | **Removed in CHAOS-2647.** The API backfill path now plans a backfill-mode `SyncRun` and fans out through `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run`; the standalone `backfill`-queue task and `sync_backfill.py` are deleted. |
| `dispatch_scheduled_syncs` | None | `default` | Fans out organization sync configurations. Source: `sync_scheduler.py`. |
| `dispatch_scheduled_metrics` | None | `default` | Fans out scheduled metrics. Source: `metrics_daily.py`. |
| `monitor_queue_depths` | None | `monitoring` | Monitors queue depths. Source: `queue_monitor.py`. |
---

## Periodic Schedule (Beat)

| Schedule | Task | Interval | Queue |
|----------|------|----------|-------|
| `dispatch-scheduled-syncs` | `dispatch_scheduled_syncs` | Every 300 seconds (5 minutes) | `default` |
| `dispatch-scheduled-metrics` | `dispatch_scheduled_metrics` | Every 300 seconds (5 minutes) | `default` |
| `run-daily-metrics` | `dispatch_daily_metrics_partitioned` | Daily at 01:00 UTC | `default` |
| `run-recommendations` | `run_recommendations_job` | Daily at 02:00 UTC | `metrics` |
| `run-release-impact-daily` | `dispatch_release_impact` | Daily at 01:30 UTC | `default` |
| `run-capacity-forecast` | `run_capacity_forecast_job` | Mondays at 04:00 UTC | `metrics` |
| `process-ingest-streams` | `run_ingest_consumer` | Every 30 seconds | `ingest` |
| `process-product-telemetry-streams` | `run_product_telemetry_consumer` | Every 30 seconds | `ingest` |
| `phone-home-heartbeat` | `phone_home_heartbeat` | Daily at 00:00 UTC | `default` |
| `dispatch-scheduled-reports` | `dispatch_scheduled_reports` | Every 300 seconds (5 minutes) | `default` |
| `monitor-queue-depths` | `monitor_queue_depths` | Every 60 seconds | `monitoring` |
| `run-membership-backfill-daily` | `dispatch_membership_backfill` | Daily at 03:30 UTC | `default` |

> **Removed in CHAOS-2600 CS5:** the `sync-team-drift` and `reconcile-team-members` beat schedules are no longer registered (they wrote Postgres `team_mappings` / replaced ClickHouse `teams.members` from Postgres). Their Celery tasks and `workers/sync_team.py` were **deleted in CS6 (CHAOS-2607)** (they were fail-closed no-ops in CS5); the `sync_teams_to_analytics` task is deleted. ClickHouse is the team/identity system of record; the admin surface and `sync teams` write it directly.
---
## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CELERY_BROKER_URL` | `redis://localhost:6379/0` | Redis broker URL |
| `CELERY_RESULT_BACKEND` | `redis://localhost:6379/0` | Redis result backend URL |
| `CLICKHOUSE_URI` | -- | ClickHouse connection for analytics tasks |
| `DATABASE_URI` | -- | PostgreSQL connection (fallback) |

### Task Limits

| Setting | Value |
|---------|-------|
| Hard time limit | 3600s (1 hour) |
| Soft time limit | 3300s (55 minutes) |
| Default retry delay | 60s |
| Max retries (default) | 3 |
| Result expiry | 86400s (24 hours) |

---

## Monitoring

### Prometheus Metrics

Every task execution records Prometheus metrics via `record_celery_task()`:
- Task name
- Completion state (success/failure)
- Duration in seconds

### Logging & Tracing

Workers initialize on startup:
- Structured logging via `configure_logging()`
- Sentry error tracking via `init_sentry()`
- OpenTelemetry distributed tracing via `init_tracing()` + `instrument_celery()`

### Health Check

```bash
# Via Celery
celery -A dev_health_ops.workers.celery_app inspect ping
```

The `health_check` task can also be invoked to verify worker responsiveness.

## Deploying with Active Workers

Worker deploys must assume a rollout can overlap with active sync, metrics,
report, ingest, or webhook tasks. The production Compose stack and Kubernetes
manifests give workers a 3700 second graceful termination window, which exceeds
the current 3600 second Celery hard task time limit. Keep that budget intact in
environment-specific overlays unless task limits are reduced first.
Kubernetes worker Deployments also set a 7600 second progress deadline. Helm,
Argo CD, Flux, and other rollout controllers must use timeouts above the same
window or they can mark a healthy drain as failed before old workers finish.

### Safe rollout sequence

1. Confirm the migration job has completed before restarting workers.
2. Inspect active and reserved work without printing task args or kwargs:

   ```bash
   dev-hops workers inspect --state active --output json
   dev-hops workers inspect --state reserved --output json
   dev-hops workers inspect --state scheduled --output json
   ```

3. If active tasks are present, start the rollout but keep the worker graceful
   shutdown budget above the hard task time limit so Celery can finish in-flight
   work after receiving `SIGTERM`.
4. Watch the same sanitized inspect commands until old workers report no active
   tasks, then allow the deployment to complete.
5. Do not enable global `task_acks_late` or `task_reject_on_worker_lost` during
   rollout. Dispatchers, heartbeat, billing email, and stream-consumer tasks are
   explicitly annotated as late-ack excluded until their child replay-safety
   issues are complete.

The inspect command intentionally omits task args, kwargs, headers, and request
properties. It returns only task identity, worker metadata, timing, and routing
keys so operators can drain workers without exposing provider tokens or other
credentials.

## Queue Cleanup and Stale Jobs

Celery uses Valkey as both broker and result backend on database 0 by default:
`CELERY_BROKER_URL=redis://.../0` and `CELERY_RESULT_BACKEND=redis://.../0`.
The app's separate `REDIS_URL` cache/streams connection uses database 1, so do
not flush that database when clearing Celery.

### Queue names

The broker queues are:

- `default`
- `metrics`
- `sync`
- `sync.github`
- `sync.gitlab`
- `sync.linear`
- `sync.jira`
- `sync.launchdarkly`
- `backfill`
- `webhooks`
- `ingest`
- `reports`
- `monitoring`

When `PROVIDER_SYNC_QUEUES_ENABLED=false`, provider sync tasks stay on the
shared `sync` queue. When the flag is enabled, known providers route to their
`sync.<provider>` queues.

### Clear queued messages

Use Celery to purge one queue or everything on the broker. Run the
`valkey-cli` commands on a host that has the CLI installed, or prefix them
with `docker compose exec valkey` inside the compose stack.

```bash
# Inspect queue depth first
valkey-cli -n 0 LLEN sync.linear

# Remove one queue's pending messages
celery -A dev_health_ops.workers.celery_app purge -Q sync.linear -f

# Remove all pending broker messages
celery -A dev_health_ops.workers.celery_app purge -f
```

### Clear stale reserved jobs

If a worker dies while holding reservations, remove the broker's unacked state
after stopping workers:

```bash
valkey-cli -n 0 DEL unacked unacked_index
```

### Clear failed result metadata

Failed jobs usually live in the result backend rather than the queue. Celery's
result keys use the default naming convention, so they can be removed directly
if you need to clear task history from Valkey:

```bash
valkey-cli -n 0 --scan --pattern 'celery-task-meta-*'
```

```bash
for key in $(valkey-cli -n 0 --scan --pattern 'celery-task-meta-*'); do
  valkey-cli -n 0 DEL "$key"
done
```

Results auto-expire after 24 hours, so manual cleanup is usually only needed
for stale test runs, failed-job triage, or a dedicated broker reset.
