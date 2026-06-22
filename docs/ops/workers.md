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
- **Queue Routing**: Defined in `workers/queues.py:7-62`. The `PROVIDER_SYNC_QUEUES_ENABLED` flag gates whether tasks are routed to provider-specific queues.

### On-Demand Trigger Paths

Operators can trigger background operations on-demand through three primary API paths. These paths bypass the periodic scheduler and enqueue tasks directly onto the Celery broker. For detailed API specifications, refer to the [GraphQL API Overview](../api/graphql-overview.md) and the [AI Reports Architecture](../architecture/ai-reports-architecture.md).

1. **Data Sync Trigger**
   * **Endpoint**: `POST /api/v1/admin/sync-configs/{config_id}/trigger` (defined in `api/admin/routers/sync.py:997-1164`)
   * **Flow**: For migrated sync configurations with `sync.migrated_trigger_routing_enabled`, builds a `SyncPlanRequest`, creates a `SyncRun`, and enqueues `dispatch_sync_run` onto the `sync` queue. Legacy configurations still create a `ScheduledJob` and a `PENDING` `JobRun`, then enqueue either `run_sync_config` or `dispatch_batch_sync` onto the `sync` queue.

2. **Historical Backfill Trigger**
   * **Endpoint**: `POST /api/v1/admin/sync-configs/{config_id}/backfill` (defined in `api/admin/routers/sync.py:1167-1233`)
   * **Flow**: Creates a `BackfillJob` record and triggers `run_backfill.delay` on the `backfill` queue.

3. **Report Execution Trigger**
   * **Trigger**: GraphQL `triggerReport` mutation or the "Run Now" button in the Report Center UI.
   * **Flow**: Creates a `ReportRun` record and enqueues `execute_saved_report` onto the `reports` queue.

### Affected Operations Quick Reference

The following table maps bare CLI commands to their Celery task equivalents, trigger paths, and required worker environment variables.

| CLI Command (Inline, May Fail) | Celery Task Equivalent | Trigger Path | Required Worker Env |
|---|---|---|---|
| `dev-hops sync git/prs/cicd/deployments/incidents/security/tests` | Migrated configs: `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run`; legacy configs: `run_sync_config` / `dispatch_batch_sync` | `POST /api/v1/admin/sync-configs/{config_id}/trigger` | `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_API_TOKEN`, `JIRA_EMAIL` |
| `dev-hops sync work-items` | Migrated configs: `dispatch_sync_run` → `run_sync_unit` → `finalize_sync_run`; legacy sync-config trigger: `run_sync_config` / `dispatch_batch_sync` with work-items handled inside the worker; standalone worker wrapper: `run_work_items_sync` | `POST /api/v1/admin/sync-configs/{config_id}/trigger` | `GITHUB_TOKEN`, `GITLAB_TOKEN`, `JIRA_API_TOKEN`, `JIRA_EMAIL` |
| `dev-hops metrics daily` | `run_daily_metrics` / `dispatch_daily_metrics_partitioned` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI` |
| `dev-hops recommendations compute` | `run_recommendations_job` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI` |
| `dev-hops work-graph build` | `run_work_graph_build` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI` |
| `dev-hops investment materialize` | `run_investment_materialize` | Periodic Beat Schedule | `CLICKHOUSE_URI`, `DATABASE_URI`, `OPENAI_API_KEY` (or other LLM keys) |
| `dev-hops backfill run` | `run_backfill` | `POST /api/v1/admin/sync-configs/{config_id}/backfill` | `CLICKHOUSE_URI`, `DATABASE_URI`, provider tokens |

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
| `sync` | Git, PR, and work-item syncs, team drift, batch sync |
| `sync.<provider>` | Provider-specific sync queues (e.g., `sync.github`, `sync.gitlab`, `sync.linear`, `sync.jira`, `sync.launchdarkly`) |
| `sync.<provider>.<class>` | Cost-class sub-queues (CHAOS-2517): `light`/`medium` on `worker`, `heavy` on `worker-heavy`. Always declared in `task_queues`; routing gated by `SYNC_COST_CLASS_QUEUES`. **Two-phase rollout: expand consumer `-Q` lists first, then flip the flag on producers.** |
| `backfill` | Historical data backfill operations |
| `reports` | AI report execution (SavedReport to ReportRun) |
| `webhooks` | Webhook event processing, billing notifications |
| `ingest` | Stream ingestion consumer |
| `monitoring` | Telemetry and queue depth monitoring |
---

## Task Registry

The system registers Celery tasks under the `workers/` directory. The primary registered tasks, their wrapped CLI-equivalent operations, and their target queues are listed in the table below.

| Task Name | Wrapped CLI Operation | Queue | Description |
|---|---|---|---|
| `dispatch_sync_run` | None | `sync` | Authorizes a planned `SyncRun`, routes each pending unit, and fans out `run_sync_unit` tasks. Source: `sync_units.py:113-190`. |
| `run_sync_unit` | None | `sync` (or `sync.<provider>` / cost-class queue) | Executes exactly one planned source/dataset/window unit and updates its status. Source: `sync_units.py:193-293`. |
| `finalize_sync_run` | None | `sync` | Aggregates unit statuses and dispatches post-sync work once all units are terminal. Source: `sync_units.py:295-513`. |
| `run_sync_config` | `sync git/prs/cicd/deployments/incidents/security/tests` + work-items | `sync` (or `sync.<provider>`) | Syncs a single repository configuration. Source: `sync_runtime.py:408-527`. |
| `dispatch_batch_sync` | None | `sync` | Discovers repositories in an organization and schedules syncs. Source: `sync_batch.py:301`. |
| `_batch_sync_callback` | None | `sync` | Callback task for batch sync completion. Source: `sync_batch.py:267`. |
| `_run_sync_for_repo` | None | `sync` | Runs sync for a single repository within a batch. Source: `sync_batch.py:550`. |
| `run_work_items_sync` | `sync work-items` | `sync` | Syncs work items from a provider. Source: `sync_misc.py:12-68`. |
| `sync_team_drift` | None | `sync` | **Fail-closed no-op (CHAOS-2600 CS5).** Returns a `deprecated` status without touching Postgres; the Postgres drift engine is removed (`TeamDriftSyncService` is dead until CS6). Source: `sync_team.py`. |
| `reconcile_team_members` | None | `sync` | **Fail-closed no-op (CHAOS-2600 CS5).** Returns a `deprecated` status; the Postgres→ClickHouse member reconcile is removed (ClickHouse is the team/identity system of record). Source: `sync_team.py`. |
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
| `run_backfill` | `backfill run` | `backfill` | Runs chunked historical sync with progress tracking. Source: `sync_backfill.py:14-173`. |
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

> **Removed in CHAOS-2600 CS5:** the `sync-team-drift` and `reconcile-team-members` beat schedules are no longer registered (they wrote Postgres `team_mappings` / replaced ClickHouse `teams.members` from Postgres). Their Celery tasks remain registered as fail-closed no-ops; the `sync_teams_to_analytics` task is deleted. ClickHouse is the team/identity system of record; the admin surface and `sync teams` write it directly.
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
