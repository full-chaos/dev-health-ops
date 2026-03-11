# Workers & Celery

Background job processing for dev-health-ops, powered by Celery with Redis as broker and result backend.

---

## Architecture

Workers execute long-running tasks (syncs, metrics computation, webhooks) asynchronously. The system consists of:

- **Celery workers** -- consume tasks from Redis queues
- **Celery beat** -- scheduler that dispatches periodic tasks on cron/interval schedules
- **Redis** -- message broker and result backend

On startup, workers automatically apply pending Alembic migrations and initialize logging, Sentry, and OpenTelemetry tracing.

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
| `sync` | Git/PR/work-item syncs, team drift, batch sync |
| `webhooks` | Webhook event processing, billing notifications |
| `ingest` | Stream ingestion consumer |
| `backfill` | Historical data backfill operations |

---

## Task Types

### Sync Tasks

| Task | Queue | Description |
|------|-------|-------------|
| `run_sync_config` | sync | Sync a single repo configuration (git, PRs, CI/CD, etc.) |
| `dispatch_batch_sync` | sync | Discover repos in an org and fan out per-repo sync tasks |
| `run_work_items_sync` | sync | Sync work items from a provider (Jira, GitHub, GitLab, Linear) |
| `sync_team_drift` | sync | Detect and reconcile team membership drift |
| `reconcile_team_members` | sync | Full team member reconciliation from provider sources |

### Metrics Tasks

| Task | Queue | Description |
|------|-------|-------------|
| `run_daily_metrics` | metrics | Compute daily repo/user metrics for a date range |
| `dispatch_daily_metrics_partitioned` | default | Partition daily metrics across orgs and fan out |
| `run_daily_metrics_batch` | metrics | Process a batch of repos for daily metrics |
| `run_complexity_job` | metrics | Analyze code complexity for repos |
| `run_dora_metrics` | metrics | Compute DORA metrics (deployment frequency, lead time, etc.) |
| `run_work_graph_build` | metrics | Build work graph edges linking issues, PRs, and commits |
| `run_investment_materialize` | metrics | Classify work units into investment categories via LLM |
| `run_capacity_forecast_job` | metrics | Weekly capacity forecasting |

### Webhook & Other Tasks

| Task | Queue | Description |
|------|-------|-------------|
| `process_webhook_event` | webhooks | Process inbound GitHub/GitLab/Jira webhook events |
| `send_billing_notification` | webhooks | Send billing-related email notifications |
| `run_ingest_consumer` | ingest | Consume from ingest streams (Redis) |
| `health_check` | default | Worker health check |
| `phone_home_heartbeat` | default | Daily heartbeat for deployment telemetry |

### Backfill Tasks

| Task | Queue | Description |
|------|-------|-------------|
| `run_backfill` | backfill | Chunked historical sync with BackfillJob progress tracking |

---

## Periodic Schedule (Beat)

| Schedule | Task | Interval |
|----------|------|----------|
| `dispatch-scheduled-syncs` | Fan out org sync configs | Every 5 minutes |
| `dispatch-scheduled-metrics` | Fan out scheduled metrics | Every 5 minutes |
| `run-daily-metrics` | Full daily metrics pass | Daily at 01:00 UTC |
| `sync-team-drift` | Detect team membership drift | Daily at 02:30 UTC |
| `reconcile-team-members` | Full team reconciliation | Daily at 03:00 UTC |
| `run-capacity-forecast` | Weekly capacity forecast | Mondays at 04:00 UTC |
| `process-ingest-streams` | Consume ingest queue | Every 30 seconds |
| `phone-home-heartbeat` | Deployment heartbeat | Daily at 00:00 UTC |

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
