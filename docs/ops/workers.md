# Workers: Celery and Go foundation

Production background job processing for dev-health-ops is currently powered by
Celery with Redis/Valkey as broker and result backend.

## Runtime status and coexistence

Phase 1 of the [Go worker migration](../architecture/go-worker-runtime-trd.md)
adds four process foundations: `dev-health-worker`, `dev-health-scheduler`,
`dev-health-reconciler`, and `dev-health-stream-runner`. It also adds shared
configuration, lifecycle, health, River storage, versioned-contract,
middleware, telemetry, and operator packages. Those components are additive
during coexistence:

- Python remains the owner of FastAPI/GraphQL, providers, processors, domain
  behavior, and every job listed in this document.
- No production job is routed to River solely because a Go binary builds,
  starts, or reports healthy.
- Celery workers and Beat schedules remain required until each job's migration
  issue changes its route behind rollback controls and passes shadow, parity,
  and canary gates.
- Separate worker stacks may run against the same representative dataset for
  on-the-fly comparison, but they must use isolated process/container projects
  and must not mutate, drain, purge, or normalize away the Celery baseline.

River queue control uses a small, bounded direct PostgreSQL pool configured by
`WORKER_DATABASE_URI`; transaction-mode PgBouncer remains supported for the
separate `POSTGRES_URI` domain pool and is insufficient as the sole production
queue-control path. Session-mode queue control remains fail-closed until it
passes the same compatibility matrix. `WORKER_DATABASE_URI` is a Go runtime
setting, not a Python database alias. Long-running processes never apply River
migrations; `MIGRATION_DATABASE_URI` is injected only into the one-shot
migration job.

All Go profiles have zero minimum replicas and the current registry routes
remain `celery`. Their readiness stays closed until a profile has complete
compiled handler coverage and its database/schema checks pass. The canonical
disabled topology and its connection budget live in
`deploy/go-workers/profiles.json`.

The Go worker exposes `/healthz` for process liveness, `/readyz` for required
dependencies, and `/metrics` for bounded Prometheus telemetry. Worker readiness
checks domain-role authorization, registry load, complete compiled-handler
coverage, available-job contract-version support, the queue-control
configuration category, queue-control connectivity, and the pinned River
schema. A `queue_control_config` failure specifically directs operators to the
dedicated DSN/mode/role-separation contract without exposing a DSN. Queue
depth, oldest eligible age, execution
saturation, and both PostgreSQL pool saturation ratios are sampled from the
live database on each metrics scrape. A database sampling failure makes that
scrape unavailable instead of publishing a misleading zero. Because Phase 1
does not compile or start any migrated River handlers, this observability does
not transfer queue ownership away from Celery.

`dev-health-workerctl` is the Phase 1 payload-redacted operator surface. It is
shipped as a dedicated non-root image target and serialized to one invocation
per semantic database with a PostgreSQL advisory transaction lock. Create its
service credential with:

```bash
dev-hops service-credentials create \
  --service worker-operator \
  --scope workers:read \
  --scope workers:operate
```

The plaintext token is printed once and is then supplied as
`WORKER_OPERATOR_TOKEN` or its `_FILE` form. Read commands include `status`,
`jobs list`, `jobs inspect`, `queues`, `routes status`, `streams status`, and
`contracts`. During Phase 1, `streams status` reports the validated disabled
deployment profiles and their Celery ownership;
live backlog and pending-entry state remains on the stream-runner metrics
surface once a stream profile is composed. Mutations require both `--reason` and
`--correlation-id`, verify an exact state transition, and persist a bounded
audit intent before changing River state. A PostgreSQL commit ambiguity is
recorded and returned as `outcome_unknown`, never as a false failure; inspect
the resource before any retry. A confirmed mutation whose audit finalization
is delayed returns `audit_pending` while retaining its durable `started`
intent. Cancel/retry remain intentionally
fail-closed for the two foundation kinds because their frozen domain links do
not yet have authoritative semantic rows; queue pause/resume, profile drain,
and sync-route pause/drain/resume are available to an authorized operator.
Encoded arguments, driver errors, DSNs, and tokens are absent from command
output and audit records.

The generic `worker_job_outbox` bridge is now route-safe foundation rather than
a dormant placeholder. Python's producer helper refuses to enqueue unless the
checked-in migration state and route pair is executable (`shadow`,
`river_canary`, or `river`), and the Go relay leaves known Celery-routed rows
untouched before independently rechecking the route at River insertion.
Unknown kinds still get claimed so contract failures can terminalize with
bounded evidence, and the
`dev-health-reconciler` command now composes the bounded immediate+poll loop
with explicit packaged-registry readiness and low-cardinality metrics. Startup
is fail-closed: the loop only opens readiness after one successful step, and
persistence failures close it instead of being reported as harmless lease
races.

The checked-in deployment profile still keeps the topology disabled
(`coexistence_disabled`, all replicas `0`), both registered kinds still route
to Celery, no current domain producer calls the bridge, and no Go handler is
compiled. Celery therefore remains the only production writer. Migration
policy is loaded at process startup: before a future rollback changes the
checked-in route, stop new production, drain or classify in-flight River work,
restore Celery routing, and restart the reconciler. Deferred generic-outbox
rows remain auditable; the bridge does not silently republish them to Celery.
The Go foundations now also include bounded read-only sync-outbox observation,
sync-schedule evaluation, and a database-backed sync-dispatch ownership fence.
Migration `0049` seeds the four fixed sync wakeup kinds as active Celery routes
at generation 1. Each Python claim binds the route and generation. For
dispatch, finalize, and discovery, the publish transaction locks both the
outbox row and route row through publish and mark; success/failure writes must
still match that active generation. `post_sync` preserves its existing
mark-before-publish commit, so its route lock ends at that commit. Unknown,
paused, and River-routed kinds are not claimable by Python. The Go reconciler
checks the persisted four-row set against the checked-in contract and closes
readiness on a pause, missing row, generation error, or route drift.

`dev-health-workerctl routes pause|drain|resume` provides the audited transition
surface. Pause and resume lock the route row first, then hold an outbox-table
barrier and re-read state and live claims, covering both Celery's
route-plus-outbox transaction and Go's outbox-only terminal commit. Resume
requires the target to equal the checked-in route. River requires an exact
compiled capability, and a transport-changing `post_sync` resume additionally
requires an external quiescer for its old generation. The shipped command
registers no capabilities, so today it can pause, inspect/drain, and resume the
same Celery route, including `post_sync`, but cannot activate River. A future
cutover quiescer must prove it honors context cancellation; its configured
timeout is cooperative.

Two dormant transaction kernels now sit behind that foundation. The scheduler
kernel locks a bounded due window with `FOR UPDATE ... SKIP LOCKED`, invokes an
injected coordinator through the same PostgreSQL transaction, and advances the
schedule marker only after that durable handoff succeeds. Its public repository
constructor embeds an opaque Celery/`coexistence_disabled` ownership policy, so
`HandoffDue` rejects mutation before beginning a transaction; Go mutation
authority requires a reviewed package-private source change. The sync reconciler
kernel can claim only unpaused River-routed rows, invoke an injected
same-transaction River publisher for at-least-once kinds, and preserve a
separate mark-before seam for `post_sync`. Neither kernel is constructed by
the scheduler or reconciler command.

This remains a fail-closed selector foundation, not River activation. All
persisted and checked-in routes remain Celery, deployment profiles remain
`coexistence_disabled` with zero replicas, and no Go sync handler is compiled.
Do not change a route to River: it would strand the wakeup. Activation still
requires a scheduler command loop and remaining policy parity; reconciler loop
wiring; split claim-commit/publish flow; concrete River publishers, handlers,
and capabilities; and the bounded post-sync quiescence/external-handoff path.

For a read-only same-snapshot comparison, set `POSTGRES_URI` or `DATABASE_URI`
and run:

```bash
go run ./cmd/dev-health-sync-parity --limit 100
```

The command holds one exported read-only `REPEATABLE READ` snapshot across the
Python and Go observations and emits only a safe match/mismatch report. It
does not claim or publish work. See the
[v2 parity evidence](../architecture/evidence/go-worker-migration/v2-sync-dispatch-parity/README.md).
The `sync-parity` target in `docker/Dockerfile` packages the Go binary, Python
observer, installed Python runtime, and checked-in sync-dispatch contract so
the same comparison can run as an isolated container. The Go reconciler image
also packages `contracts/sync-dispatch/v1`; packaging makes its registry
loadable at startup but does not wire the dormant mutation kernel.

The Go scheduler also remains blocked on the Phase 4
`sync.plan_scheduled_config` coordinator. `sync.dispatch_run` starts from an
existing `SyncRun` and cannot replace scheduled planning. Until the new
coordinator consumes a stable occurrence identity and creates the scheduled
domain plan transactionally, Celery Beat and `dispatch_scheduled_syncs` remain
the sole schedule mutation owners by default. An operator may separately set
`SYNC_SCHEDULED_OCCURRENCE_CONSUMER_ENABLED=true` to let the bounded Celery
consumer materialize pre-existing Go-authored identities; it does not own
timing or activate the Go command loop. The default-off consumer now provides
the authoritative Python planner path for a valid pending identity, while the
dormant transaction kernel still needs organization/entitlement and
missing-marker parity, catch-up and unsupported-cron policy, and a command
loop. Its ownership fence prevents the current command from mutating production
markers.

The rest of this page documents the active Celery runtime. See the
[Go worker runtime TRD](../architecture/go-worker-runtime-trd.md) for the target
topology and the
[CHAOS-3034 decision](../decisions/chaos-3034-river-compatibility.md) for the
direct-PostgreSQL compatibility boundary.

---

## Triggering operations via Celery jobs (interim workaround for CHAOS-2475)

Until the CLI argument-enforcement gaps tracked in Linear CHAOS-2475 are fixed, operators must trigger affected operations through the Celery worker and job system rather than running the bare `dev-hops` CLI directly.

### Why this workaround is required

The bare CLI runs jobs inline within the caller's process. Preflight checks in the CLI only model database URIs and organization parameters, failing to enforce required credentials or inputs like provider tokens, LLM API keys, or Stripe keys. Because of this, inline CLI commands often fail deep in the execution path or fail silently.

In contrast, Celery-triggered jobs run inside the worker process, which is started via `dev-hops workers start-worker` or `dev-hops workers start-scheduler`. The worker process is fully configured with the message broker and the worker-side environment and credentials. Routing operations through a Celery job request ensures the task runs with the worker's configured credentials.

Key implementation details:
- **Broker and Result Backend**: Configured in `workers/config.py:8-64` and initialized in `workers/celery_app.py:66-84`. The default broker and result backend URL is `redis://localhost:6379/0` (controlled by `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`).
- **Queue Routing**: Defined in `workers/queues.py:7-62`. The `PROVIDER_SYNC_QUEUES_ENABLED` flag gates provider-specific queues; `SYNC_COST_CLASS_QUEUES` further routes eligible units to light/medium/heavy sub-queues.
- **Sync Budget Guard**: Defined in `sync/budget_guard.py`. `SYNC_BUDGET_BUCKET_LIMITS` enables enforced provider budget deferrals; dry-run limits record observations without deferring work.

### On-Demand Trigger Paths

Operators can trigger background operations on-demand through three primary API paths. These paths bypass the periodic scheduler and enqueue tasks directly onto the Celery broker. For detailed API specifications, refer to the [GraphQL API Overview](../api/graphql-overview.md) and the [AI Reports Architecture](../architecture/ai-reports-architecture.md).

1. **Data Sync Trigger**
   * **Endpoint**: `POST /api/v1/admin/sync-configs/{config_id}/trigger` (defined in `api/admin/routers/sync.py:997-1164`)
   * **Flow**: Creates a canonical `JobRun` activity row, builds a `SyncPlanRequest` from the config's migrated integration, plans a `SyncRun` (one `SyncRunUnit` per source/dataset/window), stores `sync_run_id` in `JobRun.result`, and enqueues `dispatch_sync_run` onto the `sync` queue. The integration planner is the only routing path — the legacy `run_sync_config` / `dispatch_batch_sync` in-process workers were removed in CHAOS-2647.
     * The `source × dataset × window` decomposition and why **reference data** (teams/cycles/sprints) belongs on a once-per-run axis is documented in [Sync Unit Model](../architecture/sync-unit-model.md).

2. **Historical Backfill Trigger**
   * **Endpoint**: `POST /api/v1/admin/sync-configs/{config_id}/backfill` (defined in `api/admin/routers/sync.py:1167-1233`)
   * **Flow**: Creates a `BackfillJob` domain record plus the same canonical `JobRun` activity row used by manual and scheduled sync, plans a backfill-mode `SyncRun`, stores `sync_run_id` in `JobRun.result`, and enqueues `dispatch_sync_run` onto the `sync` queue (fan-out). The `BackfillJob` is linked to its run via a `sync_run:<id>` marker on `celery_task_id`, so `finalize_sync_run` updates its status and chunk counts.

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

Manual sync, scheduled sync, and sync-config backfill all persist a `JobRun` row as the admin-visible activity index and link it to the execution-truth `SyncRun` via `JobRun.result.sync_run_id`. The only difference between manual and scheduled sync is timing: the manual endpoint enqueues immediately, while `dispatch_scheduled_syncs` enqueues when the cron marker is due. Tasks like `run_work_graph_build` and `run_investment_materialize` don't persist a `JobRun` row in the database. Their execution status and progress can only be tracked through worker logs and distributed traces. For more details on sync observability, see the [Platform Sync Observability](../architecture/platform-sync-observability.md) documentation.

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
| `LINEAR_BACKFILL_MAX_WINDOW_DAYS` | `14` | Caps the window size (days) of a Linear work-item-family backfill chunk. CHAOS-2717 bounds each window's issue crawl to its own slice (`updatedAt` gte/lte), so the size balances a single unit's lease/soft-timeout budget against per-hour request volume; smaller windows re-multiply per-window teams/cycles fetches toward Linear's rate limit. Non-Linear backfills use the 7-day default. |
| `SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES` | `1` | Max expired-lease (or soft-timeout) retries for an eligible Linear work-item backfill unit before terminal `FAILED` (`worker_lost_retry_exhausted`). Retry is DISABLED on all other surfaces. |
| `SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS` | `60` | Backoff added to `available_at` when an eligible expired-lease unit is flipped to `RETRYING` before redispatch. |
| `SYNC_DISPATCH_REDISPATCH_COUNTDOWN` | `60` | Delay used when redispatching sync-run work. |
| `SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS` | `300` | Dispatch outbox claim lease duration. |
| `SYNC_SCHEDULED_OCCURRENCE_CONSUMER_ENABLED` | `false` | Enables the bounded scheduled-occurrence consumer task and its Beat entry. Keep disabled until the Go occurrence producer and operational hand-off are ready. |
| `SYNC_WATERMARK_OVERLAP` | `0` | Subtracts this many seconds from incremental watermark reads to intentionally re-read a lookback margin. |

Provider budget limits are abstract reservation units derived from the estimated shape of a sync unit. They are not the provider's raw request or GraphQL cost counters. Jira emits separate route-family buckets for REST/JQL listing (`jira:search:jira_jql`), REST issue enrichment (`jira:rest_core:jira_issue_enrichment`), optional worklog fetching (`jira:rest_core:jira_worklogs` when `JIRA_FETCH_WORKLOGS=true`), and Atlassian GraphQL enrichment (`jira:graphql_cost:jira_gql_enrichment` when `ATLASSIAN_GQL_ENABLED=true`). LaunchDarkly feature-flag sync emits `launchdarkly:*` buckets for the `flags`, `audit_log`, and `code_refs` route families (see [LaunchDarkly sync budgeting](../architecture/launchdarkly-sync-budgeting.md)). Leaving `SYNC_BUDGET_BUCKET_LIMITS` unset disables enforcement; setting it enables deferrals when the reservation would exceed a configured bucket.

| Variable | Default in deploy templates | Effect |
|---|---:|---|
| `SYNC_BUDGET_BUCKET_LIMITS` | `{"github:rest_core":250,"github:graphql_cost":500,"github:contents_blob":100,"github:secondary_abuse_risk":25,"jira:search:jira_jql":250,"jira:rest_core:jira_issue_enrichment":250,"jira:rest_core:jira_worklogs":100,"jira:graphql_cost:jira_gql_enrichment":250,"linear:graphql_cost":500}` | Enforced per-bucket reservation limits. |
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
| `dispatch_daily_metrics_for_all_orgs` | None | `default` | Fans out `dispatch_daily_metrics_partitioned` per active organization (CHAOS-2849) so `repo_metrics_daily` is populated for real (UUID-scoped) tenants, not just the blank-org default. Source: `metrics_partitioned.py`. |
| `run_daily_metrics_batch` | None | `metrics` | Processes a batch of repositories for daily metrics. Source: `metrics_partitioned.py`. |
| `run_daily_metrics_finalize_task` | None | `default` | Finalizes daily metrics computation. Source: `metrics_partitioned.py`. |
| `run_complexity_job` | None | `metrics` | Analyzes code complexity for repositories. Source: `metrics_extra.py:16-319`. |
| `dispatch_complexity_job` | None | `default` | Fans out `run_complexity_job` per active organization on an independent daily cadence (CHAOS-2850), so complexity refreshes even for orgs with infrequent syncs. Source: `metrics_extra.py`. |

> ⚠️ **Limitation (CHAOS-2888):** `run_complexity_job` computes complexity only from current `git_files`/`git_blame` contents — there is no historical file-content snapshot store. Post-sync dispatch (`post_sync_dispatch.py`) therefore enqueues it only for a current single-day sync (`metrics_backfill_days` in `(None, 1)` and `metrics_day` absent or equal to `utc_today()`); historical single-day and multi-day sync windows skip the complexity enqueue entirely and log a `historical_complexity_unsupported` diagnostic instead of fabricating a misleading flat historical trend. Daily metrics, work-graph build, and investment materialization continue to run over the full requested historical window regardless.

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
| `dispatch_scheduled_syncs` | None | `scheduler` | Fans out organization sync configurations. Source: `sync_scheduler.py`. |
| `consume_pending_scheduled_sync_occurrences` | None | `scheduler` | Default-off bounded materialization of Go-authored scheduled occurrences. Enabled only with `SYNC_SCHEDULED_OCCURRENCE_CONSUMER_ENABLED=true`. Source: `sync_scheduler.py`. |
| `dispatch_scheduled_metrics` | None | `default` | Fans out scheduled metrics. Source: `metrics_daily.py`. |
| `monitor_queue_depths` | None | `monitoring` | Monitors queue depths. Source: `queue_monitor.py`. |
---

## Periodic Schedule (Beat)

| Schedule | Task | Interval | Queue |
|----------|------|----------|-------|
| `dispatch-scheduled-syncs` | `dispatch_scheduled_syncs` | Every 300 seconds (5 minutes) | `scheduler` |
| `consume-pending-scheduled-sync-occurrences` | `consume_pending_scheduled_sync_occurrences` | Every 300 seconds (5 minutes), only when enabled | `scheduler` |
| `dispatch-scheduled-metrics` | `dispatch_scheduled_metrics` | Every 300 seconds (5 minutes) | `default` |
| `run-complexity-daily` | `dispatch_complexity_job` | Daily at 00:45 UTC | `default` |
| `run-daily-metrics` | `dispatch_daily_metrics_for_all_orgs` | Daily at 01:00 UTC | `default` |
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

## Linear Backfill Timeout & Lease Retry

Linear work-item backfills run long, provider-paced chunks. CHAOS-2717 bounds each window's issue crawl to its own slice (`updatedAt` gte/lte) instead of re-scanning from the window start to now, so the planner caps Linear work-item-family backfill windows at `LINEAR_BACKFILL_MAX_WINDOW_DAYS` (default `14`) to balance two budgets: smaller windows re-multiply the per-window fixed overhead (teams + cycles are re-fetched per unit) and push the per-hour request count toward Linear's rate limit, while larger windows lengthen a single unit's crawl toward the lease/soft-timeout budget. Non-Linear backfills keep the default 7-day window. When a worker still loses its lease mid-chunk (crash, `SIGKILL`, or a soft-timeout), the `reconcile_sync_dispatch` relay can **retry** the unit instead of failing it. The retry knobs (`SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES`, `SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS`) are listed under [Routing and budget environment](#routing-and-budget-environment); the full lifecycle is in [Dispatch Outbox](../architecture/dispatch-outbox.md) and [Data Pipeline → Retry Lifecycle](../architecture/data-pipeline.md#retry-lifecycle-expired-lease-recovery).

These unit-level expired-lease retries are **distinct** from Celery's generic per-task `Max retries (default) 3` autoretry in the table above.

Retry is **eligible only** when ALL hold: `provider == linear`, `mode == backfill`, a work-item-family dataset, the parent run is non-terminal, the unit's `expired_lease_retry_count` is below `SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES`, and **every** ClickHouse surface the chunk writes is in the proven retry-SAFE set (see the [ClickHouse retry idempotency matrix](../architecture/data-pipeline.md#clickhouse-retry-idempotency-matrix) — currently every Linear work-item backfill surface is SAFE, so no surface disables retry). Retry stays **DISABLED** for everything outside this eligibility gate — a non-eligible expired lease is terminal `FAILED` (`worker_lost`), exactly as before.

### Interpreting `retrying` vs `failed`

Operator states surface in the admin sync-run unit projection (`SyncRunUnitResponse`, `SyncRunUnitSummary`) and the backfill/job-run views:

| State / signal | Where | Meaning | Operator action |
|---|---|---|---|
| `retrying` | unit `status` | An eligible Linear backfill unit lost its lease (or hit a soft-timeout) and is waiting on its `available_at` / `next_retry_at` backoff to be redispatched. | None — recovery is automatic. Watch `retry_count` / `next_retry_at`. |
| `soft_timeout` | unit `error_category` | The unit hit the Celery soft time limit. If eligible it is now `retrying`; otherwise terminal `FAILED`. | None if `retrying`; otherwise treat like `worker_lost`. |
| `worker_lost` | unit `error_category` (terminal `FAILED`) | The lease expired and the unit was NOT retry-eligible (wrong provider/mode/dataset, or a surface without proven dedupe). | Investigate; backfill never advances watermarks, so re-triggering the window is safe. |
| `worker_lost_retry_exhausted` | unit `error_category` (terminal `FAILED`) | An eligible unit exhausted `SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES`. | Investigate the persistent cause (provider throttling, oversized window) before re-triggering. |
| `partial_failed` | run `SyncRunStatus` (`PARTIAL_FAILED`) | The run finished with a mix of `SUCCESS` and `FAILED` units. | Inspect the failed units; re-trigger the affected window if needed. |

A `retrying` unit is **not** a failure — it is in-flight recovery. Only `FAILED` units with a terminal `error_category` (`worker_lost`, `worker_lost_retry_exhausted`, or a provider/processor error) represent work that did not complete.

## Monitoring

### Prometheus Metrics

Every task execution records Prometheus metrics via `record_celery_task()`:
- Task name
- Completion state (success/failure)
- Duration in seconds

Investment LLM telemetry is dual-emitted. FastAPI exposes its process-local Prometheus
copy at `/metrics`; Celery prefork children push the equivalent bounded
`devhealth_investment_llm_*` time series through OTLP gRPC. Each child initializes its
exporter after fork and force-flushes it during graceful process shutdown, so worker
recycling and horizontal replicas do not depend on scraping a worker HTTP endpoint.

Configure `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_METRIC_EXPORT_INTERVAL` on every worker
pool. Collection topology, label restrictions, and the production verification query are
documented in [Investment LLM Telemetry](../llm/investment-llm-telemetry.md).

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

Manual Sync Now dispatch and finalization tasks are still published to the
shared `sync` queue. Any deployment that splits provider units into a dedicated
worker pool must keep that provider pool on `sync` too, or Linear/Jira/
LaunchDarkly unit queues can sit idle while dispatch waits behind saturated
generic sync work.

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
