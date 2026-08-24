---
page_id: op-workers-config
summary: Configure the live Go/River worker groups, queues, and schedules; the Celery/Beat surface is dormant history kept for context and rollback semantics.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - deploy/go-workers/deployment.json
  - contracts/jobs/v1/
  - contracts/sync-dispatch/v1/
  - src/dev_health_ops/alembic/versions/0096_enforce_unique_saved_report_schedule.py
  - src/dev_health_ops/alembic/versions/0097_backfill_report_schedule_next_run.py
  - current worker and synchronization settings
  - docs/contribute/architecture/go-worker-runtime.md
applicability: current
lifecycle: active
---

# Workers, schedules, and queues

**Go/River is the production owner of every current background job and every
current production schedule.** Every Python Celery worker and Beat service has
been stopped in production since 2026-08-19 (CHAOS-4026): they remain defined
in `compose.yml` for local-dev parity and as historical evidence of the
pre-cutover vocabulary, but nothing in production consumes them. Configure and
operate the Go worker groups and the Go scheduler described below; read the
Celery section only to understand a queue name you find in an old issue,
runbook, or `rollback_route` value.
{: .fc-page-lede }

For worker-group semantics (identity vs. routing), the two-plane
intent/serving model, and the full generated Celery→River queue mapping, see
[Go worker runtime architecture](../../contribute/architecture/go-worker-runtime.md) —
this page does not repeat that content.

## Historical Celery topology (dormant)

**ARCHIVED (CHAOS-4164, 2026-08-23):** every checked-in compose surface that
still defines the `worker`/`worker-ingest`/`worker-external-ingest`/
`worker-heavy`/`beat` Celery services -- `compose.yml`,
`deploy/docker-compose/compose.production.yml`, and
`deploy/docker-swarm/stack.yml` -- now carries an ARCHIVED banner comment at
the service definition itself, so a reader of the compose file alone (not
just this doc) sees that the fleet is not live topology. Nothing below
changes what those files run; this only makes their own text match what this
page already said.

Kept for context, not for operation. Before 2026-08-19 these were configured together:

- broker and result backend;
- worker queue lists;
- provider-specific and cost-class routing switches;
- normal and heavy-worker concurrency;
- lease, stale-detection, retry, and backoff values;
- provider budgets and deferral windows;
- Beat or scheduler ownership;
- shutdown grace periods for long-running work.

None of this is live. The queue names, the `worker`/`worker-heavy`/`worker-ingest`/`worker-external-ingest`/`beat`
service split, and the `WORKER_*_ENABLED` provider switches referenced by older
issues and runbooks are documented as historical vocabulary — with their Go
successors — in the
[generated queue mapping](../../contribute/architecture/go-worker-runtime.md#the-celery-to-river-queue-mapping-generated).
Do not bring a Celery service back up to diagnose a live incident; it has no
consumers wired to current queues and no operator has kept its routing
current since the cutover.

## Go worker groups

The deployment manifest is `deploy/go-workers/deployment.json`. Its River entries
describe deployment groups, not application worker types. Each group selects a
non-empty set of registered queues and owns its replicas, resources, autoscaling
policy, shutdown budget, and per-queue concurrency. A group name is an
observability label only — it never selects queues or changes handler
construction. See
[Worker-group semantics](../../contribute/architecture/go-worker-runtime.md#worker-group-semantics-identity-only-never-routing)
for the four consumers of that label (presence/health, `workerctl status`,
`joboperator` drain targeting, log labels) and why none of them route.

For example, these groups are valid and independently scalable:

| Group | Selected queues | Relationship |
| --- | --- | --- |
| `sync-workers` | `sync`, `sync_provider` | Disjoint from the analytics groups |
| `analytics-workers` | `investment`, `metrics`, `reports`, `workgraph` | One River client for this process |
| `metrics-overflow` | `metrics`, `webhooks` | Intentionally overlaps `metrics` |

The application keeps the canonical job-kind-to-queue mapping. It builds only
the handlers and dependencies required by the selected queues. Each worker
process constructs exactly one River client with one queue configuration map;
deploy another process when a separate boundary is needed. River safely
distributes claims among all consumers of an overlapping queue.

Do not add a queue to a group unless it is registered. The process rejects an
empty, unknown, duplicate, malformed, or conflicting selection before
readiness, and it does not support runtime queue reconfiguration.

Do not treat a healthy container as a route change. Today, "what should run"
is decided by the sync config (`IntegrationDataset.is_enabled`) and "where
it's served" is decided by `-Q` topology — the two-plane model ratified in
the [CHAOS-4054 decision record](https://linear.app/fullchaos/document/chaos-4054-two-plane-route-architecture-decision-record-7e5da955f899).
The ~40 `WORKER_*_ENABLED` provider-route switches still exist in code as of
this writing but are being deleted, not kept as a durable third plane or a
break-glass switch — do not build new operational process around them.
A route change must still identify the job kind and contract version and
prove that no duplicate or missing domain effect occurs; it no longer needs
to preserve a Celery rollback path, because there is no live Celery consumer
left to roll back to.

## Known divergences: local vs prod worker topology

Recorded under the standing order that local/prod divergence is itself a
defect (CHAOS-4164). These are known, not fixed here -- each is a rationale
for why `docker compose config` on a laptop and the same command on the prod
host legitimately disagree, so a future reader does not mistake one gap for
a bug and re-discover it from scratch.

- **Worker topology naming.** Prod splits `go-worker-sync` (`--queues=sync`)
  and `go-worker-sync-provider` (`--queues=sync_provider`) as two services.
  Local merges both into one service named `go-worker`
  (`--queues=sync,sync_provider`, `compose.yml`, with
  `PROVIDER_SYNC_QUEUES_ENABLED: "true"`). Queue *coverage* is equivalent --
  `sync_provider` has a consumer locally, so this is not a missing-consumer
  bug -- but process isolation differs (one saturated queue can starve the
  other locally) and anything keying off service *name* (dashboards, alert
  `job` regexes, runbooks) cannot match both shapes at once.
- **Profile gating.** Prod's `go-*` services sit behind the `go-workers`
  compose profile, which is why bringing the fleet up on the prod host is a
  two-pass `pull` then `up --profile go-workers`: a plain `pull` before the
  profile is selected silently skips pulling the profiled images, so the
  first `up` under the profile can start stale or missing images without
  erroring. Local's `go-*` services are not profile-gated.
- **Replica defaults.** `deploy/docker-compose/compose.go-workers.yml` sets
  `replicas: 0` as the compose *default* for `go-worker-heavy`; prod's actual
  replica count (3) comes from the deploy records, not the checked-in file. A
  reader of the file alone concludes heavy work is not running.
- **Service-list deltas.** Prod-only: `go-river-migrate`, `acr-material-init`.
  Local-only: `bugsink`, `mailpit`, `falkordb`, `riverui`,
  `go-worker-consumers-ready`, `go-worker-ready`, the `*-route-activate`
  services, `go-worker-migrate`. `go-worker-heavy`
  (`investment,metrics,reports,workgraph`) and `go-worker-ops`
  (`coverage,heartbeat,retention,webhooks`) are identical in both.
- **Archived Celery naming, checked-in vs. actually deployed.** The Celery
  service names archived in this repo's `compose.production.yml`
  (`worker`, `worker-ingest`, `worker-external-ingest`, `worker-heavy`,
  `beat`) do not match the fleet a live prod-host `docker compose config`
  snapshot showed at the time this was filed (`worker`, `worker-backfill`,
  `worker-bg`, `worker-heavy`, `worker-ingest`, `worker-wi`, `beat`) --
  `worker-external-ingest` has no counterpart in that snapshot, and
  `worker-backfill`/`worker-bg`/`worker-wi` have no counterpart in this repo
  at all. The checked-in production compose file was already not the true
  source of prod's Celery topology before either side was archived; treat
  both as historical record of *a* Celery topology, not *the* deployed one.
- **Helm/Kubernetes defaults still describe a Celery-primary deployment.**
  `deploy/helm/dev-health/values.yaml` defaults `worker.enabled`,
  `workerIngest.enabled`, `workerExternalIngest.enabled`,
  `workerHeavy.enabled`, and `beat.enabled` to `true` and `goWorkers.enabled`
  to `false`; `PAGERDUTY_WEBHOOK_TRANSPORT` there still defaults to `celery`.
  `deploy/kubernetes/beat.yaml` and `worker.yaml` are the matching plain
  manifests. None of this is what actual prod runs today -- the live prod
  fleet this page describes was verified through `docker compose`, not `helm`
  or `kubectl` -- but a reader following the Helm/Kubernetes path alone, with
  no other context, would deploy the archived Celery fleet as primary and the
  Go path as an opt-in add-on, the reverse of current reality. Left
  unarchived here deliberately: fixing the Helm/Kubernetes chart is a
  materially larger, riskier change than a compose-comment archival, and it
  is not yet known whether that deployment path has any live consumer at
  all. Tracked as a CHAOS-4164-related follow-up (ticket number pending from
  team-lead at time of writing); the archive-vs-delete call there belongs to
  chris.

## PostgreSQL requirements

Go worker processes require:

- `POSTGRES_URI` for domain state, with transaction-mode PgBouncer supported;
- `WORKER_DATABASE_URI` for direct River queue control;
- distinct unprivileged domain and queue roles;
- the pinned River schema and job registry;
- no migration DSN in long-running processes.

The one-shot migration job alone receives `MIGRATION_DATABASE_URI`. See [Databases and storage](databases-and-storage.md).

## Job and dispatch contracts

Versioned contracts under `contracts/jobs/v1/` define job envelopes, registry
entries, capability reports, deployment groups, and migration state. Sync
dispatch routes under `contracts/sync-dispatch/v1/` freeze the transport
ownership used by the scheduler and reconciler foundations.

Before changing a route (adding a job kind, moving a kind to a different
queue, or changing a queue's consumer group):

1. update and validate the versioned contract;
2. compile the matching handler;
3. prove payload and result compatibility;
4. update the selected queue group and its connection budget so the
   **selected** queue set still equals the **constructed** handler set
   (`Registry.ValidateStartup` — see
   [Process roles](../../contribute/architecture/go-worker-runtime.md#process-roles));
5. verify operator, health, metrics, and audit behavior.

Celery shadow/parity evidence and canary admission applied while both fleets
ran side by side; there is no live Celery baseline left to shadow against.

### Recover a discarded provider delivery

The Go reconciler automatically repairs a provider-unit delivery only when the
exact River job was discarded by the unhandled-kind rescue path and the
authoritative sync run and unit are still active, due, and unleased. This can
happen during a rolling deployment when a temporary worker set does not yet
register the provider handler. Pausing an integration prevents new planning;
it does not cancel work already planned for an active run.

Recovery rearms the same durable outbox row and creates one replacement River
job. It does not reset provider evidence or domain attempts, and it excludes
canceled or terminal runs and units, ordinary provider failures, and exhausted
River jobs. Monitor:

```text
worker_outbox_reconciler_terminal_deliveries_recovered_total
```

An increase paired with a deployment is expected recovery. Repeated increases
without a deployment indicate a worker group is still starting without the
complete checked-in handler registry. Do not rewrite queue rows manually. Cancel the
sync run only when the user intends to abandon the remaining work; cancellation
is not a substitute for repairing a recoverable transport delivery.

## Schedules

**The Go scheduler (`dev-health-scheduler`) is the sole production owner of
current schedules.** Celery Beat is stopped in production (CHAOS-4026,
2026-08-21) and no longer evaluates, mutates, or publishes anything; every
Beat cadence has either been proven redundant and deleted (for example the
`ask-dev-retention-sweep` entry retired below) or reimplemented natively on
the Go scheduler with organization entitlement, mutation, lease repair, and
production publication all owned there.

Run exactly one active production scheduler unless the deployment contract explicitly provides leader election or another duplicate-prevention mechanism. Verify that recurring work cannot overlap beyond provider, worker, and store capacity.

### Audit saved-report schedule ownership

Each non-null `saved_reports.schedule_id` must belong to one report. Application
schema migration `0096` enforces this rule. The migration does not choose a
winner or delete a report when it finds older duplicate data. It stops before
adding the constraint and lists the first 25 duplicate schedule IDs with their
report IDs and the total number of duplicate schedules.

Audit a database before its maintenance window:

```sql
SELECT schedule_id,
       count(*) AS report_count,
       array_agg(id ORDER BY id) AS report_ids
FROM saved_reports
WHERE schedule_id IS NOT NULL
GROUP BY schedule_id
HAVING count(*) > 1
ORDER BY schedule_id;
```

An empty result is ready for the constraint. For each returned schedule, decide
which report keeps the cadence and detach the other reports by setting their
`schedule_id` to `NULL`. Preserve every report definition unless the customer
separately requests its deletion. Rerun the audit, then rerun the migration.

The migration locks `saved_reports` writers while it repeats this audit and
adds the constraint. This closes the gap in which a new duplicate could arrive
between the check and the schema change. Plan the brief write pause as part of
the database maintenance window.

### Operate the bounded saved-report sweep

Application migration `0097` backfills `scheduled_jobs.next_run_at` for every
linked report schedule. This timestamp is a paging projection derived from the
report's last run (or creation), cron expression, and timezone. It is not proof
that a report ran: `saved_reports.last_run_at`, `report_runs`, and
`scheduled_report_occurrences` remain the execution record.

The native sweep locks at most 501 eligible schedule/report pairs per
occurrence and materializes at most 500. It orders never-materialized work
before durable replays, then oldest due time and report ID. A tenant with more
rows than the page therefore cannot abort or monopolize a global read. The
remainder stays due for the next five-minute tick and publishes:

```text
fixed_scheduler_schedule_degraded{
  schedule="scheduled_reports_dispatch",
  reason="scheduled_reports_deferred"
} 1
```

The gauge clears after a later evaluation observes no deferred remainder. A
non-zero value alone is bounded backpressure, not lost work; alert when it does
not clear across enough ticks to drain the active due population.

Audit the projection after migration:

```sql
SELECT count(*) FILTER (WHERE job.next_run_at IS NULL) AS missing_markers,
       count(*) AS linked_report_schedules
FROM scheduled_jobs AS job
JOIN saved_reports AS report ON report.schedule_id = job.id
WHERE job.job_type = 'report';
```

`missing_markers` must be zero. Migration `0097` reads in 500-row keyset
batches and stops transactionally if a legacy cron cannot be evaluated. Its
error names the scheduled-job ID without echoing tenant-authored cron text.
Repair that row to an evaluable five-field cron, then rerun the migration. New
GraphQL create and update writes evaluate the cron before persistence and keep
`next_run_at` current; the producer independently re-evaluates it as a runtime
backstop for legacy, corrupt, or direct-database rows.

The fixed scheduler stores each producer's latest degraded verdict in the same
PostgreSQL occurrence row as the committed evaluation. The
`fixed_scheduler_schedule_degraded` gauge therefore reports shared durable
state rather than one process's memory. Duplicate replicas and restarted
schedulers read the same newest verdict. A later clean evaluation stores an
empty verdict and clears the gauge. The value describes the schedule at its
last evaluation; for an infrequent schedule, it can be one full period old.

The Ask Dev expiry repair (`prune_ask_dev_conversations`) was built in Go
first, with no Celery predecessor. CHAOS-3404 added the Beat entry
`ask-dev-retention-sweep` for the same work at the same cadence as a stopgap
while `producer_version` caught up to the v3 contract.

**Resolved (CHAOS-3481, 2026-08-21):** `producer_version` for
`system.retention_cleanup` is now 3 (promoted once capability reports from
every live Go consumer proved v3 compatibility), and the SKIP-LOCKED
short-read false-success gap is closed (a `DrainConfirmer` check replaces the
non-locking `count_expired()` read that used to report `partial` for a
merely-contended backlog). Go now genuinely emits and drains this cadence.
CHAOS-4026 (2026-08-21) deleted the now-redundant Celery `ask-dev-retention-sweep`
Beat entry and its `run_ask_dev_retention_cleanup` task
(`src/dev_health_ops/workers/ask_dev_retention.py`) as part of the broader
Celery-retirement cleanup -- there is no longer a second purger to keep in
sync with. Its staged v3 contract uses the table-scoped `ask_dev_conversations`
policy at 05:30 UTC. Its cutoff is the occurrence time because each
conversation already persists its exact expiry; adding an environment
retention horizon would incorrectly extend or shorten the user's 0/30-day
choice. After activation, admission uses the canonical default-disabled
`ask_dev` decision for a never-used installation, but persisted conversation
state keeps cleanup eligible even when Ask Dev is later disabled. Feature
rollback stops new use; it never suspends expiry, deletion, or purge
obligations.

## Validate the configuration

- each emitted queue has an intended consumer;
- current routes still match the deployed runtime owner;
- worker readiness is open only for groups that can execute every admitted job
  in their selected queues;
- the effective queue set and per-queue concurrency match the deployment;
- each worker process constructs one River client;
- overlapping groups are intentional and are budgeted as separate consumers;
- scheduler ownership is singular and observable;
- queue depth and oldest age advance under a bounded job;
- retries preserve idempotency and provider budgets;
- a queue/topology rollback restores the previous `-Q` selection on the
  affected groups before the changed processes are drained or stopped —
  there is no Celery routing left to fall back to.
