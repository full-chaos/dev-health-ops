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
been stopped in production since 2026-08-19 (CHAOS-4026), and CHAOS-5589
deleted the service definitions outright from every compose surface
(`compose.yml`, `compose.production.yml`, `docker-swarm/stack.yml` -- R146:
Celery transport is not a rollback target). The Go/River fleet is every
compose file's only worker topology now, folded in as the unconditional
default the same way CHAOS-3088 first did for root `compose.yml`. Configure
and operate the Go worker groups and the Go scheduler described below; read
the Celery section only to understand a queue name you find in an old issue,
runbook, or `rollback_route` value.
{: .fc-page-lede }

For worker-group semantics (identity vs. routing), the two-plane
intent/serving model, and the full generated Celery→River queue mapping, see
[Go worker runtime architecture](../../contribute/architecture/go-worker-runtime.md) —
this page does not repeat that content.

## Historical Celery topology (dormant)

**ARCHIVED (CHAOS-4164, 2026-08-23) then DELETED (CHAOS-5589):** every
`worker`/`worker-ingest`/`worker-external-ingest`/`worker-heavy`/`beat`
Celery service definition is gone from every compose surface -- `compose.yml`
first (gated behind `profiles: [celery-legacy]` under CHAOS-3088, then
deleted outright), then `compose.production.yml` and
`deploy/docker-swarm/stack.yml` in the same ticket, once R146 established
Celery is not a rollback target and no consumer had run since 2026-08-19.
There is no compose surface left where the archived fleet can be brought
back up.

Kept for context, not for operation -- nothing below is reproducible from
this tree any more. Before 2026-08-19 these were configured together:

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

Every group's liveness and readiness probes default to
`goWorkers.defaultProbes` in `values.yaml`: `/healthz` and `/readyz` on the
`metrics` port, with `timeoutSeconds: 5` and `failureThreshold: 6`. That
timeout is deliberately generous, not cosmetic: with the old 1s default (the
Kubernetes default, previously unset) a reconciler-style group running with
`maxUnavailable: 0` could deadlock its own rolling update — a slow-but-healthy
`/readyz` response under load fails the probe, so the new pod never becomes
Ready while the old one, pinned by `maxUnavailable: 0`, can never terminate.
Set `goWorkers.groups[].livenessProbe` / `.readinessProbe` to override either
probe for one group only.

Before a worker opens its River queues, it runs preclaim-readiness: required
startup checks (domain/queue PostgreSQL authorization, the posture-manifest
lockstep proof, queued contract versions, execution liveness) that must all
pass before the process claims any job. A check that runs to completion and
reports a real problem — wrong credentials, a posture mismatch between this
binary and the applied manifest — still fails the process immediately, the
same as always. A check that only *times out* — most commonly Postgres or its
pgbouncer pooler taking longer than usual to answer a connection or a query —
is instead retried with backoff, logging each attempt at warn with how long
the process has been waiting, for up to `--preclaim-readiness-timeout`
(`goWorkers.defaultPreclaimReadinessTimeout` in `values.yaml`, 5 minutes by
default; override one group via `goWorkers.groups[].preclaimReadinessTimeout`).
Only once that budget is spent does the process exit and let Kubernetes
restart it.

This matters most during a fleet-wide rollout: a helm upgrade that restarts
every worker group's Deployments at once starts a burst of new connections
against Postgres and its pooler at the same moment, which can push an
individual readiness check past its own timeout (`--health-check-timeout`,
2 seconds by default) for as long as it takes that connection burst to
settle — independently of whether any dependency is actually broken. Without
the retry, that burst crash-loops every worker group in the fleet at once;
with it, a replica simply stays not-yet-ready and becomes ready again on its
own once the burst clears. Raise `--preclaim-readiness-timeout` if a rollout
is large enough, or Postgres small enough, that recovery regularly needs more
than 5 minutes; a shorter recovery time does not need a shorter budget, since
retries stop as soon as the checks pass.

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

### Pinning a published image in the root file

Root `compose.yml` is a **staging file**: every service that declares a
`build:` block also declares `image:` in the `${VAR:-default}` form, so an
operator pin reaches all of them and not just the long-running ones. The
one-shot setup jobs are included on purpose -- a provisioning or migration
job that keeps building locally while the processes beside it honour a
release pin is how a setup step ends up running a different build of the
same binary.

| Pin variable | Default image (`<family>:local`, never published) | Services |
| --- | --- | --- |
| `DEV_HEALTH_GO_WORKER_IMAGE` | `dev-health-go-worker` | the four `go-worker-*` processes |
| `DEV_HEALTH_GO_RECONCILER_IMAGE` | `dev-health-go-reconciler` | `go-reconciler` |
| `DEV_HEALTH_GO_SCHEDULER_IMAGE` | `dev-health-go-scheduler` | `go-scheduler` |
| `DEV_HEALTH_GO_DHO_IMAGE` | `dev-health-go-dho` | the three `go-stream-*` processes (`dho stream-runner`) |
| `DEV_HEALTH_GO_OPERATOR_IMAGE` | `dev-health-go-operator` | the four `go-sync-*-route-activate` one-shots |
| `DEV_HEALTH_GO_CONTRACTCHECK_IMAGE` | `dev-health-go-contractcheck` | `go-contractcheck` |
| `DEV_HEALTH_IMAGE` | `dev-hops-runner` | `go-river-provision`, `go-river-migrate`, and the dormant Celery services |
| `DEV_HEALTH_API_IMAGE` | `dev-hops-api` | `api`, `metrics-api`, `billing-edge`, `migrate` |
| `DEV_HEALTH_QUERY_API_IMAGE` | `dev-health-query-api` | `query-api` (the `go-api` compose profile, `deploy/go-api/compose-query-api.yml`) |

Every default is the `:local` tag of its family, a tag no registry
publishes. Unpinned, Compose has nothing to fetch under that name and
builds it from this checkout, so a start is one coherent revision. Two
consequences follow, both deliberate: with no pin set and no prior build
there is nothing for `--no-build` to run, and a `:local` tag is shared
between compose projects on the same host rather than scoped to one.
`pull_policy` is
left at Compose's own default (`missing`), so an operator pin -- a
published tag OR a content digest -- is honoured as given: `docker
compose up` reuses an already-present local image under that name, or
pulls it, without ever forcing a rebuild. `docker compose build` /
`up --build` still build every image from this tree on request.

**Keeping every family on ONE revision is the operator's job.** Two
supported paths, and only these two:

- set one release pin across every family above, then `docker compose
  pull` and `docker compose up -d --no-build`; or
- `docker compose up -d` with nothing pinned, which builds every image
  from the checkout you are standing in.

`pull_policy: missing` means Compose uses a tag that is ALREADY on the
host as-is. Defaulting to `:local` keeps a published moving tag from
becoming that stale tag by accident, but it does not make the host
stateless: a `:local` image left by an EARLIER checkout is still reused,
and on a shared machine another project's build of the same family is the
same tag. Nothing here reconciles that for you, by design -- rebuild
(`docker compose build`) when you change branches.

What it does do is fail closed. A mixed set does not run wrong: the
worker refuses readiness when the posture manifest disagrees with the
live grants, the reconciler refuses a route that drifts from the
checked-in policy, and a worker whose ClickHouse schema predates the
pinned migration says so and stops. The setup jobs still exit 0 -- they
are the thing that applied the older schema -- so read the PROCESS
readiness, not the setup exit codes, when a bring-up looks wrong. Clear
the stale tags (`docker compose down -v` plus `docker image rm` for the
families above, or `docker compose build`) and start again from one of
the two paths.

This default was `pull_policy: build` (forced rebuild every time) for one
PR revision, to guard the CHAOS-5437 cross-tree posture-manifest lockstep
(migrate and the worker fleet must come from the same tree or the worker
refuses readiness by design) -- reverted because forcing a build made a
digest pin unusable (`build tag cannot contain a digest`) and duplicated
what a deployment-wide single-sha pin already guarantees. Lockstep safety
now comes from pinning every one of these images to the SAME sha (as
JOB 6's `job6-prebuild/*:<sha>` images do), not from forcing a build on
every bring-up.

### Pinning the route-activate hook's operator image (Helm)

The chart's route-activate pre-upgrade/pre-install hook Job
(`deploy/helm/dev-health/templates/route-activate-hooks.yaml`) runs the
`dev-health-go-operator` image against the database the migrate Job in the
same release just migrated. `migrations.hook.routeActivate.image` has no
floating default: the render refuses an empty value, `:latest`, or any
other tag that is not immutable, the moment
`migrations.hook.routeActivate.enabled=true`. This closed a real production
failure: a node ran its stale, already-cached `:latest` operator binary
against a database schema newer than the posture manifest that binary
knew about, and it exited `runtime_role_unauthorized` with no diagnostics
-- the mismatch looks identical to a real authorization failure.

Accepted forms for `migrations.hook.routeActivate.image`:

- `repo@sha256:<64 hex>` -- a digest pin, with or without a tag in front.
- `repo:sha-<12 hex>` -- this repo's other immutable-tag convention (see
  [Operator commands](../runbooks/operator-commands.md)).
- `repo:local` -- only when the hook's own pull policy
  (`migrations.hook.routeActivate.pullPolicy`) is `Never` or
  `IfNotPresent`, for a side-loaded local/kind image.

Pin it to the go-operator image built from the **same commit** as
`image.repository`/`image.tag` (the api/migrate image): the operator's
posture manifest has to agree with the schema the migrate Job just
applied. When both values carry a `sha-<12 hex>` tag the chart compares
them and refuses the render on a mismatch; a digest-only pin on either
side cannot be compared this way (a digest does not encode which commit
it came from), so keeping those in step is the operator's own
responsibility, same as any other digest pin.

### billing-edge: Stripe webhook forwarding in local dev

`billing-edge` (root `compose.yml`, port `8010`) needs three secrets to
report healthy readiness in its own `/health` payload:
`STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `LICENSE_PRIVATE_KEY`. On
the real deployed stack these are configured, and `/health`'s overall
`ok`/`down` status does not depend on Stripe's own outbound reachability
(a blocked egress path shows up as `stripe_client: "down"` in the JSON
body without flipping the container's compose healthcheck, which is a
liveness probe on `:8000`, not a readiness one). In local dev, webhook
delivery uses the Stripe CLI's own forwarder beside the stack: `stripe
listen --forward-to http://localhost:8010/api/v1/billing/webhooks/stripe`
(run from a host shell, not part of this repo).

## Known divergences: local vs prod worker topology

Recorded under the standing order that local/prod divergence is itself a
defect (CHAOS-4164). These are known, not fixed here -- each is a rationale
for why `docker compose config` on a laptop and the same command on the prod
host legitimately disagree, so a future reader does not mistake one gap for
a bug and re-discover it from scratch.

- **Worker topology naming.** Prod splits `go-worker-sync` (`--queues=sync`)
  and `go-worker-sync-provider` (`--queues=sync_provider`) as two services.
  The local host stack merges both into one service named `go-worker`
  (`--queues=sync,sync_provider`, with `PROVIDER_SYNC_QUEUES_ENABLED:
  "true"`). Queue *coverage* is equivalent -- `sync_provider` has a
  consumer locally, so this is not a missing-consumer bug -- but process
  isolation differs (one saturated queue can starve the other locally) and
  anything keying off service *name* (dashboards, alert `job` regexes,
  runbooks) cannot match both shapes at once. This repo's own root
  `compose.yml` is a separate thing again: as of the Go-default cutover it
  folds in the split `go-worker-sync`/`go-worker-sync-provider` shape
  (matching prod's naming, not the local host stack's merged one) as its
  unconditional default.
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
- **Helm/Kubernetes defaults matched compose (resolved, CHAOS-4195).** Before
  this ticket, `deploy/helm/dev-health/values.yaml` defaulted `worker.enabled`,
  `workerIngest.enabled`, `workerExternalIngest.enabled`,
  `workerHeavy.enabled`, and `beat.enabled` to `true` and `goWorkers.enabled`
  to `false`, with a `PAGERDUTY_WEBHOOK_TRANSPORT` switch defaulting to
  `celery`;
  `deploy/kubernetes/beat.yaml` and `worker.yaml` were the matching plain
  manifests. Chris's ruling on CHAOS-4195 was delete, not archive (unlike the
  compose surfaces above, which have no equivalent live-vs-checked-in
  divergence to preserve): the Celery Helm templates and Kubernetes manifests
  are gone, `goWorkers.enabled` and `deploy/kubernetes/go-workers.yaml`'s
  presence in `kustomization.yaml` now default to `true`. Helm/Kubernetes now
  agree with the compose reality this page describes: Go/River primary, no
  Celery fleet to opt out of. `PAGERDUTY_WEBHOOK_TRANSPORT` itself is gone
  (CHAOS-4105): it chose between two runtimes consuming the PagerDuty webhook
  stream, and the Python one no longer exists, so there is nothing left to
  choose.

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
