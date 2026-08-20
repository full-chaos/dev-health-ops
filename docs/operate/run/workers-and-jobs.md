---
page_id: op-workers
summary: Start, verify, roll out, and recover Celery and deployment-selected Go workers.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - deploy/go-workers/deployment.json (deployment manifest)
  - contracts/jobs/v1/
  - current worker and synchronization settings
applicability: current
lifecycle: active
---

# Run workers and jobs

Celery remains the production owner of background jobs until a checked-in route
and release decision move a job family to River. The Go worker deployment
contract selects queues; it does not change the canonical job-kind-to-queue
mapping.
{: .fc-page-lede }

The Go worker, scheduler, reconciler, and stream-runner binaries are separate
process roles. A healthy process does not, by itself, authorize a route change
or production job ownership.

## Start the active runtime

Start a worker with the default queues:

```bash
dev-hops workers start-worker
```

Specify queues and concurrency when the deployment uses an explicit topology:

```bash
dev-hops workers start-worker \
  --queues default metrics sync webhooks ingest \
  --concurrency 4
```

### Start a Go worker group

`dev-health-worker` requires an explicit, static set of registered queues. The
deployment passes its whole configuration as command arguments. Queue and
concurrency arguments may be comma-separated or repeated. The process sorts and
validates the set before readiness and constructs one River client for all
selected queues.

```bash
dev-health-worker \
  -Q sync,sync_provider \
  -c sync=4,sync_provider=2 \
  --worker-group sync \
  --shutdown-timeout 960s
```

The flag surface mirrors the Python Celery worker CLI: `-Q/--queues` names the
queues to consume and `-c/--concurrency` sets the worker budget, exactly as
`celery -A dev_health_ops.workers.celery_app worker -Q … --concurrency=…` does
for the Celery fleet. `-q` and `--loglevel` are accepted as aliases.

**The two fleets do not serve the same queues.** `-Q` names queues from the Go
River topology (`coverage`, `heartbeat`, `investment`, `metrics`, `reports`,
`retention`, `sync`, `sync_provider`, `webhooks`, `workgraph`). Four of those
names — `metrics`, `reports`, `sync`, `webhooks` — are shared with the Celery
app and mean the same thing; the rest exist in only one runtime. Selecting a
queue this fleet does not serve fails before readiness, because startup
validation requires the selected queue set to equal the constructed handler
set.

**`--help` is the discovery surface.** Run `dev-health-worker --help` for the
complete option list: every flag, the environment variable it falls back to,
and its default, grouped by purpose. The reconciler, scheduler, and stream
runner print the options *they* accept, so a binary never advertises a setting
it ignores.

Resolution order is **flag > environment > default**. Every option except the
credentials below may be given either way; the flag wins when both are present,
and the process logs one warning at startup naming any setting that arrived
through the environment. `--queues` has no environment fallback: every deploy artifact passes it.

An **unknown flag is rejected before the process starts** (exit status 2). This
is the property an environment variable cannot offer: a misspelled variable
name is indistinguishable from an unset one and stays silently inert, which is
how a typo'd `OTEL_SERVICE_NAMEi` survived unnoticed in production.

`-c` is the canonical short form of `--concurrency`, matching Celery;
`--queue-concurrency` remains accepted as a deprecated alias so supervisors
outside this repository keep working.

#### Provider route switches stay in the environment

The forty `WORKER_<PROVIDER>_<DATASET>_ENABLED` switches are **not** on the CLI
and deliberately have no flag. What a worker executes follows from the queues it
subscribes to; a parallel forty-switch enablement surface does not scale, and it
is the thing being designed away.

They remain as environment variables because the Python planner reads the
identical names through `ProviderUnitRouteSwitches` to decide what to *plan*.
Producer and executor must agree: a planner that emits units for a route no
executor serves is the wedge shape CHAOS-3990 exists to prevent. `GO_PROVIDER_ROUTES=all`
(local only, requires `DEV_HEALTH_ENV=local`) is unchanged.

#### What stays in the environment

Credentials only, and deliberately. A DSN or token passed as a process argument
is readable through `ps`, `docker inspect`, and `docker compose config`, so
these ten have no flag and `--help` documents them as environment-only:

`POSTGRES_URI`, `WORKER_DATABASE_URI`, `COORDINATOR_DATABASE_URI`,
`CLICKHOUSE_URI`, `VALKEY_URI`, `SETTINGS_ENCRYPTION_KEY`,
`SETTINGS_ENCRYPTION_SALT`, `PAGER_DUTY_CLIENT_ID`, `PAGER_DUTY_SECRET`,
`WORKER_OPERATIONAL_BRIDGE_TOKEN`.

Everything else a Go worker reads is a flag, and the shipped Compose, Swarm,
Kubernetes, and Helm surfaces pass it in `command:`/`args:` — so
`docker compose config` and `kubectl describe pod` show the deployed
configuration in one place instead of a merged environment map.

The deployment owns the worker group name, replicas, resources, autoscaling,
shutdown budget, and per-queue concurrency. The binary owns neither a named
topology nor a queue remapping. An empty, unknown, duplicate, or malformed
queue selection fails before readiness. Queue selection cannot change while a
process is running.

Run one active scheduler for the production environment:

```bash
dev-hops workers start-scheduler
```

Use the deployment's process manager, container orchestrator, or service
supervisor for long-running processes. The commands above show the application
entry points; they do not replace restart, resource, or secret-management
policy.

## Match producers to consumers

The stable queue families include:

| Queue | Typical work |
| --- | --- |
| `investment` | Investment materialization and finalization |
| `metrics` | Daily and remaining metrics work |
| `reports` | Saved and scheduled report execution |
| `workgraph` | Work graph construction |
| `sync` | Synchronization coordination |
| `sync_provider` | Provider synchronization units |
| `coverage` | Synchronization coverage refresh |
| `heartbeat` | Worker heartbeat work |
| `retention` | Retention cleanup |
| `webhooks` | Provider webhook processing |

The registry is the complete set of selectable Go queues. A new queue requires
the registry and its producer mapping to change first. Every emitted queue must
then have an intended consumer; otherwise a valid job can wait indefinitely.

Review [Workers, schedules, and queues](../configure/workers-and-schedules.md)
before changing routing, concurrency, database pools, or schedule ownership.

Worker groups can be disjoint or overlap intentionally. For example, deploy
one group with `sync,sync_provider` and a second with `metrics,reports`; both
groups then scale independently. A third group may also consume `metrics` when
that queue needs a separate resource or autoscaling policy. River distributes
claims safely among every ready consumer of an overlapping queue.

Each group declares the concurrency for its own queues. A queue may therefore
have `metrics=4` in one group and `metrics=1` in another. This is deployment
capacity, not a second application-level queue mapping.

Inspect all River groups and their queue sets with the authenticated operator:

```bash
dev-health-workerctl workers queues status
```

The JSON response reports each group's canonical `queues`, desired and live
replicas, queue backlog, active jobs, drain state, and connection-budget
headroom. It is the source for the effective deployed topology; do not infer a
queue set from a service name.

To pause or resume a deliberate queue-set drain, name both the deployment group
and every affected queue. The operation is audited and requires a reason and a
correlation ID:

```bash
dev-health-workerctl workers queues drain \
  --group metrics-workers \
  --queue metrics --queue reports \
  --reason deploy_drain \
  --correlation-id rollout-2026-08-15

dev-health-workerctl workers queues undrain \
  --group metrics-workers \
  --queue metrics --queue reports \
  --reason deploy_resume \
  --correlation-id rollout-2026-08-15
```

Use process shutdown for an ordinary rollout or downscale. A queue-set drain
pauses the named River queues for the named group; it is not a replacement for
stopping one process and does not require a global queue owner.

## Verify worker health

Check that workers answer through Celery:

```bash
celery -A dev_health_ops.workers.celery_app inspect ping
```

Inspect active, reserved, and scheduled work before a rollout:

```bash
dev-hops workers inspect --state active --output json
dev-hops workers inspect --state reserved --output json
dev-hops workers inspect --state scheduled --output json
```

Verify all of the following:

- every emitted queue has an intended consumer;
- the oldest queued-job age is stable or falling;
- scheduled jobs advance only once per occurrence;
- worker logs identify the job kind and safe failure category;
- API, worker, and scheduler health remain available;
- retries advance without duplicate product-visible effects.

Use [Health checks](../observe/health-checks.md),
[Logs](../observe/logs.md), and
[Metrics and traces](../observe/metrics-and-traces.md) together. A process can
be alive while its broker, database, route, or handler dependency is not ready.

## Trigger supported work

Prefer the product or administrative trigger that owns the durable run record:

- start a synchronization or backfill through the workspace administration
  surface;
- run or schedule a report through the Report Center;
- let configured schedules enqueue recurring metrics and maintenance work.

These paths carry the workspace, credentials, authorization, and run identity
that a bare inline command may not supply. Confirm that the resulting run
appears in the owning status surface and that freshness or output advances.

## Roll out workers safely

1. Apply required database migrations before restarting application workers.
2. Confirm every emitted queue has at least one intended consumer and that the
   selected queue set for each new process is explicit.
3. Inspect active, reserved, and scheduled jobs.
4. Keep the shutdown grace period longer than the longest admitted task budget.
5. Roll workers gradually so at least one compatible consumer remains for each
   active queue.
6. Verify queue age, failure rate, retries, and downstream freshness after each
   step.
7. Keep Celery ownership in place unless a separately reviewed route change has
   completed its parity, canary, and rollback gates.

Do not purge queues, delete execution rows, or change a route merely to make a
deployment appear healthy. Those actions can discard work or create duplicate
effects.

## Recover a stalled or failing queue

First identify whether the problem is admission, routing, execution, or
downstream persistence.

- If queue depth grows, confirm the queue name and that a ready worker consumes
  it.
- If jobs remain reserved, inspect worker shutdowns, leases, time limits, and
  broker connectivity.
- If jobs retry, use the safe error category and owning run record to determine
  whether recovery is automatic.
- If a provider queue alone stalls, verify its credential, budget, and
  provider-specific consumer before changing global concurrency.
- If a route or deployment group disagrees with the checked-in contract, stop
  the incompatible process and restore the reviewed Celery configuration.

If a job looks stuck but reports no error, read
[Job recovery lifecycle](job-recovery-lifecycle.md) first: recovery after a
worker dies is gated by both the rescue horizon and the job's own timeout, so
the correct action is often to wait rather than to intervene.

Use [Worker or queue failure](../runbooks/worker-or-queue-failure.md) for the
recovery sequence. Escalate before destructive broker or database operations,
and preserve logs, run identifiers, queue names, and timestamps as evidence.
