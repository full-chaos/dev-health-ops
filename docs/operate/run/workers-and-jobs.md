---
page_id: op-workers
summary: Start, verify, roll out, and recover the active Celery worker and scheduler runtime.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - deploy/go-workers/profiles.json
  - contracts/jobs/v1/
  - current worker and synchronization settings
applicability: current
lifecycle: active
---

# Run workers and jobs

Celery remains the production owner of background jobs. Keep Celery workers
and the Celery Beat scheduler running until a checked-in route, deployment
profile, and release decision explicitly move a job family to River.
{: .fc-page-lede }

The Go worker, scheduler, reconciler, and stream-runner binaries are
coexistence foundations. A healthy Go process does not, by itself, authorize a
route change or production job ownership.

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
| `default` | Dispatch, health, and lightweight coordination |
| `metrics` | Metrics, recommendations, work graph, and investment work |
| `sync` | Planned provider synchronization units |
| `reports` | Saved and scheduled report execution |
| `webhooks` | Provider webhook processing |
| `ingest` | External ingestion |
| `monitoring` | Queue and worker telemetry |

Provider-specific and cost-class queues may also be enabled. Expand worker
consumer lists before enabling producer-side routing. Otherwise a valid job can
wait indefinitely on a queue that no process consumes.

Review [Workers, schedules, and queues](../configure/workers-and-schedules.md)
before changing routing, concurrency, database pools, or schedule ownership.

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
2. Confirm the new processes consume every queue the current producers can
   emit.
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
- If a route or deployment profile disagrees with the checked-in contract,
  restore the reviewed Celery configuration and stop the incompatible process.

Use [Worker or queue failure](../runbooks/worker-or-queue-failure.md) for the
recovery sequence. Escalate before destructive broker or database operations,
and preserve logs, run identifiers, queue names, and timestamps as evidence.

