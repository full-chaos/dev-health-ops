---
page_id: op-workers
summary: Operate active Celery jobs and observe dormant Go worker foundations without confusing process health, queue ownership, or domain completion.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/workers.md
  - docs/ops/investment-materialization.md
  - deploy/go-workers/profiles.json
  - current worker operator and telemetry implementation
applicability: current
lifecycle: active
---

# Workers, jobs, retries, and schedules

Background work has three distinct truths: the process is alive, the queue is progressing, and the domain effect completed. A healthy worker process is not proof that a job was admitted, executed, or persisted; a quiet queue is not proof that the scheduler created the expected work.
{: .fc-page-lede }

## Current runtime ownership

Celery with Valkey/Redis remains the production runtime for synchronization, metrics, reports, webhooks, materialization, and scheduled work. The Go worker, scheduler, reconciler, and stream-runner are dormant coexistence foundations. Their current profiles have zero minimum replicas and all registered job routes remain Celery-owned.

Do not route production work to River because a Go process reports healthy. Production ownership changes only through the checked-in migration state, compiled-handler coverage, parity evidence, canary controls, and rollback procedure for the specific job kind.

## Read the active Celery path

For each job, identify:

1. the user, API, scheduler, or provider event that created it;
2. the canonical domain run or activity record;
3. the queue and worker class that own execution;
4. the provider budget, lease, retry, and timeout policy;
5. the downstream Postgres or ClickHouse effect;
6. the product freshness or completion evidence.

Manual, scheduled, and backfill synchronization all plan canonical sync runs and fan out units. The trigger timing differs; operators should still use the persisted run and unit records as execution truth.

## Observe queues and execution

Monitor:

- queue depth and oldest eligible age;
- planned, dispatched, running, retrying, completed, and failed units;
- execution duration and saturation;
- lease expiration and stale-work reconciliation;
- retry reason, backoff, and exhaustion;
- provider budget deferrals and rate limits;
- downstream write failures;
- latest successful synchronization or materialization.

Never publish zero when the queue or database sample failed. Treat the signal as unavailable and inspect the dependency.

## Go worker health and metrics

The Go worker foundation exposes:

- `/healthz` for process liveness;
- `/readyz` for dependency and contract readiness;
- `/metrics` for bounded Prometheus telemetry.

Readiness checks include domain-role authorization, registry load, complete compiled-handler coverage, supported contract versions, queue-control configuration, direct queue-control connectivity, and the pinned River schema. A `queue_control_config` failure means the DSN, pooling mode, or role-separation contract is invalid; it does not expose the DSN.

Metrics include queue depth, oldest eligible age, execution saturation, and both PostgreSQL pool-saturation ratios. These signals describe the foundation and do not transfer production ownership from Celery.

## Operator CLI

`dev-health-workerctl` is the payload-redacted operator surface for the Go foundation. Create a service credential with:

```bash
dev-hops service-credentials create \
  --service worker-operator \
  --scope workers:read \
  --scope workers:operate
```

The plaintext token is shown once and is supplied as `WORKER_OPERATOR_TOKEN` or its `_FILE` form.

Read commands include:

```text
status
jobs list
jobs inspect
queues
streams status
contracts
```

Mutations require a reason and correlation ID, validate the exact state transition, and persist a bounded audit intent before changing River state. A database commit ambiguity is reported as `outcome_unknown`; inspect the resource before retrying. A confirmed mutation whose final audit write is delayed is reported as `audit_pending`.

Cancel and retry remain intentionally closed for foundation job kinds that do not yet have authoritative semantic links. Queue pause/resume and profile drain are available only to authorized operators.

## Retry and replay

Before retrying:

- identify whether the domain effect may already have committed;
- check the provider event or job idempotency key;
- inspect durable deduplication and outbox state;
- account for provider cost and rate budgets;
- distinguish an execution failure from an audit-finalization delay;
- preserve the original correlation and reason.

For PagerDuty V3, identical event replay is idempotent. Reuse of the same event identity with a different body is a conflict and must not create a second canonical write.

## Scheduler and reconciler foundations

The dormant Go scheduler and sync reconciler currently produce bounded, read-only comparison evidence. They do not claim, lock, update, publish, or repair production sync work. Unsupported schedule expressions are reported as unsupported rather than guessed.

Keep Celery Beat and the current sync scheduler active until the migration issue for a specific path explicitly changes ownership.

## Verify completion

A job is complete only when:

- the execution record is terminal and successful;
- required downstream writes are present;
- the product or administrative freshness signal advances;
- no retry, stale lease, or outbox item remains unresolved;
- the result belongs to the intended organization and source;
- any operator mutation has a durable audit outcome.
