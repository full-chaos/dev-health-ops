---
page_id: op-workers-config
summary: Configure active Celery queues and schedules while keeping dormant Go worker profiles, River routes, database roles, and rollback controls explicit.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/workers.md
  - deploy/go-workers/profiles.json
  - contracts/jobs/v1/
  - contracts/sync-dispatch/v1/
  - current worker and synchronization settings
applicability: current
lifecycle: active
---

# Workers, schedules, and queues

Celery remains the production owner of every current background job. The Go worker, scheduler, reconciler, and stream-runner binaries are additive coexistence foundations: they may build, start, expose health, and produce shadow evidence, but no job moves to River until its checked-in route, handler coverage, parity, canary, and rollback gates explicitly change.
{: .fc-page-lede }

## Active Celery topology

Configure together:

- broker and result backend;
- worker queue lists;
- provider-specific and cost-class routing switches;
- normal and heavy-worker concurrency;
- lease, stale-detection, retry, and backoff values;
- provider budgets and deferral windows;
- Beat or scheduler ownership;
- shutdown grace periods for long-running work.

A routing flag is safe only when the deployed workers consume every queue it can emit. Confirm queue names in the checked-in deployment artifact rather than assuming defaults from an older issue or runbook.

## Go coexistence profiles

The canonical profile manifest is `deploy/go-workers/profiles.json`. Current profiles:

- have zero minimum replicas;
- retain `celery` as route owner;
- remain readiness-closed without complete compiled handler coverage and compatible schemas;
- define maximum replicas and connection budgets even while disabled;
- are used for contract, health, parity, and deployment-shape validation—not production job ownership.

Do not enable a Go profile merely because its container is healthy. A route change must identify the job kind and contract version, move through shadow and canary states, preserve Celery rollback, and prove that no duplicate or missing domain effect occurs.

## PostgreSQL requirements

Go worker processes require:

- `POSTGRES_URI` for domain state, with transaction-mode PgBouncer supported;
- `WORKER_DATABASE_URI` for direct River queue control;
- distinct unprivileged domain and queue roles;
- the pinned River schema and job registry;
- no migration DSN in long-running processes.

The one-shot migration job alone receives `MIGRATION_DATABASE_URI`. See [Databases and storage](databases-and-storage.md).

## Job and dispatch contracts

Versioned contracts under `contracts/jobs/v1/` define job envelopes, registry entries, capability reports, deployment profiles, and migration state. Sync dispatch routes under `contracts/sync-dispatch/v1/` freeze the transport ownership used by the scheduler and reconciler foundations.

Before changing a route:

1. update and validate the versioned contract;
2. compile the matching handler;
3. prove payload and result compatibility;
4. run shadow/parity evidence without mutating the Celery baseline;
5. define canary admission and rollback;
6. update the deployment profile and connection budget;
7. verify operator, health, metrics, and audit behavior.

## Schedules

Celery Beat remains required for current production schedules. The Go scheduler foundation can evaluate bounded schedule timing for comparison, but it does not currently own organization entitlement, mutation, lease repair, or production publication.

Run exactly one active production scheduler unless the deployment contract explicitly provides leader election or another duplicate-prevention mechanism. Verify that recurring work cannot overlap beyond provider, worker, and store capacity.

## Validate the configuration

- each emitted queue has an intended consumer;
- current routes still match the deployed runtime owner;
- worker readiness is open only for profiles that can execute every admitted job;
- scheduler ownership is singular and observable;
- queue depth and oldest age advance under a bounded job;
- retries preserve idempotency and provider budgets;
- rollback restores Celery routing before Go processes are drained or stopped.
