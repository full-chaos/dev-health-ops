---
page_id: op-rb-worker
summary: Recover when Celery work stops advancing or a Go coexistence process, River queue, reconciler, schedule evaluator, or operator mutation fails.
content_type: runbook
owner: platform-operations
source_of_truth:
  - docs/ops/workers.md
  - docs/ops/database-connection-pooling.md
  - current worker health, queue, outbox, operator, and route contracts
applicability: current
lifecycle: active
---

# Worker or queue failure

Use this runbook when expected work is not created, a queue stops advancing, workers repeatedly retry without domain progress, or a Go coexistence process closes readiness. Start by identifying the active runtime owner; current production routes remain Celery-owned unless the checked-in migration state explicitly says otherwise.
{: .fc-page-lede }

## Preserve the failing boundary

Record:

- environment, organization, job or sync-run identity;
- expected runtime owner and checked-in route;
- queue, worker class, and deployment profile;
- oldest eligible age and last successful progress;
- retry, lease, timeout, or terminal error;
- downstream Postgres or ClickHouse effect;
- health/readiness category and source revision.

Do not record task payloads, encoded arguments, DSNs, tokens, or customer-sensitive data in an ordinary incident channel.

## Determine what failed

| Symptom | Likely boundary |
| --- | --- |
| No job or sync unit exists | API, scheduler, entitlement, planning, or producer path |
| Job exists but queue is empty | Routing, broker, outbox, or dispatch failure |
| Queue grows with no consumers | Missing worker class or mismatched queue list |
| Workers are alive but jobs do not complete | Dependency, handler, lease, timeout, downstream write, or contract failure |
| Repeated retries with no progress | Persistent auth/schema/data error or unsafe retry amplification |
| Go `/healthz` succeeds but `/readyz` fails | Role, DSN mode, registry, handler, schema, or queue-control incompatibility |
| Go profile is healthy but no work arrives | Expected while profile is disabled and route remains Celery-owned |
| Operator command reports `outcome_unknown` | Database commit ambiguity; inspect before retrying |
| Operator command reports `audit_pending` | Mutation committed; audit finalization needs recovery |

## Active Celery recovery

1. Confirm each configured queue has an intended deployed consumer.
2. Confirm provider-specific and cost-class routing settings match worker queue lists.
3. Check broker/result-backend connectivity and worker heartbeats.
4. Inspect queue depth, oldest age, leases, retries, and terminal failures.
5. Stop unsafe retry amplification before increasing concurrency.
6. Recover one bounded job.
7. Verify the domain run, downstream writes, and product freshness.
8. Restore normal concurrency only after oldest age and failure rate decline.

Do not increase worker count when the provider budget, database, queue, or downstream store is the bottleneck.

## Go coexistence recovery

A Go process may be deployed for health or parity evidence without owning production work. First inspect the checked-in job migration state and deployment profile.

For readiness failure, distinguish:

- domain PostgreSQL role or connectivity;
- direct River queue-control DSN or unsupported pooling mode;
- role-name mismatch or excessive privileges;
- job registry/profile drift;
- missing compiled handler for an admitted contract version;
- incompatible River schema;
- dependency sampling failure.

Long-running Go processes must not receive `MIGRATION_DATABASE_URI`. Correct schema through the one-shot migration process, then restart the affected process and verify readiness.

## Outbox and reconciler recovery

For the generic worker outbox:

1. Confirm the producer committed a route-executable intent.
2. Confirm known Celery-routed rows were not claimed for River.
3. Inspect reconciler readiness and the last successful loop step.
4. Distinguish a transient persistence failure from an invalid job kind or route.
5. Preserve deferred or terminal rows for audit; do not silently republish them to Celery.
6. After correction, process one bounded row and verify a single domain effect.

## Scheduler failure

Celery Beat remains the active production scheduler. Verify it is running exactly once and that due work is persisted and dispatched.

The Go scheduler foundation currently evaluates bounded schedule timing for comparison and does not own production publication. Unsupported cron grammar must remain unsupported rather than guessed.

## Retry safety

Before any retry or replay:

- determine whether the domain effect may already have committed;
- inspect idempotency and durable deduplication state;
- preserve the original correlation ID and reason;
- account for provider rate and cost budgets;
- verify revoked credentials or bindings cannot write;
- use the supported operator transition rather than direct database mutation.

Escalate when job identity, route ownership, tenant isolation, migration compatibility, data corruption, or repeated process loss is uncertain.
