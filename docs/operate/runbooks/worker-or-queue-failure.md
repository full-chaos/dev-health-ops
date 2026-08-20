---
page_id: op-rb-worker
summary: Recover when Celery work stops advancing or a Go coexistence process, River queue, reconciler, schedule evaluator, or operator mutation fails.
content_type: runbook
owner: platform-operations
source_of_truth:
  - docs/operate/run/workers-and-jobs.md
  - deploy/go-workers/CUTOVER-RUNBOOK.md
  - docs/operate/configure/databases-and-storage.md
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
- queue, worker class, and deployment group;
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
| Go profile is healthy but no work arrives | Confirm the profile is enabled and inspect the durable job and sync-dispatch routes; a checked-in River route that was not applied remains on its prior database transport |
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

A Go process may be deployed for health or parity evidence without owning production work. First inspect the checked-in job migration state and deployment manifest.

For readiness failure, distinguish:

- domain PostgreSQL role or connectivity;
- direct River queue-control DSN or unsupported pooling mode;
- role-name mismatch or excessive privileges;
- job registry/profile drift;
- missing compiled handler for an admitted contract version;
- a queued contract version this build does not support. The `queued_contract_versions` refusal names the offending queue, kind, and version, so read that log line before inspecting the queue by hand. It counts only jobs in the `available` state;
- incompatible River schema;
- dependency sampling failure.

Long-running Go processes must not receive `MIGRATION_DATABASE_URI`. Correct schema through the one-shot migration process, then restart the affected process and verify readiness.

## Post-cutover rollback

Applies once migration `0066` has moved the checked-in worker job kinds to River. The full forward procedure, including its preconditions and per-kind verification, is `deploy/go-workers/CUTOVER-RUNBOOK.md`.

Roll back when a retargeted kind shows a growing worker outbox with no corresponding River execution, when oldest-available job age climbs past its threshold for a queue and does not recover, when a Go group cannot hold readiness, or when a retargeted kind fails terminally at a rate the pre-cutover baseline did not show.

Prefer per-kind rollback. It is audited and it proves quiescence: the route controller refuses to move a kind while its outbox holds pending or claimed rows, or while a run for it is still executing. A refusal means the kind is still working — let it finish rather than forcing it.

Wholesale downgrade of the cutover migration is plain SQL and proves nothing about quiescence. Stop the Go workers and the reconciler for every retargeted queue first, confirm no run is executing and no outbox row is pending or claimed, and only then downgrade and restart the Celery consumers, Beat, and producers. Stopping Go before routes move back is the inverse of draining Celery before they moved forward, and it is what keeps two runtimes from owning the same kind.

Confirm `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER` is unset afterwards. Left set, any later migration run re-applies the cutover unattended.

## Outbox and reconciler recovery

For the generic worker outbox:

1. Confirm the producer committed a route-executable intent.
2. Confirm known Celery-routed rows were not claimed for River.
3. Inspect reconciler readiness and the last successful loop step.
4. Distinguish a transient persistence failure from an invalid job kind or route.
5. Preserve deferred or terminal rows for audit; do not silently republish them to Celery.
6. After correction, process one bounded row and verify a single domain effect.

### Scheduled report delivery abandonment

The fixed scheduler reports a pending scheduled-report run whose handoff exhausted its delivery budget as:

```text
fixed_scheduler_schedule_degraded{schedule="scheduled_reports_dispatch",reason="scheduled_reports_undeliverable"} 1
```

This is a persistent, non-fatal schedule verdict. It may coexist with healthy reports from other organizations, and it survives terminal outbox retention. The metric describes the process's last evaluation, so confirm the durable database state rather than treating one scrape as the complete incident record.

Inspect both sides of retention without selecting job arguments or detailed errors:

```sql
SELECT run.id,
       run.report_id,
       handoff.status AS retained_outbox_status,
       abandonment.abandoned_at,
       abandonment.attempt_count,
       abandonment.last_error_code
FROM report_runs AS run
LEFT JOIN worker_job_outbox AS handoff
  ON handoff.dedupe_key = 'report.run:' || run.id::text
LEFT JOIN worker_job_delivery_abandonments AS abandonment
  ON abandonment.dedupe_key = 'report.run:' || run.id::text
WHERE run.status = 'pending'
  AND (handoff.status = 'dead' OR abandonment.dedupe_key IS NOT NULL);
```

Interpret the replay state as follows:

| Delivery state | Scheduler action |
| --- | --- |
| No outbox row and no abandonment fact | Treat as never published and re-arm the linked run. |
| Pending, claimed, or delivered outbox row | Leave the handoff with its current owner. |
| Dead outbox row or retained abandonment fact | Keep the run degraded and do not mint a fresh attempt budget. |

Do not delete or update the abandonment fact, reset the pending run, or republish the dedupe key to clear the metric. Preserve the run ID and the minimal evidence above, correct the underlying contract or dependency, and escalate for the reviewed repair or cancellation path.

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
