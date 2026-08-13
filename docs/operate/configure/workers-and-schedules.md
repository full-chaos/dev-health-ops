---
page_id: op-workers-config
summary: Configure active Celery queues and schedules while keeping dormant Go worker profiles, River routes, database roles, and rollback controls explicit.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - deploy/go-workers/profiles.json
  - contracts/jobs/v1/
  - contracts/sync-dispatch/v1/
  - src/dev_health_ops/alembic/versions/0096_enforce_unique_saved_report_schedule.py
  - src/dev_health_ops/alembic/versions/0097_backfill_report_schedule_next_run.py
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
- are used for contract, health, parity, and deployment-shape validation, not production job ownership.

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

The Ask Dev expiry repair (`prune_ask_dev_conversations`) was built in Go
first, with no Celery predecessor. CHAOS-3404 has since added the Beat entry
`ask-dev-retention-sweep` for the same work at the same cadence, so it is now a
migrated schedule like the rest: the Go schedule owns that Beat entry in the
legacy inventory.

**Do not delete the Beat entry yet, and do not read "owned by Go" as "running
in Go".** `prune_ask_dev_conversations` pins contract version 3, while
`migration-state.json` declares `system.retention_cleanup` at
`producer_version` 2. The producer therefore skips every occurrence with
`consumer_version_incompatible`, and the scheduler records that as an ordinary
skipped occurrence rather than a failure -- so the Go side currently publishes
no retention job at all and looks healthy doing it. Until `producer_version`
reaches 3, the Celery sweep is the only thing purging expired conversations.
Once it does, the two must not both run. A second gap closes with the same
work: Go's drain treats a short batch as a completed drain, where Python runs a
non-locking `count_expired()` and reports `partial` -- a `SKIP LOCKED` short
read cannot distinguish a drained backlog from a contended one. Both are
tracked as CHAOS-3481. Its staged v3 contract uses
the table-scoped `ask_dev_conversations` policy at 05:30 UTC. Its cutoff is the occurrence time
because each conversation already persists its exact expiry; adding an
environment retention horizon would incorrectly extend or shorten the user's
0/30-day choice.

The v3 schema and ops consumer land before emission. The active retention
`producer_version` remains 2 until capability reports from every live ops
consumer prove v3 compatibility; the scheduler records an explicit
compatibility skip and cannot emit the v3 envelope before that cutover. After
activation, admission uses the canonical default-disabled `ask_dev` decision
for a never-used installation, but persisted conversation state keeps cleanup
eligible even when Ask Dev is later disabled. Feature rollback stops new use;
it never suspends expiry, deletion, or purge obligations.

## Validate the configuration

- each emitted queue has an intended consumer;
- current routes still match the deployed runtime owner;
- worker readiness is open only for profiles that can execute every admitted job;
- scheduler ownership is singular and observable;
- queue depth and oldest age advance under a bounded job;
- retries preserve idempotency and provider budgets;
- rollback restores Celery routing before Go processes are drained or stopped.
