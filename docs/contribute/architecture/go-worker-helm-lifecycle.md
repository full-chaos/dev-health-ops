---
page_id: con-go-worker-helm-lifecycle
summary: How the Helm chart brings a database to the state the Go worker fleet requires — three ordered migration steps, which credential each one reads, and why one environment variable decides where the River step runs.
content_type: architecture
owner: engineering
source_of_truth:
  - deploy/helm/dev-health/templates/migrate-job.yaml (the weight-0 Alembic and ClickHouse Job)
  - deploy/helm/dev-health/templates/river-hooks.yaml (the weight-5 and weight-10 hooks)
  - deploy/helm/dev-health/templates/go-worker-pdb.yaml (per-group disruption budgets)
  - src/dev_health_ops/migrate.py (which invocation runs the River migrator, and when)
  - internal/storage/river/migrate.go (role preflight and the runtime grant posture)
  - deploy/docker-compose/compose.go-workers.yml (the Compose arrangement the chart mirrors)
applicability: current
lifecycle: active
---

# Go worker migration and drain lifecycle

A database the Go worker fleet can run against needs three things applied in one
order: the application schema, the three runtime logins, and the River schema
with its grants. The chart applies all three as Helm hooks, and almost every
decision on this page follows from the fact that the order is load-bearing
rather than conventional.
{: .fc-page-lede }

This page describes boundaries an implementer has to respect: which step applies
what, which credential each step is allowed to read, and the one environment
variable that silently moves the River migration from one step to another. For
what a worker does once it is running, read
[Go worker runtime architecture](go-worker-runtime.md). For starting and
configuring workers, read
[Run workers and jobs](../../operate/run/workers-and-jobs.md).

## Three ordered steps

| Hook weight | Job | Applies |
| --- | --- | --- |
| `0` | `…-migrate` | Alembic (application schema) and the ClickHouse migrator |
| `5` | `…-provision-roles` | the three runtime logins, via `provision_river_roles.sql` |
| `10` | `…-river-migrate` | the pinned River schema and the full runtime grant posture, then re-checks it |

The order is not interchangeable. Role provisioning grants against tables that
Alembic creates, so it cannot precede the application schema. The River migrator
refuses to run until the domain and queue roles exist and can log in — its
preflight reads `pg_catalog.pg_roles` and requires `rolcanlogin` for each, so it
cannot precede provisioning. Get it wrong and every worker fails readiness with
`failed_checks:"domain_postgres"`, which names the symptom and not the cause.

This is the same ordering Compose runs: `go-river-provision` declares
`depends_on: migrate`, and `go-river-migrate` declares
`depends_on: go-river-provision`. The two hooks are off by default
(`migrations.hook.provisionRoles.enabled`, `migrations.hook.riverMigrate.enabled`),
so a deployment that has not opted in is unchanged.

Both hooks run on **`pre-install` and `pre-upgrade`**. Running provisioning only
on install would leave grant drift unrepaired, and re-running it is safe — see
[Provisioning cannot revoke what the migrator grants](#provisioning-cannot-revoke-what-the-migrator-grants).

## One DSN, two variable names

`dev-hops migrate postgres` does not only run Alembic. When
`MIGRATION_DATABASE_URI` (or `MIGRATION_DATABASE_URI_FILE`) is set, it also runs
`dev-health-worker-migrate` itself, as a second fail-closed step.

The test is presence, not truthiness: `migrate.py` checks `is not None`, so
`MIGRATION_DATABASE_URI=""` still runs the migrator. **Absent and empty are
different**, which is why Compose's migrate entrypoint *unsets* an empty value
rather than passing it through, and why an empty-string override cannot be used
to suppress the step.

That matters because the weight-0 Job runs before provisioning. If the elevated
DSN reaches it on a fresh database, the River preflight fails there — before
anything has created the roles it requires — and Helm aborts the release.

Compose avoids this by never letting the two DSNs share a variable: its
`migrate` service reads `MIGRATION_DATABASE_URI`, while the River step reads
`GO_WORKER_MIGRATION_DATABASE_URI`. The chart keeps the same separation:

- **Chart-owned credentials** (`secrets.create=true`): the migration Secret
  carries the DSN as `POSTGRES_URI`, and the River hook gets its own Secret
  carrying `MIGRATION_DATABASE_URI`. `get_postgres_uri()` normalises the value to
  the async driver form, so Alembic is unaffected by which dialect it was written
  in.
- **Operator-owned credentials** (`secrets.create=false`): one Secret serves both
  Jobs and the chart cannot restructure its keys, so the weight-0 entrypoint
  unsets the variable and passes the DSN to Alembic as `--db` instead. This is
  what Compose and the raw Kubernetes manifests already do.

### What `riverMigrate.enabled=false` means

Turning the hook off is not "River does not happen". There are two paths, and
the Secret's contents decide which one a deployment is on:

| `riverMigrate.enabled` | Elevated DSN configured | What applies the River schema |
| --- | --- | --- |
| `true` | yes | the weight-10 hook, after provisioning — the only ordering a fresh install survives |
| `false` | yes | `dev-hops migrate postgres` itself, inside the weight-0 Job. Compose-equivalent, and safe only where the roles already exist — an upgrade, never a fresh install |
| `false` | no | nothing. The operator runs `dev-health-worker-migrate` out of band |

## Which credential each step reads

Helm creates a chart's regular resources **after** its `pre-install` hooks, so a
hook cannot mount a Secret the same release is about to create — the pod waits in
`CreateContainerConfigError` until the release times out. Every hook here
therefore reads either a hook-scoped copy or a Secret that exists independently
of the release:

- when the chart owns the credentials, a copy is rendered at hook weight `-5`,
  ahead of the Jobs that mount it;
- when the operator owns them, the external Secret is referenced directly.

The render fails closed when neither is configured. Role passwords reach `psql`
as environment values referenced by `--set` arguments, never inlined into the
Job's command: an inlined value would sit in the Job spec and in every
`helm get manifest` for the life of the release.

## Connecting to PostgreSQL

Every step in this chain runs raw DDL, so all of them must reach PostgreSQL
**directly on 5432** — never through a transaction-mode pooler.

Provisioning connects by parts when the Secret supplies them
(`POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_DB`, with `PGPASSWORD` out of band),
which is what Compose's `go-river-provision` does. Parts avoid the question of
which URL dialect a configured value is written in. When only a DSN exists, it is
selected in the same order the migrate Job uses and its scheme is normalised —
`postgresql+asyncpg://` is a SQLAlchemy URL that libpq cannot consume. With
neither, the Job exits non-zero rather than falling through to a local socket
connection as the container user.

## Failure policy

The hooks set `helm.sh/hook-delete-policy: before-hook-creation` and deliberately
**not** `hook-succeeded`. A failed migration's pod has to survive for its logs;
that retention is how a missing table grant was diagnosed rather than guessed.
Helm aborts the release on any hook failure, so a half-migrated database can
never get workers pointed at it.

## Provisioning cannot revoke what the migrator grants

Re-running provisioning on every upgrade is safe, and this is worth stating
explicitly because the opposite was believed for a time and shaped an earlier
design:

- `provision_river_roles.sql` is bootstrap-only. The only `REVOKE`s left are
  `TEMPORARY` on the database and `CREATE` on schema `public`; the per-table
  whitelist that once caused an incident is gone (CHAOS-4261).
- `applyRuntimeGrants` runs unconditionally after the migrator, before the
  applied-versions list is even built. "0 pending migrations" still re-applies
  the full declared posture.

Observed across three consecutive production passes: 116 grants, then 119 (a new
table), then 119 again — no revoke at any point.

## Cutover authorisation

`dev-hops migrate postgres` deliberately defers the Celery-to-River route-table
cutover revision unless `DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1`: without it the
upgrade targets the application-schema branch head instead of every head. The
chart carries the variable on the weight-0 Job as an optional Secret key,
defaulting to unset — the same operator passthrough Compose ships.

## Drain and rollout safety

Three separate properties, each enforced at render time so a misconfiguration
fails before it reaches a cluster:

- **A grace-period floor for any group carrying the `metrics` queue.** The value
  becomes `--shutdown-timeout`, and a metrics job's registry timeout is 7200s;
  below the floor the kubelet kills a pod that still holds a claimed job. Worth
  failing the render, because the diagnosis an operator otherwise gets names the
  queue-coverage check rather than the grace period.
- **A PodDisruptionBudget per group that can run two pods.** The gate is the
  group's ceiling — the greater of its static `replicas` and its HPA
  `maxReplicas` — not the static count alone: every group ships at `replicas: 0`
  so a fresh install stays inert, and the autoscaled ones reach two pods under
  load. Groups capped at one pod get no budget on purpose, because
  `maxUnavailable: 1` over a single replica permits zero evictions and blocks
  `kubectl drain` indefinitely. The budgets use `maxUnavailable` rather than
  `minAvailable` so they keep meaning the same thing when an HPA moves the
  replica count underneath them.
- **`maxUnavailable: 0` with `maxSurge: 1`** on the request-serving Deployments,
  stated rather than inherited. Kubernetes' 25% default happens to round to zero
  at one replica; that is arithmetic, not a guarantee, and it stops holding as
  the replica count grows.

Coordinator-role groups (the reconciler and the scheduler) are singletons by
design — the chart fails closed if an active scheduler is replicated — so they
carry no autoscaler and never render a budget.
