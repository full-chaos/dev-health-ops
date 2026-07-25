# CHAOS-3033: coordinator pool activation (and the CHAOS-3113 fix)

Runtime half of the Option B PostgreSQL role split. The posture half — the
`domain`/`coordinator` partition itself, `CheckRolePosture`, and the two
readiness entry points — landed earlier on `lane/domain-grant-reconciliation`.
This document covers what makes that partition *executable*: a third runtime
pool, its configuration, its grants, and the call sites repointed onto it.

Authoritative table/privilege attribution stays in
`.github/docs-legacy/architecture/chaos-3033-role-partition-manifest.md`. Where that document
and the Go postures disagree, the Go postures
(`internal/storage/postgres/domain_authorization.go`) are what the code is built
against; disagreements are listed under "Known disagreements" below.

## Why this was needed

`coordinatorPosture()` declared 16 tables, but nothing granted them and nothing
connected as the coordinator role. Every binary connected only as the domain
role. Meanwhile the domain role's grants had been correctly narrowed, removing
the coordinator-exclusive tables. The result was a set of latent `42501`
failures on real code paths — including one, CHAOS-3113, where the failure is a
`LOCK TABLE` that PostgreSQL refuses because `SHARE ROW EXCLUSIVE` implies
`UPDATE` and the domain role holds only `SELECT`+`INSERT`.

These are measured, not inferred:
`internal/storage/postgres/coordinator_statement_privileges_integration_test.go`
executes 11 real production statement shapes as the real restricted role, with
grants applied by `ApplyPinnedMigrations`, and asserts `42501` on each.

## What changed

| Area | File | Change |
| --- | --- | --- |
| Third pool | `internal/storage/postgres/runtime.go` | `RuntimePools.Coordinator`, `CoordinatorPool()`, `RuntimeConfig.{CoordinatorURI,CoordinatorRole,CoordinatorMaxConns,RequireCoordinator}`, `WithCoordinator()` |
| Config | `internal/platform/config/config.go` | `COORDINATOR_DATABASE_URI`, `RIVER_COORDINATOR_DATABASE_ROLE`, `WORKER_COORDINATOR_DATABASE_MAX_CONNS` |
| Grants | `internal/storage/river/migrate.go` | `coordinatorGrantStatements`, `MigrationOptions.{CoordinatorRole,CoordinatorGrants}`, coordinator arm of the role preflight |
| Grant source | `cmd/dev-health-worker-migrate/main.go` | derives `CoordinatorGrants` from `postgres.CoordinatorPosture()` |
| Role creation | `docker/init-extra-dbs.sh`, `scripts/worker/provision_river_roles.sql` | create the coordinator login; drop the now-stale domain `worker_job_routes` grant |
| Repoint | `cmd/dev-health-workerctl/main.go`, `cmd/dev-health-reconciler/dependencies.go` | coordinator call sites moved off the domain pool |
| Contract | `internal/deploymentcontract/manifest.go`, `deploy/go-workers/profiles.json` | a declared coordinator budget now requires the coordinator DSN |

### Fail closed, never fall back

The coordinator pool is **opt-in per binary** (`RequireCoordinator`, set via
`WithCoordinator()`) and **has no fallback**. `CoordinatorPool()` returns
`ErrUnavailable` rather than the domain pool. A fallback would silently
re-introduce CHAOS-3113 and make the split decorative. Domain-only binaries
(`dev-health-worker`, `dev-health-stream-runner`) are unaffected and need no
coordinator DSN; every coordinator validation rule is inert for them.

The coordinator connection is modeled as a **direct, server-counted**
connection, not PgBouncer-pooled — it holds `SHARE ROW EXCLUSIVE` table locks
and `FOR UPDATE` row locks across statements in one transaction. Its bound is
therefore 1..4 (the queue-control ceiling), not the domain role's 16, and
pointing it at a transaction-mode pooler endpoint is rejected
(`ErrCoordinatorTransactionMode`).

### One grant list, not two

`internal/storage/river` cannot import `internal/storage/postgres` — that
package's own tests import river, so the reverse production import is an import
cycle. The coordinator grant set is therefore **injected** via
`MigrationOptions.CoordinatorGrants` and derived by the migration command from
`postgres.CoordinatorPosture()`, the same declaration
`CheckCoordinatorAuthorization` asserts against. A second hand-maintained list
in the migration is exactly the drift that previously let the domain grants and
the domain assertion disagree. A role without grants, or grants without a role,
is rejected rather than half-applied.

### Where each pool is used

| Binary | Coordinator pool | Stays on domain | Stays on queue |
| --- | --- | --- | --- |
| `workerctl` | `joboperator.Authenticator` (`internal_service_credentials`), `joboperator.PostgresAuditor` (`worker_operator_audits`), `syncroute.Controller` (`sync_dispatch_transport_routes` UPDATE/FOR UPDATE), `jobroute.Controller` (`worker_job_routes` + the outbox LOCK) | `PostgresDomainGuard` (`SELECT true`, no relation), the operator advisory lock, the jobroute Celery quiescer (`sync_run_units`) | `NewDirectPostgresBackend`, the River quiescer |
| `reconciler` | `jobroute.NewController` in `buildReconcilerRelay` (always constructed), `syncreconciler.NewMaterializer` (dormant path) | `LeaseRepair`, `Kernel` observe side, `Observer`, `syncroute` **fence** (SELECT-only), `syncDispatchReference` | repository, River inserter, River quiescer, Kernel mutate side |
| `scheduler` | **not repointed — blocked on CHAOS-3114** | — | — |

`syncroute` appears in both columns because it is two different constructors:
`syncroute.NewController` mutates routes (coordinator), `syncroute.New` is the
read-only fence (domain).

Both repointed binaries additionally gate readiness on
`CheckCoordinatorAuthorization`. That is not redundant with the domain check:
cross-role attribution is a *distributed* property, so only the coordinator's
own check can catch a privilege wrongly granted to the coordinator login. The
reconciler exposes it as a separately named `coordinator_postgres` readiness
check so a coordinator privilege fault is attributable.

## Deploy prerequisites — ordered

The order matters. Steps 1–2 must complete in every environment **before** the
migration in step 3, and step 3 before step 4.

1. **Provision the coordinator role and its secret, per environment.**
   Run `scripts/worker/provision_river_roles.sql` with the owner/admin
   connection, supplying `coordinator_role` and `coordinator_password` (it
   prompts without echo if omitted). It is idempotent. The role must be a
   least-privilege login: `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
   NOREPLICATION NOBYPASSRLS`. It must **own nothing** — ownership carries every
   grant option regardless of what is revoked, and readiness fails closed on an
   "owns nothing" predicate.
   Local dev and CI need nothing: `docker/init-extra-dbs.sh` creates it on first
   init.

2. **Publish `COORDINATOR_DATABASE_URI` as a secret** to `dev-health-workerctl`,
   `dev-health-reconciler`, and `dev-health-scheduler`. Its DSN user **must**
   equal `RIVER_COORDINATOR_DATABASE_ROLE` — validation rejects a mismatch,
   because otherwise readiness would check a different identity than the queries
   run as. It must point at the **direct** PostgreSQL endpoint, not the
   transaction-mode PgBouncer endpoint.

3. **Run the migration.** `dev-health-worker-migrate` now grants the coordinator
   role in the same privilege transaction as the domain and queue roles. Its
   preflight **fails closed** if the coordinator role does not yet exist as an
   eligible least-privilege login, or if the migration identity *is* the
   coordinator role. A migration run before step 1 fails loudly rather than
   granting nothing and reporting success.

4. **Deploy the binaries.** `workerctl` and the reconciler require the
   coordinator DSN at startup and will refuse to start without it
   (`ErrCoordinatorDatabaseRequired` / `configuration_error`). This is
   deliberate — see "Fail closed" above.

### Environment variables

| Name | Kind | Default | Notes |
| --- | --- | --- | --- |
| `COORDINATOR_DATABASE_URI` | secret | none | Optional in `platform/config` so domain-only workers start without it; **required** by the binaries that call `WithCoordinator()`. Direct endpoint only. |
| `RIVER_COORDINATOR_DATABASE_ROLE` | config | `devhealth_coordinator` | Must be distinct from the domain and queue roles — checked unconditionally, even on processes that never open the pool. |
| `WORKER_COORDINATOR_DATABASE_MAX_CONNS` | config | `2` | Bounded 1..4, mirroring `deploymentcontract`'s `CoordinatorMaxConnections`. |

### Connection budget

Unchanged at **93 / 100** server connections. The coordinator budget was
already modeled by `deploymentcontract`
(`BudgetSummary.DirectCoordinatorConnections`) before this change: reconciler
2×2, scheduler 2×2, `workerctl` 1×2 = 10 direct coordinator connections. This
change added only environment wiring, no connections, so
`TestManifestRejectsConnectionBudgetOverflow` is unaffected.

## What remains

- **CHAOS-3114 — `dev-health-scheduler` cannot be repointed as-is.** Its
  fixed-schedule `Engine.runOccurrence` commits, in **one** transaction,
  `fixed_schedule_occurrences` (coordinator-exclusive) together with
  `remaining_metric_runs`, `remaining_metric_partitions`, and
  `work_graph_execution_requests`/`_ledger` (domain-exclusive). No single role
  can serve that transaction under the declared postures, and the atomicity is
  deliberate. Resolving it means either splitting the commit (outbox-style) or
  granting an explicit exception — a design decision, not a pool swap. The
  binary is dormant today (`checkedInSchedulerActivation` is the zero value, so
  it opens no pool at all), so nothing regresses by leaving it. Its coordinator
  call sites, for when this is unblocked: `schedulersync.NewMutationRepository`
  (`scheduled_jobs` FOR UPDATE + UPDATE), `schedulersync.NewOccurrenceCoordinator`
  (`scheduled_sync_occurrences`), `schedulersync.NewOccurrenceReconciler`
  (`scheduled_sync_occurrences`, `scheduled_jobs`), and the mixed
  `schedulerfixed.NewEngine`.
- **Reconciler mutation pipeline** is wired correctly but dormant
  (`checkedInReconcilerActivation.syncMutation` is false). It is repointed now so
  flipping that flag cannot ship the same defect.

## Known disagreements with the manifest

- `worker_job_routes` is attributed coordinator-exclusive, and the domain role
  legitimately has no SQL site of its own for it — but the **reconciler's relay**
  read it on the domain pool on every step. That is resolved here by treating the
  reconciler as a coordinator binary (repointing the relay's route resolver),
  not by re-granting the table to the domain role. If the reconciler were ever
  reclassified as domain-only, `worker_job_routes` would have to become
  dual-granted instead.
- `fixed_schedule_occurrences` is attributed coordinator-exclusive, but its only
  writer shares a transaction with domain-exclusive tables. See CHAOS-3114.
- `docker/init-extra-dbs.sh` and `scripts/worker/provision_river_roles.sql` still
  carry their own partial domain grant lists, which predate the posture split and
  are superseded by the migration's `REVOKE ALL` + selective re-grant. Their
  stale `worker_job_routes` entry is removed here because it directly
  contradicted `domainPosture()` and would have failed domain readiness had
  provisioning run *after* a migration. The remaining overlap is harmless but
  would be better deleted once the migration is confirmed as the sole owner.
