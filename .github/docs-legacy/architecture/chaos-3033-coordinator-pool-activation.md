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
| Grants | `internal/storage/river/migrate.go` | `coordinatorGrantStatements`, `MigrationOptions.{CoordinatorRole,CoordinatorGrants,CoordinatorColumnGrants}`, `ColumnGrant`, coordinator arm of the role preflight |
| Grant source | `cmd/dev-health-worker-migrate/main.go` | derives both grant halves from `postgres.CoordinatorPosture()` |
| Repoint (CHAOS-3114) | `cmd/dev-health-scheduler/dependencies.go` | the sync repository, occurrence reconciler **and** fixed-schedule engine moved onto the coordinator pool |
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
`MigrationOptions.CoordinatorGrants` (table-wide) and
`MigrationOptions.CoordinatorColumnGrants` (column-scoped, added by
CHAOS-3114), both derived by the migration command from
`postgres.CoordinatorPosture()`, the same declaration
`CheckCoordinatorAuthorization` asserts against. A second hand-maintained list
in the migration is exactly the drift that previously let the domain grants and
the domain assertion disagree. A role without grants, or grants without a role,
is rejected rather than half-applied, as is a relation granted both table-wide
and column-scoped — the readiness check refuses that combination outright, so
provisioning it would produce a role that can never report ready.

### Where each pool is used

| Binary | Coordinator pool | Stays on domain | Stays on queue |
| --- | --- | --- | --- |
| `workerctl` | `joboperator.Authenticator` (`internal_service_credentials`), `joboperator.PostgresAuditor` (`worker_operator_audits`), `syncroute.Controller` (`sync_dispatch_transport_routes` UPDATE/FOR UPDATE), `jobroute.Controller` (`worker_job_routes` + the outbox LOCK) | `PostgresDomainGuard` (`SELECT true`, no relation), the operator advisory lock, the jobroute Celery quiescer (`sync_run_units`) | `NewDirectPostgresBackend`, the River quiescer |
| `reconciler` | `jobroute.NewController` in `buildReconcilerRelay` (always constructed), `syncreconciler.NewMaterializer` (dormant path) | `LeaseRepair`, `Kernel` observe side, `Observer`, `syncroute` **fence** (SELECT-only), `syncDispatchReference` | repository, River inserter, River quiescer, Kernel mutate side |
| `scheduler` | `schedulersync.NewMutationRepository` (`sync_configurations`/`scheduled_jobs` `FOR UPDATE OF config, job` + UPDATE), `schedulersync.NewOccurrenceCoordinator` and `NewOccurrenceReconciler` (`scheduled_sync_occurrences`), **and `schedulerfixed.NewEngine`** — the whole `runOccurrence` transaction, `fixed_schedule_occurrences` together with the domain rows its producers materialize (CHAOS-3114) | nothing composes on it; the pool is opened and gated by `domain_postgres` readiness only, so the process's own domain login is still proven to hold exactly `domainPosture` | — |

`syncroute` appears in both columns because it is two different constructors:
`syncroute.NewController` mutates routes (coordinator), `syncroute.New` is the
read-only fence (domain).

Every repointed binary additionally gates readiness on
`CheckCoordinatorAuthorization`. That is not redundant with the domain check:
cross-role attribution is a *distributed* property, so only the coordinator's
own check can catch a privilege wrongly granted to the coordinator login. The
reconciler and the scheduler each expose it as a separately named
`coordinator_postgres` readiness check so a coordinator privilege fault is
attributable.

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

Unchanged at **93 / 100** server connections, before and after CHAOS-3114. The
coordinator budget was already modeled by `deploymentcontract`
(`BudgetSummary.DirectCoordinatorConnections`): reconciler 2×2, scheduler 2×2,
`workerctl` 1×2 = 10 direct coordinator connections. The footprint is
`server_reserved` 15 + PgBouncer 25×2 + direct queue-control 18 + direct
coordinator 10 = 93. Repointing call sites moves demand between pools that are
already budgeted; it creates no new connection, so
`TestManifestRejectsConnectionBudgetOverflow` is unaffected by either half.

Within the scheduler's own `coordinator_max_connections: 2`, both loops are
single-transaction-at-a-time and run on one goroutine each: `schedulersync.Loop`
hands off and then reconciles **sequentially** in one step (and
`dueOccurrenceKeys` drains its rows before `reconcileOne` begins a
transaction), and `schedulerfixed.Engine` reads its anchor, rolls that read
back, and only then opens the occurrence transaction. Peak concurrent demand is
therefore one connection per loop — exactly 2.

## What remains

- **Reconciler mutation pipeline** is wired correctly but dormant
  (`checkedInReconcilerActivation.syncMutation` is false). It is repointed now so
  flipping that flag cannot ship the same defect.
- **The occurrence materializer (CUT-09/CUT-10)** is the sole remaining
  precondition on `checkedInSchedulerActivation.goOwnsMarkers`.
  `productionSchedulerRuntimeSources.newOccurrences` still composes
  `schedulersync.NewUnavailableMaterializer()`, so activating today would
  durably record occurrences Go can never materialize. The binary stays dormant
  and opens no pool at all. No privilege precondition remains — see below.
- **Two fixed schedules are still owned on paper only** (`daily_metrics_fanout`,
  `scheduled_metrics_dispatch`), and `scheduled_reports_dispatch` — built since
  this document was written — adds three tables to `runOccurrence`'s statement
  set that `coordinatorPosture()` does not yet declare, so it IS a live privilege
  precondition despite the line above. See
  [fixed-schedule-producers.md](fixed-schedule-producers.md) for the per-schedule
  state, the three held grant rows and why they are held, and the five statement
  shapes the coordinator statement-privilege test does not yet cover.

## CHAOS-3114 — both halves shipped

No privilege precondition remains on the scheduler. The sync-path call sites
(`schedulersync.NewMutationRepository`, `NewOccurrenceCoordinator`,
`NewOccurrenceReconciler`) moved to the coordinator pool first, removing the
42501 that `FOR UPDATE OF config, job SKIP LOCKED` would have raised on the
first real handoff — that clause needs UPDATE on the locked rows purely to take
the lock, even though the statement writes nothing else.

The second half moved `schedulerfixed.Engine.runOccurrence` onto the same pool.
It commits, in **one** transaction, `fixed_schedule_occurrences`
(coordinator-exclusive) together with the rows its producers materialize. The
resolution was to widen the COORDINATOR role to cover the whole transaction:

- The property the partition protects is that provider-sync workers — the code
  that handles third-party payloads, and the code that runs as the **domain**
  login — cannot reach control-plane tables (routes, credentials, audits,
  schedule markers). Widening the coordinator does not weaken that. Widening the
  domain role, by granting it `fixed_schedule_occurrences`, would destroy it. The
  coordinator login is used only by the scheduler, the reconciler, and
  `workerctl`.
- The atomicity is deliberate and load-bearing: it is what makes "occurrence
  recorded ⇒ work enqueued" exactly-once. Splitting it outbox-style would let a
  fixed schedule double-run or silently skip.
- Celery Beat, the thing this replaces, already runs as ONE identity spanning
  both table sets. The Go cutover is not blocked on a privilege model stricter
  than its predecessor.

The grant set is the **verified statement trace** of `runOccurrence`, not the
domain role's flags copied across. `coordinatorPosture()` gained:

| Table | Coordinator privileges | Why |
| --- | --- | --- |
| `remaining_metric_runs` | SELECT, INSERT | `remaining.StartRunTx` inserts; `loadStartedRun` re-reads on the replay arm. **No UPDATE** — every status transition is handler-side, on the domain role. |
| `remaining_metric_partitions` | SELECT, INSERT | same `StartRunTx`, plus `verifyStartedPartitions`. No UPDATE, same reason. |
| `work_graph_execution_requests` | SELECT, INSERT | `workgraph.RequestWriter.WriteTx` inserts and re-reads. No UPDATE: `Claim`/`transition` are handler-side. |
| `worker_job_outbox` | **+ INSERT** | `joboutbox.Producer.publish`, reached from every producer handoff. UPDATE was already required by the `jobroute` LOCK. |
| `worker_job_completion_fences` | `completion_key` SELECT + INSERT, **column-scoped** | `joboutbox.MarkCompletionTx`, reached transitively from `StartRunTx` and `WriteTx` on their already-succeeded replay arms. |

Two corrections to what this document previously asserted, both found by tracing
the code rather than reading the manifest:

- **`work_graph_execution_ledger` is NOT in the transaction.** `WriteTx` never
  touches it; the ledger belongs to the handler-side `Claim`. The earlier bullet
  listing it as one of the commit's domain-exclusive tables was wrong, and it is
  deliberately absent from the coordinator posture.
- **`worker_job_completion_fences` IS reached from the fixed engine.**
  `domainPosture()`'s comment previously dismissed the manifest's "possible 2nd
  reacher" flag on this table as a false positive, on the strength of a grep
  across `internal/scheduler/fixed` for `MarkCompletionTx`. The grep was scoped
  to one package while the reach is transitive, through `StartRunTx` and
  `WriteTx`. The flag was real.

The fence grant stays **column-scoped** for the same reason it is on the domain
side: `completed_at` is server-owned, and a table-wide grant would let the
coordinator forge a fence retention never reaps. Supporting that required the
migration to learn column grants —
`MigrationOptions.CoordinatorColumnGrants`, derived by
`cmd/dev-health-worker-migrate` from `CoordinatorPosture().ColumnScoped`, the
same single-declaration route the table grants already take. A relation may be
granted table-wide or column-scoped, never both; the migration rejects the
combination, because the readiness check refuses it outright.

`SELECT` on `completion_key` is required as well as `INSERT`, and not because
anything reads the row: `INSERT ... ON CONFLICT (completion_key) DO NOTHING`
names an arbiter, and PostgreSQL refuses the statement with 42501 when only
`INSERT (completion_key)` is held. Verified empirically against a live server,
both directions. This is the same class of trap as the LOCK-implied UPDATE:
invisible to any verb-based reading of the code.

All of it is measured, not inferred:
`internal/storage/postgres/fixed_engine_statement_privileges_integration_test.go`
runs the 14 real `runOccurrence` statement shapes as the real coordinator role
with grants applied by `ApplyPinnedMigrations`, and its mutation half restores
the pre-3114 grant set and demands 42501 on each of the eight statements this
change unblocked, plus a closed readiness check.

`coordinatorPolicyParity` was **deleted**, not left false. It was a bare bool
with no test, checklist, or document anywhere defining what it would prove, so
it could not honestly gate anything. `goOwnsMarkers` is now the sole composition
gate, and it stays false.

## Known disagreements with the manifest

- `worker_job_routes` is attributed coordinator-exclusive, and the domain role
  legitimately has no SQL site of its own for it — but the **reconciler's relay**
  read it on the domain pool on every step. That is resolved here by treating the
  reconciler as a coordinator binary (repointing the relay's route resolver),
  not by re-granting the table to the domain role. If the reconciler were ever
  reclassified as domain-only, `worker_job_routes` would have to become
  dual-granted instead.
- `fixed_schedule_occurrences` is attributed coordinator-exclusive, and its only
  writer shares a transaction with tables the manifest attributes to the domain
  role. **Resolved** by CHAOS-3114: the coordinator role holds both sides and
  the whole engine runs as coordinator, so the attribution stands as written —
  `fixed_schedule_occurrences` was never granted to the domain role and is not
  dual-granted. What the manifest now understates is the coordinator side of
  `remaining_metric_runs`, `remaining_metric_partitions`,
  `work_graph_execution_requests`, `worker_job_outbox` and
  `worker_job_completion_fences`: each is a dual-grant table, with the
  coordinator holding strictly less than the domain role on the first three (no
  UPDATE — the status transitions are handler-side). The Go postures are what
  the code is built against.
- `worker_job_completion_fences` carried a "possible 2nd reacher" flag the
  postures previously dismissed as a false positive. It was real; see CHAOS-3114
  above. The manifest's flag was right and the dismissal was wrong.
- `docker/init-extra-dbs.sh` and `scripts/worker/provision_river_roles.sql` still
  carry their own partial domain grant lists, which predate the posture split and
  are superseded by the migration's `REVOKE ALL` + selective re-grant. Their
  stale `worker_job_routes` entry is removed here because it directly
  contradicted `domainPosture()` and would have failed domain readiness had
  provisioning run *after* a migration. The remaining overlap is harmless but
  would be better deleted once the migration is confirmed as the sole owner.
