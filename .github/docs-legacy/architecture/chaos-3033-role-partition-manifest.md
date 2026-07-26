# CHAOS-3033 Option B grant-partition manifest (durable, single-sourced)

**Authority for role attribution: real Go SQL execution sites (this tool's
derivation + hand-verified file:line), never a Python-derived "this path
touches X" claim.** That rule was established this session after a
Python-derived retag round got 4 of 6 tables wrong (see "Corrections"
below) while every tag this tool derived from actual Go source held.

Source tree: `feat/go-default-cutover` @ `cd7aa8a8e`. Tool: `internal/domaingrants`
@ commit `5d0fe8626` on `lane/grant-surface-deriver`. A durable copy of this
same file also lives at `.remember/chaos-3033-grant-partition-manifest.md`
for discoverability outside the repo (that directory is not git-tracked;
this commit is the versioned source of truth). This revision supersedes the
first pass at `95c589722`, which predates the corrections below.

## Corrections made this round (verified against Go source, not relayed)

A follow-up round retagged 6 tables from a Python-derived source
("privilege-slice3c-budgetguard-rows.md" / "dispatch_sync_run session").
That source was itself unverifiable (see "Prior dispute" below for the full
trail — the cited file/branch doesn't exist, the underlying Go code has
zero database access by design, and the one commit message that could be
found concluded the opposite of what was relayed). A second round retracted
4 of those 6 retags after independent verification and asked me to confirm
the remaining 2. I checked all of it against `cd7aa8a8e` directly before
writing anything durable:

| Table | Retag round said | My tool said | Verified against `cd7aa8a8e` source | Verdict |
|---|---|---|---|---|
| `worker_job_routes` | coordinator (workerctl-only) | coordinator | `internal/jobroute/control.go`'s only two constructors are `cmd/dev-health-workerctl/main.go:210` AND `cmd/dev-health-reconciler/dependencies.go:273` (`jobroute.NewController(domainPool, registry, quiescer)`) — both coordinator-candidate binaries, no domain-hot-path site | **coordinator, confirmed** — refine attribution to "workerctl + reconciler," not workerctl-only |
| `provider_rate_limit_observations` | domain (ops retention) | domain | Only SQL site is `internal/jobs/system/retention_postgres.go`, ops profile | **domain, confirmed**, no coordinator split |
| `tier_limits` | out of scope | out of scope (zero Go refs) | `git grep tier_limits -- '*.go'` at `cd7aa8a8e`: zero hits | **confirmed out of scope** |
| `organizations` | asked me to confirm **coordinator-only** (scheduler) | domain (via streamhandlers) | **Both sites are real.** `internal/streamhandlers/external_postgres.go:60` (`JOIN organizations AS organization`, stream-* profiles, domain — same query as `org_licenses`/`org_feature_overrides`) AND `internal/scheduler/fixed/organizations.go` (`PostgresOrganizationLister.ActiveOrganizationIDs`, genuinely wired at `cmd/dev-health-scheduler/fixed.go:52`, scheduler, coordinator) | **NOT coordinator-only — tag as `both`.** The domain-side site was never in question; declining to confirm "coordinator-only" here, not silently accepting it. |
| `org_licenses` | asked me to resolve — "no SQL site turned up in my grep" | domain (via streamhandlers, same query as organizations) | `internal/streamhandlers/external_postgres.go:64`: `LEFT JOIN org_licenses AS license ON license.org_id = organization.id` — same query, same file, 4 lines below the `organizations` JOIN | **domain, confirmed present** — the grep that found nothing had a false negative, not evidence of absence |
| `sync_dispatch_transport_routes` | domain SELECT + coordinator SELECT+UPDATE (`both`) | had it as coordinator-only in the first manifest pass (my own compilation error — the transaction-grouping evidence already showed this, I didn't cross-reference it) | `internal/syncdispatchruntime/native_post_sync.go:160`: `JOIN public.sync_dispatch_transport_routes AS route` inside `currentPostSyncReference`, called from `Fanout` (sync profile, domain) — SELECT only from this side. Coordinator side (SELECT+UPDATE) unchanged from before, via `internal/syncroute/control.go`, workerctl+reconciler | **`both`, confirmed** — this one the retag round got right, and I'm correcting my own manifest to match, crediting the actual Go evidence (which my tool's own `Materializer`/Fanout transaction-grouping output already contained) rather than the claim itself |

Net: of 6 claims across the two rounds, the tool-and-verify approach held
on 4, corrected 1 real gap in my own manual compilation
(`sync_dispatch_transport_routes`), and caught 1 wrong "confirm this" ask
(`organizations` is not coordinator-only). `org_licenses` was a pure
resolve, not a dispute.

### Third round: `scheduled_*` shapes and `organizations` coordinator-side, resolved

A later message proposed exact privilege shapes for the 3 `scheduled_*`
tables (previously `unverified-shape`, blocked by the closure-typed
`schedulerRuntimeSources.newRepository`/`newOccurrences` fields) and asked
me to mark them `verified`. Rather than accept a second-hand shape
proposal directly, I independently re-derived each from `cd7aa8a8e` source:

- `scheduled_jobs`: `internal/scheduler/sync/transaction.go` has a claim
  query (`FOR UPDATE OF config, job SKIP LOCKED`) and `schedulerAdvanceMarkerSQL`
  (`UPDATE ... SET next_run_at = ...`). SELECT+UPDATE, no INSERT/DELETE
  anywhere — confirmed, matches the proposal exactly.
- `scheduled_sync_occurrences`: `internal/scheduler/sync/coordinator.go`
  (`INSERT ... ON CONFLICT DO NOTHING`, `SELECT ... FOR UPDATE`) +
  `occurrence_reconciler.go` (three `UPDATE` statements). SELECT+INSERT+UPDATE,
  no DELETE — confirmed, matches.
- `fixed_schedule_occurrences`: `internal/scheduler/fixed/ledger.go`
  (INSERT ON CONFLICT DO NOTHING, SELECT FOR UPDATE, an UPDATE, a plain
  SELECT). SELECT+INSERT+UPDATE, no DELETE — confirmed, matches.
- `organizations` coordinator-side: `internal/scheduler/fixed/organizations.go:91`,
  `activeOrganizationsSQL` is a plain `SELECT ... FROM public.organizations
  WHERE is_active = TRUE`, no lock clause. SELECT only — confirmed.

All four upgraded from `unverified-shape`/partial to `verified` in the
table below, with the source excerpt that confirms each. The wiring for
the `scheduled_jobs`/`scheduled_sync_occurrences` pair still goes through
the same closure-typed-field pattern my automated tool can't trace on its
own (`sources.newRepository(database.DomainPool())`,
`cmd/dev-health-scheduler/dependencies.go`) — that part of the original
flag stands: the tool's automated reachability trace didn't confirm this,
a direct source read did. `fixed_schedule_occurrences`/`organizations`'
coordinator side is wired through plain function calls
(`cmd/dev-health-scheduler/fixed.go`), no closure indirection, so that half
was always in principle traceable and the earlier `unverified-shape` tag
on `organizations`' coordinator side was more conservative than it needed
to be.

### Fourth round (CHAOS-3114): the sweep's package boundary was the blind spot

The closure-typed-field sweep below concluded "`sync_configurations` was the
only miss ... Nothing else to add." That conclusion was scoped to SQL written
**inside** `internal/scheduler/{sync,fixed}`, and the fixed-schedule engine's
transaction is not confined to those packages: `Engine.runOccurrence` hands its
`pgx.Tx` to producers that call into `internal/jobs/metrics/remaining`,
`internal/jobs/workgraph` and `internal/joboutbox`. A grep bounded by package
therefore cannot see the coordinator's full statement set — the same class of
error as the closure-typed field, one level up. The trace that closes it
follows the `tx` across package boundaries, not the file tree:

| table | role | privileges | confidence | evidence |
|---|---|---|---|---|
| remaining_metric_runs | **both** | coordinator: SELECT, INSERT | verified | `internal/jobs/metrics/remaining/postgres.go` `StartRunTx` (INSERT ... ON CONFLICT DO NOTHING) + `loadStartedRun` (replay read), reached from `internal/scheduler/fixed/producers.go` `RemainingMetricsFanoutProducer.Produce`. **No UPDATE**: every status transition is handler-side (domain). |
| remaining_metric_partitions | **both** | coordinator: SELECT, INSERT | verified | same `StartRunTx`, plus `verifyStartedPartitions`. No UPDATE, same reason. |
| work_graph_execution_requests | **both** | coordinator: SELECT, INSERT | verified | `internal/jobs/workgraph/publisher.go` `WriteTx` (INSERT ... ON CONFLICT (id) DO NOTHING + identity re-read), reached from `producers.go startGraphBuild`. No UPDATE: `Claim`/`transition` are handler-side. |
| worker_job_outbox | **both** (three roles) | coordinator: + INSERT | verified | `internal/joboutbox/producer.go` `publish`, reached from every fixed-schedule handoff. The coordinator's UPDATE was already recorded (LOCK-implied); INSERT is the addition. |
| worker_job_completion_fences | **both** | coordinator: `completion_key` SELECT + INSERT, column-scoped | verified | `internal/joboutbox/completion.go` `MarkCompletionTx`, reached transitively from `StartRunTx` and `WriteTx` on their already-succeeded replay arms. |

Two corrections to the rows below, both from reading the code rather than
relaying:

- The `worker_job_completion_fences | coordinator? | possible 2nd reacher,
  unresolved` row was **right**. A later revision of
  `internal/storage/postgres/domain_authorization.go` dismissed it as a false
  positive because a grep across `internal/scheduler/fixed` for
  `MarkCompletionTx` found nothing — but `producers.go:462` calling
  `joboutbox.CompletionKey` is not the reach; `StartRunTx`/`WriteTx` calling
  `MarkCompletionTx` is. The row is now `verified`, with a shape: SELECT as
  well as INSERT, because `INSERT ... ON CONFLICT (completion_key) DO NOTHING`
  needs SELECT on the arbiter column (verified empirically against a live
  server). It stays column-scoped — `completed_at` is server-owned.
- `work_graph_execution_ledger` is **domain-only** and is NOT part of the fixed
  engine's transaction. `WriteTx` never touches it; it belongs to the
  handler-side `Claim`. `.github/docs-legacy/architecture/chaos-3033-coordinator-pool-activation.md`
  previously listed it among the tables `runOccurrence` commits. It does not.

The dual-grant ("both") whitelist at the end of this document therefore grows
from 8 tables to 11: `remaining_metric_runs`, `remaining_metric_partitions` and
`work_graph_execution_requests` join it (`worker_job_outbox` was already on it).
`worker_job_completion_fences` is dual-held too but is not a table-wide
whitelist entry — `RolePosture.ColumnScoped` carries it for both roles. In each
of the three new entries the coordinator holds strictly LESS than the domain
role.

## Prior dispute (from commit `95c589722`, kept for the record)

Five earlier retags (`organizations`, `org_licenses`, `tier_limits`,
`worker_job_routes`, `sync_dispatch_transport_routes`, plus a
`provider_rate_limit_observations` role split) were relayed from "CUT-10's
`privilege-slice3c-budgetguard-rows.md`" and a "lane-provider-2." Before
applying them I checked: the cited file does not exist as a committed
artifact anywhere (`git log --all --diff-filter=A --name-only` finds no
commit adding it; it appears only as prose inside commit `6f95d7f87`'s
message); there is no `lane-provider-2` branch/worktree anywhere; the
underlying Go code (`budget_guard_plan.go` slice 3c, `dispatch_guard_plan.go`
slice 3a) has zero `.Exec`/`.Query` calls by explicit design ("no database,
no privileges" per their own commit messages) and is not reachable from
`feat/go-default-cutover` at all (`git merge-base --is-ancestor 6f95d7f87
cd7aa8a8e` fails); and that commit's own "Grant audit" summary concluded
the opposite of what was relayed to me ("dispatch_sync_run's entire session
is likely coordinator, not domain... not resolved unilaterally"). None of
the five were applied in `95c589722`. The corrections table above is the
resolution of that dispute, arrived at independently in a later round.

## Manifest

`confidence`: **verified** = directly confirmed `cfg.Profile != "X"` gate or
binary-exclusive wiring in Go source (this round or prior). **inferred** =
profile-level attribution from `deploy/go-workers/profiles.json` job_kinds
cross-referenced with evidence, not an automated scoped re-run.
**unverified-shape** = confirmed present in Go source but exact privilege
shape not independently derivable (closure-typed struct field blind spot,
documented in the handoff README) — grants-finisher2 must derive these by
hand, not grant blind.

| table | role | privileges | confidence | evidence |
|---|---|---|---|---|
| billing_notifications | domain | SELECT | verified | `internal/jobs/operational/postgres.go:61` (ops profile) |
| daily_metrics_partitions | domain | SELECT, INSERT, UPDATE | verified | `internal/jobs/metrics/daily/postgres.go` (heavy profile; also sync via Fanout) |
| daily_metrics_runs | domain | SELECT, INSERT, UPDATE | verified | same |
| external_ingest_batch_payloads | domain | SELECT | verified | `internal/streamhandlers/external_postgres.go:172` (stream-*) |
| external_ingest_batch_payloads | domain | DELETE (schema gap — no `allow_delete` column) | verified | `internal/streamhandlers/external_postgres.go:278,315` |
| external_ingest_batches | domain | SELECT, UPDATE | verified | `internal/streamhandlers/external_postgres.go`, `internal/externalrecompute/postgres.go` (stream-*) |
| external_ingest_batches | domain | DELETE (schema gap) | verified | `internal/jobs/system/retention_postgres.go` (ops) |
| external_ingest_recompute_jobs | domain | INSERT | verified | `internal/externalrecompute/postgres.go:75` (stream-*) |
| external_ingest_rejections | domain | INSERT | verified | `internal/streamhandlers/external_postgres.go:267` (stream-*) |
| external_ingest_sources | domain | SELECT | verified | `internal/streamhandlers/external_postgres.go:183` (stream-*) |
| feature_flags | domain | SELECT | verified | `internal/streamhandlers/external_postgres.go:58` (stream-*) |
| org_feature_overrides | domain | SELECT | verified | same query as organizations/org_licenses |
| org_licenses | domain | SELECT | verified | `internal/streamhandlers/external_postgres.go:64` |
| organizations | **both** | domain: SELECT | verified | `internal/streamhandlers/external_postgres.go:60` |
| organizations | **both** | coordinator: SELECT | verified | `internal/scheduler/fixed/organizations.go:91` (`activeOrganizationsSQL`: `SELECT id::text FROM public.organizations WHERE is_active = TRUE ...`), wired at `cmd/dev-health-scheduler/fixed.go:52`. Reachability from the binary is real (a plain function call chain, not the closure-typed-field pattern below); only the tool's automated trace didn't confirm it, direct source read did. |
| provider_rate_limit_observations | domain | SELECT, UPDATE | verified | `internal/jobs/system/retention_postgres.go` (ops, `FOR UPDATE SKIP LOCKED` chunked delete-read) |
| provider_rate_limit_observations | domain | DELETE (schema gap) | verified | same file |
| remaining_metric_partitions | domain | SELECT, INSERT, UPDATE | verified | `internal/jobs/metrics/remaining/postgres.go` (heavy + sync) |
| remaining_metric_runs | domain | SELECT, INSERT, UPDATE | verified | same |
| report_runs | domain | SELECT, UPDATE | verified | `internal/jobs/report/{postgres,query}.go` (heavy) |
| saved_reports | domain | SELECT, UPDATE | verified | same |
| sync_configurations | **both** | domain: SELECT | verified | `internal/syncdispatchruntime/native_post_sync.go:263` (sync, Fanout) |
| sync_configurations | **both** | coordinator: SELECT, UPDATE | verified | `internal/scheduler/sync/transaction.go:351,366`: `schedulerHandoffCandidatesSQL` — `FROM public.sync_configurations AS config ... FOR UPDATE OF config, job SKIP LOCKED`. The `FOR UPDATE OF config` clause names `sync_configurations` explicitly, so it needs UPDATE (row-locking semantics, same rule as `scheduled_jobs`'s claim query in the same statement). Wired from `cmd/dev-health-scheduler` through the same closure-typed `sources.newRepository`/`tx pgx.Tx`-parameter indirection that hid `scheduled_jobs` — this table slipped past the same blind spot the manifest already flagged, caught by a direct source read, not the automated tool. |
| webhook_deliveries | domain | SELECT | verified | `internal/jobs/operational/postgres.go:34` (ops) |
| work_graph_execution_ledger | domain | INSERT, UPDATE | verified | `internal/jobs/workgraph/postgres.go` (heavy + sync) |
| work_graph_execution_requests | domain | SELECT, INSERT, UPDATE | verified | `internal/jobs/workgraph/{postgres,publisher}.go` |
| worker_job_completion_fences | domain | INSERT | verified | `internal/joboutbox/completion.go:37`, heavy/sync writers |
| worker_job_completion_fences | coordinator? | INSERT (possible 2nd reacher, unresolved) | unverified-shape | `internal/scheduler/fixed/producers.go:462` calls the same helper; scheduler-side reachability not resolved by the tool |
| internal_service_credentials | coordinator | SELECT, UPDATE | verified | `internal/joboperator/auth.go:68`, workerctl `main.go:160` |
| worker_operator_audits | coordinator | INSERT, UPDATE | verified | `internal/joboperator/audit.go:32,64`, workerctl `main.go:231` |
| sync_run_reference_discoveries | coordinator | SELECT | verified | `internal/syncreconciler/materializer.go`, reconciler `dependencies.go:168` |
| sync_run_post_dispatches | coordinator | SELECT | verified | same |
| worker_job_routes | coordinator | UPDATE | verified | `internal/jobroute/control.go:158,228`; constructed from workerctl `main.go:210` AND reconciler `dependencies.go:273` — both coordinator, no domain-hot-path site found |
| scheduled_jobs | coordinator | SELECT, UPDATE | verified | `internal/scheduler/sync/transaction.go:340-371`: `schedulerClaimSQL`-style `JOIN public.scheduled_jobs ... FOR UPDATE OF config, job SKIP LOCKED` (claim) + `schedulerAdvanceMarkerSQL`: `UPDATE public.scheduled_jobs SET next_run_at = $1, updated_at = $2 WHERE id = $3` (advance marker). No INSERT/DELETE found anywhere against this table. Confirmed by direct source read, upgraded from unverified-shape after a second reader (grants-finisher2, via the lane doc) proposed this exact shape and I independently re-derived it from the same source rather than taking the proposal on trust. |
| scheduled_sync_occurrences | coordinator | SELECT, INSERT, UPDATE | verified | `internal/scheduler/sync/coordinator.go:92-116`: `schedulerInsertOccurrenceSQL` (`INSERT ... ON CONFLICT DO NOTHING`) + `schedulerSelectOccurrenceSQL` (`SELECT ... FOR UPDATE`) + three `UPDATE public.scheduled_sync_occurrences` statements in `occurrence_reconciler.go`. No DELETE found. Confirmed by direct source read. |
| fixed_schedule_occurrences | coordinator | SELECT, INSERT, UPDATE | verified | `internal/scheduler/fixed/ledger.go:230-268`: `insertOccurrenceSQL` (INSERT ON CONFLICT DO NOTHING), `selectOccurrenceSQL` (SELECT ... FOR UPDATE), `completeOccurrenceSQL` (UPDATE), `selectLastOccurrenceSQL` (SELECT). No DELETE found. Confirmed by direct source read. |
| sync_dispatch_outbox | **both** | domain: SELECT, INSERT, UPDATE | verified | hot-path writers (heavy/sync) |
| sync_dispatch_outbox | **both** | coordinator: SELECT, UPDATE | verified | `Materializer.Step` (reconciler — the transaction veto case) + `syncroute.Controller` (workerctl `main.go:206` + reconciler `dependencies.go:273`) |
| sync_run_units | **both** | domain: SELECT, UPDATE | verified | `internal/providersync` hot path |
| sync_run_units | **both** | coordinator: SELECT | verified | `Materializer.Step` |
| sync_runs | **both** | domain: SELECT, UPDATE | verified | Fanout `FOR SHARE` + `providersync` hot path |
| sync_runs | **both** | coordinator: SELECT, UPDATE | verified | `Materializer.Step` + `internal/syncreconciler/lease_repair.go` |
| sync_dispatch_transport_routes | **both** | domain: SELECT | verified | `internal/syncdispatchruntime/native_post_sync.go:160` (Fanout, `currentPostSyncReference`) |
| sync_dispatch_transport_routes | **both** | coordinator: SELECT, UPDATE | verified | `internal/syncroute/control.go`, workerctl `main.go:206` + reconciler `dependencies.go:273` |
| worker_job_runs | **both** | domain: SELECT, INSERT, UPDATE | verified | `internal/jobruntime/idempotency_postgres.go`, heavy+ops |
| worker_job_runs | **both** | coordinator: SELECT, UPDATE | verified | `internal/jobroute/control.go` LOCK/count, workerctl+reconciler |
| worker_job_outbox | **both** | domain: SELECT, INSERT | verified | `internal/joboutbox/producer.go:127-153` (`Producer.publish`: `INSERT INTO public.worker_job_outbox ... ON CONFLICT (dedupe_key) DO NOTHING`, plus the fallback `SELECT job_kind, contract_version, payload_hash, ... FROM public.worker_job_outbox WHERE dedupe_key = $1` when the insert no-ops). Constructed with the domain pool at `cmd/dev-health-worker/sync_dispatch.go:283`: `joboutbox.NewProducer(postgresDatabase.pools.Domain, registry)`. Domain-role migration grant: `internal/storage/river/migrate.go:256` (`GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO` domainRole). |
| worker_job_outbox | **both** | coordinator: SELECT, UPDATE (LOCK-implied, no UPDATE statement) | verified | `internal/jobroute/control.go:197`: `LOCK TABLE public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE`, inside `Controller.Rollback` only — confirmed by reading `ApplyCheckedIn` (lines 126-175) directly: it never touches `worker_job_outbox`, only `public.worker_job_routes`, so the LOCK is NOT also in `ApplyCheckedIn` as an earlier note assumed. `Rollback` also runs `SELECT count(*) FROM public.worker_job_outbox WHERE job_kind = $1 AND status IN ('pending', 'claimed')` at control.go:212-215 (the pending-drain check), which needs SELECT. The LOCK is the source of the UPDATE requirement: `LOCK TABLE t IN SHARE ROW EXCLUSIVE MODE` requires UPDATE/DELETE/TRUNCATE/MAINTAIN in Postgres — verified empirically on live PG16 (a role holding only SELECT+INSERT gets `permission denied for table worker_job_outbox` on the LOCK statement, no row written). `Controller` is constructed at two coordinator-candidate sites, both currently wired to the *domain* pool (not yet the coordinator pool): `cmd/dev-health-workerctl/main.go:210` → `newJobRouteController(pools.Domain, pools.QueueControl, schema, registry)` → `jobroute.NewControllerWithCeleryQuiescer(domainPool, ...)` at main.go:271-273; and `cmd/dev-health-reconciler/dependencies.go:273`: `jobroute.NewController(domainPool, registry, quiescer)`. Repointing that pool to the coordinator role is a deploy prerequisite this posture row is the readiness spec for (see `internal/storage/postgres/domain_authorization.go:599-613`'s doc comment on `coordinatorPosture`). **Three-role table:** `internal/storage/river/migrate.go:291` separately grants the *queue* role `SELECT, UPDATE, DELETE` on `worker_job_outbox` (the dispatch-drain path in `internal/joboutbox/repository.go`, constructed with the queue pool — see `cmd/dev-health-worker/operational.go:95`). The domain/coordinator split this manifest documents excludes the queue role; it is noted here only so the three-role shape isn't lost. |

## Privilege-implication rule: locking clauses need no write statement

> **This section was right, and the checker disagreed with it through three
> successive wrong models.** The rule below (verified on Postgres 16) says a role
> holding only `SELECT`+`INSERT` is denied a `SHARE ROW EXCLUSIVE` lock.
> `internal/domaingrants` nonetheless modelled every mode above `ACCESS SHARE` as
> "any one of INSERT/UPDATE/DELETE", and a test later asserted that as correct —
> contradicting this document. It is now derived from `LockTableAclCheck` directly;
> see "LOCK privilege requirements: one OR-mask per mode" below for the structure,
> the full measured table, and the additional correction that the demand is a
> DISJUNCTION rather than a conjunction with SELECT. Lesson: when the analyzer and
> this manifest disagree, the manifest was derived from measurement and the
> analyzer may be carrying a documented-but-unmodelled approximation.

A DML-verb grep (`INSERT|UPDATE|DELETE`) undercounts required privileges.
Two Postgres locking constructs imply write-level grants with no write verb
anywhere in the reaching Go statement — both verified empirically against a
live Postgres 16 instance (this is how the `worker_job_outbox` coordinator
row above was derived):

1. `LOCK TABLE t IN SHARE ROW EXCLUSIVE MODE` (and `SHARE UPDATE EXCLUSIVE`,
   `SHARE`, `EXCLUSIVE`, `ACCESS EXCLUSIVE`) requires `UPDATE`, `DELETE`,
   `TRUNCATE`, or `MAINTAIN` on the locked table — a role holding only
   `SELECT`+`INSERT` gets `permission denied` on the `LOCK` statement itself,
   before any row is touched. `ACCESS SHARE` (implied by a plain `SELECT`)
   needs only `SELECT`.
2. `SELECT ... FOR UPDATE` / `FOR NO KEY UPDATE` / `FOR SHARE` / `FOR KEY
   SHARE` all require `UPDATE` on every table in scope. `FOR UPDATE OF
   <list>` scopes the requirement to only the named tables — a joined,
   read-only table outside the `OF` list does not need `UPDATE` just because
   it appears in the same query.

Re-deriving a posture row must grep both `LOCK TABLE` and
`FOR (UPDATE|SHARE|NO KEY UPDATE|KEY SHARE)` — prefix- and JOIN-inclusive,
not restricted to `public.<table>` or `FROM|INTO` — alongside the usual
DML-verb sweep, or this class of gap recurs. A full sweep of `internal/` and
`cmd/` at `87f66b838` for all four constructs found `worker_job_outbox` as
the only table missing its lock-implied privilege from the manifest; every
other hit already matched an existing row.

## Schema-level blocker — RESOLVED by Phase 2

`external_ingest_batch_payloads`, `external_ingest_batches`,
`provider_rate_limit_observations` all need DELETE, all domain-role
retention/cleanup, and the posture was a `(table, allow_insert, allow_update)`
triple with no way to express `allow_delete` — so a required DELETE was
*unrepresentable*, not merely unlisted. That unrepresentability is what tipped
the Option A/B decision toward the role split.

Phase 2 added `AllowDelete` to `TablePrivilege` and the matching DELETE grants
to `runtimeGrantStatements`, so DELETE is now an ordinary privilege on both
sides. `internal/domaingrants` kept a hardcoded "DELETE can never be
expressed" special case after that landed, which reported three permanent
false Criticals and would have hidden a real DELETE gap behind them; it was
removed when this manifest merged into the integration branch, and
`groundtruth_test.go` now pins DELETE's presence on both lists instead.

## Unresolved cross-function transactions (hedge, not a clean-list claim)

20 sites where a `pgx.Tx` parameter's origin `Begin()` call couldn't be
traced unambiguously (tool: `DerivedSurface.UnresolvedTx`). I reviewed all
20 by hand for whether any cross the domain/coordinator boundary
specifically: 12 are `daily`/`remaining`/`workgraph`/`joboutbox` sites
ambiguous between "heavy" and "sync" (both domain, not a boundary
question); 8 are `syncreconciler`/`syncroute` sites ambiguous within
reconciler/workerctl (both coordinator). **None appeared to span
domain↔coordinator** — stated as my read of which package/binary each site
belongs to, not a formal proof. Full list: `go run ./cmd/dev-health-grantcheck
-root .` from a `feat/go-default-cutover`-checked-out worktree with this
tool's files copied in (see the handoff README's "Integration-branch
re-run" section for the exact procedure).

## Closure-typed-field sweep (`internal/scheduler/{sync,fixed}`)

`sync_configurations` was missed in the first two manifest passes because
its coordinator-side reachability goes through the same
`sources.newRepository(database.DomainPool())`-style closure-typed struct
field that already hid `scheduled_jobs` from the automated tool trace —
same blind spot, second instance. To close the class rather than find a
third instance at deploy time, I grepped every `FROM public.<table>` /
`JOIN public.<table>` and every `INSERT INTO|UPDATE|DELETE FROM public.<table>`
in `internal/scheduler/sync/*.go` and `internal/scheduler/fixed/*.go`
(excluding `_test.go`) at `cd7aa8a8e` and cross-checked the distinct table
set against the manifest:

- Read tables: `fixed_schedule_occurrences`, `organizations`,
  `scheduled_sync_occurrences`, `sync_configurations`, `scheduled_jobs`.
- Write tables: `fixed_schedule_occurrences`, `scheduled_sync_occurrences`,
  `sync_configurations` (via `FOR UPDATE OF`), `scheduled_jobs`.
- No bare/unqualified table references found (all schema-qualified with
  `public.`).
- No SQL directly in `cmd/dev-health-scheduler/*.go` itself (all SQL lives
  in the `internal/scheduler/{sync,fixed}` packages, as expected).
- One false alarm: an unfiltered pass initially surfaced `scheduler_handoffs`
  — that reference is exclusively inside `_test.go` files (`repository_test.go`
  presumably seeds a legacy/shadow table for comparison), not production code;
  confirmed via a re-run that excludes test files. Not a real gap.

**Result: `sync_configurations` was the only miss. Every other table this
sweep found was already correctly in the manifest with correct privileges.**
Nothing else to add.

## Dual-grant ("both") whitelist for the attribution test

`sync_dispatch_outbox`, `sync_run_units`, `sync_runs`, `worker_job_runs`,
`sync_dispatch_transport_routes`, `organizations`, `sync_configurations`,
`worker_job_outbox` — **8 tables**, updated from the prior 7-table list now
that `worker_job_outbox` is confirmed dual-grant (coordinator side is
LOCK-implied, not from a DML-verb sweep — see "Privilege-implication rule"
above). `worker_job_outbox` differs from the other 7: it is also granted to
a third role (queue, `internal/storage/river/migrate.go:291`) that this
domain/coordinator whitelist does not track — the "both" tag here means
"both domain and coordinator," not "only these two roles hold a grant on
this table." This is the whitelist `TestDomainGrantSurfaceMatchesQuerySurface`'s
future role-attribution extension and grants-finisher2's posture manifest
must both reference — coordinate before either side hard-codes it
independently.

## The coordinator grant surface is DERIVED and ADVISORY (CHAOS-3033)

> **This is a report, not a gate, and that is deliberate.** The coordinator check
> prints its findings and fails on nothing.
> `TestDomainGrantSurfaceMatchesQuerySurface` continues to gate the domain role
> exactly as before. **A passing coordinator run is not evidence that
> `CoordinatorPosture()` is correct.**
>
> Why: three adversarial review rounds each found new places where *the analysis
> cannot see something and the check passes anyway* — unresolved callees,
> unresolved interface dispatch, function-valued fields with no single target,
> dynamic SQL, unparsed statements, non-convergence, unrecognised lock forms,
> quoted identifiers. Each was fixed where it was found and reappeared elsewhere.
> That is one defect with many addresses, discovered one review at a time.
>
> An advisory tool that is sometimes wrong is useful: it puts derived evidence in
> front of a reviewer who can weigh it. A **gate** that passes when the analysis is
> blind is worse than no gate, because it *licenses* the hand-written rows it was
> built to check — which is how this epic produced green tickets over dormant code
> in the first place.
>
> **Promoting it to a gate requires the blind-spot closure argument**: a partition
> of everything the analysis can fail to see, with each cell either failing the
> check or documented as safe to accept **per site** (not per file), and a test per
> cell demonstrating the failure. Tracked as **CHAOS-3164**.

`TestRoleGrantSurfacesMatchQuerySurfaces` (`internal/domaingrants/role_surface_test.go`)
derives, per connection pool, which `(table, privilege)` pairs the reachable Go code
needs, and checks each against that role's own posture. It runs alongside
`TestDomainGrantSurfaceMatchesQuerySurface`, which keeps its distinct job: the
domain role's *two* hand-maintained artefacts (`runtimeGrantStatements` and
`DomainPosture`) drifted from the code while agreeing with each other. Neither gate
replaces the other.

**The coordinator has only ONE list, so its failure surface is different.**
`coordinatorGrantStatements` (`internal/storage/river/migrate.go`) is parameterized on
`options.CoordinatorGrants`, and the only production caller —
`cmd/dev-health-worker-migrate/main.go:198 coordinatorGrants()` — projects
`CoordinatorPosture()` field-for-field. A list-vs-list disagreement is therefore
structurally impossible for the coordinator, and `GroundTruth.Grants` is left nil for
it rather than synthesized from the posture and compared against itself.

### One deriver, parameterized per role — not a second copy

`DeriveForRole(root, role)` runs the same analyzer with a different syntactic seed set
(`poolRoleSeeds`). Both pools are wired through the same three shapes (a
`*pgxpool.Pool` field, a getter, a bare identifier), so the entire role-specific part
of the analysis is a name table. Every hardening pass — interprocedural SQL constant
folding, cross-function `pgx.Tx` origin tracing, unique-implementer devirtualization,
LOCK-implies-write — applies to both roles because there is one implementation of each.

A **taint barrier** stops the other role's pool root before `exprTainted`'s permissive
fall-throughs (a selector off a tainted base is tainted; a receiver with any tainted
field is tainted). Without it, `pools.Coordinator = coordinatorPool` taints the
`RuntimePools` field, the receiver heuristic taints `p` in every method, and `p.Domain`
comes back coordinator-tainted purely for being selected off a tainted base. Measured:
the barrier changes nothing on the current tree. It is kept as a guard and pinned by
`TestBarrierStopsTaintFromTheOtherRolesPool`, because its absence only becomes harmful
the moment someone adds a multi-pool struct that reaches a SQL sink.

### The blind spot that made the coordinator surface untrustworthy, and its fix

Taint died at calls through **function-typed struct fields** — this repo's DI idiom (a
composition struct of `func` fields, filled by a package-level `var`, invoked through
the field). `info.Selections` yields a `*types.Var`, not a `*types.Func`, so callee
resolution failed and everything downstream was invisible.

That hid the **entire scheduler and fixed-engine surface — 8 of `CoordinatorPosture()`'s
19 tables** (`scheduled_jobs`, `scheduled_sync_occurrences`,
`fixed_schedule_occurrences`, `organizations`, `sync_configurations`,
`remaining_metric_runs`, `remaining_metric_partitions`,
`work_graph_execution_requests`). The reconciler's equivalent hops only *appeared* to
work because `buildSyncMutationPipeline` happens to name its parameter
`coordinatorPool` and re-seeds by convention. **A gate whose completeness rests on a
parameter name is not a gate** — the three scheduler hops are the proof: they carried
the coordinator pool with their parameter named `pool` and nothing caught it.

`internal/domaingrants/funcvalue.go` resolves both forms (a named function reference,
and a function literal — the latter requiring package-level literals to be registered
as pseudo-functions, since nothing else walks them), fail-closed when a field has two
distinct targets. Coordinator surface: **11 → 18 tables**. Domain surface: **unchanged
at 32 tables with identical advisories**, verified before and after — which is luck
about this tree's parameter naming, not a property of the design.

`TestFuncValueTargetsResolveTheKnownWiringHops` pins the resolved **target** per field
rather than a count, precisely so a future `pool` → `coordinatorPool` rename cannot
make the test keep passing while the mechanism it exists to pin has stopped working.

### Attribution: what is proven, and what is deliberately advisory

A two-role posture has a failure mode one role does not — a privilege granted to the
**wrong** role, which every "is it granted somewhere" check passes.

- **CRITICAL** only where at least one proving call site is reachable **only** through
  that role's pool. Real code, real 42501.
- **ADVISORY** where every proving site is **shared** with the other role. Taint is
  tracked per `(type, field)`, not per construction site, so a repository type
  constructed from both pools has its whole method surface attributed to both.
  `internal/jobs/metrics/remaining` is the live instance: the tool derives UPDATE for
  the coordinator, and `coordinatorPosture()`'s hand-derived `{S,I}` is correctly
  **narrower**. **Do not widen a posture on a shared-evidence finding** — a hand
  derivation from the construction site is more precise there.
- **ADVISORY**, never silent: a posture row this role has no evidence for while another
  role does — a misattribution candidate. The negative direction ("role B must not hold
  X") is *not* statically provable against an under-approximation; it is proven at
  runtime by the union of each role calling `CheckRolePosture` against its own manifest,
  whose catch-all inspects the calling role. See that function's doc comment.

**On the dual-grant whitelist above:** `RoleReport.SharedPairs` makes the shared set
visible and checkable, but it is *not* a drop-in replacement for the table-level
whitelist. It is pair-level (so `sync_dispatch_transport_routes`, shared with different
privileges per role, has no shared pair); it cannot distinguish a genuine dual-grant
from a dual-constructed-type artifact (`remaining_metric_*`); and it inherits the
derivation's blind spots (`organizations` is a real dual-grant with no derived
coordinator evidence). Treat it as review input, and keep the manifest authoritative.

### Silence is not confirmation

`IncompleteRoleSurface` is first-class output: posture tables with no derived call site,
in-module wiring hops where taint stopped, and non-constant SQL sites, per role. A table
this tool cannot analyze must never look identical to one it checked and approved — that
equivalence is what licenses hand-written rows.

Still unverified on the coordinator side: **`organizations`** and
**`work_graph_execution_requests`** have posture rows with no derived evidence. If it
turns out no coordinator path touches them, that is a posture *over*-declaration and a
separate finding — route it to the posture lane rather than tightening it here, since a
wrong tightening fails closed in production.

### LOCK privilege requirements: one OR-mask per mode, derived from the backend

Three attempts at this rule produced three different wrong answers, each a
different KIND of wrong. That pattern — not any single defect — is why the model
was finally re-derived from `LockTableAclCheck`
(`src/backend/commands/lockcmds.c`) rather than reconstructed from prose again:

```
aclmask =  (mode == AccessShareLock)  ? ACL_SELECT
        : ((mode <= RowExclusiveLock) ? ACL_INSERT : 0)
aclmask |= ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_MAINTAIN
pg_class_aclcheck(rel, user, aclmask)     // ANY bit suffices
```

**It is ONE disjunction per mode.** Not "SELECT and a write privilege". A
lock-only path authorized by `UPDATE` alone needs no `SELECT`, and asserting one
over-grants — the exact failure the two-role split exists to prevent.

Measured on PostgreSQL 18.4 with each privilege granted **alone** and the `LOCK`
executed as the only statement in its transaction:

| mode (lock only) | NONE | SELECT | INSERT | UPDATE | DELETE | TRUNCATE | MAINTAIN | REFERENCES | TRIGGER |
|---|---|---|---|---|---|---|---|---|---|
| `ACCESS SHARE` | DENIED | ok | ok | ok | ok | ok | ok | DENIED | DENIED |
| `ROW SHARE` | DENIED | DENIED | ok | ok | ok | ok | ok | DENIED | DENIED |
| `ROW EXCLUSIVE` | DENIED | DENIED | ok | ok | ok | ok | ok | DENIED | DENIED |
| `SHARE UPDATE EXCLUSIVE` … `ACCESS EXCLUSIVE` | DENIED | DENIED | DENIED | ok | ok | ok | ok | DENIED | DENIED |
| **mode omitted** | DENIED | DENIED | DENIED | ok | ok | ok | ok | DENIED | DENIED |

**Correction to a claim previously recorded here (and amplified in review): the
PostgreSQL documentation is NOT wrong about `ROW SHARE`.** The docs say the
INSERT-class applies to "ROW EXCLUSIVE **or a less-conflicting mode**", which
*includes* `ROW SHARE` — matching the measurement exactly. The earlier note
asserting the docs were wrong was a misreading of the docs, not a finding. A wrong
"the docs are wrong" note is worse than the original confusion, because it teaches
the next reader to distrust the correct source.

#### Two measurement confounds, both of which hid the wrong operator

The earlier probe got the *tiers* right and the *operator* wrong, and could not
have caught it:

1. **Every grant set began with `SELECT`.** A conjunction and a disjunction
   containing SELECT are then indistinguishable. Adding SELECT to every fixture
   made the conjunction unfalsifiable.
2. **The probe appended a read after the `LOCK`.** So no grant set *without*
   SELECT could ever reach the lock's own ACL check.

The method (control rows isolating the denial to the clause under test) was right;
the fixtures defeated it. The general form: **when testing whether X is required,
no fixture may hold X for an unrelated reason.**

#### The grammar was a fail-open

The previous regex recognised `LOCK TABLE <one target> IN <mode> MODE` and
silently derived **nothing** for the other eight shapes PostgreSQL accepts —
optional `TABLE`, multiple comma-separated targets, `ONLY`, a trailing `*`, an
omitted mode (defaulting to `ACCESS EXCLUSIVE`), `NOWAIT`, arbitrary whitespace.
`LOCK public.a, public.b` in coordinator-only code would derive neither table nor
privilege, CI would pass, and production would return 42501.

A parser that ignores what it cannot recognise is the worst possible shape for a
fail-closed tool. `parseLockStatements` now handles all nine forms, and **a LOCK
it cannot fully read is a recorded fact that FAILS the gate** — its target may
never enter the derived surface at all, so an absence is not a safe default.

#### Unknown modes now genuinely fail closed

The previous version gave an unrecognised mode a *guessed* strict privilege set
and called that fail-closed. It was not: the comparator only reported the mode when
the **guess** was unsatisfied, so a role already holding `UPDATE` passed silently
with no manual-verification finding. **A guess that happens to be satisfied is
indistinguishable from knowledge.** An unrecognised mode is now refused and
reported.

### Transaction straddles are ADVISORY — co-residency is inferred, never proven

A `txorigin:` group means every statement's `pgx.Tx` traced back to the same
`Begin()` **source position**. That is a fact about where the handle came from, not
about what executes: `buildTxOrigins` performs no control-flow analysis, so two
**mutually exclusive branches** after one `Begin` — an `if`/`else`, an early
return, a `switch` — carry the same origin and were reported as a single
transaction touching both sides of the partition.

Calling that "proven" was wrong, and a premise labelled proven is worse than one
labelled inferred, because nobody re-checks it.

**Decision: downgrade rather than do the path analysis.** A straddle finding can
only push a posture *wider* (dual-grant these tables), and an over-grant emitted
on an inference is precisely what the two-role split exists to prevent — the same
reasoning that makes the `remaining_metric_*` over-approximation advisory. The
blocking signal is not lost: a table genuinely missing from the executing role's
posture still produces its own per-privilege CRITICAL when its evidence is
role-exclusive. What becomes advisory is the transaction *framing*, which is a
review aid. The traced/coarse distinction is still reported, because it tells a
reader how much weight the grouping carries.

### Attribution can be INVERTED by a parameter name, not just incomplete


A pool-typed parameter is a seed root by **spelling** (`domainPool`,
`coordinatorPool`), and spelling can contradict reality. `build(domainPool *pgxpool.Pool)`
called only with the coordinator pool was seeded by the DOMAIN run from its name
while the coordinator run's barrier discarded it — so downstream SQL looked
domain-**exclusive**, and role-exclusive evidence is precisely what this gate
escalates to CRITICAL. It would have directed the grant to the wrong role with
confidence, and every "is it granted somewhere" check would still pass.

`buildPoolParamRoles` fixes this: a role-agnostic pass records which roles are
actually passed to each pool-typed parameter at its call sites, and **call-site
evidence outranks the name**. Where no call site resolves, the name still applies —
the convention is the only signal left, and suppressing it would trade a false
attribution for a false absence. Every override is reported, because it means a
naming convention lied and the reader should know that rather than assume the
surface shrank.

### Incompleteness is REPORTED, per (table, privilege)

Enumerating what was not verified is the point of the advisory posture — the
report's value is precisely that it says what it could not see.

- **Pair granularity.** A table-level list hid the case that matters most: if
  SELECT stays visible while an UPDATE path goes dark, the table is nonempty, so a
  table-level list omits it and the exact `(table, UPDATE)` gap appears in *no*
  output at all.
- **One report, category-tagged.** `AdvisoryReport` returns every line from a
  single function, tagged by category, and a test asserts every category still
  emits. When each category was printed by its own loop in the test, a unit test
  could prove a value reached the data structure while the loop that printed it was
  deletable with everything still green — and for a tool whose only output is its
  report, a category that stopped being printed is indistinguishable from one with
  nothing to say.
- **`WIRING-HOP` is reported.** It marks where pool taint *stopped*, so any SQL
  beyond it is invisible. It was collected for three review rounds and never
  surfaced, which made it the largest unstated hole in the output.

Three exclusions from the "needs acknowledgement" annotation are legitimate and
recorded rather than silently applied: SELECT is *synthesized* onto every posture
row by `loadPosture`, so its absence on a write-only table is a representation
artifact; a privilege that satisfies a derived LOCK is justified by that lock when
it is the *sole* satisfying privilege held; and third-party function-typed fields
(`encoding/json`'s `scanner.step`, pgx's `QueuedQuery.Fn`) are filtered out because
reporting them would be pure noise.

**Known limitation, carried into the closure argument**: dynamic-SQL
acknowledgements are keyed by FILE, so a new dynamic statement in an already-
acknowledged file is accepted without review. Per-site acknowledgement is part of
the partition task (CHAOS-3164), not a patch.

### Fail-closed must not become fail-SILENT

A function-typed field with two implementations is correctly excluded from
resolution — but the exclusion used to vanish from every output, because the
`Unresolved` record's callee is the bare field name and the incompleteness filter
dropped it along with third-party methods. Conflicts are now first-class
(`FuncValueConflicts`), reported at the tainted call site, and gated. The filter
also keeps bare callee names, which cannot be third-party by construction.

### Transaction-span veto

A transaction cannot span two pools, so every table one touches must be authorized
for the single role whose pool runs it. `transactionStraddleFindings` reports a
group whose tables land on opposite sides of the partition — **always as an
ADVISORY**, for the reason given in "Transaction straddles are ADVISORY" above:
co-residency is inferred at both precisions, and a finding that can only widen a
posture must not block on an inference.

The lock path routes through the identical shared-evidence downgrade, via one
`exclusiveTo` helper rather than a copy: a copy is how one path ends up still
accepting what the other rejects.

### Reachability is static wiring, not runtime activation (decision)

The derivation walks every function that is *wired*, including code behind a
checked-in activation flag that is currently false — `cmd/dev-health-scheduler`'s
`activation.goOwnsMarkers` gate returns before opening any PostgreSQL client, yet the
scheduler's coordinator tables are still derived and still demanded. Deliberate:

- Grants ready **before** activation are the point of this epic. Runtime-reachability
  derivation would read "green, nothing needed" until someone flips the flag, then
  produce a pile of CRITICALs at the worst moment.
- Coverage must not depend on configuration, or the gate's strength varies with a
  boolean and a flag flip silently changes what CI verifies.
- The error direction is survivable: deriving dormant code can only ever ask for
  privileges that code will need once active, so the failure mode is an over-grant —
  a visible, reviewable row — rather than an outage.

Residual risk, stated rather than hidden: a seam that is wired but never activated
accretes grants permanently, and nothing here distinguishes "not yet" from
"abandoned". Mitigated by review pressure (such rows surface in the misattribution and
no-derived-evidence advisories), not by changing the reachability model. If a seam is
dead, delete the wiring — that is the signal this analyzer reads.

### Known-open findings carry a ticket

`knownOpenCriticals` records CRITICALs accepted as real whose fix belongs to
another lane. Entries match exactly one `(role, table, privilege)`, must name a
ticket, and are reported under their own category so "someone is fixing this" is
never flattened into "unreviewed".

Under the advisory posture its expiry property is **suspended**: nothing fails, so
a stale entry is reported rather than enforced. Restoring enforcement is part of
promoting the check to a gate.

Current entry — found by this checker's first run: **coordinator
`sync_dispatch_outbox` INSERT** (CHAOS-3079). `syncreconciler.Materializer.Step`
runs on the coordinator pool (`cmd/dev-health-reconciler/dependencies.go:208`) and
executes four `INSERT INTO public.sync_dispatch_outbox`
(`materializer.go:125/235/345/450`) while `coordinatorPosture()` declares
`allow_insert=false`. Latent only because `checkedInReconcilerActivation.syncMutation`
is false today.

### Harness consequence of adding a posture row

`rolePostureQuery` asserts `count(required_tables) = count(required_table_privileges)`
with **no leniency** (`domain_authorization.go:194`), and `required_tables` JOINs
`pg_class`. The GRANT side is `to_regclass`-guarded and skips a missing table silently.
So a posture row for a table the test database does not create makes the readiness check
fail while the grant reports success. Adding a new table to either posture therefore
requires a matching `CREATE TABLE` in **two** places — `startGrantHarness`
(`internal/storage/postgres/domain_grant_reconciliation_integration_test.go:209`, shared
by all 22 in-package grant tests) and `cmd/dev-health-workerctl/main_integration_test.go`,
which builds its own.
