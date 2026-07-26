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
