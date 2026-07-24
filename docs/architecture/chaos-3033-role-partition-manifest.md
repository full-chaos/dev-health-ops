# CHAOS-3033 Option B role-partition manifest

Source tree: `feat/go-default-cutover` @ `cd7aa8a8e` (per the "derive from the
tree the epic is actually building" instruction). `internal/domaingrants`
version: commit `5d0fe8626` on `lane/grant-surface-deriver` (buildTxOrigins +
UnresolvedTx included). Read the handoff `README.md` §2-4 for the analysis
method; this file is the flat, machine-checkable output grants-finisher2
implements from.

**One row per (table, privilege). `role` for a dual-grant row is expressed
per-role, not as a single set — see `provider_rate_limit_observations` for
why that matters.**

## ⚠️ Disputed rows — NOT applied, evidence below, needs reconciliation

Before the table: five retags arrived in a follow-up message, attributed to
"CUT-10's `privilege-slice3c-budgetguard-rows.md`" and a "lane-provider-2."
I checked both claims against the actual source before applying anything,
per this whole engagement's standing rule (verify, don't relay). They do not
hold up, and I have NOT applied them. Evidence:

1. **The cited file does not exist as a committed artifact anywhere.**
   `git log --all --diff-filter=A --name-only` finds no commit that adds
   `privilege-slice3c-budgetguard-rows.md`. It appears ONLY as prose inside
   one commit message (`6f95d7f87`, `lane/cut-10-sync-coordinators`,
   "CUT-10 slice 3c: native BudgetGuard pure decision layer") — the commit
   itself adds exactly two files, `budget_guard_plan.go` and
   `budget_guard_plan_test.go`, no markdown. There is no `lane-provider-2`
   branch or worktree anywhere in this repo. I cannot read the document I
   was asked to reconcile against; I can only read the commit message that
   references it.

2. **That commit's own code has zero database access**, confirmed by
   grepping the actual file content at that commit for `.Exec(`/`.Query(`:
   zero matches. Same for slice 3a's `dispatch_guard_plan.go` (commit
   `94c2e23af`) — also zero. Both commit messages say so explicitly: slice
   3a is "testable without domain grants," slice 3c is "no database, no
   clock, no privileges" by design, with "every CAS write" and the
   `_active_cooldowns` query "Deliberately NOT ported." Neither file is
   imported or called from `cmd/dev-health-worker` or
   `internal/syncdispatchruntime`'s wired code on ANY branch I checked
   (grepped for `BudgetGuard`/`DispatchGuard`/`budget_guard_plan`/
   `dispatch_guard_plan` across `cmd/dev-health-worker/*.go` and
   `internal/syncdispatchruntime/*.go` — zero hits).

3. **Neither file exists on `feat/go-default-cutover` at all** —
   `git merge-base --is-ancestor 6f95d7f87 cd7aa8a8e` fails. This code isn't
   in the tree I was told is ground truth for this manifest, independent of
   whether its claims are accurate.

4. **The commit message's own "Grant audit" summary says the opposite of
   what was relayed to me.** Verbatim, from `6f95d7f87`'s message: *"BudgetGuard.enforce_run/reconfirm_cooldowns run inside dispatch_sync_run's
   SAME session as DispatchGuard.authorize_run and \_claim_units -- a
   transaction cannot span two pools, so every table that session touches
   ... must resolve to ONE role. This likely means dispatch_sync_run's
   entire session is "coordinator," not "domain" as implicitly assumed
   before Option-B existed -- flagged as the single most important open
   item from this pass, not resolved unilaterally."* I was told this had
   been "ruled" domain and "verified." The source material I can actually
   read says the opposite lean, and explicitly says it wasn't resolved.

**What this means for the 5 retagged rows**: `organizations`, `org_licenses`,
`tier_limits`, `worker_job_routes`, `sync_dispatch_transport_routes`, and the
`provider_rate_limit_observations` split all rest on a `dispatch_sync_run`
session that has no wired, executing Go implementation on any branch I can
find. I have kept my previously-verified tags for the four tables I have
real evidence for (`organizations`/`org_licenses` via the stream-runner
feature-flag path, domain; `provider_rate_limit_observations` via retention,
domain, DELETE-schema-gap) and marked `worker_job_routes` and
`sync_dispatch_transport_routes` as **DISPUTED** in the table below rather
than silently keeping my tag or silently taking the retag. `tier_limits` has
zero Go references anywhere (confirmed earlier, Python-only, out of scope
regardless of this dispute).

If `dispatch_sync_run`'s session really does turn out to span these tables
once its I/O layer is written, the commit's own conclusion is the one to
build on: that session likely needs to run as coordinator, not domain — the
opposite direction from what was relayed to me. Either way, this needs a
human or an independently-reproducible artifact, not a second-hand summary
I couldn't locate the source of.

## Verified / inferred rows

`confidence`: **verified** = I directly confirmed a `cfg.Profile != "X"`
gate or binary-exclusive wiring in Go source. **inferred** = profile-level
attribution from `deploy/go-workers/profiles.json` job_kinds cross-referenced
with evidence file paths, not an automated scoped re-run. **unverified-shape**
= confirmed present in Go source but privilege shape not derivable by the
tool (documented limitation) — do not grant blind.

| table | privileges | role | confidence | evidence |
|---|---|---|---|---|
| billing_notifications | SELECT | domain | verified | `internal/jobs/operational/postgres.go:61` (ops profile, `operational.go` gate) |
| daily_metrics_partitions | SELECT, INSERT, UPDATE | domain | verified | `internal/jobs/metrics/daily/postgres.go` (heavy profile `daily.go` gate; also reached via sync profile's Fanout, same role) |
| daily_metrics_runs | SELECT, INSERT, UPDATE | domain | verified | same |
| external_ingest_batch_payloads | SELECT | domain | verified | `internal/streamhandlers/external_postgres.go:172` (stream-* profiles) |
| external_ingest_batch_payloads | DELETE (schema gap — no `allow_delete` column) | domain | verified | `internal/streamhandlers/external_postgres.go:278,315` |
| external_ingest_batches | SELECT, UPDATE | domain | verified | `internal/streamhandlers/external_postgres.go`, `internal/externalrecompute/postgres.go` (stream-*) |
| external_ingest_batches | DELETE (schema gap) | domain | verified | `internal/jobs/system/retention_postgres.go` (ops profile) |
| external_ingest_recompute_jobs | INSERT | domain | verified | `internal/externalrecompute/postgres.go:75` (stream-*) |
| external_ingest_rejections | INSERT | domain | verified | `internal/streamhandlers/external_postgres.go:267` (stream-*) |
| external_ingest_sources | SELECT | domain | verified | `internal/streamhandlers/external_postgres.go:183` (stream-*) |
| feature_flags | SELECT | domain | verified | `internal/streamhandlers/external_postgres.go:58` (stream-*) |
| org_feature_overrides | SELECT | domain | verified | same |
| org_licenses | SELECT | domain | verified | same |
| organizations | SELECT | domain | verified | same |
| provider_rate_limit_observations | SELECT, UPDATE | domain | verified | `internal/jobs/system/retention_postgres.go` (ops profile, `FOR UPDATE SKIP LOCKED` chunked delete-read) |
| provider_rate_limit_observations | DELETE (schema gap) | domain | verified | same file |
| remaining_metric_partitions | SELECT, INSERT, UPDATE | domain | verified | `internal/jobs/metrics/remaining/postgres.go` (heavy + sync via Fanout) |
| remaining_metric_runs | SELECT, INSERT, UPDATE | domain | verified | same |
| report_runs | SELECT, UPDATE | domain | verified | `internal/jobs/report/{postgres,query}.go` (heavy profile, `reports.go` gate) |
| saved_reports | SELECT, UPDATE | domain | verified | same |
| sync_configurations | SELECT | domain | verified | `internal/syncdispatchruntime/native_post_sync.go:263` (sync profile, Fanout tx) |
| webhook_deliveries | SELECT | domain | verified | `internal/jobs/operational/postgres.go:34` (ops) |
| work_graph_execution_ledger | INSERT, UPDATE | domain | verified | `internal/jobs/workgraph/postgres.go` (heavy + sync) |
| work_graph_execution_requests | SELECT, INSERT, UPDATE | domain | verified | `internal/jobs/workgraph/{postgres,publisher}.go` |
| worker_job_completion_fences | INSERT | domain | verified | `internal/joboutbox/completion.go:37`, reached from heavy/sync writers |
| worker_job_completion_fences | INSERT (possible 2nd reacher) | coordinator? | **unverified-shape** | `internal/scheduler/fixed/producers.go:462` also calls the same `joboutbox.CompletionKey`/`MarkCompletionTx` helper — reachability from scheduler not resolved by the tool (same closure-taint limitation as the 3 scheduler tables below); if real, this becomes a `both` row |
| internal_service_credentials | SELECT, UPDATE | coordinator | verified | `internal/joboperator/auth.go:68`, workerctl `main.go:160` (binary-exclusive) |
| worker_operator_audits | INSERT, UPDATE | coordinator | verified | `internal/joboperator/audit.go:32,64`, workerctl `main.go:231` |
| sync_run_reference_discoveries | SELECT | coordinator | verified | `internal/syncreconciler/materializer.go`, reconciler `dependencies.go:168` |
| sync_run_post_dispatches | SELECT | coordinator | verified | same |
| scheduled_jobs | SELECT, **UPDATE** (per required_table_privileges convention for a table with any write; exact write shape NOT independently derived) | coordinator | **unverified-shape** | `internal/scheduler/sync/repository.go:166` -- confirmed present via direct read; reachability blocked by a closure-typed struct field (`schedulerRuntimeSources.newRepository`) the analyzer can't trace. Do not grant blind. |
| scheduled_sync_occurrences | SELECT, INSERT, UPDATE (bounded guess from table name + write-heavy occurrence-tracking pattern; NOT independently derived) | coordinator | **unverified-shape** | `internal/scheduler/sync/*.go` -- same blind spot |
| fixed_schedule_occurrences | SELECT, INSERT, UPDATE (same caveat) | coordinator | **unverified-shape** | `internal/scheduler/fixed/*.go` -- same blind spot |
| sync_dispatch_outbox | SELECT, INSERT, UPDATE (domain) **+** SELECT, UPDATE (coordinator) | **both** | verified | domain: hot-path writers (heavy/sync). coordinator: `Materializer.Step` (reconciler, one transaction with sync_run_reference_discoveries/sync_run_post_dispatches -- the veto case) AND `syncroute.Controller` (workerctl `main.go:206` + reconciler `dependencies.go:145`) |
| sync_run_units | SELECT, UPDATE (domain) **+** SELECT (coordinator) | **both** | verified | domain: `providersync` hot path. coordinator: `Materializer.Step` |
| sync_runs | SELECT, UPDATE (domain) **+** SELECT, UPDATE (coordinator) | **both** | verified | domain: Fanout `FOR SHARE` + `providersync` hot path. coordinator: `Materializer.Step` + `internal/syncreconciler/lease_repair.go` |
| worker_job_runs | SELECT, INSERT, UPDATE (domain) **+** SELECT, UPDATE (coordinator) | **both** | verified | domain: `internal/jobruntime/idempotency_postgres.go`, called from heavy+ops. coordinator: `internal/jobroute/control.go` LOCK/count, workerctl-only |
| worker_job_routes | UPDATE | coordinator | verified (per my own evidence) — **DISPUTED, see above** | `internal/jobroute/control.go:158,228`, workerctl-only wiring confirmed; a claimed additional sync-worker write path could not be verified (§ Disputed rows) |
| sync_dispatch_transport_routes | SELECT (baseline) + UPDATE | coordinator | verified (per my own evidence) — **DISPUTED, see above** | `internal/syncroute/control.go`, reached from workerctl `main.go:206` AND reconciler `dependencies.go:145` (both coordinator-candidates, so this stays coordinator-only under my evidence, not domain+coordinator); a claimed sync-worker write path could not be verified |

## Schema-level blocker (unchanged from the original report)

`external_ingest_batch_payloads`, `external_ingest_batches`,
`provider_rate_limit_observations` all need DELETE, all trace to
domain-role retention/cleanup code, and `required_table_privileges` has no
`allow_delete` column at all -- this can't be granted with the current
schema shape regardless of role assignment. Needs a schema decision
(add the column, or a different mechanism) before implementation, same as
originally reported.

## Attribution-test coordination note

The dual-grant ("both") set above is: `sync_dispatch_outbox`,
`sync_run_units`, `sync_runs`, `worker_job_runs` -- **4 tables**, not the 3
originally scoped from `Materializer.Step` alone (`worker_job_runs` was
added once `internal/jobroute/control.go`'s workerctl-side LOCK/count
requirement was folded in). This is the whitelist
`TestDomainGrantSurfaceMatchesQuerySurface`'s future role-attribution
extension and grants-finisher2's posture manifest both need to agree on.
The disputed `worker_job_routes`/`sync_dispatch_transport_routes` rows are
NOT in this set pending the reconciliation above -- if they turn out to be
real dual-grants, the whitelist grows to 6.
