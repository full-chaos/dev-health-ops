---
page_id: op-rb-operator-commands
summary: Full dev-health-workerctl operator command inventory -- backfill/redrive, blocked-partition repair, team attribution + ownership repair, workgraph rebuild, full reset -- with the Python-legacy verbs it replaces marked or deleted.
content_type: runbook
owner: platform-operations
source_of_truth:
  - cmd/dev-health-workerctl/main.go
  - cmd/dev-health-workerctl/repair_metrics_execution.go
  - cmd/dev-health-workerctl/repair_workgraph.go
  - cmd/dev-health-workerctl/trigger_workgraph.go
  - cmd/dev-health-workerctl/trigger_investment.go
  - docs/go-migration-matrix.md
  - docs/contribute/architecture/team-attribution.md
applicability: current
lifecycle: active
---

# Operator commands for the Go worker stack

Prod runs the Go worker fleet (River-queue based). Celery workers/Beat were **stopped in prod 2026-08-19**
(CHAOS-4026) and are not live; see [Worker or queue failure](worker-or-queue-failure.md) and
[Run workers and jobs](../run/workers-and-jobs.md) for the current runtime map.
{: .fc-page-lede }

Every mutating command below is `dev-health-workerctl` (Go binary; compose service `go-workerctl`, profile
`go-cutover`). **Pull both profiles explicitly before using it** --
`--profile go-cutover --profile go-workers pull -q go-workerctl` -- the default `--profile go-workers pull`
does not include `go-cutover` and a real redrive has already run a stale operator image that silently lacked
a needed subcommand because of this.

Auth: `WORKER_OPERATOR_TOKEN`/`WORKER_OPERATOR_TOKEN_FILE` for job-list/cancel/retry and the providersync/
sync-dispatch-outbox cleanups; `WORKER_METRIC_REPAIR_TOKEN` for every metrics/workgraph repair verb (one
shared repair token across metrics execution repair, workgraph repair, and finalize repair, per chris's
CHAOS-5042 ruling). `dev-hops` = the Python CLI (`src/dev_health_ops/cli.py`); every `dev-hops metrics ...`
verb is marked **legacy** or **deleted** below -- several bypass the Go native executors entirely even where
a native executor exists for that family (`docs/go-migration-matrix.md` METRICS section, "CLI verb layer
largely bypasses" note).

## (a) Daily metrics backfill / redrive per org/day/family

| Command | Source | When to use |
|---|---|---|
| `dev-health-workerctl metrics daily-start --org <uuid> --day <YYYY-MM-DD> [--to <YYYY-MM-DD>] [--repo-id <uuid> ...]` | `main.go:957-1033` (`dispatchMetricsDailyStart`, CHAOS-5055) | Start a fresh daily-metrics run for an org/day range (optionally repo-scoped) through the same `StartRunTx` coordinator path the automatic post-sync/fixed-schedule fanout uses. Bounded to 31 days per call. |
| `dev-health-workerctl metrics daily-redrive --org <uuid> --from <YYYY-MM-DD> --to <YYYY-MM-DD> --review-evidence "<text>"` | `main.go:781-897` (CHAOS-4358) | Repair a run stranded because River discarded every `daily_partition` job dispatched for it. Repairs the compatibility-bridge partition ledger first, then republishes. Bulk path only ever authorizes `retry_safe`, never `confirm_succeeded`. `--review-evidence` is required, free text, no default. |
| `dev-health-workerctl metrics finalize-redrive --org <uuid> --from <YYYY-MM-DD> --to <YYYY-MM-DD> [--include-succeeded=true\|false] [--dry-run] --review-evidence "<text>"` | `main.go:1295-1378` (CHAOS-4405) | Re-run `metrics.daily_finalize` for a day **already completed**, to backfill fields added later (e.g. `compounding_risk_daily(team)`, `team_cognitive_load_daily` after CHAOS-4399). `--include-succeeded` defaults **true** -- that's the point of this verb; pass `=false` to restrict to the never-attempted/failed/expired-lease subset instead. `--review-evidence` required unless `--dry-run`. |
| `dev-health-workerctl metrics partition-recompute --org <uuid> --from <YYYY-MM-DD> --to <YYYY-MM-DD> --family repo_user_commit [--dry-run] --review-evidence "<text>"` | `main.go:1623-1694` (CHAOS-4459) | Repair partitions where ALL partitions succeeded but were computed under a now-known-wrong writer (CHAOS-4341's `org_id=''` writer bug). Only recovery path for a "succeeded but wrong" partition. `--family` is restricted to `daily.SupportedPartitionRecomputeFamilies` (today: `repo_user_commit` only) -- it scopes audit intent, not the recompute blast radius: every family in the partition is recomputed, not just the named one. |
| `dev-health-workerctl metrics remaining start --family <complexity\|dora\|release_impact\|work_item_attribution\|capacity\|recommendations> --org <uuid> --day <YYYY-MM-DD> [--to <YYYY-MM-DD>] --review-evidence "<text>"` | `main.go:1763-1931` (CHAOS-4254) | Dispatch a NEW remaining-metrics run for a historical (org, family, day) that was **never dispatched at all** -- outside what `daily-redrive`/`jobs retry` can recover. Bounded to 31 days per call; refuses today and the future (a day still open could race the automatic trigger and double-write). |
| `dev-health-workerctl metrics remaining trigger-backstop --family <work_item_attribution\|complexity\|dora\|release_impact\|capacity\|recommendations> --org <uuid> [--day <YYYY-MM-DD>] [--today] --review-evidence "<text>" [--team <uuid>\|--all-teams] [--window <days>]` | `main.go:2148-2285` | Trigger a fixed-schedule backstop family NOW instead of waiting for its own occurrence (e.g. work_item_attribution's watermark-driven recompute). `--day` is a **dedup key for the run this becomes, not a compute window** -- work_item_attribution always recomputes from its live watermark regardless of `--day`. Defaults to yesterday UTC; `--today` is required to target today explicitly (coexists with, never suppresses, the schedule's own occurrence -- the two compete for the family's single worker slot, not correctness). `capacity`/`recommendations` require exactly one of `--team`/`--all-teams`; every other family ignores both. |
| `dev-hops metrics daily` / `rebuild` | `workerctl_dispatch.py` -> `dev-health-workerctl metrics daily-start` | No longer legacy (CHAOS-5055/#2232): dispatches through the row above instead of computing in Python. The old direct-compute `job_daily.py` CLI wrappers were dead code (unwired, zero callers) and were deleted (CHAOS-5307). |
| `dev-hops metrics complexity` / `dora` / `capacity` | `workerctl_dispatch.py` -> `metrics remaining trigger-backstop --family <name>` | No longer legacy (CHAOS-5055/#2232): dispatches through the row above instead of computing in Python. The old direct-compute `job_complexity_db.py`/`job_dora.py`/`job_capacity.py` CLI wrappers were dead code (unwired, zero callers) and were deleted (CHAOS-5307); their compute functions are unaffected. |
| `dev-hops metrics compounding-risk` | `job_compounding_risk.py` | **Legacy**, duplicate coverage -- `job_daily.py`'s finalize already writes this nightly. |
| `dev-hops metrics validate-flags` | `job_ff_validation.py` | Read-only diagnostic, no write. Safe to run any time. |

Daily-metrics family compute status (source of truth: `internal/jobs/metrics/daily/families.json` +
`docs/go-migration-matrix.md`): all real families are NATIVE (Go computes and writes) **except**
`compounding_risk`'s TEAM scope -- **deleted today via #2275/CHAOS-5084** if that PR has merged when you
read this, otherwise still Python at finalize time, CHAOS-4287 open -- and `team_cognitive_load`'s Python
fallback path (retained but skip-gated). Confirm current status against `families.json` and
`docs/go-migration-matrix.md` before relying on this paragraph; it is prose, not the generated table.

## (b) Blocked/failed partition inspection and repair

| Command | Source | When to use |
|---|---|---|
| `dev-health-workerctl metrics daily-blocked --org <uuid> [--limit N]` | `main.go:1046-1089` (CHAOS-5040) | Read-only. List blocked runs for an org with failure reasons, failed/succeeded partition counts. No `--repair` flag exists on purpose -- the only safe way out is `daily-redrive`. |
| `dev-health-workerctl metrics list-ambiguous-executions [--org <uuid>]` | `repair_metrics_execution.go:150-220` | Read-only. Lists `metric_compatibility_executions` rows stuck `state='ambiguous'`. Each row's output includes a ready-to-copy `metrics execution-repair` command with `--execution`/`--expected-state`/`--expected-attempt-count` pre-filled. |
| `dev-health-workerctl metrics execution-repair --execution <uuid> --expected-state <executing\|ambiguous> --expected-attempt-count <N> --resolution <confirm_succeeded\|retry_safe> --review-evidence "<text>" [--output-evidence '<json>'] [--dry-run]` | `repair_metrics_execution.go:29-113` (CHAOS-5042) | Per-execution repair when a family's readers would SUM-duplicate on a bulk `retry_safe` (e.g. `file_hotspots`). `confirm_succeeded` requires real `--output-evidence` JSON describing the output that already exists; refused for `retry_safe`. |
| `dev-health-workerctl metrics daily-finalize --run <uuid> --review-evidence "<text>"` **or** `--all-complete [--limit N] --review-evidence "<text>"` | `main.go:1091-1273` (CHAOS-4389, finalize-ledger repair CHAOS-4409) | Repair a run stuck `status='running'` with 100% partitions succeeded whose ONE `metrics.daily_finalize` job was discarded. `--all-complete` only ever touches never-attempted (`finalization_status='pending'`) rows; a run whose finalize already ran needs `--run` individually (a human must confirm it didn't already write real output). |
| `dev-health-workerctl jobs list [--state <s> ...] [--kind K] [--queue Q] [--limit N]` | `main.go:706-728` | Read-only. Generic River job listing (default states: available/retryable/running/scheduled). |
| `dev-health-workerctl jobs inspect <id>` | `main.go:729-741` | Read-only. Full job detail by River job id. |
| `dev-health-workerctl jobs cancel <id> --reason <code> --correlation-id <id>` / `jobs retry <id> --reason <code> --correlation-id <id>` | `main.go:742-769` | Generic job-level cancel/retry, audited via `joboperator.Service`'s Action/audit pipeline (unlike the metrics/workgraph repair verbs, which bypass it -- see each verb's own doc comment). |
| `dev-health-workerctl workgraph list-ambiguous [--org <uuid>]` | `repair_workgraph.go:77-131` | Read-only. Lists `work_graph_execution_requests` rows stuck `state='ambiguous'` on both the request and its ledger row, unleased. Each row includes a ready-to-copy `workgraph repair` command. |
| `dev-health-workerctl workgraph repair --request <uuid> --resolution <confirm_succeeded\|retry_safe> --expected-attempt-count <N> --review-evidence "<text>" [--output-evidence '<json>'] [--dry-run]` | `repair_workgraph.go:144-228` (CHAOS-5042) | Repair a stuck `workgraph.build`/`investment.materialize` ledger row. Shares the finalize/metrics repair token (`WORKER_METRIC_REPAIR_TOKEN`). |
| `dev-health-workerctl sync-dispatch-outbox close-backlog [--dry-run] [--batch-size N]` | `main.go:1585-1621` (CHAOS-4583) | Drain a pre-existing `sync_dispatch_outbox` backlog; the forward reconciler stage only prevents new backlog, it doesn't retroactively clean an existing one. Not org-scoped. |

Related reference (not a command, background): [Job recovery lifecycle](../run/job-recovery-lifecycle.md) --
River only rescues a stuck-`running` job after `max(RescueStuckJobsAfter=1h default, kind timeout)`; a job
"stuck" for less than that is not yet eligible for automatic rescue.

## (c) Team attribution + ownership repair

No dedicated `workerctl` subcommand exists for team-attribution repair specifically; it is recovered by
re-running the sync/work-graph/investment chain below, because attribution is derived at query time from
`work_item_team_attributions` (`ReplacingMergeTree`, `FINAL` + fence read) rather than stored denormalized.

**Snapshot first.** `work_item_team_attributions` is `ReplacingMergeTree(computed_at)` -- ClickHouse's
background merges physically collapse each `ORDER BY` key to its newest version over time, so there is no
way to reconstruct "before" state from the table once merges have run. **Before step 2 below**, snapshot the
per-org primary-source distribution:

```sql
SELECT source, count() FROM work_item_team_attributions FINAL
WHERE org_id = {org} AND is_primary = 1 GROUP BY source
```

Diff it against the same query after step 5. This is a prerequisite, not an optional nicety
(`docs/contribute/architecture/team-attribution.md` §5).

| Command | Source | When to use |
|---|---|---|
| `dev-hops sync work-items` / `dev-hops backfill run` | `docs/contribute/architecture/team-attribution.md` §5 -- Python CLI trigger shell over the native Go sync-dispatch path | Recompute `work_item_team_attributions` for an org; **must run for ALL providers** (Linear AND GitHub/GitLab) -- Linear-only recomputes nothing, since PR/MR rows and their edges come from the git providers. `--org` is optional (derived from the sync config). |
| `dev-health-workerctl workgraph trigger --org <uuid> [--from <YYYY-MM-DD>] [--to <YYYY-MM-DD>] --review-evidence "<text>" [--dry-run]` | `trigger_workgraph.go:98-283` (CHAOS-5172) | Step 3: enqueue a fresh `workgraph.build` request through the same `workgraph.RequestWriter.WriteTx` path the automatic post-sync/scheduled producers use. |
| `dev-health-workerctl investment trigger --org <uuid> [--from <YYYY-MM-DD>] [--to <YYYY-MM-DD>] --review-evidence "<text>" [--dry-run]` | `trigger_investment.go:73-222` (CHAOS-5173) | Step 4, native path: enqueue a fresh `investment.materialize` request through the native executor. Drops every flag with no Go-side equivalent (`--window-days`, `--repo-id`, `--team-id`, every LLM flag, `--force`, `--persist-evidence-snippets`, `--allow-unscoped`, `--analytics-db`/`--db`) -- only an org id and an optional `--from`/`--to` window exist on the request. |
| `dev-hops investment materialize --force` | `work_graph/runner.py` `run_investment_materialization` | **Legacy** -- separate entry point from the native River kind (CHAOS-4767 tracks removing it). Still the only documented CLI path for step 4 in the team-attribution recovery runbook; `investment trigger` is newer and preferred but not yet confirmed as its drop-in replacement for every case `--force` covers. |
| `dev-health-workerctl providersync retire-linear-pseudo-projects [--org <uuid>] [--dry-run]` | `main.go:1408-1466` (CHAOS-4530 follow-up) | One-time cleanup of `{org_id}:linear:{team_key}` pseudo-project rows in `projects`. Destructive (physical delete), authorized before any ClickHouse call is attempted. |
| `dev-health-workerctl providersync retire-stale-linear-project-ownership [--org <uuid>] [--dry-run]` | `main.go:1486-1558` (CHAOS-4548) | One-time cleanup of stale `team_project_ownership` rows still stamped with the old team-key `project_key`. Destructive, same authorization gate as the pseudo-projects cleanup. |
| Team membership resolution (admin override layer) | `docs/contribute/architecture/team-attribution.md` §CHAOS-4321 | Not a CLI command. Admin panel `/org/admin/identities` writes `identities.team_ids`; `teams.manual_members` is the admin-exclusive override roster. No CLI mutation path exists. |

**Team-attribution recovery order** (`docs/contribute/architecture/team-attribution.md` §5): (1) merge +
deploy the mechanism, (2) backfill ALL providers via sync/backfill, (3) work-graph build, (4) investment
materialize `--force` (or `investment trigger`), (5) verify via the query-time join (coverage %, chord). The
backfill runner only re-runs the sync job -- it does **not** fan out to work-graph or investment
automatically; both must be triggered explicitly.

Attribution precedence (for diagnosing a "wrong team" symptom): repo/project ownership → 2-layer membership
resolution (admin override in `identities`/`teams.manual_members`, else provider-imported
`team_memberships`/`teams.members`) → `linked_issue` inheritance → `author_membership` → `manual_fallback`.
9 total precedence tiers; `docs/contribute/architecture/team-attribution.md` §0.1/§0.2 is authoritative.

## (d) Workgraph rebuild (issue-PR links, operational edges, pr_commit)

| Command | Source | When to use |
|---|---|---|
| `dev-health-workerctl workgraph trigger ...` | `trigger_workgraph.go:98-283` (CHAOS-5172) | Enqueue a FRESH `workgraph.build` request through the same coordinator path the automatic producers use, instead of a second, unguarded Python compute. |
| `dev-health-workerctl investment trigger ...` | `trigger_investment.go:73-222` (CHAOS-5173) | Enqueue a fresh `investment.materialize` request through the native executor (`internal/jobs/investment/nativeexecutor.go`). |
| `dev-hops work-graph build` | `work_graph/runner.py` `run_work_graph_build` | **Legacy** -- direct Python compute, bypasses the worker's own dispatch/idempotency. Prefer `workgraph trigger`. |
| `dev-hops investment materialize [--force]` | `work_graph/runner.py` `run_investment_materialization` | **Legacy**, separate entry point from the native River kind (CHAOS-4767 tracks removal). |

See §(c) above for the full ordered recovery sequence these two commands participate in.

## (e) Sync / re-ingest from providers

Provider sync raw ingestion is essentially 100% NATIVE for every provider/dataset pair (github, gitlab,
jira, linear, launchdarkly, pagerduty -- see `docs/go-migration-matrix.md` SYNC's generated table; the one
exception is jira team-membership auto-import, still Python, CHAOS-4198). `dev-hops sync <git|prs|blame|
cicd|deployments|incidents|teams|work-items>` is an operator-trigger shell over the same native Go
sync-dispatch path (`sync_processor.register_commands`) -- the CLI verb dispatches through the native path,
it is not itself a Python compute engine, unlike the metrics CLI verbs in §(a).

Run `dev-hops sync --help` for the exact current flag syntax before using it in prod.

Incremental-window/backfill semantics (watermarks, heavy-dataset window ratchet, corrupt-watermark recovery)
are documented in [Ingestion and backfills](../run/ingestion-and-backfills.md), which describes planner
behavior rather than giving concrete invocations.

## (f) Full reset / recompute from scratch

**No single "wipe and re-sync" command exists.** Every command above is scoped to an (org, day-range[,
family]) tuple. **Not rehearsed, no timings** -- this is a composed sequence assembled from the commands
above, not a tested runbook. Dry-run it against a non-prod target first.

1. `migrate` (Alembic + ClickHouse) → `go-river-provision` (grants) → `go-river-migrate` (River schema) →
   `go-contractcheck` → workers/reconciler/scheduler/stream runners. This ordering is a dependency chain, not
   a convention -- see [Run workers and jobs § Deploy the Go fleet in order](../run/workers-and-jobs.md#deploy-the-go-fleet-in-order).
2. Re-run `dev-hops sync <dataset>` (or the native sync-dispatch path directly) per provider, per org --
   raw ingestion, native.
3. `metrics daily-start` (or the automatic post-sync fanout) per org/day-range -- daily metrics, mostly
   native.
4. `workgraph trigger` then `investment trigger` (or `dev-hops investment materialize --force`) -- per
   §(c)'s ordering, with the snapshot-first step from §(c) taken before step 2.
5. Team ownership/attribution falls out of steps 2-3 automatically (native `sync.team_repo_ownership_derivation`);
   admin overrides in `identities`/`teams.manual_members` are **not** re-derivable from providers and must
   be re-entered by hand.

**Not re-derivable on a wipe** (found, not exhaustive -- back these up separately if a wipe is ever planned):

- **Admin-authored team-attribution overrides**: `identities.team_ids` and `teams.manual_members` (ClickHouse,
  not Postgres) -- written only through `/org/admin/identities` and the admin Identities screen /
  drift-approval flow, never by any sync/import path.
- **Minted operator credentials**: `WORKER_METRIC_REPAIR_TOKEN`, `WORKER_OPERATOR_TOKEN`, and any other
  minted secret -- not sync-derived data. A wipe of the credential volume (`go_worker_operator_token`)
  requires re-minting; a stale cached operator token has already caused a real `authentication_failed` block
  during a redrive.
- **Audit history / operator action log** (`joboperator.Service`'s Action/audit pipeline records) -- no
  provider re-sync recreates a record of past operator interventions.
- **Historical ledger state** (`daily_metrics_partitions`/`daily_metrics_runs`/`metric_compatibility_executions`)
  -- process bookkeeping, not reconstructible from providers; a fresh compute gets a different
  run/partition/execution id lineage.

Everything else (raw provider entities, computed daily/remaining metrics, team ownership derivation,
workgraph edges, investment quotes) is re-derivable, since sync/metrics/ownership/workgraph/investment are
all on native or bridge-triggerable recompute paths today.

## See also

- [Worker or queue failure](worker-or-queue-failure.md) -- symptom-driven triage and recovery.
- [Run workers and jobs](../run/workers-and-jobs.md) -- starting/rolling out the Go worker fleet itself.
- [Job recovery lifecycle](../run/job-recovery-lifecycle.md) -- when River rescues a stuck job on its own.
- [Go migration matrix](../../go-migration-matrix.md) -- generated Go/Python executor status per family;
  regenerate via `scripts/gen_go_migration_matrix_docs.py`, never hand-edit its generated blocks.
- [Team attribution architecture](../../contribute/architecture/team-attribution.md) -- precedence tiers and
  the full recovery narrative.
- [Backup and restore](../maintain/backup-and-restore.md) -- `scripts/backup-standing.sh` covers the
  local/dev standing stack only; it explicitly must never be run back onto the live standing stack, and a
  live-prod restore is not yet scripted (CHAOS-4091).
