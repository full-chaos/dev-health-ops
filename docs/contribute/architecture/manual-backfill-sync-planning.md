---
page_id: con-manual-backfill-sync-planning
summary: How a manual Sync Now or Backfill request reaches the Go scheduler planner instead of Python's plan_sync_run, and the rollout flag that scopes it to planner-managed configs only.
content_type: architecture
owner: engineering
source_of_truth:
  - internal/scheduler/sync/backfill_planner.go (BuildBackfillPlan)
  - internal/scheduler/sync/materializer.go (loadMaterializationPlan's sync_manual_triggers lookup, Materialize's mode routing)
  - src/dev_health_ops/sync/execution_trigger.py (create_sync_execution_trigger's Go hand-off branch, await_sync_execution_trigger_materialized)
  - src/dev_health_ops/alembic/versions/0118_add_sync_manual_triggers.py
applicability: current
lifecycle: active
---

# Manual Sync Now + Backfill planning architecture

A manual "Sync Now" or an operator/admin Backfill request used to plan units
by calling Python's `plan_sync_run` in-process, synchronously, inside the
admin API request. CHAOS-4602 moves that planning into the Go scheduler --
for **planner-managed configs only** -- behind a rollout flag, reusing the
same occurrence identity space and pickup path a cron tick already uses.
{: .fc-page-lede }

## Scope: planner-managed parent configs only

A non-planner-managed config (a child config pinned to one explicit
`source_id`) still plans via the unchanged, untouched `plan_sync_run` call.
The routing decision lives in one place --
`create_sync_execution_trigger` (`execution_trigger.py`) checks
`config.planner_managed AND SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED` (env
flag, default OFF at launch, flipped PERMANENTLY ON by CHAOS-4629 landing --
chris ruling, 2026-08-31) before choosing the Go hand-off branch over the
pre-existing in-process call. It is also enforced structurally, not just by
that routing check: `NativeMaterializer.Materialize`'s own eligibility gate
rejects any occurrence whose `ConfigPlannerManaged` is false
(`ErrOccurrenceIneligible`), so a routing bug cannot silently mis-plan a
child config through the Go path -- it can only fail loud. Porting the
non-planner-managed case is tracked separately (CHAOS-4604).

## The seam: one new table, one reused table

`scheduled_sync_occurrences` (migration 0050) already holds the stable
identity and plan links for a cron-minted occurrence, and the Go
reconciler's pickup query (`internal/scheduler/sync/occurrence_reconciler.go`'s
`dueOccurrenceKeysSQL`/`lockPendingOccurrenceSQL`) is producer-agnostic --
it does not care whether Python or Go inserted the row. CHAOS-4602 reuses
this table verbatim for a manual/backfill occurrence and adds exactly one
new table, `sync_manual_triggers` (migration 0118), 1:1 with one occurrence:
the mode override (`incremental`/`full_resync`/`backfill`) and, for
backfill, the selector (`since`/`before`/`source_ids`/`dataset_keys`) and
`triggered_by` (`manual`/`backfill`) that carries through verbatim to the
resulting `sync_runs.triggered_by`.

`loadMaterializationPlan` (`materializer.go`) looks up this table by
occurrence_id before deciding mode or eligibility. Finding a row makes its
mode authoritative over the config's own persisted `sync_options`, and
skips the `schedule_cron`-required check entirely -- this is what makes an
intentionally UNSCHEDULED, planner-managed config eligible for a manual
trigger at all (the motivating case: an unscheduled Jira config with no
explicit repo/project scope). Finding no row leaves every check exactly as
it was for an ordinary cron tick.

## The planner: BuildBackfillPlan

`BuildScheduledPlan` explicitly rejects backfill mode
(`ErrBackfillScheduled`). `BuildBackfillPlan` (`backfill_planner.go`) is its
pure sibling, porting Python's `_build_planned_units` +
`_resolve_windows`'s BACKFILL branch (`_backfill_windows`/
`_chunk_to_window`) and both family folds
(`_build_work_item_family_units`/`_build_fold_family_units`) for the one
real difference backfill introduces: a dataset's window resolution can fan
out into many chunked windows (one `PlannedUnit` each, via `ChunkDateRange`)
instead of the single watermark-derived window every other mode plans. It
is verified against the live Python planner (parity oracle,
`planner_oracle_test.go::TestBuildBackfillPlanMatchesLivePythonPlanner`),
not just a hand-copied expectation table.

`Materialize` routes `Mode == "backfill"` to `BuildBackfillPlan` and every
other mode to `BuildScheduledPlan`, unchanged.

## Deterministic IDs: no pre-created JobRun

Go derives `job_run_id`/`sync_run_id` deterministically from `occurrence_id`
(`deterministicMaterializationIDs`, `uuid.NewSHA1` under a fixed namespace
UUID) -- the same derivation a cron-minted occurrence already used. Python's
Go hand-off branch (`_create_go_manual_sync_execution_trigger`) therefore
does **not** pre-create a `JobRun` row the way the legacy in-process path
does (`ensure_pending_sync_job_run`): there is nothing for a
pre-created id to match, and Go's `persistDomainGraph`/`persistCoordinatorGraph`
write the real rows once the occurrence reconciles.

## The response contract: bounded, async-native await

The admin router commits the occurrence + trigger insert, then bounds the
wait on Go materializing it via `await_sync_execution_trigger_materialized`
(modeled on `await_reference_discovery_terminal`'s typed-outcome shape, but
deliberately **not** using its blocking `time.sleep` mechanism: this
function runs inside the admin router's real async request/response cycle,
where a blocking sleep would stall the whole event loop for every
concurrent request, not just this one -- it uses `asyncio.sleep` between
short `session.run_sync` reads instead). Three outcomes:

- **materialized** -- the same `sync_run_id`-present response shape the
  in-process path already returns.
- **pending** (deadline, `SYNC_MANUAL_TRIGGER_AWAIT_SECONDS`, default 10s)
  -- `{"status": "pending", "occurrence_id": ...}`, never an error; the
  occurrence keeps reconciling in the background.
- **failed** -- Go quarantined the occurrence (identity conflict or retry
  exhaustion) -- a client-visible failure, never folded into a silent
  pending.

## Known limitation, ported anyway

Python's `_is_non_project_jira_source` (CHAOS-4582) guards against a
legacy-shape Jira source row (`external_id` literally `"jira"`, no
explicit-project-scope marker) that is guaranteed to 400 against Jira's
work-items route. This guard predates CHAOS-4602's own source discovery and
such rows already exist in production (and locally) independent of who
wrote them, so it is ported here too, as `PlanSource.NonProjectJiraSource`
-- resolved by `loadPlanSources` (which has the DB access the check needs)
and read by the otherwise-pure planner as a plain bool.
