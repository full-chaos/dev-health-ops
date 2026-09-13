#!/usr/bin/env python3
"""Render the generated Python<->Go live-path tables in the ledger page.

CHAOS-4433: "This is what I mean when I say python <> go compatibility isn't
tracking." (chris, 2026-08-28). Two Done tickets (CHAOS-4323, CHAOS-3716) were
read as "ported" while the live writers stayed Python, because nothing in the
repo recorded, per job kind / bridge route / Python worker module, who
produces, what gates it, and who actually writes each table today.

Same shape as ``scripts/gen_queue_mapping_docs.py`` (CHAOS-4044): the
mechanical facts (which kinds/routes/files currently exist) are read straight
from the producers below; the curated columns (producer/writer file:line,
trigger, gate, tables, evidence, state, ticket) are hand-authored in the three
``_LEDGER`` dicts in this file, cited per row. Three consistency guards below
make that curation honest: the generator raises ``SystemExit`` -- refuses to
render -- the moment a kind, bridge route, or worker module appears in the
live producer that has no curated row, or a curated row that no longer
matches a live producer. That is the drift gate CHAOS-4433 item 3 asks for:
add/remove/rename a kind in ``contracts/jobs/v1/registry.json``, a route in
``internal/syncdispatchruntime/bridge.go``, or a file under
``src/dev_health_ops/workers/*.py`` without a matching ledger row here, and
this script -- and therefore
``tests/docs/test_python_go_ledger_drift.py`` -- fails loudly.

Evidence for every curated row was gathered read-only against this worktree
(``rg``/``codegraph``/direct file reads) plus, for the team-items family, a
ClickHouse ``system.query_log`` readback against local REAL data (org
``70d529e0-3c06-4597-8480-794fd02328b6``) re-executed 2026-08-28 -- see the
"local" evidence entries below and the page's "How this was verified"
section. Anything marked ``unverified (argued)`` was not re-executed this
session and must be re-proven before being relied on for a build/no-build
decision.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "contracts" / "jobs" / "v1" / "registry.json"
MIGRATION_STATE_PATH = ROOT / "contracts" / "jobs" / "v1" / "migration-state.json"
BRIDGE_PATH = ROOT / "internal" / "syncdispatchruntime" / "bridge.go"
WORKERS_DIR = ROOT / "src" / "dev_health_ops" / "workers"
# CHAOS-4602: the native per-occurrence source (repo/project) discovery step
# is neither a River job kind, a bridge.go route, nor a workers/*.py file --
# it is an API-request-time / materializer-time side effect, which is exactly
# why the by-mechanism audits that built the three tables below missed it
# entirely (CHAOS-4602's own executed finding). sourceDiscoveryProviders is
# its own small, closed registry, mechanically enumerable the same way.
SOURCE_DISCOVERY_PATH = ROOT / "internal" / "scheduler" / "sync" / "source_discovery.go"
DOC_PATH = ROOT / "docs" / "reference" / "runtime" / "python-go-live-path-ledger.md"

KIND_BEGIN = "<!-- BEGIN GENERATED KIND LEDGER -->"
KIND_END = "<!-- END GENERATED KIND LEDGER -->"
ROUTE_BEGIN = "<!-- BEGIN GENERATED BRIDGE ROUTE LEDGER -->"
ROUTE_END = "<!-- END GENERATED BRIDGE ROUTE LEDGER -->"
WORKER_BEGIN = "<!-- BEGIN GENERATED WORKER FILE LEDGER -->"
WORKER_END = "<!-- END GENERATED WORKER FILE LEDGER -->"
SOURCE_BEGIN = "<!-- BEGIN GENERATED SOURCE DISCOVERY LEDGER -->"
SOURCE_END = "<!-- END GENERATED SOURCE DISCOVERY LEDGER -->"

BRIDGE_CALL_RE = re.compile(r'bridge\.(?:call|callWithResult)\(ctx, "([^"]+)"')
SOURCE_DISCOVERY_PROVIDERS_RE = re.compile(
    r"var sourceDiscoveryProviders = map\[string\]bool\{(?P<body>.*?)\n\}",
    re.DOTALL,
)
SOURCE_DISCOVERY_PROVIDER_KEY_RE = re.compile(r'"(\w+)":\s*true')


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_registry_kinds() -> set[str]:
    data = _load_json(REGISTRY_PATH)
    return {job["kind"] for job in data["jobs"]}


def load_migration_state() -> dict[str, dict]:
    data = _load_json(MIGRATION_STATE_PATH)
    return {job["kind"]: job for job in data["jobs"]}


def load_bridge_routes() -> set[str]:
    text = BRIDGE_PATH.read_text(encoding="utf-8")
    return set(BRIDGE_CALL_RE.findall(text))


def load_worker_files() -> set[str]:
    return {p.name for p in WORKERS_DIR.glob("*.py")}


def load_source_discovery_providers() -> set[str]:
    text = SOURCE_DISCOVERY_PATH.read_text(encoding="utf-8")
    match = SOURCE_DISCOVERY_PROVIDERS_RE.search(text)
    if match is None:
        raise SystemExit(
            "gen_python_go_ledger_docs: sourceDiscoveryProviders map literal not "
            f"found in {SOURCE_DISCOVERY_PATH}"
        )
    return set(SOURCE_DISCOVERY_PROVIDER_KEY_RE.findall(match.group("body")))


# ---------------------------------------------------------------------------
# CURATED: one entry per River job kind (contracts/jobs/v1/registry.json).
# Evidence gathered 2026-08-28 (CHAOS-4433 lane, read-only rg/codegraph pass
# over this worktree at ops main tip 7ea3cad23, plus a re-executed CH
# query_log readback for the team-items rows -- see doc "local" citations).
# ---------------------------------------------------------------------------
KIND_LEDGER: dict[str, dict[str, str]] = {
    # --- investment family ---------------------------------------------
    # investment.chunk/dispatch/finalize rows REMOVED under CHAOS-4438: these
    # were dead Go shells (wired, never invoked, zero producers) documented
    # here as such -- the kinds are now deleted outright from registry.json,
    # jobcontract, and workgraph, not merely dead. A ledger row for a kind
    # gen_python_go_ledger_docs's own consistency guard cannot see in the live
    # producer would itself become the stale-citation defect class this file
    # exists to prevent (see CHAOS-5153, the sibling matrix generator's
    # analogous reverse guard).
    "investment.materialize": {
        "producer": "`internal/syncdispatchruntime/native_post_sync.go:230-232` (`workGraph.StartRequestTx`)",
        "trigger": "post-sync",
        "gate": "`plan.Investment` (`native_post_sync.go:610`, set when `git \\|\\| hasWorkItems`)",
        "writer": "Go-native. `cmd/dev-health-worker/workgraph.go` `buildNativeInvestmentExecutor` constructs `investment.NewNativeExecutor` (`internal/jobs/investment/nativeexecutor.go:58`) over a ClickHouse reader/writer, and `addWorkgraphWorker`'s materialize case REFUSES the kind (`errWorkerDependencyUnavailable`) when that executor is nil rather than falling back -- the HTTP compatibility variant was deleted by CHAOS-3092 (#2352). The interface it satisfies is `workgraph.NativeExecutor` (renamed from the bridge-era `CompatibilityExecutor` by CHAOS-5398).",
        "tables": "ClickHouse `work_unit_investments`, `work_unit_repo_effort`, `work_unit_investment_quotes` (`src/dev_health_ops/metrics/sinks/clickhouse/investment.py:117-186`)",
        "evidence": "argued \u2014 code read; `rg` over `cmd/dev-health-worker/workgraph.go` and `internal/jobs/workgraph` finds no HTTP compatibility executor and no bridge POST for this kind",
        "state": "native",
        "ticket": "n/a \u2014 native since CHAOS-4441/#2227; CHAOS-3092 (#2352) deleted the dead HTTP variant, and CHAOS-3092 (leftovers) deleted `worker_workgraph.py`'s POST /execute route plus the plain (unchunked) Celery task it called (`run_investment_materialize`) -- migration-state.json's `rollback_route` for this kind is now `none`, not `celery`. The Python `materialize_investments()` compute itself is retained: still reachable via the separate `dispatch_investment_materialize_partitioned` -> `run_investment_materialize_chunk` chord (`post_sync_dispatch.py`, `external_ingest/recompute.py`); CHAOS-4767 owns its removal.",
    },
    # --- metrics daily family ---------------------------------------------
    "metrics.daily_dispatch": {
        "producer": "`internal/scheduler/fixed/producers.go:442` (fanout) + `native_post_sync.go` `DailyPostSyncWriter.StartRunTx`",
        "trigger": "post-sync + fixed schedule (backstop)",
        "gate": "none found (queue-selected only, `cmd/dev-health-worker/daily.go:36`)",
        "writer": "Go `internal/jobs/metrics/daily/postgres.go` (`daily.NewDispatcher`, wired `daily.go:135`)",
        "tables": "`public.daily_metrics_runs` (`postgres.go:219,296`), `public.daily_metrics_partitions` (`postgres.go:263`)",
        "evidence": "argued — code read, not re-executed this session",
        "state": "native",
        "ticket": "n/a (orchestration only; per-family compute tracked on `metrics.daily_partition`)",
    },
    "metrics.daily_partition": {
        "producer": "River worker `cmd/dev-health-worker/daily.go` (`daily.NewPartitionHandler(store, publisher)` -- no compatibility executor parameter)",
        "trigger": "driven by dispatch/run rows",
        "gate": "none (family selection is unconditional fan-out)",
        "writer": "Go-native, every family. CHAOS-3092 (PR-A) deleted the Python compatibility bridge outright: `internal/jobs/metrics/daily/compatibility_http.go`, the `daily.CompatibilityExecutor` interface, the `ComputePartition` call in `PartitionHandler.Work`, the skip-families negotiation, and the Python route `POST /internal/worker/daily-metrics/v1/execute` are all gone. Every `internal/jobs/metrics/daily/families.json` family is computed by a registered `NativeFamilyExecutor` built in `cmd/dev-health-worker/daily.go` (`dailyNativeFamilyRegistrations`); a family whose executor cannot be constructed is a worker STARTUP ERROR naming the family and its cause, never a silent fall-open, and a family that fails at runtime holds the whole partition `failed` (re-dispatchable) rather than completing over the gap. `families.json`'s `pre_bridge`/`post_bridge` phase names are historical: the boundary they once named was the bridge call, and what survives is only the ORDERING (a family whose input another family writes in the same partition runs second, e.g. `work_item_state` after `work_item_attribution`, `compounding_risk` after `repo_user_commit`).",
        "tables": "`public.daily_metrics_partitions` plus per-family ClickHouse output tables",
        "evidence": "argued \u2014 code read; `rg` over `internal/jobs/metrics/daily` and `src/dev_health_ops/api/internal/worker_metrics.py` finds no bridge call site or daily execute route",
        "state": "native",
        "ticket": "CHAOS-3092 (PR-A) -- bridge deleted; per-family Python compute deleted by its own child tickets",
    },
    "metrics.daily_finalize": {
        "producer": "River worker `cmd/dev-health-worker/daily.go` (`daily.NewFinalizeHandler(store)`)",
        "trigger": "follows partition completion (same run)",
        "gate": "none found",
        "writer": "Go-native. CHAOS-3092 PR-A' deleted the bridge's `Finalize` call and the Python finalize branch of the daily execute route; PR-A deleted the rest of the bridge. Run bookkeeping is Go (`internal/jobs/metrics/daily/postgres.go`), and every finalize-scope family (`ic_finalize`, `team_cognitive_load`, `team_complexity`, `benchmarking`, `compounding_risk_team`) is a registered `NativeFinalizeFamilyExecutor`. A recognised finalize family with NO registered executor fails the run loudly with `ErrFinalizeFamilyIncomplete` instead of silently leaving its rows unwritten.",
        "tables": "`public.daily_metrics_runs` (Go); ClickHouse `user_metrics_daily` (`job_daily.py:2125 write_user_metrics` -> `wellbeing.py`'s sink), `ic_landscape_rolling_30d`, and team-level metric tables (Python finalize compute -- corrected 2026-08-28 per codex review across 2 rounds: an earlier draft omitted the Python writer and its output tables entirely, then a follow-up correction still missed `user_metrics_daily`)",
        "evidence": "argued — code read, not re-executed this session",
        "state": "native",
        "ticket": "CHAOS-3092 (PR-A'/PR-A)",
    },
    # --- metrics remaining family ---------------------------------------------
    "metrics.remaining.capacity": {
        "producer": "`internal/scheduler/fixed/inventory.go:197` (capacity_forecast_weekly_fanout)",
        "trigger": "schedule (WeeklyAt Mon 04:00 UTC)",
        "gate": "ClickHouse schema check in `NewCapacityExecutor` (`internal/jobs/metrics/remaining/capacity_native.go:94`)",
        "writer": "Go `internal/jobs/metrics/remaining/capacity_native_clickhouse.go:243`",
        "tables": "`capacity_forecasts`",
        "evidence": "argued — code read; wired `cmd/dev-health-worker/daily.go:359-393,401-410`",
        "state": "native",
        "ticket": "n/a — Python `job_capacity.py` (`run_capacity_forecast`) and its `worker_metrics.py` `_run_capacity` dead-code caller are both DELETED entirely (CHAOS-5336); no live Python producer remains",
    },
    "metrics.remaining.complexity": {
        "producer": "`internal/scheduler/fixed/inventory.go:58` (complexity_daily_fanout)",
        "trigger": "schedule (DailyAt 00:45 UTC)",
        "gate": "ClickHouse schema check in `NewComplexityExecutor` (`internal/jobs/metrics/remaining/complexity_native_clickhouse.go:44`, `verifyComplexitySchema`)",
        "writer": "Go `internal/jobs/metrics/remaining/complexity_native_clickhouse.go` (`writeFileComplexitySnapshots`, `writeRepoComplexityDaily`)",
        "tables": "`file_complexity_snapshots`, `repo_complexity_daily`",
        "evidence": "argued — wired `daily.go:486-527`",
        "state": "native",
        "ticket": "n/a — Python `run_complexity_db_job`/`job_complexity_db.py` DELETED entirely (CHAOS-4291); no per-day helper survives as a fixtures/runner.py dependency, unlike release_impact -- fixtures/runner.py now seeds file_complexity_snapshots/repo_complexity_daily from the frozen Go-executor parity golden JSON instead (a plain load, no Python compute left to call)",
    },
    "metrics.remaining.dora": {
        "producer": "`internal/scheduler/fixed/inventory.go:147` (dora_daily_fanout)",
        "trigger": "schedule (DailyAt 02:15 UTC) + post-sync",
        "gate": "ordering/schema checks in `NewDORAExecutor` (`internal/jobs/metrics/remaining/dora_native.go:108`)",
        "writer": "Go `internal/jobs/metrics/remaining/dora_native_clickhouse.go:379`",
        "tables": "`dora_metrics_daily`",
        "evidence": "argued — wired `daily.go:315-353,415-427`",
        "state": "native",
        "ticket": "n/a — Python `job_dora.py` (`run_dora_metrics_job`) and its `worker_metrics.py` `_run_dora` dead-code caller are both DELETED entirely (CHAOS-5336); no live Python producer remains",
    },
    "metrics.remaining.membership_backfill": {
        "producer": "`internal/scheduler/fixed/inventory.go:176` (membership_backfill_daily_fanout)",
        "trigger": "schedule (DailyAt 03:30 UTC, safety net) + event-driven post-sync materializer (primary)",
        "gate": "schema check in `NewMembershipExecutor` (`internal/jobs/metrics/remaining/membership_native.go:99`); no readiness gate of its own -- the scheduler-level RequiresGraphBuild prerequisite (`internal/scheduler/fixed/producers.go`) already withholds the partition until the org's work-graph build has durably completed",
        "writer": "Go `internal/jobs/metrics/remaining/membership_native_clickhouse.go:101` (`ComputeOrg`)",
        "tables": "`work_unit_membership`, `work_unit_membership_runs`, `work_unit_membership_scoped_runs`",
        "evidence": "argued — wired `daily.go:469-509,546-556` (re-verified against the #2173 recommendations-native-registration merge-main tip: line numbers moved again when that PR's own recommendations block was resolved ahead of this one during the merge; round-3 codex finding, #2177, is the reason this citation exists at all -- prior citation pointed at unrelated capacity-family lines)",
        "state": "native",
        "ticket": "CHAOS-4282 -- Python `backfill_memberships` (`work_graph/investment/backfill.py:176`) retired to the cutover",
    },
    "metrics.remaining.work_item_attribution": {
        "producer": "`internal/scheduler/fixed/inventory.go:197` (work_item_attribution_daily_fanout)",
        "trigger": "schedule (DailyAt 02:30 UTC) -- no post-sync trigger, this schedule is the only one",
        "gate": "ClickHouse schema check in `NewWorkItemAttributionExecutor` (`internal/jobs/metrics/remaining/work_item_attribution_native.go:111`)",
        "writer": "Go `internal/jobs/metrics/remaining/work_item_attribution_write.go:130`",
        "tables": "`work_item_team_attributions`, `work_item_attribution_backstop_runs`, `work_item_attribution_backstop_scoped_runs`",
        "evidence": "argued — code read; wired `cmd/dev-health-worker/daily.go:415-464,511-538`",
        "state": "native",
        "ticket": "CHAOS-3092 PR-B -- Go-native from birth, no Celery predecessor (retired Python daily sweep unconditionally re-derived every item; this is a scoped staleness-window backstop, not a port)",
    },
    "metrics.remaining.recommendations": {
        "producer": "`internal/scheduler/fixed/inventory.go:127` (recommendations_daily_fanout)",
        "trigger": "schedule (DailyAt 02:00 UTC, safety net behind a finalize-gated primary trigger)",
        "gate": "schema check in `NewRecommendationsExecutor` (`internal/jobs/metrics/remaining/recommendations_native.go:160`) + the CHAOS-2373 daily-metrics readiness gate inside `ComputePartition` (`recommendations_native.go:377,428`)",
        "writer": "Go `internal/jobs/metrics/remaining/recommendations_native_clickhouse.go:134`",
        "tables": "`recommendations_daily`",
        "evidence": "argued — wired `daily.go:421-461,557-567` (re-verified against the merge of origin/main into this branch; line numbers moved when #2177's membership block landed ahead of this one during conflict resolution)",
        "state": "native",
        "ticket": "n/a — the Go worker no longer routes this kind to the bridge (daily.go); verified nothing else reaches worker_metrics.py's compatibility handler either, so it and its `_REMAINING_RUNNERS` dispatch entry are deleted. `_compute_recommendations_for_org` (`workers/recommendations_tasks.py:333`) is retained as a directly-unit-tested compute path (see this file's own WORKER_FILE_LEDGER['recommendations_tasks.py'], now category d) -- tests/test_recommendations_task.py is its only remaining caller",
    },
    "metrics.remaining.release_impact": {
        "producer": "`internal/scheduler/fixed/inventory.go:75` (release_impact_daily_fanout)",
        "trigger": "schedule (DailyAt 01:30 UTC)",
        "gate": "ClickHouse schema check in `NewReleaseImpactExecutor` (`internal/jobs/metrics/remaining/release_impact_native_executor.go:69`) -- verifies release_impact_daily's engine, version column, and sorting key (`verifyReleaseImpactSchema`, `release_impact_native_clickhouse.go`)",
        "writer": "Go `internal/jobs/metrics/remaining/release_impact_native_clickhouse.go` (`writeReleaseImpactRows`)",
        "tables": "`release_impact_daily`",
        "evidence": "argued — wired `daily.go:590-621`",
        "state": "native",
        "ticket": "n/a — Python `run_release_impact_job`/`job_release_impact.py` DELETED entirely (CHAOS-5234/CHAOS-5244); `release_impact.py`'s per-day helper (`_compute_day`) and `write_release_impact_daily` survive only as `fixtures/runner.py`'s local/CI fixture-data-generation dependency (an unrelated live caller, not a golden-comparison artifact) -- CHAOS-5250 tracks porting that path to the Go executor and deleting `_compute_day`",
    },
    # --- operational / system / report / sync family ---------------------------------------------
    "operational.billing_notification": {
        "producer": "`cmd/dev-health-worker/operational.go:120-131`",
        "trigger": "manual (billing event enqueues run)",
        "gate": "`descriptor.Executable()` (route=river)",
        "writer": "Go `internal/jobs/operational/billinghandler.go` (`BillingHandler.Work`) -- owns the completion fence, the owner-email lookup, all seven email renderings and the provider send; no Python callback",
        "tables": "`public.billing_notifications` (Go) for the `claimed_at`/`completed_at` completion fence; `public.users`/`public.memberships`/`public.organizations` read-only for the owner lookup",
        "evidence": "argued — code read + `internal/jobs/operational/testdata/billing_email/*.json` golden fixtures generated from the retired Python renderer",
        "state": "native",
        "ticket": "n/a — Python `system_ops.py send_billing_notification`, its fence helpers, `api/services/billing_emails.py` and the `/api/internal/worker-operational/billing` route DELETED entirely (CHAOS-5353)",
    },
    "operational.webhook_delivery": {
        "producer": "`cmd/dev-health-worker/operational.go:132-143`",
        "trigger": "manual (webhook receipt enqueues)",
        "gate": "`descriptor.Executable()` (route=river)",
        "writer": "Go `internal/jobs/operational/handler.go` (`WebhookHandler.Work`) -- routes natively to `SyncDispatchWriter.TriggerScopedSync` or an explicit counted ignore (`recordIgnoredWebhookEvent`); no Python callback",
        "tables": "`public.scheduled_sync_occurrences`/`public.sync_manual_triggers` (Go) via the native sync-dispatch path; `github_app_installations`/`github_app_events` (Go) for the two native GitHub App event types",
        "evidence": "argued — code read",
        "state": "native",
        "ticket": "CHAOS-5320 (this PR) — `system_webhooks.py:63 process_webhook_event` and the HTTP bridge it dispatched to are deleted entirely",
    },
    "report.execute_on_demand": {
        "producer": "`cmd/dev-health-worker/reports.go:36-96` (`buildReportWorker`)",
        "trigger": "manual (on-demand report request)",
        "gate": "`descriptor.Executable()` (route=river); both report kinds gated together (`reports.go:57`)",
        "writer": "Go `internal/jobs/report/runtime.go:18-40` (`NewClickHouseQueryAdapter`/`NewDeterministicRenderer`/`NewSHA256ArtifactAdapter`/`NewInAppNotificationAdapter`)",
        "tables": "`report_runs` (Postgres) + generated artifact row",
        "evidence": "argued — code read; CUT-03 wired real adapters into the binary per `reports.go:17-20`",
        "state": "native",
        "ticket": "CHAOS-4440 (stale docstring, package doc still claims Celery-routed/dormant)",
    },
    "report.execute_scheduled": {
        "producer": "`cmd/dev-health-worker/reports.go:36-96` (shared build)",
        "trigger": "schedule (cron-driven report)",
        "gate": "same as `report.execute_on_demand`",
        "writer": "same `runtime.go` adapters, `NewScheduledHandler` (`report.go:196`)",
        "tables": "same as `report.execute_on_demand`",
        "evidence": "argued — code read",
        "state": "native",
        "ticket": "CHAOS-4440 (stale docstring)",
    },
    "sync.provider_unit": {
        "producer": "`internal/jobs/providerunit/providerunit.go:538 (*Handler).Work`",
        "trigger": "post-sync (leased unit from sync-run dispatch)",
        "gate": "route=`river_canary` (`Executable()` true for canary too)",
        "writer": "`internal/providersync/repository_postgres.go:144 (*PostgresRepository).Complete`",
        "tables": "`public.sync_run_units`, `public.sync_run_unit_effect_chunks`, `public.sync_run_unit_chunk_checkpoints`",
        "evidence": "argued — code read; no Python call found in the `Work()` path",
        "state": "native",
        "ticket": "n/a — matches migration-state.json canary state, no gap",
    },
    "sync.team_autoimport": {
        "producer": "enqueue: `cmd/dev-health-worker/sync_dispatch.go:195 teamAutoimportPostSyncWriter.PublishTx`, called from `NativePostSyncService.publishTeamAutoimport` (`native_post_sync.go:337-344`) only when `plan.TeamAutoimport` is true; dequeue/consumer: `internal/syncdispatchruntime/worker.go:95 RegisterTeamAutoimportWorker`, wired `sync_dispatch.go:565`. Per-provider dispatch is split BEFORE any bridge call: `teamCatalogAutoimportBridge.TeamAutoImport` (`cmd/dev-health-worker/team_catalog_clients.go:426`) resolves the sync run's own provider (`resolveTeamCatalogProvider`), and if that provider has a registered entry in the `nativeTeamCatalogCollectors` map (`sync_dispatch.go:549`, now linear/github/gitlab/jira) it runs the Go collector directly and never calls `bridge.TeamAutoImport` at all. jira's own collector (`JiraTeamCatalogCollector`, `internal/providersync/jira_team_catalog_route.go`) was the last entry added, closing the map to every provider `team_provider_capabilities()` lists as import-capable. The fallback branch is NOT provider-exclusive at the routing layer -- ANY provider absent from that map reaches the wrapped bridge, which today means only pagerduty and launchdarkly sync runs (a `SyncRunReferenceDiscovery` row is created unconditionally for every sync run, `sync/planner.py:380`, regardless of provider); `_provider_capability` (`team_autoimport.py:45`) is false for both (neither is in `_IMPORTER_MODULES`, `team_autoimport.py:20-24`), so their bridge call is a no-op zero-summary against a route that no longer exists Python-side (see the bridge-route entry below) -- the fallback's `bridge.CoordinatorBridge.TeamAutoImport` HTTP POST, if it were ever reached by a real import-capable provider, would now 404. There is no `exclude_providers` field on the wire; the routing decision is made entirely in Go before any HTTP call, and the bridge request the wrapped `HTTPBridge.TeamAutoImport` (`bridge.go:113`) would send is unconditionally `{organization_id, sync_run_id}` only (`teamAutoImportReference`, `bridge.go:92-96`)",
        "trigger": "post-sync (best-effort, fire-and-forget)",
        "gate": "Go-side: `plan.TeamAutoimport` (`native_post_sync.go:560-577`) = OR of the org's 3 CHAOS-4323 sync_options flags (`auto_import_teams`/`auto_import_projects`/`auto_import_members`) -- when all 3 are false, the job is never enqueued at all, not merely a no-op inside the populator. Provider routing gate: presence of a registered entry in `nativeTeamCatalogCollectors` (now linear/github/gitlab/jira) selects native collector vs. Python bridge fallback per sync run's own provider (see producer column -- no per-call exclude list, a whole sync run either goes native or goes to the bridge). `run_post_sync_team_autoimport`, the Python function this bridge fallback used to reach, is deleted (see the bridge-route entry below) -- every import-capable provider is native now, so there is no live Python-side gate left to describe",
        "writer": "linear/github/gitlab/jira sync runs: Go-native collectors registered in `nativeTeamCatalogCollectors` (`cmd/dev-health-worker/sync_dispatch.go`), writing via `providerfoundation.CredentialResolver` + per-provider ClickHouse effects -- #1989 `27bef7286` (Linear), #1984 `950752653` (GitHub), #1985 `5bff38a5a` (GitLab), jira's own collector completing the set. CHAOS-4498: the operator-triggered backfill tool (`backfill/runner.py`) no longer calls `run_team_autoimport_strict` directly for any provider -- it arms the SAME `sync_run_reference_discoveries` ledger + outbox row sync-time dispatch uses and waits for a terminal outcome, so a backfill reaches `TeamCatalogDiscoveryExecutor` exactly like any other sync run: native collector for every import-capable provider, the same (now permanently no-op for pagerduty/launchdarkly) `Fallback` bridge otherwise. `team_autoimport_{linear,github,gitlab,jira}.py`'s `populate()` functions are therefore no longer execution-reachable via sync-time OR backfill dispatch for any provider",
        "tables": "ClickHouse `teams`, `team_memberships`, `team_project_ownership`, `projects`, `sprints`",
        "evidence": "local — re-armed reference-discovery run, org 70d529e0, 2026-08-29 07:11 UTC (lane-4431-linear-route close-out + CHAOS-4431/4432/4434 close-out comments): outcome `native` for linear with `rows_written` for teams/team_memberships/team_project_ownership; ClickHouse `teams.updated_at` matches the re-arm instant; `system.query_log` writer `clickhouse-go/2.47.0`, not Python's `clickhouse-connect`. jira's own native-collector parity is argued (code read + `go test ./internal/providersync/... ./cmd/dev-health-worker/...`, table-driven fixture parity against `team_autoimport_jira.py`'s test fixtures), not yet re-executed against org 70d529e0 real data from this worktree -- tracked under the same 5.6 prod readback as linear/github/gitlab.",
        "state": "native for linear/github/gitlab/jira, both sync-time and backfill dispatch, among every provider whose populate() ever actually wrote rows -- pagerduty/launchdarkly sync runs still reach the bridge call mechanically but no-op there (not import-capable, see producer column), and that bridge call's Python side no longer exists at all (see the bridge-route entry below)",
        "ticket": "CHAOS-4435 (delete kind/routes/modules now that every provider is native — the backfill blocker CHAOS-4498 closed) + CHAOS-4492 (ledger regen, 5.6 prod readback still pending for jira same as linear/github/gitlab)",
    },
    "sync.team_repo_ownership_derivation": {
        "producer": "native Go post-sync derivation (CHAOS-4365 1b)",
        "trigger": "post-sync",
        "gate": "none (celery_removed, rollback=none)",
        "writer": "Go `TeamRepoOwnershipDerivationService` (`deriveTeamRepoOwnership`, CHAOS-4365)",
        "tables": "`team_repo_ownership` (source=inferred)",
        "evidence": "local — `.remember/python-bridge-route-inventory-2026-08-28.md` EXECUTED VERDICT table; downstream of the Python-written tables above, not a replacement for them",
        "state": "native",
        "ticket": "CHAOS-4365 (Done)",
    },
    "system.heartbeat": {
        "producer": "`cmd/dev-health-worker/operational.go:144-155`",
        "trigger": "schedule",
        "gate": "`descriptor.Executable()` (route=river)",
        "writer": "Python `system_ops.py:22 phone_home_heartbeat`",
        "tables": "Python-owned `audit_logs` row + external `TELEMETRY_ENDPOINT` POST",
        "evidence": "argued — code read; `internal/jobs/system/heartbeat.go:12-33` docstring is accurate and non-stale about this (explicitly says 'CLASSIFICATION: python_compatibility, not native Go')",
        "state": "bridge",
        "ticket": "CUT-20 (code label, no Linear ticket found — see report)",
    },
    "system.retention_cleanup": {
        "producer": "`cmd/dev-health-worker/operational.go:156-167`",
        "trigger": "schedule (per-policy)",
        "gate": "`descriptor.Executable()` (route=river); policy dispatch `retention.go:89-105`",
        "writer": "Go `internal/jobs/system/retention_postgres.go:39,91,168` + `internal/joboutbox/repository.go:265`",
        "tables": "`provider_rate_limit_observations`, `dev_conversations`/`dev_conversation_tombstones`, `external_ingest_batches`, `worker_job_outbox`",
        "evidence": "argued — code read; 4 policies bound, fully Go-native",
        "state": "native",
        "ticket": "n/a — no gap",
    },
    "system.sync_coverage_refresh": {
        "producer": "`internal/jobs/synccoverage/handler.go:34 (*Handler).Work`",
        "trigger": "schedule",
        "gate": "`descriptor.Executable()` (route=river, rollback=none)",
        "writer": "`internal/synccoverage/projector.go:76`",
        "tables": "`sync_coverage_projections`",
        "evidence": "argued — code read; zero Python involvement, matches migration-state.json exactly",
        "state": "native",
        "ticket": "n/a — no gap",
    },
    "workgraph.build": {
        "producer": "`internal/scheduler/fixed/producers.go:826` (startGraphBuild) + `native_post_sync.go:222` (`workGraph.StartRequestTx`)",
        "trigger": "post-sync + fixed-schedule prerequisite (before membership projection)",
        "gate": "none found",
        "writer": "Go-native, entirely. `NewBuildHandler` takes NO executor argument at all (`internal/jobs/workgraph/handler.go`, CHAOS-4924): every stage is a native pre-step (`workgraphBuildPreSteps`, `cmd/dev-health-worker/workgraph.go`) plus Go request/ledger plumbing (`internal/jobs/workgraph/postgres.go`). The absence of a bridge is STRUCTURAL -- a compile-time fact, not a runtime nil-check -- because Python's remaining `build()` compute was already a 0-stats no-op before the cutover.",
        "tables": "`public.work_graph_execution_requests`, `public.work_graph_execution_ledger` (Go); LLM categorization outcome + evidence (Python)",
        "evidence": "argued \u2014 code read; `buildHandler` has no NativeExecutor FIELD, so a bridge call cannot be reintroduced without changing the type",
        "state": "native",
        "ticket": "n/a \u2014 native since CHAOS-4924; CHAOS-3092 (#2352) deleted the dead workgraph HTTP executor and `ExecutorPythonCompatibility`. `worker_workgraph.py`'s POST /execute route (this kind never actually reached it -- see investment.materialize's own ticket note) was itself deleted under CHAOS-3092 (leftovers); no compatibility route remains in this file at all.",
    },
}

# ---------------------------------------------------------------------------
# CURATED: one entry per bridge.go route (internal/syncdispatchruntime/bridge.go).
# ---------------------------------------------------------------------------
BRIDGE_ROUTE_LEDGER: dict[str, dict[str, str]] = {
    "/api/internal/worker-sync/dispatch": {
        "go_caller": "`bridge.go:98 HTTPBridge.Dispatch` — interface method exists but `RegisterWorkers` (`worker.go:72-87`) takes the 4 Native services, never `bridge`; no live registrant calls `.Dispatch()`",
        "python_handler": "`worker_sync.py:205 dispatch_reference` -> `sync_units.py:761 dispatch_sync_run`",
        "computes": "reads/writes `sync_run`, `sync_run_unit`, dispatches units",
        "state": "dead in live wiring — superseded by `NativeDispatchSyncRunService` (CHAOS-4175, Done)",
        "ticket": "CHAOS-4175 (Done)",
    },
    "/api/internal/worker-sync/finalize": {
        "go_caller": "`bridge.go:102 HTTPBridge.Finalize` — same as above, no live registrant",
        "python_handler": "`worker_sync.py:235 finalize_reference` -> `sync_units.py:2053 finalize_sync_run`",
        "computes": "finalizes `sync_run`, coverage-cache invalidation, compute checkpoints",
        "state": "dead in live wiring — superseded by `NativeFinalizeSyncRunService` (CHAOS-4175, Done)",
        "ticket": "CHAOS-4175 (Done)",
    },
    "/api/internal/worker-sync/reference-discovery": {
        "go_caller": "`bridge.go:106 HTTPBridge.Discover` — same, no live registrant",
        "python_handler": "`worker_sync.py:246 reference_discovery_reference` -> `reference_discovery.py:56 run_sync_reference_discovery`",
        "computes": "reference-discovery orchestration (claim/lease/heartbeat/outbox)",
        "state": "dead in live wiring — superseded by `NativeReferenceDiscoveryService` (CHAOS-4175, Done)",
        "ticket": "CHAOS-4175 (Done)",
    },
    "/api/internal/worker-sync/team-autoimport": {
        "go_caller": "`bridge.go:113 TeamAutoImport`, called from `teamCatalogAutoimportBridge.TeamAutoImport` (`cmd/dev-health-worker/team_catalog_clients.go:426`), the wrapper `RegisterTeamAutoimportWorker` (`worker.go:93`) registers, wired `sync_dispatch.go:565`. `teamCatalogAutoimportBridge` resolves the run's own provider and calls its embedded `CoordinatorBridge.TeamAutoImport` (i.e. this route) for ANY provider absent from `nativeTeamCatalogCollectors`, but that map now covers every import-capable provider (linear/github/gitlab/jira) -- this call is unreachable-but-still-wired dead code for all of them, reachable in practice only for pagerduty/launchdarkly (both no-op on the Python side, see the row below). Deliberately left in place rather than ripped out (tracked as CHAOS-4435 scope, not this change): removing it cleanly needs a narrower `CoordinatorBridge` seam than the one `RegisterTeamAutoimportWorker` currently requires",
        "python_handler": "DELETED. `worker_sync.py`'s `/team-autoimport` route and `team_autoimport.py`'s `run_post_sync_team_autoimport` were removed once jira -- the last import-capable provider still reaching this route -- got its own native Go collector (`JiraTeamCatalogCollector`). A POST to this path now 404s; the still-wired Go caller to the left never fires for a real import-capable provider, so this never happens in practice, but if it ever did (a new capability added to a not-yet-native provider) it would fail loudly with a 404 rather than silently succeed or silently no-op",
        "computes": "nothing — Python side deleted. Historically computed the same tables as reference-discovery-populate, best-effort variant, jira sync-time dispatch only",
        "state": "dead — Python route and handler function deleted entirely; the Go caller and its wrapper are unreachable-but-still-wired dead code (every import-capable provider is native)",
        "ticket": "CHAOS-4435 (delete the dead Go wiring — the backfill blocker CHAOS-4498 closed) + CHAOS-4492 (ledger regen)",
    },
}

# ---------------------------------------------------------------------------
# CURATED: one entry per file under src/dev_health_ops/workers/*.py.
# Categories: (a) LIVE, (b) CELERY-TASK-ONLY/DEAD, (c) LIBRARY/SHARED,
# (d) TEST/FIXTURE ONLY.
# ---------------------------------------------------------------------------
WORKER_FILE_LEDGER: dict[str, dict[str, str]] = {
    "__init__.py": {
        "category": "c",
        "evidence": "package marker, no logic",
        "ticket": "n/a",
    },
    "async_runner.py": {
        "category": "c",
        "evidence": "imported by live sync_bootstrap.py, processors/dataset_adapters.py, api/external_ingest/consumer.py, and feature_flag_sync.py (itself LIVE per that file's own row) — 'run coroutine inside Celery task' helper. work_graph_tasks.py, a former importer, was deleted (CHAOS-3093)",
        "ticket": "n/a",
    },
    "celery_app.py": {
        "category": "c",
        "evidence": "imported by ~20 workers files, including live ones, for `@celery_app.task` — Celery app factory, still load-bearing for the decorator even on live functions",
        "ticket": "n/a",
    },
    "config.py": {
        "category": "c",
        "evidence": "used by queues.py, celery_app.py, and api/external_ingest/stream_health.py — env/config constants. queue_monitor.py, external_ingest_reconciler.py, and sync_reconciler.py, former importers, were deleted (CHAOS-3093)",
        "ticket": "n/a",
    },
    "feature_flag_sync.py": {
        "category": "a",
        "evidence": "LIVE — corrected 2026-08-28 per codex review across 2 rounds: an earlier draft wrongly claimed zero importers, then a follow-up correction wrongly said both helpers are called unconditionally. Actual shape: `_run_feature_flags_dataset` (`dataset_adapters.py:684`, called from the live dataset dispatcher at `:758`) branches per provider (`dataset_adapters.py:694-712`) -- calls `_sync_gitlab_feature_flags` for `provider=='gitlab'`, `_sync_launchdarkly_feature_flags` for `provider=='launchdarkly'`, raises `ValueError` for any other provider",
        "ticket": "n/a — live",
    },
    "post_sync_dispatch.py": {
        "category": "a",
        "evidence": "build_post_sync_dispatch_payload called sync_units.py:2274, inside finalize_sync_run (live via worker_sync.py:26)",
        "ticket": "n/a",
    },
    "provider_family_contract.py": {
        "category": "a",
        "evidence": "imported by provider_unit_route.py & sync_units.py:122, reached from dispatch_sync_run",
        "ticket": "n/a",
    },
    "provider_unit_route.py": {
        "category": "a",
        "evidence": "imported sync_units.py:125, used sync_units.py:999-1000 inside dispatch_sync_run",
        "ticket": "n/a",
    },
    "rate_limit_defer.py": {
        "category": "a",
        "evidence": "plan_rate_limit_deferral imported sync_units.py:130-132, called sync_units.py:1514 inside dispatch_sync_run",
        "ticket": "n/a",
    },
    "recommendations_tasks.py": {
        "category": "d",
        "evidence": "worker_metrics.py's _run_recommendations bridge handler (its only production caller) is deleted -- verified no live wiring reaches it; _compute_recommendations_for_org is now exercised only by tests/test_recommendations_task.py",
        "ticket": "n/a",
    },
    "reference_discovery.py": {
        "category": "a",
        "evidence": "imported directly worker_sync.py:22; served by /reference-discovery. The narrower /reference-discovery-populate route this file also used to serve (`run_reference_discovery_populate_for_sync_run`) is deleted -- jira going native closed out every provider that route ever reached for real",
        "ticket": "n/a",
    },
    "sync_bootstrap.py": {
        "category": "a",
        "evidence": "imported by reference_discovery.py/team_autoimport.py/sync_units.py:134; resolve_run_auth reached from dispatch_sync_run",
        "ticket": "n/a",
    },
    "sync_units.py": {
        "category": "a",
        "evidence": "dispatch_sync_run/finalize_sync_run imported worker_sync.py:26, served by /dispatch and /finalize",
        "ticket": "n/a",
    },
    "system_ops.py": {
        "category": "a",
        "evidence": "imported worker_operational.py (phone_home_heartbeat only, serving /heartbeat) — CHAOS-5353 deleted send_billing_notification and its fence helpers from this module",
        "ticket": "n/a",
    },
    "system_tasks.py": {
        "category": "c",
        "evidence": "corrected 2026-09-06 (CHAOS-5320): NOT a dead shim, but for a different reason than before — `api/webhooks/router.py`'s `process_webhook_event` import and `api/billing/router.py`'s `send_billing_notification`/`.delay(...)` call site (both gated behind `route_requires_celery`) are DELETED; `route_requires_celery` itself is deleted (job_routes.py). CHAOS-5353 then deleted `send_billing_notification` itself, so this shim re-exported only `health_check` and `phone_home_heartbeat`. CHAOS-3093 (PR2b) deleted `health_check` outright (no dispatch site of any kind) and dropped `phone_home_heartbeat`'s `@celery_app.task` decorator (Celery has had zero consumers since CHAOS-4026) -- this shim now re-exports only `phone_home_heartbeat`, a plain function. `system_tasks.py`'s only remaining live importer is `workers/tasks.py`'s barrel re-export.",
        "ticket": "CHAOS-4439 (dead worker modules) -- the router-coordination caveat from the prior entry no longer applies",
    },
    "task_utils.py": {
        "category": "c",
        "evidence": "imported by live files (sync_units, reference_discovery, team_autoimport) — shared credential/cache helpers; system_webhooks.py was also an importer until CHAOS-4105 deleted it, and work_graph_tasks.py until CHAOS-3093 deleted it",
        "ticket": "n/a",
    },
    "tasks.py": {
        "category": "b",
        "evidence": "Celery task-name aggregator (__all__ re-export of every task); no api/internal reference — dead since CHAOS-4026",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "team_autoimport_categories.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py (live) and the provider variants",
        "ticket": "n/a",
    },
    "team_autoimport_github.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; ported CHAOS-4434 (`950752653`, Done) — a GitHub sync run's post-sync/reference-discovery dispatch now resolves to the Go-native collector in `nativeTeamCatalogCollectors` and never calls the bridge, so `populate()` is no longer reached via sync-time dispatch. CHAOS-4498 (this PR): the backfill path no longer calls `run_team_autoimport_strict` directly either (it now arms the same ledger/outbox row and routes through `TeamCatalogDiscoveryExecutor`, which resolves GitHub to this same native collector, never the Python populator) — `populate()` in this file is no longer reachable from any live production or operator call path. Not yet deleted: that is CHAOS-4435's scope, now fully unblocked now that jira (the shared kind/route/module's other remaining dependent) is also native",
        "ticket": "CHAOS-4435 (delete — the backfill blocker CHAOS-4498 closed, and jira going native removes the last shared kind/route/module dependency)",
    },
    "team_autoimport_gitlab.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; ported CHAOS-4432 (`5bff38a5a`, Done) — a GitLab sync run's post-sync/reference-discovery dispatch now resolves to the Go-native collector and never calls the bridge, so `populate()` is no longer reached via sync-time dispatch. CHAOS-4498 (this PR): the backfill path no longer calls `run_team_autoimport_strict` directly either — same routing story as `team_autoimport_github.py` above. `populate()` in this file is no longer reachable from any live production or operator call path; deletion is CHAOS-4435's scope, now unblocked",
        "ticket": "CHAOS-4435 (delete — the backfill blocker CHAOS-4498 closed)",
    },
    "team_autoimport_jira.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; ported — jira's own native Go collector (`JiraTeamCatalogCollector`, `internal/providersync/jira_team_catalog_route.go`) is now registered in `nativeTeamCatalogCollectors`, so a Jira sync run's post-sync/reference-discovery dispatch never calls the bridge, and `_resolve_populator` (`team_autoimport.py:_GO_NATIVE_PROVIDERS`) refuses provider=jira outright the same way it already refused linear (CHAOS-4555) -- no current or future caller can import this module at all. `populate()` in this file is no longer reachable from any live production or operator call path; deletion is CHAOS-4435's scope, now unblocked (jira was the last provider blocking it)",
        "ticket": "CHAOS-4435 (delete — the backfill blocker CHAOS-4498 closed, reachability closed the same way CHAOS-4555 closed it for linear)",
    },
    "team_autoimport_linear.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; ported CHAOS-4431 (`27bef7286`, Done) — the previously-unwired Go route (CHAOS-3716) is now registered in `nativeTeamCatalogCollectors`, so a Linear sync run's post-sync/reference-discovery dispatch never calls the bridge. CHAOS-4498 (this PR): the backfill path no longer calls `run_team_autoimport_strict` directly either — same routing story as `team_autoimport_github.py` above. `populate()` in this file is no longer reachable from any live production or operator call path. CHAOS-4555 (2026-08-30): unlike github/gitlab, this unreachability is no longer just an emergent property of caller wiring — `team_autoimport._resolve_populator` (`team_autoimport.py:103-111`) refuses provider=linear outright, so no current or future caller (of `run_team_autoimport`/`run_team_autoimport_strict`, or of `_resolve_populator` directly) can import this module at all; deletion is CHAOS-4435's scope, now unblocked",
        "ticket": "CHAOS-4435 (delete — the backfill blocker CHAOS-4498 closed, reachability closed by CHAOS-4555)",
    },
    "team_autoimport.py": {
        "category": "a",
        "evidence": "run_post_sync_team_autoimport (formerly imported worker_sync.py:27, served by /team-autoimport) is deleted -- jira going native removed its last caller. `run_team_autoimport`/`run_team_autoimport_strict` remain, now refusing every import-capable provider (linear+jira explicitly via `_GO_NATIVE_PROVIDERS`, github/gitlab implicitly via Go-side routing) before ever resolving a populator",
        "ticket": "CHAOS-4435 (delete the now-fully-dead run_team_autoimport/run_team_autoimport_strict + their bridge routes)",
    },
}

# ---------------------------------------------------------------------------
# CURATED: one entry per provider in sourceDiscoveryProviders
# (internal/scheduler/sync/source_discovery.go), CHAOS-4602. Every provider
# here has its sources (repos for github/gitlab, projects for jira)
# discovered from the provider's own API and upserted into
# integration_sources -- distinct from an admin manually adding one.
# ---------------------------------------------------------------------------
SOURCE_DISCOVERY_LEDGER: dict[str, dict[str, str]] = {
    "github": {
        "producer": "`internal/scheduler/sync/source_discovery.go` `NativeSourceDiscoveryService.discoverGitHub`, called from `loadMaterializationPlan` before `loadPlanSources` reads sources for planning (CHAOS-4602)",
        "plane": "native Go, inside the scheduled-sync materializer, once per occurrence",
        "trigger": "every scheduled occurrence for a config with no explicit scope (`sync_configurations.source_id IS NULL`)",
        "evidence": "argued — code read this PR; local integration-test evidence in `internal/scheduler/sync/source_discovery_integration_test.go` (idempotent upsert never flips `is_enabled`, discovery runs before `loadPlanSources`, explicit-scope configs skip it, a failure does not fail the occurrence)",
        "state": "native (this PR) — the Python one-shot at sync-config creation (`src/dev_health_ops/discovery/repos.py::discover_repos_for_config`) stays wired as an INTERIM, now-superseded mechanism (it still runs once at config creation; it is not yet deleted)",
        "ticket": "CHAOS-4602 (this PR); retiring the Python one-shot is follow-up once a prod readback confirms the Go step as the live writer, the same pattern CHAOS-4435 already uses for the team-catalog populators above",
    },
    "gitlab": {
        "producer": "`internal/scheduler/sync/source_discovery.go` `NativeSourceDiscoveryService.discoverGitLab`, same call site as github",
        "plane": "native Go, inside the scheduled-sync materializer, once per occurrence",
        "trigger": "every scheduled occurrence for a config with no explicit scope",
        "evidence": "argued — code read this PR; same integration-test evidence as github",
        "state": "native (this PR) — the Python one-shot at sync-config creation stays wired as an interim, now-superseded mechanism, not yet deleted",
        "ticket": "CHAOS-4602 (this PR); retiring the Python one-shot is follow-up",
    },
    "jira": {
        "producer": "`internal/scheduler/sync/source_discovery.go` `NativeSourceDiscoveryService.discoverJira`, same call site as github/gitlab",
        "plane": "native Go, inside the scheduled-sync materializer, once per occurrence",
        "trigger": "every scheduled occurrence for a config with no explicit scope",
        "evidence": "argued — code read this PR; local integration-test evidence (source_discovery_integration_test.go). CHAOS-4602's own executed finding: jira had NO source-discovery mechanism at all before this PR (`discover_repos_for_config`'s jira branch does not exist on main as of this PR; CHAOS-4584 is a separate, still-in-flight Python interim, not yet merged) -- this Go step is jira's FIRST source-discovery mechanism, not a port of an existing one",
        "state": "native (this PR, and jira's ONLY mechanism -- there is no Python one-shot to supersede for jira the way there is for github/gitlab)",
        "ticket": "CHAOS-4602 (this PR) + CHAOS-4584 (interim Python jira branch at config-creation time, separate/in-flight, does not block this PR) + CHAOS-4576 (Jira REFERENCE discovery -- boards/sprints -- to native Go; a different discovery surface, coordinate if the same route can carry both)",
    },
}

_CATEGORY_LABEL = {
    "a": "LIVE",
    "b": "CELERY-TASK-ONLY / DEAD",
    "c": "LIBRARY / SHARED",
    "d": "TEST/FIXTURE ONLY",
}


def _consistency_guard(
    label: str, live: set[str], curated: set[str], hint: str
) -> None:
    missing = live - curated
    extra = curated - live
    if missing:
        raise SystemExit(
            f"gen_python_go_ledger_docs: {label} {sorted(missing)} exist in the live "
            f"producer but have no curated ledger row in scripts/gen_python_go_ledger_docs.py. {hint}"
        )
    if extra:
        raise SystemExit(
            f"gen_python_go_ledger_docs: {label} {sorted(extra)} have a curated ledger row "
            f"but no longer exist in the live producer -- remove the stale row (or, if renamed, "
            f"update it) in scripts/gen_python_go_ledger_docs.py. {hint}"
        )


def render_kind_block() -> str:
    live_kinds = load_registry_kinds()
    migration_state = load_migration_state()
    _consistency_guard(
        "registry kind(s)",
        live_kinds,
        set(KIND_LEDGER),
        "Add a KIND_LEDGER row (producer/trigger/gate/writer/tables/evidence/state/ticket) for it.",
    )
    if live_kinds != set(migration_state):
        raise SystemExit(
            "gen_python_go_ledger_docs: registry.json and migration-state.json kind sets disagree "
            f"({sorted(live_kinds)} vs {sorted(migration_state)}) -- fix the contract tree first."
        )
    lines = [
        KIND_BEGIN,
        "| kind | producer | trigger | gate | writer | tables written | evidence | state | ticket |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for kind in sorted(KIND_LEDGER):
        row = KIND_LEDGER[kind]
        ms = migration_state[kind]
        state_note = (
            f"{row['state']} (migration-state: `{ms['state']}`/`{ms['route']}`)"
        )
        lines.append(
            f"| `{kind}` | {row['producer']} | {row['trigger']} | {row['gate']} | "
            f"{row['writer']} | {row['tables']} | {row['evidence']} | {state_note} | {row['ticket']} |"
        )
    lines.append(KIND_END)
    return "\n".join(lines)


def render_route_block() -> str:
    live_routes = load_bridge_routes()
    _consistency_guard(
        "bridge.go route(s)",
        live_routes,
        set(BRIDGE_ROUTE_LEDGER),
        "Add a BRIDGE_ROUTE_LEDGER row for it.",
    )
    lines = [
        ROUTE_BEGIN,
        "| route | Go caller | Python handler | computes/writes | state | ticket |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for route in sorted(BRIDGE_ROUTE_LEDGER):
        row = BRIDGE_ROUTE_LEDGER[route]
        lines.append(
            f"| `{route}` | {row['go_caller']} | {row['python_handler']} | "
            f"{row['computes']} | {row['state']} | {row['ticket']} |"
        )
    lines.append(ROUTE_END)
    return "\n".join(lines)


def render_worker_block() -> str:
    live_files = load_worker_files()
    _consistency_guard(
        "src/dev_health_ops/workers/*.py file(s)",
        live_files,
        set(WORKER_FILE_LEDGER),
        "Add a WORKER_FILE_LEDGER row for it (category a/b/c/d + evidence + ticket).",
    )
    lines = [
        WORKER_BEGIN,
        "| file | category | evidence | ticket |",
        "| --- | --- | --- | --- |",
    ]
    for filename in sorted(WORKER_FILE_LEDGER):
        row = WORKER_FILE_LEDGER[filename]
        label = _CATEGORY_LABEL[row["category"]]
        lines.append(
            f"| `{filename}` | {label} | {row['evidence']} | {row['ticket']} |"
        )
    lines.append(WORKER_END)
    return "\n".join(lines)


def render_source_discovery_block() -> str:
    live_providers = load_source_discovery_providers()
    _consistency_guard(
        "source-discovery provider(s)",
        live_providers,
        set(SOURCE_DISCOVERY_LEDGER),
        "Add a SOURCE_DISCOVERY_LEDGER row (producer/plane/trigger/evidence/state/ticket) for it.",
    )
    lines = [
        SOURCE_BEGIN,
        "| provider | producer | plane | trigger | evidence | state | ticket |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for provider in sorted(SOURCE_DISCOVERY_LEDGER):
        row = SOURCE_DISCOVERY_LEDGER[provider]
        lines.append(
            f"| `{provider}` | {row['producer']} | {row['plane']} | {row['trigger']} | "
            f"{row['evidence']} | {row['state']} | {row['ticket']} |"
        )
    lines.append(SOURCE_END)
    return "\n".join(lines)


def _replace_block(
    doc: str, begin: str, end: str, rendered: str, doc_path: Path
) -> str:
    start = doc.find(begin)
    stop = doc.find(end)
    if start == -1 or stop == -1 or stop < start:
        raise SystemExit(
            f"gen_python_go_ledger_docs: markers {begin}/{end} not found in {doc_path}"
        )
    stop += len(end)
    return f"{doc[:start]}{rendered}{doc[stop:]}"


def update_doc() -> None:
    doc = DOC_PATH.read_text(encoding="utf-8")
    doc = _replace_block(doc, KIND_BEGIN, KIND_END, render_kind_block(), DOC_PATH)
    doc = _replace_block(doc, ROUTE_BEGIN, ROUTE_END, render_route_block(), DOC_PATH)
    doc = _replace_block(doc, WORKER_BEGIN, WORKER_END, render_worker_block(), DOC_PATH)
    doc = _replace_block(
        doc, SOURCE_BEGIN, SOURCE_END, render_source_discovery_block(), DOC_PATH
    )
    DOC_PATH.write_text(doc, encoding="utf-8")


if __name__ == "__main__":
    update_doc()
