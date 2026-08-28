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
DOC_PATH = ROOT / "docs" / "reference" / "runtime" / "python-go-live-path-ledger.md"

KIND_BEGIN = "<!-- BEGIN GENERATED KIND LEDGER -->"
KIND_END = "<!-- END GENERATED KIND LEDGER -->"
ROUTE_BEGIN = "<!-- BEGIN GENERATED BRIDGE ROUTE LEDGER -->"
ROUTE_END = "<!-- END GENERATED BRIDGE ROUTE LEDGER -->"
WORKER_BEGIN = "<!-- BEGIN GENERATED WORKER FILE LEDGER -->"
WORKER_END = "<!-- END GENERATED WORKER FILE LEDGER -->"

BRIDGE_CALL_RE = re.compile(r'bridge\.(?:call|callWithResult)\(ctx, "([^"]+)"')


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


# ---------------------------------------------------------------------------
# CURATED: one entry per River job kind (contracts/jobs/v1/registry.json).
# Evidence gathered 2026-08-28 (CHAOS-4433 lane, read-only rg/codegraph pass
# over this worktree at ops main tip 7ea3cad23, plus a re-executed CH
# query_log readback for the team-items rows -- see doc "local" citations).
# ---------------------------------------------------------------------------
KIND_LEDGER: dict[str, dict[str, str]] = {
    # --- investment family ---------------------------------------------
    "investment.chunk": {
        "producer": "Go handler wired, never invoked -- `internal/jobs/workgraph/handler.go:158-163`",
        "trigger": "none — no producer ever creates a `work_graph_execution_requests` row with this kind",
        "gate": "n/a",
        "writer": "Python `work_graph_tasks.run_investment_materialize_chunk.run` (bridge target, unreachable) — `src/dev_health_ops/api/internal/worker_workgraph.py:186`",
        "tables": "none written (never invoked)",
        "evidence": "argued — rg found zero `WriteTx`/enqueue callers for this kind repo-wide (2026-08-28)",
        "state": "dead-code",
        "ticket": "CHAOS-4438 (dead investment.* kinds)",
    },
    "investment.dispatch": {
        "producer": "Go handler wired, never invoked -- `internal/jobs/workgraph/handler.go:152-157`",
        "trigger": "none — only reachable via the legacy Celery-chord function, not scheduled and Celery is retired prod-wide",
        "gate": "n/a",
        "writer": "Python `work_graph_tasks.py:508 dispatch_investment_materialize_partitioned` (Celery-only, unreachable)",
        "tables": "none written (never invoked)",
        "evidence": "argued — rg found zero `WriteTx`/enqueue callers for this kind repo-wide (2026-08-28)",
        "state": "dead-code",
        "ticket": "CHAOS-4438 (dead investment.* kinds)",
    },
    "investment.finalize": {
        "producer": "Go handler wired, never invoked -- `internal/jobs/workgraph/handler.go:164-169`",
        "trigger": "none — no producer ever creates a `work_graph_execution_requests` row with this kind",
        "gate": "n/a",
        "writer": "Python `work_graph_tasks.finalize_investment_materialize_partitioned.run` (bridge target, unreachable) — `worker_workgraph.py:187`",
        "tables": "none written (never invoked)",
        "evidence": "argued — rg found zero `WriteTx`/enqueue callers for this kind repo-wide (2026-08-28)",
        "state": "dead-code",
        "ticket": "CHAOS-4438 (dead investment.* kinds)",
    },
    "investment.materialize": {
        "producer": "`internal/syncdispatchruntime/native_post_sync.go:230-232` (`workGraph.StartRequestTx`)",
        "trigger": "post-sync",
        "gate": "`plan.Investment` (`native_post_sync.go:610`, set when `git \\|\\| hasWorkItems`)",
        "writer": "Go handler orchestrates only (`internal/jobs/workgraph/handler.go:24-68,146-151`); compute is Python `work_graph_tasks.py:172-278` -> `materialize_investments()` (`src/dev_health_ops/work_graph/investment/materialize.py:1169-1854`)",
        "tables": "ClickHouse `work_unit_investments`, `work_unit_repo_effort`, `work_unit_investment_quotes` (`src/dev_health_ops/metrics/sinks/clickhouse/investment.py:117-186`)",
        "evidence": "argued — code read, not re-executed this session",
        "state": "bridge",
        "ticket": "CHAOS-4441 (workgraph subsystem gap)",
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
        "producer": "River worker `cmd/dev-health-worker/daily.go:235` (`daily.NewPartitionHandler`)",
        "trigger": "driven by dispatch/run rows",
        "gate": "none (family selection is unconditional fan-out)",
        "writer": "MIXED: Go-native for `team_wellbeing` (CHAOS-4276, `daily.go:212-234` `NewTeamWellbeingExecutor`) and `repo_user_commit` (CHAOS-4275, `NewRepoUserCommitExecutor`); the other ~21 families still bridge via `internal/jobs/metrics/daily/compatibility_http.go` -> POST `/internal/worker/daily-metrics/v1/execute` -> `worker_metrics.py:1727` -> `src/dev_health_ops/metrics/job_daily.py:1104 run_daily_metrics_job` (NOT `workers/metrics_daily.py`, which is dead Celery-only code, see worker-file ledger)",
        "tables": "`public.daily_metrics_partitions` plus per-family ClickHouse output tables",
        "evidence": "argued — corrects both `.remember/chaos-3092-port-inventory-2026-08-25.md`'s '21/23 Python' framing (2 families are now native) and a prior fork's mis-citation of `workers/metrics_daily.py` (that file is dead; the live module is `src/dev_health_ops/metrics/job_daily.py`)",
        "state": "mixed (2 native, ~21 bridge)",
        "ticket": "CHAOS-3092 children (per-family, tracked separately; not re-enumerated here)",
    },
    "metrics.daily_finalize": {
        "producer": "River worker `cmd/dev-health-worker/daily.go:236` (`daily.NewFinalizeHandler(store, compatibility)`)",
        "trigger": "follows partition completion (same run)",
        "gate": "none found",
        "writer": "Go run bookkeeping (`postgres.go:630,661,1100,1126,1160`); the actual finalize compute is Python `worker_metrics.py:1688 _run_daily_direct` (operation='finalize') -> `src/dev_health_ops/metrics/job_daily.py:1997 run_daily_metrics_finalize` -- IC cross-repo aggregation, reading already-persisted `user_metrics_daily`/`work_item_user_metrics_daily`",
        "tables": "`public.daily_metrics_runs` (Go); ClickHouse `ic_landscape_rolling_30d` and team-level metric tables (Python finalize compute -- corrected 2026-08-28 per codex review, an earlier draft of this row omitted the Python writer and its output tables entirely)",
        "evidence": "argued — code read, not re-executed this session",
        "state": "mixed",
        "ticket": "CHAOS-3092 children (per-family)",
    },
    # --- metrics remaining family ---------------------------------------------
    "metrics.remaining.capacity": {
        "producer": "`internal/scheduler/fixed/inventory.go:197` (capacity_forecast_weekly_fanout)",
        "trigger": "schedule (WeeklyAt Mon 04:00 UTC)",
        "gate": "ClickHouse schema check in `NewCapacityExecutor` (`internal/jobs/metrics/remaining/capacity_native.go:87`)",
        "writer": "Go `internal/jobs/metrics/remaining/capacity_native_clickhouse.go:243`",
        "tables": "`capacity_forecasts`",
        "evidence": "argued — code read; wired `cmd/dev-health-worker/daily.go:359-393,401-410`",
        "state": "native",
        "ticket": "n/a — Python `_run_capacity` (`worker_metrics.py:1752`) is dead code, see worker-file dead-code child ticket",
    },
    "metrics.remaining.complexity": {
        "producer": "`internal/scheduler/fixed/inventory.go:58` (complexity_daily_fanout)",
        "trigger": "schedule (DailyAt 00:45 UTC)",
        "gate": "none (always bridge)",
        "writer": "Python `run_complexity_db_job` `src/dev_health_ops/metrics/job_complexity_db.py:238`",
        "tables": "`file_complexity_snapshots`, `repo_complexity_daily`",
        "evidence": "argued — code read",
        "state": "bridge",
        "ticket": "CHAOS-3092 (metrics families)",
    },
    "metrics.remaining.dora": {
        "producer": "`internal/scheduler/fixed/inventory.go:147` (dora_daily_fanout)",
        "trigger": "schedule (DailyAt 02:15 UTC) + post-sync",
        "gate": "ordering/schema checks in `NewDORAExecutor` (`internal/jobs/metrics/remaining/dora_native.go:100`)",
        "writer": "Go `internal/jobs/metrics/remaining/dora_native_clickhouse.go:379`",
        "tables": "`dora_metrics_daily`",
        "evidence": "argued — wired `daily.go:315-353,415-427`",
        "state": "native",
        "ticket": "n/a — Python `_run_dora` (`worker_metrics.py:1797`) is dormant/dead",
    },
    "metrics.remaining.membership_backfill": {
        "producer": "`internal/scheduler/fixed/inventory.go:176` (membership_backfill_daily_fanout)",
        "trigger": "schedule (DailyAt 03:30 UTC, safety net) + event-driven post-sync materializer (primary)",
        "gate": "none",
        "writer": "Python `backfill_memberships` `src/dev_health_ops/work_graph/investment/backfill.py:176`",
        "tables": "`work_unit_membership`, `work_unit_membership_runs`",
        "evidence": "argued — code read",
        "state": "bridge",
        "ticket": "CHAOS-3092 (metrics families)",
    },
    "metrics.remaining.recommendations": {
        "producer": "`internal/scheduler/fixed/inventory.go:127` (recommendations_daily_fanout)",
        "trigger": "schedule (DailyAt 02:00 UTC, safety net behind a finalize-gated primary trigger)",
        "gate": "none",
        "writer": "Python `_compute_recommendations_for_org` `src/dev_health_ops/workers/recommendations_tasks.py:334`",
        "tables": "`recommendations_daily`",
        "evidence": "argued — code read",
        "state": "bridge",
        "ticket": "CHAOS-3092 (metrics families)",
    },
    "metrics.remaining.release_impact": {
        "producer": "`internal/scheduler/fixed/inventory.go:75` (release_impact_daily_fanout)",
        "trigger": "schedule (DailyAt 01:30 UTC)",
        "gate": "none",
        "writer": "Python `run_release_impact_job` `src/dev_health_ops/metrics/job_release_impact.py:29`",
        "tables": "`release_impact_daily`",
        "evidence": "argued — code read",
        "state": "bridge",
        "ticket": "CHAOS-3092 (metrics families)",
    },
    # --- operational / system / report / sync family ---------------------------------------------
    "operational.billing_notification": {
        "producer": "`cmd/dev-health-worker/operational.go:120-131`",
        "trigger": "manual (billing event enqueues run)",
        "gate": "`descriptor.Executable()` (route=river)",
        "writer": "Python `system_ops.py:25 send_billing_notification`",
        "tables": "Python-owned (email dispatch; no dedicated table)",
        "evidence": "argued — code read",
        "state": "bridge",
        "ticket": "CHAOS-3952 (idempotency-key gap) + CHAOS-4440 (stale docstring)",
    },
    "operational.webhook_delivery": {
        "producer": "`cmd/dev-health-worker/operational.go:132-143`",
        "trigger": "manual (webhook receipt enqueues)",
        "gate": "`descriptor.Executable()` (route=river)",
        "writer": "Python `system_webhooks.py:63 process_webhook_event`",
        "tables": "Python-owned (github/gitlab/jira event tables)",
        "evidence": "argued — code read",
        "state": "bridge",
        "ticket": "CHAOS-4440 (stale docstring)",
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
        "producer": "enqueue: `cmd/dev-health-worker/sync_dispatch.go:195 teamAutoimportPostSyncWriter.PublishTx` (stages the job in the post-sync fanout transaction, unconditionally on every sync run); dequeue/consumer: `internal/syncdispatchruntime/worker.go:95 RegisterTeamAutoimportWorker`, wired `sync_dispatch.go:565` (corrected 2026-08-28 per codex review -- an earlier draft cited only the consumer registration as the producer)",
        "trigger": "post-sync (best-effort, fire-and-forget)",
        "gate": "per-config teams/projects/members selections (CHAOS-4323); read inside the Python populators, see CHAOS-4430 for the executed proof of whether the selections are honoured",
        "writer": "`bridge.go:113 TeamAutoImport` -> POST `/api/internal/worker-sync/team-autoimport` -> `worker_sync.py:269 team_autoimport_reference` -> `workers/team_autoimport.py:228 run_post_sync_team_autoimport` -> per-provider `populate()` (`team_autoimport_{linear,github,gitlab,jira}.py`)",
        "tables": "ClickHouse `teams`, `team_memberships`, `team_project_ownership`",
        "evidence": "local — CH `system.query_log`, org 70d529e0: 102 INSERT queries touching `default.teams` via `clickhouse-connect` (Python client) in the last 2 days as of 2026-08-28 22:03 UTC; zero Go-client inserts in the same window (re-executed this session)",
        "state": "bridge",
        "ticket": "CHAOS-4198 (port populators) with children CHAOS-4430 (trigger/gate proof), CHAOS-4431 (Linear), CHAOS-4432 (GitLab), CHAOS-4434 (GitHub) — all In Progress",
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
        "writer": "Python `system_ops.py:172 phone_home_heartbeat`",
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
        "writer": "Go request/ledger plumbing only (`internal/jobs/workgraph/postgres.go:69,105,135,185`); compute is Python `worker_workgraph.py:367 execute` (fenced subprocess-per-request, LLM categorization)",
        "tables": "`public.work_graph_execution_requests`, `public.work_graph_execution_ledger` (Go); LLM categorization outcome + evidence (Python)",
        "evidence": "argued — rg over `internal/` finds zero graph-construction compute, only dispatcher/request/ledger state-machine code",
        "state": "bridge",
        "ticket": "CHAOS-4441 (workgraph subsystem gap)",
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
        "python_handler": "`worker_sync.py:250 reference_discovery_reference` -> `reference_discovery.py:56 run_sync_reference_discovery`",
        "computes": "reference-discovery orchestration (claim/lease/heartbeat/outbox)",
        "state": "dead in live wiring — superseded by `NativeReferenceDiscoveryService` (CHAOS-4175, Done); its populate step still bridges, see next row",
        "ticket": "CHAOS-4175 (Done)",
    },
    "/api/internal/worker-sync/reference-discovery-populate": {
        "go_caller": "`bridge.go:133 PopulateReferenceDiscovery`, called from `bridge_discovery_executor.go:45`, wired into `NativeReferenceDiscoveryService`'s executor — LIVE, synchronous, blocking",
        "python_handler": "`worker_sync.py:286 reference_discovery_populate_reference` -> `reference_discovery.py:226` -> `team_autoimport.py:169 run_team_autoimport_strict` -> per-provider `populate()`",
        "computes": "teams/members/memberships/project-ownership rows, all 4 providers; credential decrypt + PagerDuty OAuth rotation Python-side",
        "state": "bridge — live",
        "ticket": "CHAOS-4198 (+ children CHAOS-4430/4431/4432/4434)",
    },
    "/api/internal/worker-sync/team-autoimport": {
        "go_caller": "`bridge.go:113 TeamAutoImport`, called from `RegisterTeamAutoimportWorker` (`worker.go:93`), wired `cmd/dev-health-worker/sync_dispatch.go:565` — LIVE, fire-and-forget",
        "python_handler": "`worker_sync.py:269 team_autoimport_reference` -> `team_autoimport.py:228 run_post_sync_team_autoimport` -> same per-provider `populate()`",
        "computes": "same tables as reference-discovery-populate, best-effort variant",
        "state": "bridge — live",
        "ticket": "CHAOS-4198 (+ children CHAOS-4430/4431/4432/4434)",
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
        "evidence": "imported by live sync_bootstrap/system_ops/system_webhooks/work_graph_tasks (and dead metrics_daily/feature_flag_sync) — 'run coroutine inside Celery task' helper",
        "ticket": "n/a",
    },
    "celery_app.py": {
        "category": "c",
        "evidence": "imported by ~20 workers files, including live ones, for `@celery_app.task` — Celery app factory, still load-bearing for the decorator even on live functions",
        "ticket": "n/a",
    },
    "config.py": {
        "category": "c",
        "evidence": "used by queue_monitor.py, queues.py, celery_app.py, sync_reconciler.py, external_ingest_reconciler.py, and api/external_ingest/stream_health.py — env/config constants",
        "ticket": "n/a",
    },
    "external_ingest_recompute.py": {
        "category": "b",
        "evidence": "`@celery_app.task` x2 (L230,L313); sole importer tasks.py:1; zero hits in api/internal/*.py",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "external_ingest_reconciler.py": {
        "category": "b",
        "evidence": "`@celery_app.task` (L59 prune_external_ingest_batches); sole importer tasks.py:9",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "feature_flag_sync.py": {
        "category": "a",
        "evidence": "LIVE — corrected 2026-08-28 per codex review, an earlier draft wrongly claimed zero importers. `_sync_gitlab_feature_flags`/`_sync_launchdarkly_feature_flags` imported and called unconditionally at `processors/dataset_adapters.py:684`, inside `_run_feature_flags_dataset`, itself called from `dataset_adapters.py:758` (the live provider-sync dataset dispatcher, GitLab/LaunchDarkly feature-flag datasets)",
        "ticket": "n/a — live",
    },
    "job_outbox.py": {
        "category": "a",
        "evidence": "enqueue_worker_job called sync_units.py:1047, inside dispatch_sync_run (live via worker_sync.py:26)",
        "ticket": "n/a",
    },
    "job_routes.py": {
        "category": "a",
        "evidence": "resolve_worker_job_route called sync_units.py:999, inside dispatch_sync_run",
        "ticket": "n/a",
    },
    "metrics_daily.py": {
        "category": "b",
        "evidence": "`@celery_app.task` (L19 run_daily_metrics); sole importer tasks.py:5; live daily-metrics route calls dev_health_ops.metrics.job_daily instead (worker_metrics.py:1682) — different module",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "metrics_extra.py": {
        "category": "b",
        "evidence": "`@celery_app.task` x2 (L14/L88); sole importer tasks.py:6-9; live complexity/DORA routes call metrics.job_complexity_db/job_dora instead",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "org_guard.py": {
        "category": "c",
        "evidence": "corrected 2026-08-28 per codex review (a 3rd caller exists, but is itself dead): `organization_exists_sync` is also called at `sync/execution_trigger.py:325`, inside `_require_locked_scheduled_eligibility` — but that function's ONLY caller is `create_scheduled_sync_execution_trigger` (`execution_trigger.py:74`), whose ONLY caller is `sync_scheduler.py:318` (dead, category b below). `create_sync_execution_trigger` (the function the LIVE admin router `api/admin/routers/sync.py` calls) does NOT reach this eligibility check. Do not delete without re-verifying this chain at delete time — a future refactor could make `create_scheduled_sync_execution_trigger` live again",
        "ticket": "CHAOS-4439 (re-verify chain before deleting)",
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
    "queue_monitor.py": {
        "category": "b",
        "evidence": "`@celery_app.task` (L84 monitor_queue_depths); importers celery_app.py (comment only) + tasks.py:8; no route caller",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "queues.py": {
        "category": "c",
        "evidence": "imported only by config.py:1 — per-provider queue-name constants",
        "ticket": "n/a",
    },
    "rate_limit_defer.py": {
        "category": "a",
        "evidence": "plan_rate_limit_deferral imported sync_units.py:130-132, called sync_units.py:1514 inside dispatch_sync_run",
        "ticket": "n/a",
    },
    "recommendations_tasks.py": {
        "category": "a",
        "evidence": "_compute_recommendations_for_org imported/called worker_metrics.py:1833/1863, served by /remaining-metrics/v1/execute",
        "ticket": "n/a",
    },
    "reference_discovery.py": {
        "category": "a",
        "evidence": "imported directly worker_sync.py:22-25; served by /reference-discovery and /reference-discovery-populate routes",
        "ticket": "n/a",
    },
    "report_task.py": {
        "category": "b",
        "evidence": "corrected 2026-08-28 per codex review: `execute_saved_report` also has a live caller — `api/graphql/resolvers/reports.py:617`, inside the on-demand report GraphQL mutation, calls `execute_saved_report.apply_async(...)` wrapped in `try/except (ImportError, AttributeError): pass`. That call is a Celery dispatch with no consumer (Celery retired, CHAOS-4026) so it is a live call site with a dead effect — deleting this file changes that call from 'silently enqueues into a void' to 'silently ImportErrors, same net no-op' (the except clause already handles absence), but the reports.py:617 call site must be removed/updated in the SAME change, not left importing a deleted module",
        "ticket": "CHAOS-4439 (coordinate with reports.py:617 removal, not a pure file deletion)",
    },
    "runner.py": {
        "category": "c",
        "evidence": "used by src/dev_health_ops/cli.py:728,842 (register_commands) — operator CLI for Celery queue inspection, not the Go bridge",
        "ticket": "n/a",
    },
    "sync_bootstrap.py": {
        "category": "a",
        "evidence": "imported by reference_discovery.py/team_autoimport.py/sync_units.py:134; resolve_run_auth reached from dispatch_sync_run",
        "ticket": "n/a",
    },
    "sync_reconciler.py": {
        "category": "b",
        "evidence": "`@celery_app.task` x2 (L84 reconcile_sync_dispatch, L131 prune_rate_limit_observations); sole importer tasks.py:11-13",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "sync_scheduler.py": {
        "category": "b",
        "evidence": "`@celery_app.task` (L394 dispatch_scheduled_syncs); sole importer tasks.py:14",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "sync_units.py": {
        "category": "a",
        "evidence": "dispatch_sync_run/finalize_sync_run imported worker_sync.py:26, served by /dispatch and /finalize",
        "ticket": "n/a",
    },
    "system_ops.py": {
        "category": "a",
        "evidence": "imported worker_operational.py:15-18 (health_check, phone_home_heartbeat, send_billing_notification)",
        "ticket": "n/a",
    },
    "system_tasks.py": {
        "category": "c",
        "evidence": "corrected 2026-08-28 per codex review: NOT a dead shim — `api/webhooks/router.py:34` and `api/billing/router.py:45` import `process_webhook_event`/`send_billing_notification` from this module and call `.delay(...)`/`.apply_async(...)` on them, gated behind `if route_requires_celery(route):`. Since `operational.webhook_delivery`/`billing_notification` are `route=river` in migration-state.json, that gate evaluates false in production today, so the call is live-but-inert (same 'live call site, dead effect' shape as report_task.py above) — the module itself cannot be deleted without removing these two router imports first",
        "ticket": "CHAOS-4439 (coordinate with api/webhooks/router.py + api/billing/router.py, not a pure file deletion)",
    },
    "system_webhooks.py": {
        "category": "a",
        "evidence": "imported worker_operational.py:19,166 (process_webhook_event)",
        "ticket": "n/a",
    },
    "task_utils.py": {
        "category": "c",
        "evidence": "imported by live files (sync_units, reference_discovery, team_autoimport, work_graph_tasks, system_webhooks) and dead ones — shared credential/cache helpers",
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
        "evidence": "imported by team_autoimport.py; sole live writer of GitHub teams/team_memberships",
        "ticket": "CHAOS-4434 (port to Go, In Progress)",
    },
    "team_autoimport_gitlab.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; sole live writer of GitLab teams/team_memberships/team_project_ownership",
        "ticket": "CHAOS-4432 (port to Go, In Progress)",
    },
    "team_autoimport_jira.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; sole live writer of Jira team_project_ownership",
        "ticket": "CHAOS-4198 (not yet split into a Jira child)",
    },
    "team_autoimport_linear.py": {
        "category": "a",
        "evidence": "imported by team_autoimport.py; sole live writer of Linear teams/team_memberships/team_project_ownership — a Go route exists (CHAOS-3716) but is unwired",
        "ticket": "CHAOS-4431 (wire Go route, In Progress)",
    },
    "team_autoimport.py": {
        "category": "a",
        "evidence": "run_post_sync_team_autoimport imported worker_sync.py:27, served by /team-autoimport",
        "ticket": "CHAOS-4198",
    },
    "team_drift_sync.py": {
        "category": "b",
        "evidence": "`@celery_app.task` (L56 sync_team_drift); sole importer tasks.py:22",
        "ticket": "CHAOS-4439 (dead worker modules)",
    },
    "work_graph_tasks.py": {
        "category": "a",
        "evidence": "imported inline worker_workgraph.py:147, dispatched by worker_workgraph_router routes",
        "ticket": "n/a",
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
    DOC_PATH.write_text(doc, encoding="utf-8")


if __name__ == "__main__":
    update_doc()
