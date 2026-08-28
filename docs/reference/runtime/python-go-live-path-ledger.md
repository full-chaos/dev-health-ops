---
page_id: ref-python-go-live-path-ledger
summary: Per River job kind, bridge route, and Python worker module -- who produces it, what gates it, who writes it today, and the executed evidence for that claim.
content_type: generated-reference
owner: platform-operations
source_of_truth:
  - contracts/jobs/v1/registry.json
  - contracts/jobs/v1/migration-state.json
  - internal/syncdispatchruntime/bridge.go
  - src/dev_health_ops/workers/*.py
  - scripts/gen_python_go_ledger_docs.py (curated columns; regenerate, do not hand-edit the generated blocks)
applicability: current
lifecycle: active
---

# Python↔Go live-path ledger

chris, 2026-08-28: *"This is what I mean when I say python <> go compatibility isn't tracking. 4th time today I've had to tell an agent autoimport does NOT exist anymore."* and *"make sure EVERYTHING IS BEING TRACKED SO IT CAN BE FOUND."*
{: .fc-page-lede }

Two Linear tickets were marked Done (CHAOS-4323 "remove the auto-import setting", CHAOS-3716 "Linear reference-catalog parity") and read by later agents as "team auto-import is ported to Go." Neither claim was true of the *write path*: the config flag was removed and a Go route was built, but every team/member/project-ownership row in production is still written by Python (`sync.team_autoimport`, bridge to `team_autoimport_{linear,github,gitlab,jira}.py`). "Done" described a step, not the whole path, and nothing in the repo recorded that distinction per kind or route -- so it kept getting re-asserted as finished.

This page is the fix: one row per River job kind, one row per `bridge.go` route, one row per Python worker module, each with an executed-or-argued evidence citation and a ticket if the row represents unfinished or dead work. A [drift gate](#drift-gate) fails CI the moment a kind, route, or file is added, removed, or renamed without a matching row here.

The drift gate only guarantees *coverage* (every kind/route/file has a row) -- it cannot verify a row's *content* is correct, since that requires reading the code. Two pre-merge review rounds on this PR caught 8 factual errors this way, across the worker-file table (`feature_flag_sync.py` wrongly called an orphan, then its call path mis-described as unconditional; `org_guard.py`'s evidence chain was incomplete) and the River-kinds table (`metrics.daily_finalize`'s Python writer and two of its three output tables were omitted; `sync.team_autoimport`'s producer column cited the consumer registration instead of the enqueue site, then the enqueue itself was wrongly described as unconditional when it is gated by the org's 3 CHAOS-4323 flags; a worker-module count was off by one). All are corrected below and marked `corrected 2026-08-28` in their evidence -- treat any row without that note as unreviewed-past-this-PR's-drift-gate, not as independently verified.

## How to read this page

- **state** is the header line's own claim (`native` / `bridge` / `mixed` / `dead-code`), followed by what `contracts/jobs/v1/migration-state.json` itself claims (`state`/`route`). The two can legitimately disagree: `migration-state.json`'s `route: river` only says River is the **dispatch transport** -- it says nothing about where the **compute** happens. A kind can be `go_default`/`river` in the contract and still bridge every byte of its actual work to Python. That gap is exactly what caused the confusion this ticket exists to close.
- **evidence** is labeled `local` (re-executed against real synced data in org `70d529e0-3c06-4597-8480-794fd02328b6` on the shared dev stack), `fixtures` (generated, contrived), `live` (prod; this lane has no prod access, so no `live` rows below), or `argued` (established by reading code this session, not re-executed -- the tables/writers claim came from `rg`/direct file reads against ops main tip `7ea3cad23` on 2026-08-28, not from running the code path).
- The three tables below (kinds, routes, worker files) are **generated** -- do not hand-edit the text between a `<!-- BEGIN/END GENERATED ... -->` marker pair. Edit the curated `_LEDGER` dicts in `scripts/gen_python_go_ledger_docs.py` and run:

  ```bash
  PYTHONPATH=src .venv/bin/python scripts/gen_python_go_ledger_docs.py
  ```

  then commit the diff in the same PR as whatever code change caused it.

## Sync family

`sync.team_autoimport` is the kind that caused this ticket. Its dispatch is Go, gated by the OR of the org's 3 CHAOS-4323 flags (`auto_import_teams`/`projects`/`members` -- all 3 false means the job is never enqueued); its actual team/member/project-ownership writes are 100% Python today, for all four providers, reached through the narrow `reference-discovery-populate` bridge (synchronous, inside sync finalization) and the best-effort `team-autoimport` bridge (post-sync, fire-and-forget). `sync.team_repo_ownership_derivation` is a *different*, fully Go-native producer downstream of those Python-written tables -- it derives `team_repo_ownership` (source=`inferred`) from ownership rows Python already wrote; it does not replace them, and it is a lower-specificity signal than a direct provider write.

```mermaid
flowchart LR
  subgraph GoDispatch["Go (native)"]
    PS["post-sync / native_post_sync.go"] -->|"enqueues"| K1["River kind: sync.team_autoimport"]
    RD["NativeReferenceDiscoveryService.Discover<br/>(native_reference_discovery.go:132)"] -->|"synchronous, blocking"| BDE["bridgeDiscoveryExecutor<br/>(bridge_discovery_executor.go:45)"]
  end
  K1 -->|"bridge.go:113 TeamAutoImport()"| R1["POST /api/internal/worker-sync/team-autoimport"]
  BDE -->|"bridge.go:133 PopulateReferenceDiscovery()"| R2["POST /api/internal/worker-sync/reference-discovery-populate"]
  subgraph Python["Python (bridge target)"]
    R1 --> H1["worker_sync.py:269 team_autoimport_reference"]
    R2 --> H2["worker_sync.py:286 reference_discovery_populate_reference"]
    H1 --> TA["workers/team_autoimport.py:228 run_post_sync_team_autoimport"]
    H2 --> TAS["workers/team_autoimport.py:169 run_team_autoimport_strict"]
    TA --> POP["per-provider populate()<br/>team_autoimport_{linear,github,gitlab,jira}.py"]
    TAS --> POP
  end
  POP --> CH[("ClickHouse: teams, team_memberships,<br/>team_project_ownership")]
  DERIVE["Go-native: TeamRepoOwnershipDerivationService<br/>(CHAOS-4365, deriveTeamRepoOwnership)"] -->|"reads (downstream, not a replacement)"| CH
  DERIVE --> TRO[("team_repo_ownership<br/>source=inferred")]
  LGN["Go Linear route exists but UNWIRED<br/>(CHAOS-3716: linear_reference_catalog_effects_clickhouse.go)"] -. "would replace one leg of POP for Linear only<br/>-- see CHAOS-4431" .-> CH
```

A wider ownership-attribution diagram (native_team / issue_project / assignee_membership / linked_issue precedence, the tracker-agnostic `team_project_ownership` -> `work_items` -> `team_repo_ownership` derivation chain) already exists and should not be re-derived: see [Team attribution architecture, "Sync produces team_project_ownership" diagram](../../contribute/architecture/team-attribution.md).

Live-path verdict for the four providers, evidence re-executed this session (`local`, org `70d529e0`):

| item | linear | github | gitlab | pagerduty |
| --- | --- | --- | --- | --- |
| `teams` | Python (bridge) | Python (bridge) | Python (bridge) | Go native (`PagerDutyTeamsRouteHandler`) |
| `team_memberships` | Python (bridge) | Python (bridge) | Python (bridge) | -- |
| `team_project_ownership` | Python (bridge, native source) | none written | Python (bridge, provider_access source) | -- |
| `team_repo_ownership` (derivation) | Go native, downstream of the above | Go native, downstream | Go native, downstream | Go native, downstream |

Evidence (`local`, re-executed 2026-08-28 22:03 UTC against the shared stack's ClickHouse): `system.query_log` shows 102 `INSERT` queries touching `default.teams` via `clickhouse-connect` (Python) in the trailing 2 days, plus 5 queries with an empty `http_user_agent` and 4 from an older `clickhouse-connect/0.15.1` client -- **zero** rows from a Go ClickHouse client in the same window. Ports in progress: CHAOS-4431 (wire the unwired Linear Go route), CHAOS-4432 (GitLab Go writer), CHAOS-4434 (GitHub Go writer), all children of CHAOS-4198 and all **In Progress** as of this page's last edit.

## Metrics family

Two sub-families dispatch through two different compatibility bridges. `daily_*` fans out ~23 per-repo/per-org metric computations; `remaining.*` is 6 independent kinds, of which 2 are already Go-native.

```mermaid
flowchart LR
  subgraph GoOrchestration["Go (native orchestration)"]
    DISP["metrics.daily_dispatch<br/>daily.NewDispatcher"] --> PART["metrics.daily_partition<br/>daily.NewPartitionHandler"]
    PART --> FIN["metrics.daily_finalize<br/>daily.NewFinalizeHandler"]
    REM["metrics.remaining.*<br/>6 independent kinds"]
  end
  PART -->|"native, no bridge"| TW["team_wellbeing (CHAOS-4276)"]
  PART -->|"native, no bridge"| RUC["repo_user_commit (CHAOS-4275)"]
  PART -->|"bridge: compatibility_http.go<br/>POST /internal/worker/daily-metrics/v1/execute"| DBR["worker_metrics.py:1727<br/>-&gt; metrics/job_daily.py:1104 run_daily_metrics_job<br/>(~21 remaining families)"]
  REM -->|"native, no bridge"| CAP["metrics.remaining.capacity<br/>capacity_native_clickhouse.go"]
  REM -->|"native, no bridge"| DORA["metrics.remaining.dora<br/>dora_native_clickhouse.go"]
  REM -->|"bridge: compatibility_http.go<br/>POST /internal/worker/remaining-metrics/v1/execute"| RBR["worker_metrics.py:2670 execute_remaining_metrics"]
  RBR --> COMP["job_complexity_db.py (complexity)"]
  RBR --> MEMB["work_graph/investment/backfill.py (membership_backfill)"]
  RBR --> REC["workers/recommendations_tasks.py (recommendations)"]
  RBR --> RI["metrics/job_release_impact.py (release_impact)"]
  TW & RUC & DBR & CAP & DORA & COMP & MEMB & REC & RI --> CHM[("ClickHouse: per-family metric tables")]
```

`workgraph.build` and the `investment.*` family share a second, separate bridge endpoint (`/internal/worker/workgraph/v1/execute`) with zero Go-native compute anywhere -- only React-to-request/ledger plumbing is Go:

```mermaid
flowchart LR
  subgraph Go["Go (request/ledger plumbing only)"]
    WGREQ["workgraph.build<br/>internal/jobs/workgraph/postgres.go"]
    INVREQ["investment.materialize<br/>internal/jobs/workgraph/handler.go"]
    DEAD["investment.dispatch / investment.chunk / investment.finalize<br/>handlers wired, never invoked -- DEAD CODE"]
  end
  WGREQ -->|"compatibility_http.go -&gt; POST /internal/worker/workgraph/v1/execute"| WGP["worker_workgraph.py:367 execute<br/>(LLM categorization, fenced subprocess)"]
  INVREQ -->|"same bridge, different operation key"| INVP["work_graph_tasks.py -&gt; materialize_investments()<br/>work_graph/investment/materialize.py:1169-1854"]
  WGP --> WGT[("work_graph_execution_requests / _ledger<br/>+ LLM categorization outcome")]
  INVP --> INVT[("ClickHouse: work_unit_investments,<br/>work_unit_repo_effort, work_unit_investment_quotes")]
  DEAD -.->|"no producer ever creates a request row"| WGP
```

## Operational, system, and report family

Two `state=go_default` Python-facing docstrings were found stale/inverted during this ticket's research: `worker_operational.py`'s module docstring calls its handlers "dormant Go operational handlers" when `operational.billing_notification`, `operational.webhook_delivery`, and `system.heartbeat` are the live production path; `internal/jobs/report/report.go` and `runtime.go` call the report kernel "dormant... while report jobs remain Celery-routed" when `report.execute_on_demand`/`execute_scheduled` are in fact fully Go-native (CUT-03). Both are corrected in the generated table below and filed as a tracking gap (see [Known tracking gaps](#known-tracking-gaps-opened-from-this-page)) rather than silently fixed here, since they are unrelated code-comment edits outside this PR's diff surface.

`sync.provider_unit`, `system.retention_cleanup`, and `system.sync_coverage_refresh` are fully Go-native with no discrepancy against `migration-state.json`.

## River job kinds

<!-- BEGIN GENERATED KIND LEDGER -->
| kind | producer | trigger | gate | writer | tables written | evidence | state | ticket |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `investment.chunk` | Go handler wired, never invoked -- `internal/jobs/workgraph/handler.go:158-163` | none — no producer ever creates a `work_graph_execution_requests` row with this kind | n/a | Python `work_graph_tasks.run_investment_materialize_chunk.run` (bridge target, unreachable) — `src/dev_health_ops/api/internal/worker_workgraph.py:186` | none written (never invoked) | argued — rg found zero `WriteTx`/enqueue callers for this kind repo-wide (2026-08-28) | dead-code (migration-state: `go_default`/`river`) | CHAOS-4438 (dead investment.* kinds) |
| `investment.dispatch` | Go handler wired, never invoked -- `internal/jobs/workgraph/handler.go:152-157` | none — only reachable via the legacy Celery-chord function, not scheduled and Celery is retired prod-wide | n/a | Python `work_graph_tasks.py:508 dispatch_investment_materialize_partitioned` (Celery-only, unreachable) | none written (never invoked) | argued — rg found zero `WriteTx`/enqueue callers for this kind repo-wide (2026-08-28) | dead-code (migration-state: `go_default`/`river`) | CHAOS-4438 (dead investment.* kinds) |
| `investment.finalize` | Go handler wired, never invoked -- `internal/jobs/workgraph/handler.go:164-169` | none — no producer ever creates a `work_graph_execution_requests` row with this kind | n/a | Python `work_graph_tasks.finalize_investment_materialize_partitioned.run` (bridge target, unreachable) — `worker_workgraph.py:187` | none written (never invoked) | argued — rg found zero `WriteTx`/enqueue callers for this kind repo-wide (2026-08-28) | dead-code (migration-state: `go_default`/`river`) | CHAOS-4438 (dead investment.* kinds) |
| `investment.materialize` | `internal/syncdispatchruntime/native_post_sync.go:230-232` (`workGraph.StartRequestTx`) | post-sync | `plan.Investment` (`native_post_sync.go:610`, set when `git \|\| hasWorkItems`) | Go handler orchestrates only (`internal/jobs/workgraph/handler.go:24-68,146-151`); compute is Python `work_graph_tasks.py:172-278` -> `materialize_investments()` (`src/dev_health_ops/work_graph/investment/materialize.py:1169-1854`) | ClickHouse `work_unit_investments`, `work_unit_repo_effort`, `work_unit_investment_quotes` (`src/dev_health_ops/metrics/sinks/clickhouse/investment.py:117-186`) | argued — code read, not re-executed this session | bridge (migration-state: `go_default`/`river`) | CHAOS-4441 (workgraph subsystem gap) |
| `metrics.daily_dispatch` | `internal/scheduler/fixed/producers.go:442` (fanout) + `native_post_sync.go` `DailyPostSyncWriter.StartRunTx` | post-sync + fixed schedule (backstop) | none found (queue-selected only, `cmd/dev-health-worker/daily.go:36`) | Go `internal/jobs/metrics/daily/postgres.go` (`daily.NewDispatcher`, wired `daily.go:135`) | `public.daily_metrics_runs` (`postgres.go:219,296`), `public.daily_metrics_partitions` (`postgres.go:263`) | argued — code read, not re-executed this session | native (migration-state: `go_default`/`river`) | n/a (orchestration only; per-family compute tracked on `metrics.daily_partition`) |
| `metrics.daily_finalize` | River worker `cmd/dev-health-worker/daily.go:236` (`daily.NewFinalizeHandler(store, compatibility)`) | follows partition completion (same run) | none found | Go run bookkeeping (`postgres.go:630,661,1100,1126,1160`); the actual finalize compute is Python `worker_metrics.py:1688 _run_daily_direct` (operation='finalize') -> `src/dev_health_ops/metrics/job_daily.py:1997 run_daily_metrics_finalize` -- IC cross-repo aggregation, reading already-persisted `user_metrics_daily`/`work_item_user_metrics_daily` | `public.daily_metrics_runs` (Go); ClickHouse `user_metrics_daily` (`job_daily.py:2125 write_user_metrics` -> `wellbeing.py`'s sink), `ic_landscape_rolling_30d`, and team-level metric tables (Python finalize compute -- corrected 2026-08-28 per codex review across 2 rounds: an earlier draft omitted the Python writer and its output tables entirely, then a follow-up correction still missed `user_metrics_daily`) | argued — code read, not re-executed this session | mixed (migration-state: `go_default`/`river`) | CHAOS-3092 children (per-family) |
| `metrics.daily_partition` | River worker `cmd/dev-health-worker/daily.go:235` (`daily.NewPartitionHandler`) | driven by dispatch/run rows | none (family selection is unconditional fan-out) | MIXED: Go-native for `team_wellbeing` (CHAOS-4276, `daily.go:212-234` `NewTeamWellbeingExecutor`) and `repo_user_commit` (CHAOS-4275, `NewRepoUserCommitExecutor`); the other ~21 families still bridge via `internal/jobs/metrics/daily/compatibility_http.go` -> POST `/internal/worker/daily-metrics/v1/execute` -> `worker_metrics.py:1727` -> `src/dev_health_ops/metrics/job_daily.py:1104 run_daily_metrics_job` (NOT `workers/metrics_daily.py`, which is dead Celery-only code, see worker-file ledger) | `public.daily_metrics_partitions` plus per-family ClickHouse output tables | argued — corrects both `.remember/chaos-3092-port-inventory-2026-08-25.md`'s '21/23 Python' framing (2 families are now native) and a prior fork's mis-citation of `workers/metrics_daily.py` (that file is dead; the live module is `src/dev_health_ops/metrics/job_daily.py`) | mixed (2 native, ~21 bridge) (migration-state: `go_default`/`river`) | CHAOS-3092 children (per-family, tracked separately; not re-enumerated here) |
| `metrics.remaining.capacity` | `internal/scheduler/fixed/inventory.go:197` (capacity_forecast_weekly_fanout) | schedule (WeeklyAt Mon 04:00 UTC) | ClickHouse schema check in `NewCapacityExecutor` (`internal/jobs/metrics/remaining/capacity_native.go:87`) | Go `internal/jobs/metrics/remaining/capacity_native_clickhouse.go:243` | `capacity_forecasts` | argued — code read; wired `cmd/dev-health-worker/daily.go:359-393,401-410` | native (migration-state: `go_default`/`river`) | n/a — Python `_run_capacity` (`worker_metrics.py:1752`) is dead code, see worker-file dead-code child ticket |
| `metrics.remaining.complexity` | `internal/scheduler/fixed/inventory.go:58` (complexity_daily_fanout) | schedule (DailyAt 00:45 UTC) | none (always bridge) | Python `run_complexity_db_job` `src/dev_health_ops/metrics/job_complexity_db.py:238` | `file_complexity_snapshots`, `repo_complexity_daily` | argued — code read | bridge (migration-state: `go_default`/`river`) | CHAOS-3092 (metrics families) |
| `metrics.remaining.dora` | `internal/scheduler/fixed/inventory.go:147` (dora_daily_fanout) | schedule (DailyAt 02:15 UTC) + post-sync | ordering/schema checks in `NewDORAExecutor` (`internal/jobs/metrics/remaining/dora_native.go:100`) | Go `internal/jobs/metrics/remaining/dora_native_clickhouse.go:379` | `dora_metrics_daily` | argued — wired `daily.go:315-353,415-427` | native (migration-state: `go_default`/`river`) | n/a — Python `_run_dora` (`worker_metrics.py:1797`) is dormant/dead |
| `metrics.remaining.membership_backfill` | `internal/scheduler/fixed/inventory.go:176` (membership_backfill_daily_fanout) | schedule (DailyAt 03:30 UTC, safety net) + event-driven post-sync materializer (primary) | none | Python `backfill_memberships` `src/dev_health_ops/work_graph/investment/backfill.py:176` | `work_unit_membership`, `work_unit_membership_runs` | argued — code read | bridge (migration-state: `go_default`/`river`) | CHAOS-3092 (metrics families) |
| `metrics.remaining.recommendations` | `internal/scheduler/fixed/inventory.go:127` (recommendations_daily_fanout) | schedule (DailyAt 02:00 UTC, safety net behind a finalize-gated primary trigger) | none | Python `_compute_recommendations_for_org` `src/dev_health_ops/workers/recommendations_tasks.py:334` | `recommendations_daily` | argued — code read | bridge (migration-state: `go_default`/`river`) | CHAOS-3092 (metrics families) |
| `metrics.remaining.release_impact` | `internal/scheduler/fixed/inventory.go:75` (release_impact_daily_fanout) | schedule (DailyAt 01:30 UTC) | none | Python `run_release_impact_job` `src/dev_health_ops/metrics/job_release_impact.py:29` | `release_impact_daily` | argued — code read | bridge (migration-state: `go_default`/`river`) | CHAOS-3092 (metrics families) |
| `operational.billing_notification` | `cmd/dev-health-worker/operational.go:120-131` | manual (billing event enqueues run) | `descriptor.Executable()` (route=river) | Python `system_ops.py:25 send_billing_notification` | Python-owned (email dispatch; no dedicated table) | argued — code read | bridge (migration-state: `go_default`/`river`) | CHAOS-3952 (idempotency-key gap) + CHAOS-4440 (stale docstring) |
| `operational.webhook_delivery` | `cmd/dev-health-worker/operational.go:132-143` | manual (webhook receipt enqueues) | `descriptor.Executable()` (route=river) | Python `system_webhooks.py:63 process_webhook_event` | Python-owned (github/gitlab/jira event tables) | argued — code read | bridge (migration-state: `go_default`/`river`) | CHAOS-4440 (stale docstring) |
| `report.execute_on_demand` | `cmd/dev-health-worker/reports.go:36-96` (`buildReportWorker`) | manual (on-demand report request) | `descriptor.Executable()` (route=river); both report kinds gated together (`reports.go:57`) | Go `internal/jobs/report/runtime.go:18-40` (`NewClickHouseQueryAdapter`/`NewDeterministicRenderer`/`NewSHA256ArtifactAdapter`/`NewInAppNotificationAdapter`) | `report_runs` (Postgres) + generated artifact row | argued — code read; CUT-03 wired real adapters into the binary per `reports.go:17-20` | native (migration-state: `go_default`/`river`) | CHAOS-4440 (stale docstring, package doc still claims Celery-routed/dormant) |
| `report.execute_scheduled` | `cmd/dev-health-worker/reports.go:36-96` (shared build) | schedule (cron-driven report) | same as `report.execute_on_demand` | same `runtime.go` adapters, `NewScheduledHandler` (`report.go:196`) | same as `report.execute_on_demand` | argued — code read | native (migration-state: `go_default`/`river`) | CHAOS-4440 (stale docstring) |
| `sync.provider_unit` | `internal/jobs/providerunit/providerunit.go:538 (*Handler).Work` | post-sync (leased unit from sync-run dispatch) | route=`river_canary` (`Executable()` true for canary too) | `internal/providersync/repository_postgres.go:144 (*PostgresRepository).Complete` | `public.sync_run_units`, `public.sync_run_unit_effect_chunks`, `public.sync_run_unit_chunk_checkpoints` | argued — code read; no Python call found in the `Work()` path | native (migration-state: `canary`/`river_canary`) | n/a — matches migration-state.json canary state, no gap |
| `sync.team_autoimport` | enqueue: `cmd/dev-health-worker/sync_dispatch.go:195 teamAutoimportPostSyncWriter.PublishTx`, called from `NativePostSyncService.publishTeamAutoimport` (`native_post_sync.go:337-344`) only when `plan.TeamAutoimport` is true; dequeue/consumer: `internal/syncdispatchruntime/worker.go:95 RegisterTeamAutoimportWorker`, wired `sync_dispatch.go:565` (corrected 2026-08-28 per codex review, twice -- an earlier draft cited only the consumer as the producer, then wrongly called the enqueue unconditional) | post-sync (best-effort, fire-and-forget) | Go-side: `plan.TeamAutoimport` (`native_post_sync.go:560-577`) = OR of the org's 3 CHAOS-4323 sync_options flags (`auto_import_teams`/`auto_import_projects`/`auto_import_members`) -- when all 3 are false, the job is never enqueued at all, not merely a no-op inside the populator. Python-side: `run_post_sync_team_autoimport` re-reads the same 3 flags independently once the job DOES run; see CHAOS-4430 for the executed proof of whether the populators honour them individually | `bridge.go:113 TeamAutoImport` -> POST `/api/internal/worker-sync/team-autoimport` -> `worker_sync.py:269 team_autoimport_reference` -> `workers/team_autoimport.py:228 run_post_sync_team_autoimport` -> per-provider `populate()` (`team_autoimport_{linear,github,gitlab,jira}.py`) | ClickHouse `teams`, `team_memberships`, `team_project_ownership` | local — CH `system.query_log`, org 70d529e0: 102 INSERT queries touching `default.teams` via `clickhouse-connect` (Python client) in the last 2 days as of 2026-08-28 22:03 UTC; zero Go-client inserts in the same window (re-executed this session) | bridge (migration-state: `go_default`/`river`) | CHAOS-4198 (port populators) with children CHAOS-4430 (trigger/gate proof), CHAOS-4431 (Linear), CHAOS-4432 (GitLab), CHAOS-4434 (GitHub) — all In Progress |
| `sync.team_repo_ownership_derivation` | native Go post-sync derivation (CHAOS-4365 1b) | post-sync | none (celery_removed, rollback=none) | Go `TeamRepoOwnershipDerivationService` (`deriveTeamRepoOwnership`, CHAOS-4365) | `team_repo_ownership` (source=inferred) | local — `.remember/python-bridge-route-inventory-2026-08-28.md` EXECUTED VERDICT table; downstream of the Python-written tables above, not a replacement for them | native (migration-state: `celery_removed`/`river`) | CHAOS-4365 (Done) |
| `system.heartbeat` | `cmd/dev-health-worker/operational.go:144-155` | schedule | `descriptor.Executable()` (route=river) | Python `system_ops.py:172 phone_home_heartbeat` | Python-owned `audit_logs` row + external `TELEMETRY_ENDPOINT` POST | argued — code read; `internal/jobs/system/heartbeat.go:12-33` docstring is accurate and non-stale about this (explicitly says 'CLASSIFICATION: python_compatibility, not native Go') | bridge (migration-state: `go_default`/`river`) | CUT-20 (code label, no Linear ticket found — see report) |
| `system.retention_cleanup` | `cmd/dev-health-worker/operational.go:156-167` | schedule (per-policy) | `descriptor.Executable()` (route=river); policy dispatch `retention.go:89-105` | Go `internal/jobs/system/retention_postgres.go:39,91,168` + `internal/joboutbox/repository.go:265` | `provider_rate_limit_observations`, `dev_conversations`/`dev_conversation_tombstones`, `external_ingest_batches`, `worker_job_outbox` | argued — code read; 4 policies bound, fully Go-native | native (migration-state: `go_default`/`river`) | n/a — no gap |
| `system.sync_coverage_refresh` | `internal/jobs/synccoverage/handler.go:34 (*Handler).Work` | schedule | `descriptor.Executable()` (route=river, rollback=none) | `internal/synccoverage/projector.go:76` | `sync_coverage_projections` | argued — code read; zero Python involvement, matches migration-state.json exactly | native (migration-state: `celery_removed`/`river`) | n/a — no gap |
| `workgraph.build` | `internal/scheduler/fixed/producers.go:826` (startGraphBuild) + `native_post_sync.go:222` (`workGraph.StartRequestTx`) | post-sync + fixed-schedule prerequisite (before membership projection) | none found | Go request/ledger plumbing only (`internal/jobs/workgraph/postgres.go:69,105,135,185`); compute is Python `worker_workgraph.py:367 execute` (fenced subprocess-per-request, LLM categorization) | `public.work_graph_execution_requests`, `public.work_graph_execution_ledger` (Go); LLM categorization outcome + evidence (Python) | argued — rg over `internal/` finds zero graph-construction compute, only dispatcher/request/ledger state-machine code | bridge (migration-state: `go_default`/`river`) | CHAOS-4441 (workgraph subsystem gap) |
<!-- END GENERATED KIND LEDGER -->

## Bridge routes (`internal/syncdispatchruntime/bridge.go`)

Three of the five routes (`dispatch`, `finalize`, `reference-discovery`) are dead in the live binary: `RegisterWorkers` (`internal/syncdispatchruntime/worker.go:72-87`) takes the four `Native*Service` types, never the `HTTPBridge` -- the interface methods still exist and still compile, but nothing calls them. Only `team-autoimport` and `reference-discovery-populate` are reachable at runtime.

<!-- BEGIN GENERATED BRIDGE ROUTE LEDGER -->
| route | Go caller | Python handler | computes/writes | state | ticket |
| --- | --- | --- | --- | --- | --- |
| `/api/internal/worker-sync/dispatch` | `bridge.go:98 HTTPBridge.Dispatch` — interface method exists but `RegisterWorkers` (`worker.go:72-87`) takes the 4 Native services, never `bridge`; no live registrant calls `.Dispatch()` | `worker_sync.py:205 dispatch_reference` -> `sync_units.py:761 dispatch_sync_run` | reads/writes `sync_run`, `sync_run_unit`, dispatches units | dead in live wiring — superseded by `NativeDispatchSyncRunService` (CHAOS-4175, Done) | CHAOS-4175 (Done) |
| `/api/internal/worker-sync/finalize` | `bridge.go:102 HTTPBridge.Finalize` — same as above, no live registrant | `worker_sync.py:235 finalize_reference` -> `sync_units.py:2053 finalize_sync_run` | finalizes `sync_run`, coverage-cache invalidation, compute checkpoints | dead in live wiring — superseded by `NativeFinalizeSyncRunService` (CHAOS-4175, Done) | CHAOS-4175 (Done) |
| `/api/internal/worker-sync/reference-discovery` | `bridge.go:106 HTTPBridge.Discover` — same, no live registrant | `worker_sync.py:250 reference_discovery_reference` -> `reference_discovery.py:56 run_sync_reference_discovery` | reference-discovery orchestration (claim/lease/heartbeat/outbox) | dead in live wiring — superseded by `NativeReferenceDiscoveryService` (CHAOS-4175, Done); its populate step still bridges, see next row | CHAOS-4175 (Done) |
| `/api/internal/worker-sync/reference-discovery-populate` | `bridge.go:133 PopulateReferenceDiscovery`, called from `bridge_discovery_executor.go:45`, wired into `NativeReferenceDiscoveryService`'s executor — LIVE, synchronous, blocking | `worker_sync.py:286 reference_discovery_populate_reference` -> `reference_discovery.py:226` -> `team_autoimport.py:169 run_team_autoimport_strict` -> per-provider `populate()` | teams/members/memberships/project-ownership rows, all 4 providers; credential decrypt + PagerDuty OAuth rotation Python-side | bridge — live | CHAOS-4198 (+ children CHAOS-4430/4431/4432/4434) |
| `/api/internal/worker-sync/team-autoimport` | `bridge.go:113 TeamAutoImport`, called from `RegisterTeamAutoimportWorker` (`worker.go:93`), wired `cmd/dev-health-worker/sync_dispatch.go:565` — LIVE, fire-and-forget | `worker_sync.py:269 team_autoimport_reference` -> `team_autoimport.py:228 run_post_sync_team_autoimport` -> same per-provider `populate()` | same tables as reference-discovery-populate, best-effort variant | bridge — live | CHAOS-4198 (+ children CHAOS-4430/4431/4432/4434) |
<!-- END GENERATED BRIDGE ROUTE LEDGER -->

A separate bridge, `internal/syncdispatchruntime/budget_estimate_bridge.go`, calls `POST /api/internal/worker-sync/dispatch-budget-estimate` (6 per-provider Python budget estimators, ~2000 LOC) and `internal/jobs/pagerduty/compatibility.go` calls a PagerDuty reconciliation bridge from `dev-health-stream-runner`. Both are live, both are out of `bridge.go`'s literal scope so the drift gate does not enumerate them, and both already have owning tickets (CHAOS-4198 for the budget-estimate layer, CHAOS-4105 for PagerDuty) -- listed here so this page stays the complete picture even where the mechanical gate's scope is narrower.

## Python worker modules (`src/dev_health_ops/workers/*.py`)

Category key: **LIVE** = reached today from a live FastAPI bridge route. **CELERY-TASK-ONLY / DEAD** = decorated as a Celery task with no live route caller; Celery itself has been archived in production since 2026-08-19 (CHAOS-4026), so these files have zero production callers. **LIBRARY / SHARED** = helper code imported by other worker modules, not itself a route or task target. **TEST/FIXTURE ONLY** = referenced only from `tests/` (none found).

<!-- BEGIN GENERATED WORKER FILE LEDGER -->
| file | category | evidence | ticket |
| --- | --- | --- | --- |
| `__init__.py` | LIBRARY / SHARED | package marker, no logic | n/a |
| `async_runner.py` | LIBRARY / SHARED | imported by live sync_bootstrap/system_ops/system_webhooks/work_graph_tasks (and dead metrics_daily/feature_flag_sync) — 'run coroutine inside Celery task' helper | n/a |
| `celery_app.py` | LIBRARY / SHARED | imported by ~20 workers files, including live ones, for `@celery_app.task` — Celery app factory, still load-bearing for the decorator even on live functions | n/a |
| `config.py` | LIBRARY / SHARED | used by queue_monitor.py, queues.py, celery_app.py, sync_reconciler.py, external_ingest_reconciler.py, and api/external_ingest/stream_health.py — env/config constants | n/a |
| `external_ingest_recompute.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` x2 (L230,L313); sole importer tasks.py:1; zero hits in api/internal/*.py | CHAOS-4439 (dead worker modules) |
| `external_ingest_reconciler.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` (L59 prune_external_ingest_batches); sole importer tasks.py:9 | CHAOS-4439 (dead worker modules) |
| `feature_flag_sync.py` | LIVE | LIVE — corrected 2026-08-28 per codex review across 2 rounds: an earlier draft wrongly claimed zero importers, then a follow-up correction wrongly said both helpers are called unconditionally. Actual shape: `_run_feature_flags_dataset` (`dataset_adapters.py:684`, called from the live dataset dispatcher at `:758`) branches per provider (`dataset_adapters.py:694-712`) -- calls `_sync_gitlab_feature_flags` for `provider=='gitlab'`, `_sync_launchdarkly_feature_flags` for `provider=='launchdarkly'`, raises `ValueError` for any other provider | n/a — live |
| `job_outbox.py` | LIVE | enqueue_worker_job called sync_units.py:1047, inside dispatch_sync_run (live via worker_sync.py:26) | n/a |
| `job_routes.py` | LIVE | resolve_worker_job_route called sync_units.py:999, inside dispatch_sync_run | n/a |
| `metrics_daily.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` (L19 run_daily_metrics); sole importer tasks.py:5; live daily-metrics route calls dev_health_ops.metrics.job_daily instead (worker_metrics.py:1682) — different module | CHAOS-4439 (dead worker modules) |
| `metrics_extra.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` x2 (L14/L88); sole importer tasks.py:6-9; live complexity/DORA routes call metrics.job_complexity_db/job_dora instead | CHAOS-4439 (dead worker modules) |
| `org_guard.py` | LIBRARY / SHARED | corrected 2026-08-28 per codex review (a 3rd caller exists, but is itself dead): `organization_exists_sync` is also called at `sync/execution_trigger.py:325`, inside `_require_locked_scheduled_eligibility` — but that function's ONLY caller is `create_scheduled_sync_execution_trigger` (`execution_trigger.py:74`), whose ONLY caller is `sync_scheduler.py:318` (dead, category b below). `create_sync_execution_trigger` (the function the LIVE admin router `api/admin/routers/sync.py` calls) does NOT reach this eligibility check. Do not delete without re-verifying this chain at delete time — a future refactor could make `create_scheduled_sync_execution_trigger` live again | CHAOS-4439 (re-verify chain before deleting) |
| `post_sync_dispatch.py` | LIVE | build_post_sync_dispatch_payload called sync_units.py:2274, inside finalize_sync_run (live via worker_sync.py:26) | n/a |
| `provider_family_contract.py` | LIVE | imported by provider_unit_route.py & sync_units.py:122, reached from dispatch_sync_run | n/a |
| `provider_unit_route.py` | LIVE | imported sync_units.py:125, used sync_units.py:999-1000 inside dispatch_sync_run | n/a |
| `queue_monitor.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` (L84 monitor_queue_depths); importers celery_app.py (comment only) + tasks.py:8; no route caller | CHAOS-4439 (dead worker modules) |
| `queues.py` | LIBRARY / SHARED | imported only by config.py:1 — per-provider queue-name constants | n/a |
| `rate_limit_defer.py` | LIVE | plan_rate_limit_deferral imported sync_units.py:130-132, called sync_units.py:1514 inside dispatch_sync_run | n/a |
| `recommendations_tasks.py` | LIVE | _compute_recommendations_for_org imported/called worker_metrics.py:1833/1863, served by /remaining-metrics/v1/execute | n/a |
| `reference_discovery.py` | LIVE | imported directly worker_sync.py:22-25; served by /reference-discovery and /reference-discovery-populate routes | n/a |
| `report_task.py` | CELERY-TASK-ONLY / DEAD | corrected 2026-08-28 per codex review: `execute_saved_report` also has a live caller — `api/graphql/resolvers/reports.py:617`, inside the on-demand report GraphQL mutation, calls `execute_saved_report.apply_async(...)` wrapped in `try/except (ImportError, AttributeError): pass`. That call is a Celery dispatch with no consumer (Celery retired, CHAOS-4026) so it is a live call site with a dead effect — deleting this file changes that call from 'silently enqueues into a void' to 'silently ImportErrors, same net no-op' (the except clause already handles absence), but the reports.py:617 call site must be removed/updated in the SAME change, not left importing a deleted module | CHAOS-4439 (coordinate with reports.py:617 removal, not a pure file deletion) |
| `runner.py` | LIBRARY / SHARED | used by src/dev_health_ops/cli.py:728,842 (register_commands) — operator CLI for Celery queue inspection, not the Go bridge | n/a |
| `sync_bootstrap.py` | LIVE | imported by reference_discovery.py/team_autoimport.py/sync_units.py:134; resolve_run_auth reached from dispatch_sync_run | n/a |
| `sync_reconciler.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` x2 (L84 reconcile_sync_dispatch, L131 prune_rate_limit_observations); sole importer tasks.py:11-13 | CHAOS-4439 (dead worker modules) |
| `sync_scheduler.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` (L394 dispatch_scheduled_syncs); sole importer tasks.py:14 | CHAOS-4439 (dead worker modules) |
| `sync_units.py` | LIVE | dispatch_sync_run/finalize_sync_run imported worker_sync.py:26, served by /dispatch and /finalize | n/a |
| `system_ops.py` | LIVE | imported worker_operational.py:15-18 (health_check, phone_home_heartbeat, send_billing_notification) | n/a |
| `system_tasks.py` | LIBRARY / SHARED | corrected 2026-08-28 per codex review: NOT a dead shim — `api/webhooks/router.py:34` and `api/billing/router.py:45` import `process_webhook_event`/`send_billing_notification` from this module and call `.delay(...)`/`.apply_async(...)` on them, gated behind `if route_requires_celery(route):`. Since `operational.webhook_delivery`/`billing_notification` are `route=river` in migration-state.json, that gate evaluates false in production today, so the call is live-but-inert (same 'live call site, dead effect' shape as report_task.py above) — the module itself cannot be deleted without removing these two router imports first | CHAOS-4439 (coordinate with api/webhooks/router.py + api/billing/router.py, not a pure file deletion) |
| `system_webhooks.py` | LIVE | imported worker_operational.py:19,166 (process_webhook_event) | n/a |
| `task_utils.py` | LIBRARY / SHARED | imported by live files (sync_units, reference_discovery, team_autoimport, work_graph_tasks, system_webhooks) and dead ones — shared credential/cache helpers | n/a |
| `tasks.py` | CELERY-TASK-ONLY / DEAD | Celery task-name aggregator (__all__ re-export of every task); no api/internal reference — dead since CHAOS-4026 | CHAOS-4439 (dead worker modules) |
| `team_autoimport.py` | LIVE | run_post_sync_team_autoimport imported worker_sync.py:27, served by /team-autoimport | CHAOS-4198 |
| `team_autoimport_categories.py` | LIVE | imported by team_autoimport.py (live) and the provider variants | n/a |
| `team_autoimport_github.py` | LIVE | imported by team_autoimport.py; sole live writer of GitHub teams/team_memberships | CHAOS-4434 (port to Go, In Progress) |
| `team_autoimport_gitlab.py` | LIVE | imported by team_autoimport.py; sole live writer of GitLab teams/team_memberships/team_project_ownership | CHAOS-4432 (port to Go, In Progress) |
| `team_autoimport_jira.py` | LIVE | imported by team_autoimport.py; sole live writer of Jira team_project_ownership | CHAOS-4198 (not yet split into a Jira child) |
| `team_autoimport_linear.py` | LIVE | imported by team_autoimport.py; sole live writer of Linear teams/team_memberships/team_project_ownership — a Go route exists (CHAOS-3716) but is unwired | CHAOS-4431 (wire Go route, In Progress) |
| `team_drift_sync.py` | CELERY-TASK-ONLY / DEAD | `@celery_app.task` (L56 sync_team_drift); sole importer tasks.py:22 | CHAOS-4439 (dead worker modules) |
| `work_graph_tasks.py` | LIVE | imported inline worker_workgraph.py:147, dispatched by worker_workgraph_router routes | n/a |
<!-- END GENERATED WORKER FILE LEDGER -->

## Known tracking gaps opened from this page

Filed as children of CHAOS-3092 (Go Worker Runtime Migration) because they had no owning ticket at the time this page was written:

- **CHAOS-4438** -- dead `investment.dispatch`/`investment.chunk`/`investment.finalize` kinds: handlers fully wired, zero producer anywhere ever creates a request row for them.
- **CHAOS-4439** -- 9 dead Celery-only Python worker modules with genuinely zero live callers, safe to delete now that Celery is archived (CHAOS-4026): `external_ingest_recompute.py`, `external_ingest_reconciler.py`, `metrics_daily.py`, `metrics_extra.py`, `queue_monitor.py`, `sync_reconciler.py`, `sync_scheduler.py`, `team_drift_sync.py`, `tasks.py`. Three more need coordinated (not standalone) removal: `report_task.py` and `system_tasks.py` have live-but-inert callers (a Celery dispatch call with no consumer today) in `reports.py`/`api/webhooks/router.py`/`api/billing/router.py` that must be removed in the same change; `org_guard.py`'s one non-obviously-dead caller chain should be re-verified at delete time. (`feature_flag_sync.py` was in an earlier draft of this list -- it is LIVE, see the worker-file table, and was removed after a pre-merge review caught the error.)
- **CHAOS-4440** -- stale/inverted "dormant" doc comments in `worker_operational.py` and `internal/jobs/report/{report,runtime}.go`: they describe the live production path as dormant/Celery-routed.
- **CHAOS-4441** -- `workgraph.build` + `investment.materialize` compute has no Go-native port and no owning ticket: both bridge 100% of their compute to Python with zero native alternative anywhere in the tree.

See each ticket for the exact file:line evidence cited above.

## Drift gate

`tests/docs/test_python_go_ledger_drift.py` runs inside the unmarked pure-Python unit suite (`ci/local_validate.sh`'s full-suite stage, and CI's `pytest tests -m "not benchmark and not clickhouse"`), so it cannot be silently skipped. It fails when:

1. `contracts/jobs/v1/registry.json` gains, loses, or renames a job kind without a matching `KIND_LEDGER` row in `scripts/gen_python_go_ledger_docs.py`.
2. `internal/syncdispatchruntime/bridge.go` gains, loses, or renames a `bridge.call`/`bridge.callWithResult` route string without a matching `BRIDGE_ROUTE_LEDGER` row.
3. `src/dev_health_ops/workers/*.py` gains or loses a file without a matching `WORKER_FILE_LEDGER` row.
4. The generated blocks in this page are stale relative to the curated dicts (someone edited a `_LEDGER` entry but forgot to regenerate).

`test_every_registry_kind_and_bridge_route_and_worker_file_has_a_curated_row` is the falsification control: it plants an untracked kind/route/file in memory and asserts the generator's consistency guard actually raises, so the guard's ability to fail is proven on every CI run rather than assumed.
