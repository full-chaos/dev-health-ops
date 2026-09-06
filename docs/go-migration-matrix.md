---
page_id: ref-go-migration-matrix
summary: One row per CLI area (sync, metrics, recommendations, AI, investment/work-graph, webhooks, streams, scheduler/reconciler/operator) -- who computes and writes it today, Go or Python.
content_type: generated-reference
owner: platform-operations
source_of_truth:
  - contracts/provider-matrix/v1/matrix.json (SYNC's provider x dataset table -- fully generated, do not hand-edit)
  - internal/jobs/metrics/daily/families.json (METRICS' daily-family table)
  - internal/jobs/metrics/remaining/families.json (METRICS' remaining-family table; `port` field mirrors daily's convention as of CHAOS-5030, but contracts/native-families/v1/native-families.json is the actual executor authority -- see below)
  - contracts/native-families/v1/native-families.json (Go-emitted, AST-derived from cmd/dev-health-worker/daily.go by cmd/dev-health-worker/native_families_artifact_test.go -- the executor source of truth for METRICS)
  - scripts/gen_go_migration_matrix_docs.py (curated citation/ticket text + generator; regenerate, do not hand-edit the generated blocks)
applicability: current
lifecycle: active
---

# Go migration matrix

Companion to [`python-go-live-path-ledger.md`](reference/runtime/python-go-live-path-ledger.md) (narrative +
mermaid diagrams) and [`contracts/provider-matrix/v1/README.md`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/provider-matrix/v1/README.md)
(the frozen provider x dataset parity contract, CUT-08). This page's top-level sections mirror the operator's
own mental model -- the CLI areas `dev-hops`/`dev-health-workerctl` expose -- with the generated,
drift-gated tables nested under them (chris, 2026-09-04). Every generated table's Executor column answers
"who runs this today":

**Read this first: worker kind status is NOT CLI verb status.** A `metrics.remaining.*` or
`metrics.daily_partition` family reading NATIVE below describes the WORKER's own dispatch path only.
Several `dev-hops metrics ...`/`dev-hops recommendations compute` CLI verbs call the Python compute function
**directly**, bypassing the Go worker's native executor entirely, even for families the worker itself now
runs natively (`dev-hops metrics dora`/`capacity`/`recommendations` are the clearest examples -- see METRICS
and RECOMMENDATIONS below). Read a row's CLI-verb sub-table and its generated-table row as two independent
claims, never one implying the other. Tickets to reconcile this split are being filed by the scribe; cited
here once numbered.

**Executor legend**

- **NATIVE** -- Go computes and writes it; no Python call in the live path.
- **COMPAT-Python** -- Go dispatches (River kind, HTTP bridge, or job shell) but Python does the actual
  compute/write.
- **PYTHON-ONLY** -- no Go path exists at all (not even a dispatch shell).
- **PARTIAL** -- split within the row; the citation column says which half is which.
- **N/A** -- the row exists for coverage but isn't a compute path (e.g. a read-only diagnostic).

**What's generated vs. hand-curated.** SYNC's provider x dataset table is 100% generated from
`contracts/provider-matrix/v1/matrix.json` -- the same frozen, CI-verified contract
`internal/providersync/capability_matrix.go` and `src/dev_health_ops/workers/provider_unit_route.py` already
drift-test against (`tests/workers/test_provider_matrix_contract.py`); nothing there is hand-typed. METRICS'
two family tables are generated from `families.json`'s family-name sets (coverage) plus
`contracts/native-families/v1/native-families.json` (the Executor verdict itself) -- a Go-emitted artifact
`cmd/dev-health-worker/native_families_artifact_test.go` statically parses out of `daily.go`'s own
registration wiring, so no curated Python dict or hand-set JSON field can silently drift from what the
worker actually executes. INVESTMENT/WORK-GRAPH's table is entirely hand-curated (no registry file exists
for those 5 kinds; see [Known gaps](#known-gaps-not-fixed-in-this-pr)). Every CLI-verb sub-table under SYNC/
METRICS/RECOMMENDATIONS/WEBHOOKS/STREAMS/SCHEDULER-RECONCILER-OPERATOR is hand-curated prose (read against
both CLI trees -- Python `dev_health_ops.cli` and Go `cmd/dev-health-workerctl`/`dev-health-stream-runner` --
at the pinned sha below), because no JSON registry maps a CLI verb to a River kind or Python entrypoint.

Regenerate the generated tables after any change to a source-of-truth file:

```bash
PYTHONPATH=src .venv/bin/python scripts/gen_go_migration_matrix_docs.py
UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate
```

`scripts/check_go_migration_matrix_docs_drift.py` (wrapped by `tests/docs/test_go_migration_matrix_drift.py`)
fails CI the moment a generated block disagrees with its producer, or a family/dataset gains or loses a row
without the doc being regenerated in the same PR. `cmd/dev-health-worker/native_families_artifact_test.go`
separately fails CI if `contracts/native-families/v1/native-families.json` disagrees with `daily.go`'s actual
wiring.

**Last verified:** `e3e2e77c48a9e4902e48d962b8292f1b408bf47b` (ops main, 2026-09-04) -- the commit every
hand-curated citation/CLI-verb row on this page was read against. The generated tables always reflect
whatever their producer files say at build time, independent of this date.

## SYNC

Provider sync's raw ingestion is entirely NATIVE (below); its **CLI-verb layer** is a mix of Go operator
tooling and Python trigger shells over the same native path:

| CLI verb/area | Executor | Writer call site | Ticket |
|---|---|---|---|
| `dev-hops sync` (git/prs/blame/cicd/deployments/incidents/teams/work-items) | NATIVE (worker-side; Python CLI verbs are operator-trigger shells over the same Go sync-dispatch path, `sync_processor.register_commands`) | `internal/providersync/*` -- see the generated table below, all NATIVE except jira memberships | CHAOS-4198 (jira memberships) |
| `dev-health-workerctl providersync retire-linear-pseudo-projects` / `retire-stale-linear-project-ownership` | NATIVE | `cmd/dev-health-workerctl/main.go:1199-1372` | -- |
| `dev-health-workerctl sync-dispatch-outbox close-backlog` | NATIVE | `cmd/dev-health-workerctl/main.go:1379-1453` | -- |

### Provider sync, by provider x dataset (generated from `contracts/provider-matrix/v1/matrix.json`)

Raw ingestion only (the ClickHouse writer for the entity's own sync-time fetch) -- NOT the daily-metrics
aggregation layer built on top of these tables (see METRICS below).

<!-- BEGIN GENERATED PROVIDER SYNC MATRIX -->
| Provider | Dataset | Executor | Route destinations (tables written) | Route ready | Plannable |
| --- | --- | --- | --- | --- | --- |
| github | `blame` | NATIVE | `git_blame`, `github_blame_path_progress` | True | True |
| github | `cicd` | NATIVE | `ci_acceptance_checks`, `ci_job_runs`, `ci_pipeline_runs`, `coverage_snapshots`, `test_case_results`, `test_suite_results` | True | True |
| github | `commit-stats` | NATIVE | `git_commit_stats` | True | True |
| github | `commits` | NATIVE | `git_commits` | True | True |
| github | `deployments` | NATIVE | `deployments` | True | True |
| github | `files` | NATIVE | `git_files` | True | True |
| github | `pr-comments` | NATIVE | `git_pull_request_reviews`, `git_pull_requests` | True | False |
| github | `pr-reviews` | NATIVE | `git_pull_request_reviews`, `git_pull_requests` | True | False |
| github | `prs` | NATIVE | `git_pull_request_reviews`, `git_pull_requests` | True | True |
| github | `repo-metadata` | NATIVE | `repos` | True | True |
| github | `security` | NATIVE | `security_alerts` | True | True |
| github | `tests` | NATIVE | `ci_acceptance_checks`, `ci_job_runs`, `ci_pipeline_runs`, `coverage_snapshots`, `test_case_results`, `test_suite_results` | True | False |
| github | `work-item-comments` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| github | `work-item-history` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| github | `work-item-labels` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| github | `work-item-projects` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| github | `work-items` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | True |
| gitlab | `blame` | NATIVE | `git_blame` | True | True |
| gitlab | `cicd` | NATIVE | `ci_acceptance_checks`, `ci_job_runs`, `ci_pipeline_runs`, `coverage_snapshots`, `test_case_results`, `test_suite_results` | True | True |
| gitlab | `commit-stats` | NATIVE | `git_commit_stats` | True | True |
| gitlab | `commits` | NATIVE | `git_commits` | True | True |
| gitlab | `deployments` | NATIVE | `deployments` | True | True |
| gitlab | `feature-flags` | NATIVE | `feature_flag`, `feature_flag_event`, `work_graph_edges` | True | True |
| gitlab | `files` | NATIVE | `git_files` | True | True |
| gitlab | `incidents` | NATIVE | `operational_incidents`, `operational_service_repository_mappings`, `operational_services` | True | True |
| gitlab | `pr-comments` | NATIVE | `git_pull_request_reviews`, `git_pull_requests` | True | False |
| gitlab | `pr-reviews` | NATIVE | `git_pull_request_reviews`, `git_pull_requests` | True | False |
| gitlab | `prs` | NATIVE | `git_pull_request_reviews`, `git_pull_requests` | True | True |
| gitlab | `repo-metadata` | NATIVE | `repos` | True | True |
| gitlab | `security` | NATIVE | `security_alerts` | True | True |
| gitlab | `tests` | NATIVE | `ci_acceptance_checks`, `ci_job_runs`, `ci_pipeline_runs`, `coverage_snapshots`, `test_case_results`, `test_suite_results` | True | False |
| gitlab | `work-item-comments` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| gitlab | `work-item-history` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| gitlab | `work-item-labels` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| gitlab | `work-item-projects` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| gitlab | `work-items` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | True |
| jira | `incidents` | NATIVE | `operational_incidents` | True | True |
| jira | `work-item-comments` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items`, `worklogs` | True | False |
| jira | `work-item-history` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items`, `worklogs` | True | False |
| jira | `work-item-labels` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items`, `worklogs` | True | False |
| jira | `work-item-projects` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items`, `worklogs` | True | False |
| jira | `work-items` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items`, `worklogs` | True | True |
| launchdarkly | `feature-flags` | NATIVE | `feature_flag`, `feature_flag_event`, `feature_flag_link`, `work_graph_edges` | True | True |
| linear | `work-item-comments` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| linear | `work-item-history` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| linear | `work-item-labels` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| linear | `work-item-projects` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | False |
| linear | `work-items` | NATIVE | `ai_attribution`, `estimate_coverage_metrics_daily`, `investment_classifications_daily`, `investment_metrics_daily`, `issue_type_metrics_daily`, `project_membership_transitions`, `projects`, `sprints`, `work_item_cycle_times`, `work_item_dependencies`, `work_item_interactions`, `work_item_metrics_daily`, `work_item_reopen_events`, `work_item_state_durations_daily`, `work_item_team_attributions`, `work_item_transitions`, `work_item_user_metrics_daily`, `work_items` | True | True |
| pagerduty | `business-services` | NATIVE | `operational_services` | True | True |
| pagerduty | `escalation-policies` | NATIVE | `operational_escalation_policies` | True | True |
| pagerduty | `incident-alerts` | NATIVE | `operational_alerts` | True | True |
| pagerduty | `incident-log-entries` | NATIVE | `operational_incident_timeline_events` | True | True |
| pagerduty | `incident-notes` | NATIVE | `operational_incident_notes` | True | True |
| pagerduty | `incidents` | NATIVE | `operational_incidents` | True | True |
| pagerduty | `on-calls` | NATIVE | `operational_on_call_assignments` | True | True |
| pagerduty | `schedules` | NATIVE | `operational_on_call_schedules` | True | True |
| pagerduty | `services` | NATIVE | `operational_service_repository_mappings`, `operational_services` | True | True |
| pagerduty | `teams` | NATIVE | `operational_teams` | True | True |
| pagerduty | `users` | NATIVE | `operational_users` | True | True |
<!-- END GENERATED PROVIDER SYNC MATRIX -->

**Correction to prior record:** `.remember/lane-common-brief.md:25` and `go-worker-migration-handoff.md:28`
(2026-09-04, 05:08 PDT) state "NO Go file references test_case_results/test_suite_results/testops tables."
This is wrong for raw ingestion -- see the `tests` dataset rows above (`internal/providersync/github_tests_effects_clickhouse.go:152,185,218`,
and the explicitly cross-provider `testOpsEffects`, `internal/providersync/testops_effects.go:23-28`, doc
comment: "the single six-destination effect projection shared by GitHub and GitLab TestOps handlers"). What
was still 100% Python at the time of that note is the separate daily-metrics *aggregation* layer built on
top of these tables -- `testops_pipeline`/`testops_test`/`testops_coverage` under METRICS below. The brief
conflated the two layers. **Superseded 2026-09-04 (CHAOS-4284): all three of those families are now NATIVE
too**, so neither layer is Python-only any more. **Further superseded 2026-09-05 (CHAOS-5245): `compute_testops.py`
and `compute_testops_risk.py` are both deleted entirely** -- their native Go executors (CHAOS-4284/CHAOS-4294)
have no Python fallback left; see the METRICS table below.

**Doc-drift finding (filed as a follow-up ticket, not fixed here):** `gitlab` `incidents` is present and
`native_go`/`route_ready` in `matrix.json` (table above reflects this correctly) but has no entry in the
separate `contracts/providers/v1/route-families.json` provider-route-family/budget contract -- a
cross-contract inconsistency between the repo's two provider-sync contract files, not a missing Go writer
(the writer exists: `internal/providersync/gitlab_incidents_effects_clickhouse.go:125`).

## METRICS

`metrics.daily_partition` (native families run inside `dailyNativeFamilyRegistrations`,
`cmd/dev-health-worker/daily.go` ~L680-820; everything else falls through to `HTTPCompatibilityExecutor` ->
`POST /internal/worker/daily-metrics/v1/execute` -> `job_daily.py:1104 run_daily_metrics_job`) and 7
independent `metrics.remaining.*` River kinds (`daily.go:566-646`) are the two WORKER-side families below.
The CLI verb layer used to bypass both -- several `dev-hops metrics` verbs called the Python compute
function directly, even for families whose worker kind is now native. CHAOS-5055/#2232 repointed the
LIVE `daily`/`rebuild`/`complexity`/`dora`/`capacity` verbs to dispatch through the Go worker instead
(rows below); CHAOS-5307 then deleted the orphaned direct-compute functions those verbs used to call,
which had been unreachable dead code (never wired into `cli.py`'s argparse tree) since #2232 landed:

| CLI verb | Executor | Writer call site | Ticket |
|---|---|---|---|
| `dev-hops metrics daily` / `rebuild` | NATIVE (dispatch) | `workerctl_dispatch.py` `_cmd_metrics_daily`/`_cmd_metrics_rebuild` -> `dev-health-workerctl metrics daily-start` -- the worker's own native/bridge split decides the rest (CHAOS-5055/#2232). The old direct-Python-compute `job_daily.py` `_cmd_metrics_daily`/`_cmd_metrics_rebuild` (never wired into `cli.py`, zero callers) were deleted (CHAOS-5307); `run_daily_metrics_job`/`run_daily_metrics_finalize` themselves are unaffected -- other live callers remain (the worker bridge, `scripts/compute_metrics_daily.py`, fixtures) | CHAOS-5055/CHAOS-5307 |
| `dev-health-workerctl metrics partition-recompute` | PARTIAL | `internal/jobs/metrics/daily/partition_recompute.go` -- Go-native REDRIVE only (bumps `daily_metrics_runs.generation`, republishes the partition claim); the recompute itself then follows the ordinary native/bridge split below, it is not a compute engine on its own | CHAOS-4459 |
| `dev-health-workerctl metrics daily-redrive` / `daily-finalize` / `finalize-redrive` | NATIVE (ledger repair) -> triggers the ordinary native/bridge split on replay | `cmd/dev-health-workerctl/main.go:785-1193` | CHAOS-4358/4389/4405 |
| `dev-hops metrics complexity` / `dora` / `capacity` | NATIVE (dispatch) | `workerctl_dispatch.py` -> `dev-health-workerctl metrics remaining trigger-backstop --family <complexity\|dora\|capacity>` (CHAOS-5055/#2232). The old direct-Python-compute `job_complexity_db.py`/`job_dora.py`/`job_capacity.py` CLI wrappers (never wired into `cli.py`, zero callers) were deleted (CHAOS-5307); the underlying compute functions themselves are unaffected -- other live callers remain (the worker bridge, GraphQL resolvers, fixtures/tests). `metrics release-impact`'s own module (`job_release_impact.py`) no longer exists as a file at all. | CHAOS-5055/CHAOS-5307 |
| `dev-hops metrics validate-flags` | **N/A -- confirmed still a read-only diagnostic**, no ClickHouse write, no worker path | `job_ff_validation.py` `_cmd_validate_flags` -> `run_validate_flags` (prints a report only) | -- |
| `dev-hops metrics compounding-risk` | COMPAT-Python (standalone CLI wrapper; duplicate coverage -- `job_daily.py`'s finalize already writes `compounding_risk_daily` nightly regardless) | `job_compounding_risk.py:318` | CHAOS-4287 |
| `dev-health-workerctl metrics remaining start` | NATIVE (manual backfill trigger) | `cmd/dev-health-workerctl/main.go:1598-1686` -- help text is stale, only lists complexity/dora/release_impact (doesn't mention membership_backfill/recommendations/work_item_attribution, which also exist) | CHAOS-4254 |
| membership_backfill / cognitive load / benchmarking | no dedicated Python CLI verb found | see the two tables below | -- |

### Daily metrics families (`internal/jobs/metrics/daily/families.json`)

<!-- BEGIN GENERATED DAILY METRICS MATRIX -->
| Family | Executor | Citation | Ticket |
| --- | --- | --- | --- |
| ai_governance | NATIVE | Go: `internal/jobs/metrics/daily/ai_governance_native_executor.go` (`AIGovernanceExecutor`) | CHAOS-4285 (Done) |
| ai_impact | NATIVE | Go: `internal/jobs/metrics/daily/ai_impact_native_executor.go` (`AIImpactExecutor`) | CHAOS-4280 (Done) |
| ai_workflow | NATIVE | Go: `internal/jobs/metrics/aiworkflow/compute.go` (`Compute`) -- ports the now-DELETED `work_graph/extractors/ai_workflow.py:extract_ai_workflow_from_pull_requests` (CHAOS-5242); no Python fallback | CHAOS-4286 |
| benchmarking | NATIVE | Python: `benchmarking/runner.py:259 run_benchmarking_for_day` | CHAOS-4288 |
| cicd | NATIVE | Go: `internal/jobs/metrics/daily/cicd/` | CHAOS-4292 (Done) |
| compounding_risk | NATIVE, post_bridge (repo) / COMPAT-Python (finalize) | Python: `job_daily.py:568 _write_compounding_risk_for_day` (repo scope, now native); `job_daily.py:613 _write_compounding_risk_team_rows_for_day` (team scope, still Python) | CHAOS-4287 |
| deploy | NATIVE | Go: `internal/jobs/metrics/daily/deploy_native_executor.go` | CHAOS-4293 (Done) |
| file_hotspots | NATIVE | Go: `internal/jobs/metrics/daily/file_hotspots_native_executor.go` | CHAOS-4277 (Done) |
| file_risk_hotspots | NATIVE | Go: `internal/jobs/metrics/daily/` (`FileRiskHotspotsExecutor`, `daily.go`) | CHAOS-4277 (Done) |
| ic_finalize | NATIVE | Python: `compute_ic.py` (`compute_ic_metrics_daily`, `compute_ic_landscape_rolling`; finalize scope) | CHAOS-4290 |
| incident | NATIVE | Go: `internal/jobs/metrics/daily/incident_native_executor.go` (Python bridge was permanently zero-yield for this family, CHAOS-4269) | CHAOS-4295 (Done) |
| repo_user_commit | NATIVE | Go: `internal/jobs/metrics/daily/repouser/` (`RepoUserCommitExecutor`) | CHAOS-4275 (Done) |
| review_edges | NATIVE | Python: `reviews.py:22 compute_review_edges_daily` | CHAOS-4279 |
| team_cognitive_load | NATIVE | Go: `internal/jobs/metrics/daily/team_cognitive_load_native_executor.go` (finalize scope, co-registered with ic_finalize) + `team_cognitive_load_clickhouse.go`. No Python remainder. | CHAOS-5141 |
| team_complexity | NATIVE | Go: `internal/jobs/metrics/daily/team_complexity_native_executor.go` (finalize scope, no co-registration dependency) + `team_complexity_clickhouse.go`. No Python remainder. | CHAOS-5051 |
| team_wellbeing | NATIVE | Go: `internal/jobs/metrics/daily/wellbeing_native_executor.go` | CHAOS-4276 (Done) |
| testops_coverage | NATIVE | Go: `internal/jobs/metrics/daily/testops_native_executor.go` (`TestopsCoverageExecutor`), latest snapshot picked in ClickHouse. CHAOS-5245 deleted the Python compute entirely -- no fallback left. | CHAOS-4284 (Done) |
| testops_pipeline | NATIVE | Go: `internal/jobs/metrics/daily/testops_native_executor.go` (`TestopsPipelineExecutor`), reuses `internal/jobs/metrics/testops/compute.go`'s pure compute. CHAOS-5245 deleted the Python compute (`compute_testops.py`) entirely -- no fallback left. | CHAOS-4284 (Done) |
| testops_risk | NATIVE | Go: `internal/jobs/metrics/daily/testops_risk_native_executor.go`, reuses `internal/jobs/metrics/testops/compute.go`'s pure compute. CHAOS-5245 deleted the Python compute (`compute_testops_risk.py`) entirely -- no fallback left. | CHAOS-4294 (Done) |
| testops_test | NATIVE | Go: `internal/jobs/metrics/daily/testops_native_executor.go` (`TestopsTestExecutor`); its ClickHouse reader reduces `test_case_results` per `case_name` in-database, so the 200k `DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS` cap has no native equivalent. CHAOS-5245 deleted the Python compute entirely -- no fallback left. | CHAOS-4284 (Done) |
| work_graph_edges | NATIVE | Python: `ai_workflow.py extract_review_deployment_incident_edges` | CHAOS-4286 |
| work_item | NATIVE | Go: `internal/jobs/metrics/daily/work_item_native_executor.go` -- pre_bridge, ordered after `work_item_attribution` by families.json's `after` edge; reuses `internal/jobs/metrics/workitemmetrics`'s pure compute (shared with the providersync sync-time deriver); ports `compute_work_items.py:1075 compute_work_item_metrics_daily` | CHAOS-4283 |
| work_item_attribution | NATIVE | Go: `internal/jobs/metrics/daily/work_item_attribution_native_executor.go` -- pre_bridge; ports `compute_work_items.py:1189 compute_work_item_team_attributions`, the FULL daily compute (distinct from §3's native staleness-only backstop of the same table). Runs before its three readers via families.json's `after` edges | CHAOS-4283 |
| work_item_estimate | NATIVE | Go: `internal/jobs/metrics/daily/work_item_estimate_native_executor.go` -- pre_bridge, ordered after `work_item_attribution`; same shared compute; ports `compute_work_items.py:1425 compute_estimate_coverage_metrics_daily` | CHAOS-4283 |
| work_item_state | NATIVE | Go: `internal/jobs/metrics/daily/work_item_state_native_executor.go` -- pre_bridge, ordered after the now-native `work_item_attribution` that writes the `work_item_team_attributions` it reads | CHAOS-4278 (Done) |
<!-- END GENERATED DAILY METRICS MATRIX -->

**`internal/jobs/metrics/testops/compute.go`** is the pure Go compute shared by `testops_risk` and the three
`testops_{pipeline,test,coverage}` families. It was written by CHAOS-4294 as an internal dependency of
`testops_risk`'s own input recompute and was, for a period, *built but not wired* -- no native family
consumed it and nothing wrote `testops_{pipeline,test,coverage}_metrics_daily`. **CHAOS-4284 closed that
gap**: those three families are NATIVE above, implemented in
`internal/jobs/metrics/daily/testops_native_executor.go` on top of this same compute, with cap-free
ClickHouse readers in `testops_native_clickhouse.go`. **CHAOS-5245 (folded from CHAOS-5246) deleted the
Python compute entirely** for all four families (`compute_testops.py`, `compute_testops_risk.py`, their
`job_daily.py` compute+write wiring, and the sink write methods) -- the native Go executors have no Python
fallback left. The former live-Python-oracle rot guards for `compute.go` (the `testdata/python_*_oracle.py`
scripts, `compute_test.go`, the `check_go.sh` `live-python-oracles` registrations) are deleted with them;
`internal/pythonparity`'s Neumaier-sum guards and `accumulator_test.go`'s three pure-Go accumulator-parity
tests (streaming vs. slice API) are what's left proving `compute.go` itself. The one exception:
`pipeline_stability_fma_golden_test.go` reads a FROZEN `tests/fixtures/pipeline_stability_fma_golden.json`
fixture (no live Python involved) and is now the permanent regression contract for
`computePipelineStability`'s FMA-safety fix (CHAOS-4818) -- its own rot guard
(`TestPipelineStabilityFMAGoldenMatchesLivePython`, which re-ran the now-deleted generator every CI run) is
deleted, the frozen file and this one test survive.

### Remaining metrics families (`internal/jobs/metrics/remaining/families.json`)

<!-- BEGIN GENERATED REMAINING METRICS MATRIX -->
| Family | Executor | Citation | Route transport | Ticket |
| --- | --- | --- | --- | --- |
| capacity | NATIVE | Go: `internal/jobs/metrics/remaining/capacity_native.go`, `capacity_native_clickhouse.go` | river, native (`daily.go:571-581`) | CUT-20 R2 (Done) |
| complexity | COMPAT-Python | Python: `api/internal/worker_metrics.py _run_complexity` -> `job_complexity_db.py:238 run_complexity_db_job` | river, bridge (`daily.go:582-585`, uses `compatibility` directly) | CHAOS-4291 |
| dora | NATIVE | Go: `internal/jobs/metrics/remaining/dora_native.go`, `dora_native_clickhouse.go` | river, native (`daily.go:586-598`) | CHAOS-3092 R1 (Done) |
| membership_backfill | NATIVE | Go: `internal/jobs/metrics/remaining/membership_native.go` | river, native (`daily.go:599-609`) | CHAOS-4282 (Done) |
| recommendations | NATIVE | Go: `internal/jobs/metrics/remaining/recommendations_native.go` | river, native (`daily.go:610-620`) | CHAOS-4281/CHAOS-3092 (Done) |
| release_impact | NATIVE | Go: `internal/jobs/metrics/remaining/release_impact_native_executor.go`, `release_impact_native_clickhouse.go`. CHAOS-5244: Python daily-compute orchestrator (`job_release_impact.py`, `compute_release_impact_daily`) deleted -- job compute deleted; `release_impact.py`'s `_compute_day` survives only as `fixtures/runner.py`'s local/CI fixture-generation dependency, fixture-generation path pending CHAOS-5250 | river, native (`daily.go:590-621`) | CHAOS-4296 (Done) |
| work_item_attribution | NATIVE (narrow: staleness backstop only) | Go: `internal/jobs/metrics/remaining/work_item_attribution_native.go` -- CHAOS-3092 PR-B staleness-window backstop, NOT the full daily attribution compute (that's §2's `work_item_attribution` row, native as of CHAOS-5078) | river, native (`daily.go:625-634`) | CHAOS-3092 PR-B (Done) |
<!-- END GENERATED REMAINING METRICS MATRIX -->

## RECOMMENDATIONS

| CLI verb | Executor | Writer call site | Ticket |
|---|---|---|---|
| `dev-hops recommendations compute` | COMPAT-Python (direct call) | `cli.py:260-287 _register_recommendations_commands` -> `_cmd_recommendations_compute` -> `RuleEngine` directly -- **bypasses** the now-native `metrics.remaining.recommendations` worker kind | -- |
| `metrics.remaining.recommendations` (worker kind) | NATIVE | see METRICS' remaining-families table above | CHAOS-4281/CHAOS-3092 (Done) |

## AI

| Area | Executor | Writer call site | Ticket |
|---|---|---|---|
| ai_governance / ai_impact / ai_workflow | NATIVE | see METRICS' daily-families table above (all three now native; this hand-authored row is not generator-checked and had drifted stale for all three, not just the family this row's own PR ported -- caught by codex round chaos-5220-r1) | CHAOS-4285/4280/4286 |
| **ai attribution** | **PARTIAL** | WRITE path: NATIVE for github (`internal/providersync/github_work_items_ai_attribution_effects_clickhouse.go`, part of native work-items sync) and gitlab/linear (`gitlab_work_item_derived.go:423`, `linear_work_items_derived.go:51,283` -- both build/write the `ai_attribution` projection as part of native work-items sync); jira explicitly writes **zero** rows by design ("evaluated-empty effect", `jira_work_item_derived.go:16-21` -- no AI-attribution signal exists for jira, not a gap). READ path: Python still consumes `ai_attribution` as an input to ai_governance/ai_impact compute (`job_daily.py:1677 ai_loader.load_ai_pr_attributions`) -- that consumption stays COMPAT along with those two families. | none found |

## INVESTMENT / WORK-GRAPH

| CLI verb | Executor | Writer call site | Ticket |
|---|---|---|---|
| `dev-hops work-graph build` | COMPAT-Python | `work_graph/runner.py run_work_graph_build`, wired through the SAME `workgraph.build` bridge as the worker path (table below) | CHAOS-4441 |
| `dev-hops investment materialize` | COMPAT-Python | `work_graph/runner.py run_investment_materialization`. NOTE: the CLI verb is a SEPARATE entry point from the `investment.materialize` River kind, which is NATIVE (table below) -- the CLI still runs the Python implementation directly, and re-pointing it is CHAOS-4767's follow-up. | CHAOS-4767 |

No `families.json` equivalent exists for these 5 River kinds (`internal/jobs/families.json` does not exist)
-- the table below is entirely hand-tracked in `WORKGRAPH_INVESTMENT_LEDGER` in
`scripts/gen_go_migration_matrix_docs.py`; there is no live producer to drift-guard against mechanically.
Recommendations/DORA/cognitive-load rows cross-reference METRICS above rather than re-deriving there.

<!-- BEGIN GENERATED WORKGRAPH INVESTMENT MATRIX -->
| Kind/area | Executor | Citation | Route transport | Ticket |
| --- | --- | --- | --- | --- |
| DORA | NATIVE | see §3 | river, native | CHAOS-3092 R1 (Done) |
| cognitive load (team_cognitive_load) | NATIVE | see §2 | river, native -- finalize scope, co-registered with ic_finalize | CHAOS-5141 |
| investment.materialize | NATIVE | Go: `internal/jobs/investment/nativeexecutor.go` (implements the same `workgraph.CompatibilityExecutor` seam the bridge did) -> `materialize.go` orchestrator -> `chquery` fetch + `materializecomponent.go` assembly + `categorize` LLM plane + `chwrite` write. Python `materialize.py:1169-1854 materialize_investments()` is retained but no longer reached from the worker path (removal is CHAOS-4767) | river, native -- `addWorkgraphWorker`'s `KindInvestmentMaterialize` case takes `nativeInvestment` | CHAOS-4441 (cutover landed) |
| recommendations | NATIVE | see §3 | river, native | CHAOS-4281/CHAOS-3092 (Done) |
| workgraph.build | COMPAT-Python (narrow native pre/post-step) | Go: `internal/jobs/workgraph/prestep.go` (issue-PR edge mapping, runs BEFORE the bridge) + one `poststep.go` edge type (runs AFTER); Python: `worker_workgraph.py:367 execute` (LLM categorization -- "Python owns 100% of the compute" per prestep.go's own doc comment) | bridge -- `addWorkgraphWorker`'s `KindWorkGraphBuild` case still takes the HTTP `executor` | CHAOS-4924 (six remaining sub-builders + cutover) |
<!-- END GENERATED WORKGRAPH INVESTMENT MATRIX -->

~~**Built but unwired:** `internal/jobs/investment/materializecomponent.go` ... has **zero non-test
callers** anywhere in the tree.~~ **RESOLVED (CHAOS-4441 cutover):** the executor wiring landed.
`internal/jobs/investment/nativeexecutor.go` implements the same `workgraph.CompatibilityExecutor` seam
the HTTP bridge implements, `materialize.go` orchestrates fetch -> assembly -> categorize -> write, and
`addWorkgraphWorker` hands the `investment.materialize` case that executor instead of the bridge.
Scheduler, reconciler, outbox and the `work_graph_execution_request:<id>` completion fence are unchanged
by construction -- the seam is the same interface.

**How to check this claim rather than trust it:** `contracts/native-families/v1/native-families.json`'s
`workgraph` section is AST-derived from `addWorkgraphWorker`'s dispatch switch, and this document's §4
generator refuses to render when the two disagree. That guard exists because CHAOS-4441 was marked Done
on 2026-09-03 while every kind still dispatched to Python and no artifact in the tree could contradict
it.

## WEBHOOKS

| Area | Executor | Writer call site | Ticket |
|---|---|---|---|
| `operational.webhook_delivery` | COMPAT-Python (Go job shell only) | Go: `internal/jobs/operational/http.go:19,35,44,50,63` (`webhookEndpoint`) <- `cmd/dev-health-worker/operational.go:65` (`POST /api/internal/worker-operational/webhook`); Python: `worker_operational.py:119 process_webhook_reference` -> `system_webhooks.py:63 process_webhook_event`. 100% of webhook receipt/parse/reconciliation is Python -- no native pre-step exists (unlike `workgraph.build`'s prestep). | CHAOS-4440 (stale docstring only) |

## STREAMS

| Profile | Executor | Writer call site | Ticket |
|---|---|---|---|
| `ingest` (internal product events) | NATIVE | `internal/streamhandlers/`; `cmd/dev-health-stream-runner/dependencies.go:10` profile list | -- |
| `product-telemetry` | NATIVE -- a real separate handler, not folded into `ingest` (scheduler still names it `process-product-telemetry-streams`, `internal/scheduler/fixed/inventory.go:535`, but it's dispatched via the `ingest` binary's `productTelemetryHandlerKind`, `dependencies.go:60,199,521-522`) | `internal/streamhandlers/product_telemetry.go:52-58` | -- |
| `external` | NATIVE | `dependencies.go:10` | -- |
| `pagerduty` | PARTIAL -- Go stream shell native, Python compute (per the 2026-08-28 snapshot, not independently re-verified this pass) | `dependencies.go:10`; `internal/jobs/pagerduty/compatibility.go` | CHAOS-4105 (Backlog) |

## SCHEDULER / RECONCILER / OPERATOR

All NATIVE, no Python involvement found in any of the three:

| Binary | Executor | Note |
|---|---|---|
| `dev-health-scheduler` | NATIVE | `cmd/dev-health-scheduler/` -- writes `worker_job_outbox` only |
| `dev-health-reconciler` | NATIVE | `cmd/dev-health-reconciler/` -- relays outbox into `river_job` |
| `dev-health-workerctl` (operator CLI: status/jobs/queues/routes/job-routes/contracts) | NATIVE | pure Go, no Python calls found in this CLI's own dispatch tree |

## Out of migration scope (Python by design)

Chris, 2026-09-04 05:54: these `dev-hops` CLI areas are **not part of the Go worker migration** -- separate
Python functions, untouched by design, no further tracing needed:

- `dev-hops audit *` (completeness/schema/perf/coverage subcommands) -- likely out of date
- `dev-hops maintenance *` (cleanup-tokens/cleanup-all/scrub-error-text/backfill-ask-dev-ephemeral-expiry) --
  likely out of date
- `dev-hops push` (customer-push external ingestion, CHAOS-2700)
- `dev-hops billing` / `service-credentials` / `admin` / `migrate`
- `dev-hops workers inspect` -- candidate for deprecation

## Known gaps (not fixed in this PR)

- ~~**`internal/jobs/families.json` does not exist** -- workgraph/investment kinds have no machine-readable
  registry, so the INVESTMENT/WORK-GRAPH table cannot be mechanically drift-gated.~~ **PARTLY CLOSED
  (CHAOS-4441):** the EXECUTOR column is now drift-gated -- `native-families.json` grew a `workgraph`
  section AST-derived from the dispatch switch, and §4's generator fails when the curated ledger
  disagrees with it. Still hand-maintained and ungated: the citation, route-transport and ticket columns.
  Adding a `port`-style field to a workgraph/investment
  registry file is proposed as a follow-up ticket, not done here (a Go schema change, out of scope for a
  docs+tooling change).
- **`gitlab` `incidents` cross-contract inconsistency** -- see the callout under SYNC above.
- **`team_complexity`** -- see the callout under METRICS above.
