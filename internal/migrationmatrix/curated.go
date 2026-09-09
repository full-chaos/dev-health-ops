// Code content generated once from scripts/gen_go_migration_matrix_docs.py's
// curated *_LEDGER dicts (CHAOS-5473 port), then hand-maintained here. Every
// string is transcribed verbatim (byte-identical proof: lane-5473-matrix scratch,
// see the PR body) -- this is prose/citation text, not logic.
package migrationmatrix

// GoExecutorTranslation maps matrix.json's go_executor values to this page's
// Executor legend. A value not in this map is a genuinely new state the legend
// hasn't seen yet -- RenderProviderSyncBlock refuses rather than guessing.
var GoExecutorTranslation = map[string]string{
	"native_go": "NATIVE",
}

// DailyCitationRow is one curated §2 row: citation/ticket prose only. The
// Executor verdict itself comes from native-families.json, never this struct.
type DailyCitationRow struct {
	Citation string
	Ticket   string
}

// DailyCitationLedger is CURATED: §2 citation/ticket text only,
// internal/jobs/metrics/daily/families.json. Ported verbatim from
// scripts/gen_go_migration_matrix_docs.py's DAILY_CITATION_LEDGER (CHAOS-5473).
var DailyCitationLedger = map[string]DailyCitationRow{
	"ai_governance":         {Citation: "Go: `internal/jobs/metrics/daily/ai_governance_native_executor.go` (`AIGovernanceExecutor`)", Ticket: "CHAOS-4285 (Done)"},
	"ai_impact":             {Citation: "Go: `internal/jobs/metrics/daily/ai_impact_native_executor.go` (`AIImpactExecutor`)", Ticket: "CHAOS-4280 (Done)"},
	"ai_workflow":           {Citation: "Go: `internal/jobs/metrics/aiworkflow/compute.go` (`Compute`) -- ports the now-DELETED `work_graph/extractors/ai_workflow.py:extract_ai_workflow_from_pull_requests` (CHAOS-5242); no Python fallback", Ticket: "CHAOS-4286"},
	"benchmarking":          {Citation: "Go: `internal/jobs/metrics/daily/benchmarking_finalize_native_executor.go` (finalize scope). CHAOS-4288 deleted the Python compute (`src/dev_health_ops/metrics/benchmarking/`) entirely -- no fallback left.", Ticket: "CHAOS-4288 (Done)"},
	"cicd":                  {Citation: "Go: `internal/jobs/metrics/daily/cicd/`", Ticket: "CHAOS-4292 (Done), CHAOS-5312 (Done)"},
	"compounding_risk":      {Citation: "Go: `internal/jobs/metrics/daily/compounding_risk_native_executor.go` (`CompoundingRiskExecutor`)", Ticket: "CHAOS-4287/CHAOS-5308 (Done)"},
	"compounding_risk_team": {Citation: "Go: `internal/jobs/metrics/daily/compounding_risk_team_native_executor.go` (`CompoundingRiskTeamExecutor`)", Ticket: "CHAOS-5084 (Done)"},
	"deploy":                {Citation: "Go: `internal/jobs/metrics/daily/deploy_native_executor.go`", Ticket: "CHAOS-4293 (Done)"},
	"file_hotspots":         {Citation: "Go: `internal/jobs/metrics/daily/file_hotspots_native_executor.go`", Ticket: "CHAOS-4277 (Done)"},
	"file_risk_hotspots":    {Citation: "Go: `internal/jobs/metrics/daily/` (`FileRiskHotspotsExecutor`, `daily.go`)", Ticket: "CHAOS-4277 (Done)"},
	"ic_finalize":           {Citation: "Go: `internal/jobs/metrics/daily/ic_finalize_native_executor.go` (`ICFinalizeExecutor`, finalize scope, co-registered with `team_cognitive_load`). CHAOS-4290 PR3 deleted the Python compute (`compute_ic.py`'s `compute_ic_metrics_daily` / `compute_ic_landscape_rolling`) entirely -- no fallback left.", Ticket: "CHAOS-4290 (Done)"},
	"incident":              {Citation: "Go: `internal/jobs/metrics/daily/incident_native_executor.go` (Python bridge was permanently zero-yield for this family, CHAOS-4269)", Ticket: "CHAOS-4295 (Done), CHAOS-5313 (Done)"},
	"repo_user_commit":      {Citation: "Go: `internal/jobs/metrics/daily/repouser/` (`RepoUserCommitExecutor`)", Ticket: "CHAOS-4275 (Done)"},
	"review_edges":          {Citation: "Go: `internal/jobs/metrics/daily/review_edges_native_executor.go` (pre_bridge). CHAOS-4279 deleted the Python compute (`reviews.py compute_review_edges_daily`) entirely -- no fallback left.", Ticket: "CHAOS-4279 (Done)"},
	"team_cognitive_load":   {Citation: "Go: `internal/jobs/metrics/daily/team_cognitive_load_native_executor.go` (finalize scope, co-registered with ic_finalize) + `team_cognitive_load_clickhouse.go`. No Python remainder.", Ticket: "CHAOS-5141"},
	"team_complexity":       {Citation: "Go: `internal/jobs/metrics/daily/team_complexity_native_executor.go` (finalize scope, no co-registration dependency) + `team_complexity_clickhouse.go`. No Python remainder.", Ticket: "CHAOS-5051"},
	"team_wellbeing":        {Citation: "Go: `internal/jobs/metrics/daily/wellbeing_native_executor.go`", Ticket: "CHAOS-4276 (Done), CHAOS-5311 (Done)"},
	"testops_coverage":      {Citation: "Go: `internal/jobs/metrics/daily/testops_native_executor.go` (`TestopsCoverageExecutor`), latest snapshot picked in ClickHouse. CHAOS-5245 deleted the Python compute entirely -- no fallback left.", Ticket: "CHAOS-4284 (Done)"},
	"testops_pipeline":      {Citation: "Go: `internal/jobs/metrics/daily/testops_native_executor.go` (`TestopsPipelineExecutor`), reuses `internal/jobs/metrics/testops/compute.go`'s pure compute. CHAOS-5245 deleted the Python compute (`compute_testops.py`) entirely -- no fallback left.", Ticket: "CHAOS-4284 (Done)"},
	"testops_risk":          {Citation: "Go: `internal/jobs/metrics/daily/testops_risk_native_executor.go`, reuses `internal/jobs/metrics/testops/compute.go`'s pure compute. CHAOS-5245 deleted the Python compute (`compute_testops_risk.py`) entirely -- no fallback left.", Ticket: "CHAOS-4294 (Done)"},
	"testops_test":          {Citation: "Go: `internal/jobs/metrics/daily/testops_native_executor.go` (`TestopsTestExecutor`); its ClickHouse reader reduces `test_case_results` per `case_name` in-database, so the 200k `DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS` cap has no native equivalent. CHAOS-5245 deleted the Python compute entirely -- no fallback left.", Ticket: "CHAOS-4284 (Done)"},
	"work_graph_edges":      {Citation: "Go: `internal/jobs/metrics/daily/work_graph_edges_native_executor.go` (`WorkGraphEdgesExecutor`)", Ticket: "CHAOS-4286 (Done)"},
	"work_item":             {Citation: "Go: `internal/jobs/metrics/daily/work_item_native_executor.go` -- pre_bridge, ordered after `work_item_attribution` by families.json's `after` edge; reuses `internal/jobs/metrics/workitemmetrics`'s pure compute (shared with the providersync sync-time deriver); ported `compute_work_item_metrics_daily` (compute_work_items.py), deleted entirely by CHAOS-5310/CHAOS-3092 (fully native, no remaining Python caller)", Ticket: "CHAOS-4283"},
	"work_item_attribution": {Citation: "Go: `internal/jobs/metrics/daily/work_item_attribution_native_executor.go` -- pre_bridge; ported `compute_work_item_team_attributions` (compute_work_items.py), the FULL daily compute (distinct from §3's native staleness-only backstop of the same table), deleted entirely by CHAOS-5321/CHAOS-3092 (fully native, no remaining Python caller). Runs before its three readers via families.json's `after` edges", Ticket: "CHAOS-4283"},
	"work_item_estimate":    {Citation: "Go: `internal/jobs/metrics/daily/work_item_estimate_native_executor.go` -- pre_bridge, ordered after `work_item_attribution`; same shared compute; ports `compute_work_items.py:1425 compute_estimate_coverage_metrics_daily`", Ticket: "CHAOS-4283"},
	"work_item_state":       {Citation: "Go: `internal/jobs/metrics/daily/work_item_state_native_executor.go` -- pre_bridge, ordered after the now-native `work_item_attribution` that writes the `work_item_team_attributions` it reads; ported `compute_work_item_state_durations_daily` (compute_work_item_state_durations.py), deleted entirely by CHAOS-5321/CHAOS-3092 (fully native, no remaining Python caller)", Ticket: "CHAOS-4278 (Done)"},
}

// RemainingCitationRow is one curated §3 row.
type RemainingCitationRow struct {
	Citation string
	Route    string
	Ticket   string
}

// RemainingExecutorLedger is CURATED: §3 citation/ticket text only,
// internal/jobs/metrics/remaining/families.json. Ported verbatim from
// scripts/gen_go_migration_matrix_docs.py's REMAINING_EXECUTOR_LEDGER (CHAOS-5473).
var RemainingExecutorLedger = map[string]RemainingCitationRow{
	"capacity":              {Citation: "Go: `internal/jobs/metrics/remaining/capacity_native.go`, `capacity_native_clickhouse.go`", Route: "river, native (`daily.go:571-581`)", Ticket: "CUT-20 R2 (Done)"},
	"complexity":            {Citation: "Go: `internal/jobs/metrics/remaining/complexity_native.go`, `complexity_native_clickhouse.go`", Route: "river, native (`daily.go:486-527`)", Ticket: "CHAOS-4291 (Done)"},
	"dora":                  {Citation: "Go: `internal/jobs/metrics/remaining/dora_native.go`, `dora_native_clickhouse.go`", Route: "river, native (`daily.go:586-598`)", Ticket: "CHAOS-3092 R1 (Done)"},
	"membership_backfill":   {Citation: "Go: `internal/jobs/metrics/remaining/membership_native.go`", Route: "river, native (`daily.go:599-609`)", Ticket: "CHAOS-4282 (Done)"},
	"recommendations":       {Citation: "Go: `internal/jobs/metrics/remaining/recommendations_native.go`", Route: "river, native (`daily.go:610-620`)", Ticket: "CHAOS-4281/CHAOS-3092 (Done)"},
	"release_impact":        {Citation: "Go: `internal/jobs/metrics/remaining/release_impact_native_executor.go`, `release_impact_native_clickhouse.go`. CHAOS-5244: Python daily-compute orchestrator (`job_release_impact.py`, `compute_release_impact_daily`) deleted -- job compute deleted; `release_impact.py`'s `_compute_day` survives only as `fixtures/runner.py`'s local/CI fixture-generation dependency, fixture-generation path pending CHAOS-5250", Route: "river, native (`daily.go:590-621`)", Ticket: "CHAOS-4296 (Done)"},
	"work_item_attribution": {Citation: "Go: `internal/jobs/metrics/remaining/work_item_attribution_native.go` -- CHAOS-3092 PR-B staleness-window backstop, NOT the full daily attribution compute (that's §2's `work_item_attribution` row, native as of CHAOS-5078)", Route: "river, native (`daily.go:625-634`)", Ticket: "CHAOS-3092 PR-B (Done)"},
}

// WorkgraphInvestmentRow is one curated §4 row -- entirely hand-maintained,
// no families.json equivalent exists for workgraph/investment kinds.
type WorkgraphInvestmentRow struct {
	Executor string
	Citation string
	Route    string
	Ticket   string
}

// WorkgraphInvestmentLedger is CURATED §4. Ported verbatim from
// scripts/gen_go_migration_matrix_docs.py's WORKGRAPH_INVESTMENT_LEDGER (CHAOS-5473).
var WorkgraphInvestmentLedger = map[string]WorkgraphInvestmentRow{
	"DORA":                                 {Executor: "NATIVE", Citation: "see §3", Route: "river, native", Ticket: "CHAOS-3092 R1 (Done)"},
	"cognitive load (team_cognitive_load)": {Executor: "NATIVE", Citation: "see §2", Route: "river, native -- finalize scope, co-registered with ic_finalize", Ticket: "CHAOS-5141"},
	"investment.materialize":               {Executor: "NATIVE", Citation: "Go: `internal/jobs/investment/nativeexecutor.go` (implements the same `workgraph.NativeExecutor` seam the bridge did) -> `materialize.go` orchestrator -> `chquery` fetch + `materializecomponent.go` assembly + `categorize` LLM plane + `chwrite` write. Python `materialize.py:1169-1854 materialize_investments()` is retained but no longer reached from the worker path (removal is CHAOS-4767)", Route: "river, native -- `addWorkgraphWorker`'s `KindInvestmentMaterialize` case takes `nativeInvestment`", Ticket: "CHAOS-4441 (cutover landed)"},
	"recommendations":                      {Executor: "NATIVE", Citation: "see §3", Route: "river, native", Ticket: "CHAOS-4281/CHAOS-3092 (Done)"},
	"workgraph.build":                      {Executor: "NATIVE", Citation: "Go: `internal/jobs/workgraph/handler.go`'s `buildHandler` runs the full `buildPreStepOrder()` sequence (issue<->PR/issue<->commit/PR<->commit edges, flag-guards, operational-incident, issue<->issue edges) natively, no bridge call at all. Python's `WorkGraphBuilder.build()` (`src/dev_health_ops/work_graph/builder.py`) is DELETED -- every stage it used to run was already a 0-stats no-op by the time of this cutover", Route: "river, native -- `addWorkgraphWorker`'s `KindWorkGraphBuild` case takes no executor at all", Ticket: "CHAOS-4924 (cutover landed)"},
}

// FinalizeTarget is the (namespace, family) a finalize-scope Python call
// writes/computes for -- namespace is "daily" (§2) or "remaining" (§3).
type FinalizeTarget struct {
	Namespace string
	Family    string
}

// FinalizeCallIrregularFamily names a finalize call whose name does NOT embed
// its family in the `_write_<family>_..._for_day` shape by convention -- see
// scripts/gen_go_migration_matrix_docs.py's (deleted) history for the class of
// call this covers. Empty today: every finalize-scope Python write has been
// deleted outright (CHAOS-5141/5084/5051/4290/4288); kept as the extension
// point a future irregular finalize write needs.
var FinalizeCallIrregularFamily = map[string]FinalizeTarget{}

// FinalizeCallDormantSkipGated forces a call that WOULD resolve via the naming
// convention to be treated as out-of-scope because it is proven
// skip_families-gated with no reachable fallback. Empty today; kept as the
// extension point for the next instance of this class (see history above).
var FinalizeCallDormantSkipGated = map[string]FinalizeTarget{}
