package daily

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func mustTime(t *testing.T, hour, minute, second int) time.Time {
	t.Helper()
	return time.Date(2026, 8, 15, hour, minute, second, 0, time.UTC)
}

func floatPtr(v float64) *float64 { return &v }
func uintPtr(v uint32) *uint32    { return &v }

// buildTestopsFixture returns the SAME raw rows
// testdata/python_testops_risk_oracle.py constructs, translated to this
// package's Go row shapes. Any edit to one side must mirror the other, or
// TestTestopsRiskComputeMatchesLivePythonProduction stops proving anything.
func buildTestopsFixture(t *testing.T) (
	repoID uuid.UUID, day time.Time,
	pipelineRuns []testopsPipelineRunRow,
	suites []testopsSuiteRow,
	cases []testopsCaseRow,
	historicalFailedNames map[string]struct{},
	coverage, priorCoverage []testopsCoverageSnapshotRow,
) {
	t.Helper()
	const orgID = "00000000-0000-4000-8000-000000000009"
	repoID = uuid.MustParse("00000000-0000-4000-8000-000000000001")
	day = time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	teamA, svcA := strPtr("team-a"), strPtr("svc-a")
	teamB, svcB := strPtr("team-b"), strPtr("svc-b")

	pipelineRuns = []testopsPipelineRunRow{
		{
			RepoID: repoID, Status: strPtr("success"),
			QueuedAt: timePtr(mustTime(t, 9, 0, 0)), StartedAt: mustTime(t, 9, 1, 0), FinishedAt: timePtr(mustTime(t, 9, 11, 0)),
			RetryCount: 0, TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
		{
			RepoID: repoID, Status: strPtr("failed"),
			QueuedAt: timePtr(mustTime(t, 10, 0, 0)), StartedAt: mustTime(t, 10, 2, 0), FinishedAt: timePtr(mustTime(t, 10, 20, 0)),
			RetryCount: 1, TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
		{
			RepoID: repoID, Status: strPtr("success"),
			QueuedAt: timePtr(mustTime(t, 11, 0, 0)), StartedAt: mustTime(t, 11, 3, 0), FinishedAt: timePtr(mustTime(t, 11, 9, 0)),
			RetryCount: 0, TeamID: teamB, ServiceID: svcB, OrgID: orgID,
		},
	}

	suites = []testopsSuiteRow{
		{
			RepoID: repoID, RunID: "run-1", SuiteID: "suite-1",
			TotalCount: 10, PassedCount: 8, FailedCount: 2, QuarantinedCount: 1,
			DurationSecs: floatPtr(42.0), StartedAt: timePtr(mustTime(t, 9, 1, 0)), FinishedAt: timePtr(mustTime(t, 9, 5, 0)),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
		{
			RepoID: repoID, RunID: "run-2", SuiteID: "suite-2",
			TotalCount: 5, PassedCount: 3, FailedCount: 2,
			DurationSecs: floatPtr(88.0), StartedAt: timePtr(mustTime(t, 10, 2, 0)), FinishedAt: timePtr(mustTime(t, 10, 10, 0)),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
	}

	cases = []testopsCaseRow{
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "test_flaky", Status: strPtr("passed"), RetryAttempt: 1},
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "test_flaky", Status: strPtr("failed"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-2", SuiteID: "suite-2", CaseName: "test_recurrent_failure", Status: strPtr("failed"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-2", SuiteID: "suite-2", CaseName: "test_stable", Status: strPtr("passed"), RetryAttempt: 0},
	}

	historicalFailedNames = map[string]struct{}{"test_recurrent_failure": {}}

	coverage = []testopsCoverageSnapshotRow{
		{
			RepoID: repoID, RunID: "run-1", SnapshotID: "snap-1",
			LinesTotal: uintPtr(1000), LinesCovered: uintPtr(800),
			LineCoveragePct: floatPtr(80.0), BranchCoveragePct: floatPtr(70.0),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
	}
	priorCoverage = []testopsCoverageSnapshotRow{
		{
			RepoID: repoID, RunID: "run-0", SnapshotID: "snap-0",
			LinesTotal: uintPtr(1000), LinesCovered: uintPtr(850),
			LineCoveragePct: floatPtr(85.0), BranchCoveragePct: floatPtr(72.0),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
	}
	return
}

func timePtr(v time.Time) *time.Time { return &v }

// releaseConfidenceAsMap/qualityDragAsMap/pipelineStabilityAsMap build the
// exact snake_case shape testdata/python_testops_risk_oracle.py's
// canonical() prints (asdict(record) with computed_at popped, repo_id
// stringified, day as an ISO date -- not an RFC3339 timestamp, which is why
// this converts explicitly rather than round-tripping through
// encoding/json's default time.Time marshaling).
func releaseConfidenceAsMap(row *testopsReleaseConfidenceRow) map[string]any {
	return map[string]any{
		"repo_id":                 row.RepoID.String(),
		"day":                     row.Day.Format("2006-01-02"),
		"confidence_score":        row.ConfidenceScore,
		"pipeline_success_factor": row.PipelineSuccessFactor,
		"test_pass_factor":        row.TestPassFactor,
		"coverage_factor":         row.CoverageFactor,
		"flake_penalty":           row.FlakePenalty,
		"regression_penalty":      row.RegressionPenalty,
		"factors_json":            row.FactorsJSON,
		"team_id":                 derefStr(row.TeamID),
		"service_id":              derefStr(row.ServiceID),
		"org_id":                  row.OrgID,
	}
}

func qualityDragAsMap(row *testopsQualityDragRow) map[string]any {
	return map[string]any{
		"repo_id":                   row.RepoID.String(),
		"day":                       row.Day.Format("2006-01-02"),
		"drag_hours":                row.DragHours,
		"failure_rework_hours":      row.FailureReworkHours,
		"flake_investigation_hours": row.FlakeInvestigationHours,
		"queue_wait_hours":          row.QueueWaitHours,
		"retry_overhead_hours":      row.RetryOverheadHours,
		"factors_json":              row.FactorsJSON,
		"team_id":                   derefStr(row.TeamID),
		"service_id":                derefStr(row.ServiceID),
		"org_id":                    row.OrgID,
	}
}

func pipelineStabilityAsMap(row *testopsPipelineStabilityRow) map[string]any {
	var recovery any
	if row.MedianRecoveryTimeSeconds != nil {
		recovery = *row.MedianRecoveryTimeSeconds
	}
	return map[string]any{
		"repo_id":                      row.RepoID.String(),
		"day":                          row.Day.Format("2006-01-02"),
		"stability_index":              row.StabilityIndex,
		"success_rate_7d":              row.SuccessRate7d,
		"success_rate_trend":           row.SuccessRateTrend,
		"failure_clustering_score":     row.FailureClusteringScore,
		"median_recovery_time_seconds": recovery,
		"team_id":                      derefStr(row.TeamID),
		"service_id":                   derefStr(row.ServiceID),
		"org_id":                       row.OrgID,
	}
}

// normalizeOracleTeamService replaces a Python `null` team_id/service_id
// (decoded as a nil interface{} by encoding/json) with "" to match
// derefStr's nil-pointer convention above -- both sides mean "absent",
// just represented differently after their respective round trips.
func normalizeOracleTeamService(row map[string]any) map[string]any {
	for _, key := range []string{"team_id", "service_id"} {
		if row[key] == nil {
			row[key] = ""
		}
	}
	return row
}

// TestTestopsRiskComputeMatchesLivePythonProduction executes the production
// Python testops_risk compute path (compute_pipeline_metrics_daily ->
// compute_test_metrics_daily -> compute_coverage_metrics_daily ->
// compute_release_confidence/compute_quality_drag/compute_pipeline_stability)
// on a fixed multi-team-group case and compares it, row for row including
// factors_json, against this package's Go port. A parity test never
// observed to fail is not evidence (go-checks skill): perturbing any single
// input row's status/duration/coverage value in the fixture flips the
// expected numbers on both sides identically, which is how this test was
// authored against the oracle's own printed output before being pinned here.
func TestTestopsRiskComputeMatchesLivePythonProduction(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		t.Fatal("PYTHON is required for the live testops_risk Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "testdata/python_testops_risk_oracle.py")
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "daily")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute production Python testops_risk oracle: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	var oracle struct {
		ReleaseConfidence []map[string]any `json:"release_confidence"`
		QualityDrag       []map[string]any `json:"quality_drag"`
		PipelineStability []map[string]any `json:"pipeline_stability"`
	}
	output := bytes.TrimSpace(stdout.Bytes())
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	if err := json.Unmarshal(output, &oracle); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}

	repoID, day, pipelineRuns, suites, cases, historicalFailedNames, coverage, priorCoverage := buildTestopsFixture(t)
	computedAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	pipelineMetrics := computeTestopsPipelineMetrics(repoID, pipelineRuns)
	testMetrics := computeTestopsTestMetrics(repoID, suites, cases, historicalFailedNames)
	coverageMetric := computeTestopsCoverageMetric(repoID, coverage, priorCoverage)

	var pipe *testopsPipelineMetric
	if n := len(pipelineMetrics); n > 0 {
		pipe = &pipelineMetrics[n-1]
	}
	var test *testopsTestMetric
	if len(testMetrics) > 0 {
		test = &testMetrics[0]
	}

	releaseConfidenceRow := computeReleaseConfidence(repoID, day, pipe, test, coverageMetric, computedAt)
	qualityDragRow := computeQualityDrag(repoID, day, pipe, test, computedAt)
	pipelineStabilityRow := computePipelineStability(repoID, day, pipelineMetrics, computedAt)

	if len(oracle.ReleaseConfidence) != 1 || releaseConfidenceRow == nil {
		t.Fatalf("release_confidence row count mismatch: python=%d go_nil=%v", len(oracle.ReleaseConfidence), releaseConfidenceRow == nil)
	}
	assertJSONEqual(t, "release_confidence", normalizeOracleTeamService(oracle.ReleaseConfidence[0]), releaseConfidenceAsMap(releaseConfidenceRow))

	if len(oracle.QualityDrag) != 1 || qualityDragRow == nil {
		t.Fatalf("quality_drag row count mismatch: python=%d go_nil=%v", len(oracle.QualityDrag), qualityDragRow == nil)
	}
	assertJSONEqual(t, "quality_drag", normalizeOracleTeamService(oracle.QualityDrag[0]), qualityDragAsMap(qualityDragRow))

	if len(oracle.PipelineStability) != 1 || pipelineStabilityRow == nil {
		t.Fatalf("pipeline_stability row count mismatch: python=%d go_nil=%v", len(oracle.PipelineStability), pipelineStabilityRow == nil)
	}
	assertJSONEqual(t, "pipeline_stability", normalizeOracleTeamService(oracle.PipelineStability[0]), pipelineStabilityAsMap(pipelineStabilityRow))
}

func assertJSONEqual(t *testing.T, label string, want, got map[string]any) {
	t.Helper()
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("%s mismatch:\npython=%s\ngo=    %s", label, wantJSON, gotJSON)
	}
}
