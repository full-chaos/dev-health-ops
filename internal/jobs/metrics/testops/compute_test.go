package testops

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

// Each of the three tests below executes the PRODUCTION Python compute
// function (compute_testops.py) against a REAL row pulled once from the
// shared dev stack's ClickHouse (org 70d529e0-3c06-4597-8480-794fd02328b6,
// the local real-data proof org -- see .remember/lane-common-brief.md's
// evidence-label rules) and pinned as a frozen literal on both sides (the
// Python testdata/*.py script and this file's Go fixture), rather than
// querying ClickHouse at test time -- reproducible without live access,
// same tradeoff internal/jobs/metrics/daily/repouser's "frozen ... golden
// vs live Python" test already makes. Any edit to a fixture must mirror
// both sides or the test stops proving anything (same rule as
// internal/jobs/metrics/daily/testops_risk_native_test.go's synthetic
// oracle).

// runPythonOracle executes a testdata/*.py oracle script and, on success,
// writes a proof marker (markerName) into DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR
// -- required (not merely read), matching internal/jobs/metrics/daily/repouser's
// golden_rot_guard_test.go and internal/jobs/metrics/numerical's own rot
// guards: ci/check_go.sh's live-python-oracles step verifies each marker
// file exists and reads "executed" after the run, so this package's four
// oracle tests cannot be silently skipped, renamed, or filtered out of a
// -run pattern without the STANDING GATE noticing (not just a manual
// `go test` invocation this session happened to run).
func runPythonOracle(t *testing.T, script, markerName string) []map[string]any {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		t.Fatal("PYTHON is required for the live testops Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, filepath.Join("testdata", script))
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "testops")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute production Python oracle %s: %v\nstdout:\n%s\nstderr:\n%s", script, err, stdout.String(), stderr.String())
	}
	output := bytes.TrimSpace(stdout.Bytes())
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	var rows []map[string]any
	if err := json.Unmarshal(output, &rows); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}
	if writeErr := os.WriteFile(filepath.Join(proofDirectory, markerName), []byte("executed"), 0o644); writeErr != nil {
		t.Fatalf("write live-python-oracle proof: %v", writeErr)
	}
	return rows
}

func assertRowsEqual(t *testing.T, label string, want, got map[string]any) {
	t.Helper()
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("%s mismatch:\npython=%s\ngo=    %s", label, wantJSON, gotJSON)
	}
}

func floatPtr(v float64) *float64 { return &v }
func uintPtr(v uint32) *uint32    { return &v }

// TestComputePipelineMetricsMatchesLivePythonProductionOnRealRow uses the
// real ci_pipeline_runs row pinned in testdata/python_pipeline_metrics_oracle.py.
func TestComputePipelineMetricsMatchesLivePythonProductionOnRealRow(t *testing.T) {
	wantRows := runPythonOracle(t, "python_pipeline_metrics_oracle.py", "testops-pipeline-golden")
	if len(wantRows) != 1 {
		t.Fatalf("python produced %d rows, want 1", len(wantRows))
	}

	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	rows := []PipelineRunRow{
		{
			RepoID: repoID, Status: strPtr("success"),
			QueuedAt:        floatTimePtr(2026, 8, 27, 19, 39, 4),
			StartedAt:       floatTime(2026, 8, 27, 19, 39, 4),
			FinishedAt:      floatTimePtr(2026, 8, 27, 19, 54, 46),
			DurationSeconds: floatPtr(942.0), QueueSeconds: floatPtr(0.0),
			RetryCount: 0, TeamID: nil, ServiceID: nil, OrgID: orgID,
		},
	}
	metrics := ComputePipelineMetrics(repoID, rows, "", nil)
	if len(metrics) != 1 {
		t.Fatalf("go produced %d rows, want 1", len(metrics))
	}
	got := map[string]any{
		"repo_id": metrics[0].RepoID.String(), "day": "2026-08-27",
		"pipelines_count": float64(metrics[0].PipelinesCount), "success_count": float64(metrics[0].SuccessCount),
		"failure_count": float64(metrics[0].FailureCount), "cancelled_count": float64(metrics[0].CancelledCount),
		"success_rate": metrics[0].SuccessRate, "failure_rate": metrics[0].FailureRate,
		"cancel_rate": metrics[0].CancelRate, "rerun_rate": metrics[0].RerunRate,
		"median_duration_seconds": *metrics[0].MedianDurationSeconds, "p95_duration_seconds": *metrics[0].P95DurationSeconds,
		"avg_queue_seconds": *metrics[0].AvgQueueSeconds, "p95_queue_seconds": *metrics[0].P95QueueSeconds,
		"team_id": nil, "service_id": nil, "org_id": metrics[0].OrgID,
	}
	assertRowsEqual(t, "pipeline_metrics", wantRows[0], got)
}

func floatTime(year int, month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
}
func floatTimePtr(year int, month time.Month, day, hour, minute, second int) *time.Time {
	v := floatTime(year, month, day, hour, minute, second)
	return &v
}

// fakeRepoTeamResolver is a minimal RepoTeamResolver for tests -- an exact
// repoName match returns the fixed team, everything else resolves to
// ("", "") (Python's None, None miss).
type fakeRepoTeamResolver struct {
	repoName, teamID, teamName string
}

func (r fakeRepoTeamResolver) ResolveRepo(repoName string) (string, string) {
	if repoName == r.repoName {
		return r.teamID, r.teamName
	}
	return "", ""
}

// TestComputePipelineMetricsGroupingMatchesLivePythonProduction is codex
// adversarial review round 1's (CHAOS-4294) own P1+P2 repro, executed
// against the production Python authority: two pipeline runs for the same
// repo/team, one with service_id=nil and one with service_id="" -- these
// must stay TWO separate output rows (Python's dict key is the raw
// (repo_id, team_id, service_id) tuple; None != "") -- and both rows' raw
// team_id=nil must resolve to the repo-pattern-matched team, not stay nil.
// An earlier revision of ComputePipelineMetrics got both wrong (merged the
// two rows into one via a `derefStr`-collapsed grouping key, and never
// consulted a resolver at all); this test is the proof the fix holds,
// pinned directly against Python rather than a hand-derived expectation.
func TestComputePipelineMetricsGroupingMatchesLivePythonProduction(t *testing.T) {
	wantRows := runPythonOracle(t, "python_pipeline_metrics_grouping_oracle.py", "testops-pipeline-grouping-golden")
	if len(wantRows) != 2 {
		t.Fatalf("python produced %d rows, want 2", len(wantRows))
	}

	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	const orgID = "00000000-0000-4000-8000-000000000009"
	repoName := "acme/service"
	resolver := fakeRepoTeamResolver{repoName: repoName, teamID: "team-pattern", teamName: "Team Pattern"}

	dt := func(hour int) time.Time { return time.Date(2026, 8, 15, hour, 0, 0, 0, time.UTC) }
	rows := []PipelineRunRow{
		{
			RepoID: repoID, Status: strPtr("success"),
			StartedAt: dt(9), FinishedAt: floatTimePtr(2026, 8, 15, 9, 0, 0),
			DurationSeconds: floatPtr(60.0), TeamID: nil, ServiceID: nil, OrgID: orgID,
		},
		{
			RepoID: repoID, Status: strPtr("failure"),
			StartedAt: dt(10), FinishedAt: floatTimePtr(2026, 8, 15, 10, 0, 0),
			DurationSeconds: floatPtr(90.0), TeamID: nil, ServiceID: strPtr(""), OrgID: orgID,
		},
	}
	metrics := ComputePipelineMetrics(repoID, rows, repoName, resolver)
	if len(metrics) != 2 {
		t.Fatalf("go produced %d rows, want 2 (nil and \"\" service_id must stay separate groups): %#v", len(metrics), metrics)
	}
	for i, m := range metrics {
		if m.TeamID == nil || *m.TeamID != "team-pattern" {
			t.Fatalf("row %d team_id=%v, want repo-pattern-resolved \"team-pattern\"", i, m.TeamID)
		}
		var serviceID any
		if m.ServiceID != nil {
			serviceID = *m.ServiceID
		}
		got := map[string]any{
			"repo_id": m.RepoID.String(), "day": "2026-08-15",
			"pipelines_count": float64(m.PipelinesCount), "success_count": float64(m.SuccessCount),
			"failure_count": float64(m.FailureCount), "cancelled_count": float64(m.CancelledCount),
			"success_rate": m.SuccessRate, "failure_rate": m.FailureRate,
			"cancel_rate": m.CancelRate, "rerun_rate": m.RerunRate,
			"median_duration_seconds": *m.MedianDurationSeconds, "p95_duration_seconds": *m.P95DurationSeconds,
			"avg_queue_seconds": nil, "p95_queue_seconds": nil,
			"team_id": *m.TeamID, "service_id": serviceID, "org_id": m.OrgID,
		}
		assertRowsEqual(t, "pipeline_metrics_grouping", wantRows[i], got)
	}
}

// TestComputeTestMetricsMatchesLivePythonProductionOnRealRows uses the real
// test_suite_results/test_case_results rows pinned in
// testdata/python_test_metrics_oracle.py.
func TestComputeTestMetricsMatchesLivePythonProductionOnRealRows(t *testing.T) {
	wantRows := runPythonOracle(t, "python_test_metrics_oracle.py", "testops-test-golden")
	if len(wantRows) != 1 {
		t.Fatalf("python produced %d rows, want 1", len(wantRows))
	}

	repoID := uuid.MustParse("920f9442-07df-4217-4dc4-c5833c0b8268")
	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	suiteA := "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9"
	suiteB := "7209e391dcc3588bd0c8e3f7dc73c738ff4dfd979144aacded7086ee4fa0cfeb"
	suites := []SuiteRow{
		{
			RepoID: repoID, RunID: "32662748666", SuiteID: suiteA,
			TotalCount: 7, PassedCount: 5, FailedCount: 0, SkippedCount: 2, ErrorCount: 0, QuarantinedCount: 0,
			DurationSeconds: floatPtr(11.953),
			StartedAt:       floatTimePtrMicro(2026, 8, 23, 19, 55, 43, 892000),
			FinishedAt:      floatTimePtrMicro(2026, 8, 23, 19, 55, 55, 845000),
			OrgID:           orgID,
		},
		{
			RepoID: repoID, RunID: "32671439506", SuiteID: suiteB,
			TotalCount: 7, PassedCount: 5, FailedCount: 0, SkippedCount: 2, ErrorCount: 0, QuarantinedCount: 0,
			DurationSeconds: floatPtr(12.022),
			StartedAt:       floatTimePtrMicro(2026, 8, 23, 22, 43, 55, 801000),
			FinishedAt:      floatTimePtrMicro(2026, 8, 23, 22, 44, 7, 823000),
			OrgID:           orgID,
		},
	}
	cases := []CaseRow{
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_list_authenticated_user_repos_includes_private", Status: strPtr("passed")},
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_list_public_repos_from_github_org", Status: strPtr("passed")},
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_search_public_repos", Status: strPtr("passed")},
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_list_public_repos_from_user", Status: strPtr("passed")},
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_github_invalid_token", Status: strPtr("passed")},
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_access_private_repo_without_token", Status: strPtr("skipped")},
		{RepoID: repoID, RunID: "32662748666", SuiteID: suiteA, CaseName: "test_access_private_repo_with_valid_token", Status: strPtr("skipped")},
		{RepoID: repoID, RunID: "32671439506", SuiteID: suiteB, CaseName: "test_search_public_repos", Status: strPtr("passed")},
	}
	metrics := ComputeTestMetrics(repoID, suites, cases, map[string]struct{}{}, "", nil)
	if len(metrics) != 1 {
		t.Fatalf("go produced %d rows, want 1", len(metrics))
	}
	m := metrics[0]
	got := map[string]any{
		"repo_id": m.RepoID.String(), "day": "2026-08-23",
		"total_cases": float64(m.TotalCases), "passed_count": float64(m.PassedCount),
		"failed_count": float64(m.FailedCount), "skipped_count": float64(m.SkippedCount),
		"quarantined_count": float64(m.QuarantinedCount), "pass_rate": m.PassRate,
		"failure_rate": m.FailureRate, "flake_rate": m.FlakeRate,
		"retry_dependency_rate": m.RetryDependencyRate, "total_suites": float64(m.TotalSuites),
		"suite_duration_p50_seconds": *m.SuiteDurationP50Seconds, "suite_duration_p95_seconds": *m.SuiteDurationP95Seconds,
		"failure_recurrence_score": m.FailureRecurrence, "team_id": nil, "service_id": nil, "org_id": m.OrgID,
	}
	assertRowsEqual(t, "test_metrics", wantRows[0], got)
}

func floatTimePtrMicro(year int, month time.Month, day, hour, minute, second, micro int) *time.Time {
	v := time.Date(year, month, day, hour, minute, second, micro*1000, time.UTC)
	return &v
}

// TestComputeCoverageMetricMatchesLivePythonProductionOnRealRows uses the
// real coverage_snapshots rows pinned in
// testdata/python_coverage_metrics_oracle.py.
func TestComputeCoverageMetricMatchesLivePythonProductionOnRealRows(t *testing.T) {
	wantRows := runPythonOracle(t, "python_coverage_metrics_oracle.py", "testops-coverage-golden")
	if len(wantRows) != 1 {
		t.Fatalf("python produced %d rows, want 1", len(wantRows))
	}

	repoID := uuid.MustParse("d29d160a-95fe-5b45-d4c1-fd1f5427b772")
	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	current := []CoverageSnapshotRow{
		{
			RepoID: repoID, RunID: "32793481613", SnapshotID: "79959989f28edea99e50b1bdf6168d3ec22233e471d710a7f6dc32552f81986e",
			LinesTotal: uintPtr(14236), LinesCovered: uintPtr(8401),
			LineCoveragePct: floatPtr(59.01236302332116), BranchCoveragePct: floatPtr(53.07480008491968),
			ServiceID: strPtr("src"), OrgID: orgID,
		},
	}
	prior := []CoverageSnapshotRow{
		{
			RepoID: repoID, RunID: "32768344924", SnapshotID: "1063c0023418eb49fd545fe9e2228aea380ba0a07c4c2dac312d3a95500f075c",
			LinesTotal: uintPtr(14235), LinesCovered: uintPtr(8400),
			LineCoveragePct: floatPtr(59.00948366701792), BranchCoveragePct: floatPtr(53.06815768985774),
			ServiceID: strPtr("src"), OrgID: orgID,
		},
	}
	m := ComputeCoverageMetric(repoID, current, prior, "", nil)
	if m == nil {
		t.Fatal("go produced no row")
	}
	got := map[string]any{
		"repo_id": m.RepoID.String(), "day": "2026-08-25",
		"line_coverage_pct": *m.LineCoveragePct, "branch_coverage_pct": *m.BranchCoveragePct,
		"lines_total": float64(*m.LinesTotal), "lines_covered": float64(*m.LinesCovered),
		"coverage_delta_pct": *m.CoverageDeltaPct, "uncovered_files_count": float64(m.UncoveredFilesCount),
		"coverage_regression_count": float64(m.CoverageRegressionCount),
		"team_id":                   nil, "service_id": *m.ServiceID, "org_id": m.OrgID,
	}
	assertRowsEqual(t, "coverage_metric", wantRows[0], got)
}
