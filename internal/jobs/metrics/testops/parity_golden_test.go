package testops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestComputePipelineMetricsMatchesFrozenPythonGolden,
// TestComputeTestMetricsMatchesFrozenPythonGolden and
// TestComputeCoverageMetricMatchesFrozenPythonGolden are FROZEN-golden
// tests, not live-dual-execution ones: there is no Python compute for these
// families left in this repository (see compute.go's package doc comment
// for the Go port's own Python-authority citation), so nothing can execute
// a live oracle at test time.
//
// The golden fixtures below were captured by running the Python authority's
// own frozen oracle scripts (testdata/python_pipeline_metrics_oracle.py,
// python_pipeline_metrics_grouping_oracle.py, python_test_metrics_oracle.py,
// python_coverage_metrics_oracle.py -- reproduced verbatim as the input
// fixtures below) against that revision's Python source, offline, ONCE, via
// a throwaway script never committed -- the same capture discipline
// internal/jobs/metrics/daily/icfinalize/parity_golden_test.go and
// internal/jobs/metrics/daily/team_complexity_parity_golden_test.go's own
// goldens describe. The captured values are exactly what those oracle
// scripts' own fixture inputs produce, not a value derived by intuition.
//
// This proves each family's FULL row output, field for field, against the
// Python authority -- the percentile-kernel pin in fma_golden_test.go covers
// only the shared percentile function, not a family's row shape.

func loadTestopsTestdata(t *testing.T, name string, v any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
}

// -----------------------------------------------------------------------
// testops_pipeline
// -----------------------------------------------------------------------

type pipelineGoldenRow struct {
	RepoID                string   `json:"repo_id"`
	PipelinesCount        int      `json:"pipelines_count"`
	SuccessCount          int      `json:"success_count"`
	FailureCount          int      `json:"failure_count"`
	CancelledCount        int      `json:"cancelled_count"`
	SuccessRate           float64  `json:"success_rate"`
	FailureRate           float64  `json:"failure_rate"`
	CancelRate            float64  `json:"cancel_rate"`
	RerunRate             float64  `json:"rerun_rate"`
	MedianDurationSeconds *float64 `json:"median_duration_seconds"`
	P95DurationSeconds    *float64 `json:"p95_duration_seconds"`
	AvgQueueSeconds       *float64 `json:"avg_queue_seconds"`
	P95QueueSeconds       *float64 `json:"p95_queue_seconds"`
	TeamID                *string  `json:"team_id"`
	ServiceID             *string  `json:"service_id"`
	OrgID                 string   `json:"org_id"`
}

type pipelineGoldenDocument struct {
	RealRow  []pipelineGoldenRow `json:"real_row"`
	Grouping []pipelineGoldenRow `json:"grouping"`
}

func toPipelineGoldenRow(m PipelineMetric) pipelineGoldenRow {
	return pipelineGoldenRow{
		RepoID: m.RepoID.String(), PipelinesCount: m.PipelinesCount,
		SuccessCount: m.SuccessCount, FailureCount: m.FailureCount, CancelledCount: m.CancelledCount,
		SuccessRate: m.SuccessRate, FailureRate: m.FailureRate, CancelRate: m.CancelRate, RerunRate: m.RerunRate,
		MedianDurationSeconds: m.MedianDurationSeconds, P95DurationSeconds: m.P95DurationSeconds,
		AvgQueueSeconds: m.AvgQueueSeconds, P95QueueSeconds: m.P95QueueSeconds,
		TeamID: m.TeamID, ServiceID: m.ServiceID, OrgID: m.OrgID,
	}
}

// TestComputePipelineMetricsMatchesFrozenPythonGolden covers two frozen
// cases from testdata/testops_pipeline_parity_golden.json:
//
//   - "real_row": ComputePipelineMetrics's full field set on a single real
//     ci_pipeline_runs row (testdata/python_pipeline_metrics_oracle.py's own
//     fixture) -- every duration/queue field populated.
//   - "grouping": two runs for the same repo/team but ServiceID nil vs ""
//     (testdata/python_pipeline_metrics_grouping_oracle.py's own fixture) --
//     proves they stay separate output rows and that a repo-pattern
//     resolver's team_id wins over a nil raw team_id.
func TestComputePipelineMetricsMatchesFrozenPythonGolden(t *testing.T) {
	var golden pipelineGoldenDocument
	loadTestopsTestdata(t, "testops_pipeline_parity_golden.json", &golden)

	t.Run("real_row", func(t *testing.T) {
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
		if len(golden.RealRow) != 1 {
			t.Fatalf("golden has %d real_row entries, want 1", len(golden.RealRow))
		}
		if got, want := toPipelineGoldenRow(metrics[0]), golden.RealRow[0]; !reflect.DeepEqual(got, want) {
			t.Errorf("pipeline real_row mismatch:\n got  %+v\n want %+v", got, want)
		}
	})

	t.Run("grouping", func(t *testing.T) {
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
		if len(golden.Grouping) != 2 {
			t.Fatalf("golden has %d grouping entries, want 2", len(golden.Grouping))
		}
		for i := range metrics {
			if got, want := toPipelineGoldenRow(metrics[i]), golden.Grouping[i]; !reflect.DeepEqual(got, want) {
				t.Errorf("pipeline grouping row %d mismatch:\n got  %+v\n want %+v", i, got, want)
			}
		}
	})
}

// -----------------------------------------------------------------------
// testops_test
// -----------------------------------------------------------------------

type testGoldenRow struct {
	RepoID                  string   `json:"repo_id"`
	TotalCases              int      `json:"total_cases"`
	PassedCount             int      `json:"passed_count"`
	FailedCount             int      `json:"failed_count"`
	SkippedCount            int      `json:"skipped_count"`
	QuarantinedCount        int      `json:"quarantined_count"`
	PassRate                float64  `json:"pass_rate"`
	FailureRate             float64  `json:"failure_rate"`
	FlakeRate               float64  `json:"flake_rate"`
	RetryDependencyRate     float64  `json:"retry_dependency_rate"`
	TotalSuites             int      `json:"total_suites"`
	SuiteDurationP50Seconds *float64 `json:"suite_duration_p50_seconds"`
	SuiteDurationP95Seconds *float64 `json:"suite_duration_p95_seconds"`
	FailureRecurrence       float64  `json:"failure_recurrence_score"`
	TeamID                  *string  `json:"team_id"`
	ServiceID               *string  `json:"service_id"`
	OrgID                   string   `json:"org_id"`
}

func toTestGoldenRow(m TestMetric) testGoldenRow {
	return testGoldenRow{
		RepoID: m.RepoID.String(), TotalCases: m.TotalCases, PassedCount: m.PassedCount,
		FailedCount: m.FailedCount, SkippedCount: m.SkippedCount, QuarantinedCount: m.QuarantinedCount,
		PassRate: m.PassRate, FailureRate: m.FailureRate, FlakeRate: m.FlakeRate,
		RetryDependencyRate: m.RetryDependencyRate, TotalSuites: m.TotalSuites,
		SuiteDurationP50Seconds: m.SuiteDurationP50Seconds, SuiteDurationP95Seconds: m.SuiteDurationP95Seconds,
		FailureRecurrence: m.FailureRecurrence, TeamID: m.TeamID, ServiceID: m.ServiceID, OrgID: m.OrgID,
	}
}

// TestComputeTestMetricsMatchesFrozenPythonGolden uses the real
// test_suite_results/test_case_results rows pinned in
// testdata/python_test_metrics_oracle.py (two suites, total_count=7 each;
// eight case rows spanning passed/skipped, none retried) -- frozen the same
// way TestComputePipelineMetricsMatchesFrozenPythonGolden's cases are.
func TestComputeTestMetricsMatchesFrozenPythonGolden(t *testing.T) {
	var golden []testGoldenRow
	loadTestopsTestdata(t, "testops_test_parity_golden.json", &golden)
	if len(golden) != 1 {
		t.Fatalf("golden has %d entries, want 1", len(golden))
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
	if got, want := toTestGoldenRow(metrics[0]), golden[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("test_metrics mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func floatTimePtrMicro(year int, month time.Month, day, hour, minute, second, micro int) *time.Time {
	v := time.Date(year, month, day, hour, minute, second, micro*1000, time.UTC)
	return &v
}

// -----------------------------------------------------------------------
// testops_coverage
// -----------------------------------------------------------------------

type coverageGoldenRow struct {
	RepoID                  string   `json:"repo_id"`
	LineCoveragePct         *float64 `json:"line_coverage_pct"`
	BranchCoveragePct       *float64 `json:"branch_coverage_pct"`
	LinesTotal              *uint32  `json:"lines_total"`
	LinesCovered            *uint32  `json:"lines_covered"`
	CoverageDeltaPct        *float64 `json:"coverage_delta_pct"`
	UncoveredFilesCount     int      `json:"uncovered_files_count"`
	CoverageRegressionCount int      `json:"coverage_regression_count"`
	TeamID                  *string  `json:"team_id"`
	ServiceID               *string  `json:"service_id"`
	OrgID                   string   `json:"org_id"`
}

func toCoverageGoldenRow(m CoverageMetric) coverageGoldenRow {
	return coverageGoldenRow{
		RepoID: m.RepoID.String(), LineCoveragePct: m.LineCoveragePct, BranchCoveragePct: m.BranchCoveragePct,
		LinesTotal: m.LinesTotal, LinesCovered: m.LinesCovered, CoverageDeltaPct: m.CoverageDeltaPct,
		UncoveredFilesCount: m.UncoveredFilesCount, CoverageRegressionCount: m.CoverageRegressionCount,
		TeamID: m.TeamID, ServiceID: m.ServiceID, OrgID: m.OrgID,
	}
}

func uintPtr(v uint32) *uint32 { return &v }

// TestComputeCoverageMetricMatchesFrozenPythonGolden uses the real
// coverage_snapshots rows pinned in testdata/python_coverage_metrics_oracle.py.
func TestComputeCoverageMetricMatchesFrozenPythonGolden(t *testing.T) {
	var golden []coverageGoldenRow
	loadTestopsTestdata(t, "testops_coverage_parity_golden.json", &golden)
	if len(golden) != 1 {
		t.Fatalf("golden has %d entries, want 1", len(golden))
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
	if got, want := toCoverageGoldenRow(*m), golden[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("coverage_metric mismatch:\n got  %+v\n want %+v", got, want)
	}
}
