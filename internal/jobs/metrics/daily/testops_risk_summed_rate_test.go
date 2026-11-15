package daily

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The release confidence factors are the repository's summed counts over their denominators (pm ruling A, 11-15):
// skipped cases are not in the pass-rate denominator, several (team, service) groups are one repository, and a zero
// denominator is absent (null in factors_json, adds 0 to the score: missing, never healthy).
func riskSummedRow(t *testing.T, runs []testops.PipelineRunRow, suites []testops.SuiteRow) *testopsReleaseConfidenceRow {
	t.Helper()
	repo := uuid.New()
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	pipelineAccumulator := testops.NewPipelineAccumulator(repo, "r", nil)
	for _, run := range runs {
		run.RepoID = repo
		pipelineAccumulator.Add(run)
	}
	pipelineMetrics := pipelineAccumulator.Finish()
	testMetrics := testops.ComputeTestMetrics(repo, withRepo(suites, repo), nil, nil, "", nil)
	var pipe *testops.PipelineMetric
	if n := len(pipelineMetrics); n > 0 {
		pipe = &pipelineMetrics[n-1]
	}
	var test *testops.TestMetric
	if len(testMetrics) > 0 {
		test = &testMetrics[0]
	}
	confidencePipe, confidenceTest := repoSummedConfidenceInputs(pipe, pipelineMetrics, test, testMetrics)
	row := computeReleaseConfidence(repo, day, confidencePipe, confidenceTest, nil, day)
	if row == nil {
		t.Fatal("no row")
	}
	return row
}

func withRepo(suites []testops.SuiteRow, repo uuid.UUID) []testops.SuiteRow {
	out := append([]testops.SuiteRow(nil), suites...)
	for i := range out {
		out[i].RepoID = repo
	}
	return out
}

func riskRuns(team, status string, n int) []testops.PipelineRunRow {
	runs := make([]testops.PipelineRunRow, 0, n)
	started := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		s, tm := status, team
		finished := started.Add(time.Minute)
		runs = append(runs, testops.PipelineRunRow{Status: &s, StartedAt: started, FinishedAt: &finished, TeamID: &tm, OrgID: "org"})
	}
	return runs
}

func riskSuite(passed, failed, skipped uint32) testops.SuiteRow {
	return testops.SuiteRow{RunID: "r", SuiteID: "s", TotalCount: passed + failed + skipped, PassedCount: passed, FailedCount: failed, SkippedCount: skipped, OrgID: "org"}
}

func TestReleaseConfidenceTestPassRateLeavesSkippedOutOfTheDenominator(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		passed, failed, skipped      uint32
		wantRate                     string
		wantFactor, wantScoreAtLeast float64
	}{
		{"99 passed, 0 failed, 901 skipped", 99, 0, 901, `"test_pass_rate": 1.0`, 0.3, 0.3},
		{"90 passed, 0 failed, 10 skipped", 90, 0, 10, `"test_pass_rate": 1.0`, 0.3, 0.3},
		{"50 passed, 50 failed (control)", 50, 50, 0, `"test_pass_rate": 0.5`, 0.15, 0.15},
		{"20 passed, 5 failed, 75 skipped", 20, 5, 75, `"test_pass_rate": 0.8`, 0.24, 0.24},
	} {
		row := riskSummedRow(t, nil, []testops.SuiteRow{riskSuite(tc.passed, tc.failed, tc.skipped)})
		if !strings.Contains(row.FactorsJSON, tc.wantRate) || row.TestPassFactor != tc.wantFactor {
			t.Errorf("%s: factors %s factor %v, want %s and factor %v", tc.name, row.FactorsJSON, row.TestPassFactor, tc.wantRate, tc.wantFactor)
		}
	}
}

func TestReleaseConfidencePipelineSuccessIsTheRepositorysSummedRate(t *testing.T) {
	runs := append(riskRuns("team-a", "success", 99), riskRuns("team-z", "failed", 1)...)
	row := riskSummedRow(t, runs, nil)
	if !strings.Contains(row.FactorsJSON, `"pipeline_success_rate": 0.99`) || row.PipelineSuccessFactor != 0.396 {
		t.Fatalf("factors %s pipeline factor %v, want 99 of 100 runs: 0.99 and 0.396 (the last group alone is 0.0)", row.FactorsJSON, row.PipelineSuccessFactor)
	}
}

// Completed runs only: cancelled, skipped and running runs are out of the denominator (pm ruling on the pipeline rates).
func TestReleaseConfidencePipelineSuccessDividesByCompletedRuns(t *testing.T) {
	runs := append(riskRuns("team-a", "success", 3), riskRuns("team-a", "failed", 1)...)
	for _, status := range []string{"cancelled", "cancelled", "skipped", "running"} {
		runs = append(runs, riskRuns("team-b", status, 1)...)
	}
	row := riskSummedRow(t, runs, nil)
	if !strings.Contains(row.FactorsJSON, `"pipeline_success_rate": 0.75`) || row.PipelineSuccessFactor != 0.3 {
		t.Fatalf("factors %s pipeline factor %v, want 3 of 4 completed runs: 0.75 and 0.3 (over all 8 runs it would be 0.375)", row.FactorsJSON, row.PipelineSuccessFactor)
	}
	// Only cancelled and skipped runs: no completed run, absent.
	row = riskSummedRow(t, append(riskRuns("team-a", "cancelled", 2), riskRuns("team-a", "skipped", 1)...), nil)
	if !strings.Contains(row.FactorsJSON, `"pipeline_success_rate": null`) {
		t.Fatalf("no completed run must be null: %s", row.FactorsJSON)
	}
}

func TestReleaseConfidenceZeroDenominatorIsAbsentAndMissingInTheScore(t *testing.T) {
	// Only skipped cases and no pipeline run: both rates absent (null), the score is the flake term alone.
	row := riskSummedRow(t, nil, []testops.SuiteRow{riskSuite(0, 0, 100)})
	if !strings.Contains(row.FactorsJSON, `"pipeline_success_rate": null`) || !strings.Contains(row.FactorsJSON, `"test_pass_rate": null`) {
		t.Fatalf("zero denominators must be null: %s", row.FactorsJSON)
	}
	if row.PipelineSuccessFactor != 0 || row.TestPassFactor != 0 || row.ConfidenceScore != 0.1 {
		t.Fatalf("factors %v/%v score %v, want 0/0 and 0.1 (an absent factor is missing, never healthy)", row.PipelineSuccessFactor, row.TestPassFactor, row.ConfidenceScore)
	}
	// Runs that exist but none passed or failed a case: still absent for the test rate.
	row = riskSummedRow(t, riskRuns("team-a", "success", 2), []testops.SuiteRow{riskSuite(0, 0, 5)})
	if !strings.Contains(row.FactorsJSON, `"pipeline_success_rate": 1.0`) || !strings.Contains(row.FactorsJSON, `"test_pass_rate": null`) {
		t.Fatalf("pipeline rate present, test rate absent expected: %s", row.FactorsJSON)
	}
}
