package testops

import (
	"math"
	"testing"

	"github.com/google/uuid"
)

// The pipeline success and failure rates divide by the completed runs of the day (succeeded or failed). Cancelled,
// skipped and unfinished runs are counted in pipelines_count and in neither rate.
func TestPipelineRatesDivideByCompletedRuns(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000013802")
	team, service := strPtr("t1"), strPtr("a")
	var rows []PipelineRunRow
	for hour, status := range []string{
		"success", "success", "success", "success", "success", "success",
		"failed", "failed", "cancelled", "skipped", "running",
	} {
		rows = append(rows, pipelineRun(repoID, hour, status, 60, team, service, 0))
	}
	acc := NewPipelineAccumulator(repoID, "acme/widgets", nil)
	for _, row := range rows {
		acc.Add(row)
	}
	perGroup := acc.Finish()
	if len(perGroup) != 1 {
		t.Fatalf("Finish() = %d groups, want 1", len(perGroup))
	}
	merged := acc.FinishRepoDay()
	if merged == nil {
		t.Fatal("FinishRepoDay() = nil")
	}
	for name, got := range map[string]PipelineMetric{"FinishRepoDay": *merged, "Finish": perGroup[0]} {
		completed := got.SuccessCount + got.FailureCount
		t.Logf("%s: stored success_rate=%.4f failure_rate=%.4f (pipelines_count=%d) | copy: completed runs only -> success=%.4f failure=%.4f (success+failure=%d)",
			name, got.SuccessRate, got.FailureRate, got.PipelinesCount,
			float64(got.SuccessCount)/float64(completed), float64(got.FailureCount)/float64(completed), completed)
		if got.PipelinesCount != 11 || got.SuccessCount != 6 || got.FailureCount != 2 || got.CancelledCount != 1 {
			t.Errorf("%s: counts = %d/%d/%d/%d, want 11/6/2/1", name, got.PipelinesCount, got.SuccessCount, got.FailureCount, got.CancelledCount)
		}
		if math.Abs(got.SuccessRate-0.75) > 1e-12 {
			t.Errorf("%s: success rate = %.4f, want 0.7500", name, got.SuccessRate)
		}
		if math.Abs(got.FailureRate-0.25) > 1e-12 {
			t.Errorf("%s: failure rate = %.4f, want 0.2500", name, got.FailureRate)
		}
		if math.Abs(got.SuccessRate+got.FailureRate-1) > 1e-12 {
			t.Errorf("%s: success + failure = %.4f, want 1", name, got.SuccessRate+got.FailureRate)
		}
		if math.Abs(got.CancelRate-1.0/11) > 1e-12 {
			t.Errorf("%s: cancel rate = %.4f, want 1/11 (over every run)", name, got.CancelRate)
		}
	}
}

// A day with no completed run stores 0 for both rates (the columns are not nullable). Every reader takes the rate
// from the counts with NULL on 0, or leaves the day out, so this 0 is never shown or scored.
func TestPipelineRatesWithNoCompletedRunAreZero(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000013803")
	got := finishRepoDay(t,
		pipelineRun(repoID, 1, "cancelled", 60, nil, nil, 0),
		pipelineRun(repoID, 2, "running", 60, nil, nil, 1),
	)
	if got == nil {
		t.Fatal("FinishRepoDay() = nil")
	}
	if got.PipelinesCount != 2 || got.SuccessCount != 0 || got.FailureCount != 0 {
		t.Fatalf("counts = %d/%d/%d, want 2/0/0", got.PipelinesCount, got.SuccessCount, got.FailureCount)
	}
	if got.SuccessRate != 0 || got.FailureRate != 0 {
		t.Errorf("rates = %v/%v, want 0/0 with no completed run", got.SuccessRate, got.FailureRate)
	}
	if math.Abs(got.CancelRate-0.5) > 1e-12 || math.Abs(got.RerunRate-0.5) > 1e-12 {
		t.Errorf("cancel/rerun = %v/%v, want 0.5/0.5 over every run", got.CancelRate, got.RerunRate)
	}
}
