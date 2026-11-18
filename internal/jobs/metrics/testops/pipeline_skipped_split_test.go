package testops

import (
	"math"
	"testing"

	"github.com/google/uuid"
)

// A run stored as skipped (GitLab since the ingest stops mapping it to cancelled) leaves CancelledCount and
// CancelRate; the total and the success and failure rates do not move.
func TestSkippedRunLeavesCancelRateOnly(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000014022")
	team, service := strPtr("t1"), strPtr("a")
	metric := func(skippedStatus string) PipelineMetric {
		acc := NewPipelineAccumulator(repoID, "acme/widgets", nil)
		for hour, status := range []string{"success", "success", "success", "failed", "cancelled", skippedStatus} {
			acc.Add(pipelineRun(repoID, hour, status, 60, team, service, 0))
		}
		merged := acc.FinishRepoDay()
		if merged == nil {
			t.Fatal("FinishRepoDay() = nil")
		}
		return *merged
	}
	before, after := metric("cancelled"), metric("skipped")
	if before.CancelledCount != 2 || after.CancelledCount != 1 {
		t.Errorf("CancelledCount before/after = %d/%d, want 2/1", before.CancelledCount, after.CancelledCount)
	}
	if math.Abs(before.CancelRate-2.0/6) > 1e-12 || math.Abs(after.CancelRate-1.0/6) > 1e-12 {
		t.Errorf("CancelRate before/after = %.4f/%.4f, want 0.3333/0.1667", before.CancelRate, after.CancelRate)
	}
	if before.PipelinesCount != after.PipelinesCount || before.SuccessCount != after.SuccessCount || before.FailureCount != after.FailureCount ||
		before.SuccessRate != after.SuccessRate || before.FailureRate != after.FailureRate {
		t.Errorf("total/success/failure moved: before %+v after %+v", before, after)
	}
}
