package testops

import (
	"testing"

	"github.com/google/uuid"
)

// A run the provider no longer has is stored with status "deleted". It says nothing about the run's outcome, so it is
// left out of the pipeline count and of every rate that divides by it.
func TestDeletedRunsAreLeftOutOfPipelineCounts(t *testing.T) {
	repo := uuid.New()
	rows := []PipelineRunRow{
		pipelineRun(repo, 1, "success", 60, nil, nil, 0),
		pipelineRun(repo, 2, "failure", 60, nil, nil, 0),
		pipelineRun(repo, 3, StatusDeleted, 60, nil, nil, 1),
		pipelineRun(repo, 4, " Deleted ", 60, nil, nil, 0),
	}
	metrics := ComputePipelineMetrics(repo, rows, "acme/widgets", nil)
	if len(metrics) != 1 {
		t.Fatalf("groups = %d, want 1", len(metrics))
	}
	got := metrics[0]
	if got.PipelinesCount != 2 || got.SuccessCount != 1 || got.FailureCount != 1 || got.SuccessRate != 0.5 || got.RerunRate != 0 {
		t.Fatalf("count %d success %d failure %d rate %v rerun %v, want 2/1/1/0.5/0 with the deleted rows left out",
			got.PipelinesCount, got.SuccessCount, got.FailureCount, got.SuccessRate, got.RerunRate)
	}
}

func TestOnlyDeletedIsExcludedFromPipelineCounts(t *testing.T) {
	for status, want := range map[string]bool{
		"deleted": true, " DELETED ": true, "success": false, "failure": false, "running": false, "queued": false, "cancelled": false, "": false,
	} {
		if got := ExcludedFromPipelineCounts(status); got != want {
			t.Errorf("ExcludedFromPipelineCounts(%q) = %v, want %v", status, got, want)
		}
	}
}
