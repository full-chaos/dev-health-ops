package cicd

import "testing"

// A run the provider no longer has is stored with status "deleted"; the daily pipeline count leaves it out.
func TestDeletedRunsAreLeftOutOfTheDailyPipelineCount(t *testing.T) {
	rows := []PipelineRunRow{
		{RepoID: repoA, RunID: "1", Status: "success", StartedAt: dt(1, 0, 24), FinishedAt: dtPtr(1, 5, 24)},
		{RepoID: repoA, RunID: "2", Status: "failure", StartedAt: dt(2, 0, 24), FinishedAt: dtPtr(2, 5, 24)},
		{RepoID: repoA, RunID: "3", Status: "deleted", StartedAt: dt(3, 0, 24), FinishedAt: dtPtr(3, 5, 24)},
		{RepoID: repoA, RunID: "4", Status: " Deleted ", StartedAt: dt(4, 0, 24), FinishedAt: dtPtr(4, 5, 24)},
	}
	metrics := ComputeCICDMetricsDaily(day, rows, computedAt)
	if len(metrics) != 1 || metrics[0].PipelinesCount != 2 || metrics[0].SuccessRate != 0.5 {
		t.Fatalf("metrics = %#v, want one repo with 2 pipelines and success rate 0.5", metrics)
	}
}
