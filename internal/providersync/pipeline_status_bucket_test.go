package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// pipelineBuckets are a daily row's six bucket counts: success, failure, cancelled, skipped, unfinished, other.
type pipelineBuckets [6]int

const (
	inSuccess = iota
	inFailure
	inCancelled
	inSkipped
	inUnfinished
	inOther
)

func bucketsOfStatuses(statuses ...string) (pipelineBuckets, int) {
	repo := uuid.MustParse("00000000-0000-4000-8000-000000014019")
	accumulator := testops.NewPipelineAccumulator(repo, "", nil)
	for _, status := range statuses {
		accumulator.Add(testops.PipelineRunRow{RepoID: repo, Status: &status, StartedAt: time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC), OrgID: "org-1"})
	}
	metric := accumulator.FinishRepoDay()
	if metric == nil {
		return pipelineBuckets{}, 0
	}
	return pipelineBuckets{metric.SuccessCount, metric.FailureCount, metric.CancelledCount, metric.SkippedCount,
		metric.UnfinishedCount, metric.OtherCount}, metric.PipelinesCount
}

// Every pipeline status the GitHub and GitLab ingest store lands in exactly one bucket of the daily row; a GitLab
// skipped pipeline is stored as skipped and lands in the skipped bucket, as a GitHub one does.
func TestEveryStoredPipelineStatusOfTheIngestLandsInOneBucket(t *testing.T) {
	cells := []struct {
		name, stored string
		bucket       int
	}{
		{"github conclusion success", mapGitHubTestsStatus("completed", "success"), inSuccess},
		{"github conclusion failure", mapGitHubTestsStatus("completed", "failure"), inFailure},
		{"github conclusion startup_failure", mapGitHubTestsStatus("completed", "startup_failure"), inFailure},
		{"github conclusion action_required", mapGitHubTestsStatus("completed", "action_required"), inFailure},
		{"github conclusion timed_out", mapGitHubTestsStatus("completed", "timed_out"), inFailure},
		{"github conclusion cancelled", mapGitHubTestsStatus("completed", "cancelled"), inCancelled},
		{"github conclusion neutral", mapGitHubTestsStatus("completed", "neutral"), inCancelled},
		{"github conclusion skipped", mapGitHubTestsStatus("completed", "skipped"), inSkipped},
		{"github status in_progress", mapGitHubTestsStatus("in_progress", ""), inUnfinished},
		{"github status requested", mapGitHubTestsStatus("requested", ""), inUnfinished},
		{"github status waiting", mapGitHubTestsStatus("waiting", ""), inUnfinished},
		{"github status pending", mapGitHubTestsStatus("pending", ""), inUnfinished},
		{"github status queued", mapGitHubTestsStatus("queued", ""), inUnfinished},
		{"github conclusion stale", mapGitHubTestsStatus("completed", "stale"), inOther},
		{"github completed with no conclusion", mapGitHubTestsStatus("completed", ""), inOther},
		{"gitlab success", mapGitLabTestsPipelineStatus("success"), inSuccess},
		{"gitlab failed", mapGitLabTestsPipelineStatus("failed"), inFailure},
		{"gitlab canceled", mapGitLabTestsPipelineStatus("canceled"), inCancelled},
		{"gitlab cancelled", mapGitLabTestsPipelineStatus("cancelled"), inCancelled},
		{"gitlab skipped", mapGitLabTestsPipelineStatus("skipped"), inSkipped},
		{"gitlab running", mapGitLabTestsPipelineStatus("running"), inUnfinished},
		{"gitlab manual", mapGitLabTestsPipelineStatus("manual"), inUnfinished},
		{"gitlab scheduled", mapGitLabTestsPipelineStatus("scheduled"), inUnfinished},
		{"gitlab pending", mapGitLabTestsPipelineStatus("pending"), inUnfinished},
		{"gitlab created", mapGitLabTestsPipelineStatus("created"), inUnfinished},
		{"gitlab waiting_for_resource", mapGitLabTestsPipelineStatus("waiting_for_resource"), inUnfinished},
		{"gitlab preparing", mapGitLabTestsPipelineStatus("preparing"), inUnfinished},
		{"gitlab canceling", mapGitLabTestsPipelineStatus("canceling"), inOther},
		{"gitlab unknown status", mapGitLabTestsPipelineStatus("some_new_state"), inOther},
	}
	var all []string
	var want pipelineBuckets
	for _, cell := range cells {
		var one pipelineBuckets
		one[cell.bucket] = 1
		if got, pipelines := bucketsOfStatuses(cell.stored); got != one || pipelines != 1 {
			t.Errorf("%s: stored %q, buckets %v (pipelines %d), want %v", cell.name, cell.stored, got, pipelines, one)
		}
		all = append(all, cell.stored)
		want[cell.bucket]++
	}
	got, pipelines := bucketsOfStatuses(all...)
	sum := 0
	for _, count := range got {
		sum += count
	}
	if got != want || pipelines != len(cells) || sum != pipelines {
		t.Errorf("all stored statuses: buckets %v (sum %d, pipelines %d), want %v and %d", got, sum, pipelines, want, len(cells))
	}
	if got, pipelines := bucketsOfStatuses(testops.StatusDeleted); pipelines != 0 || got != (pipelineBuckets{}) {
		t.Errorf("a run the open phase stored as %q: buckets %v, pipelines %d; want no row", testops.StatusDeleted, got, pipelines)
	}
}

// A GitLab skipped pipeline alone: skipped 1, cancelled 0, pipelines = the sum of the buckets.
func TestGitLabSkippedPipelineCountsAsSkipped(t *testing.T) {
	stored := mapGitLabTestsPipelineStatus("skipped")
	if stored != "skipped" {
		t.Fatalf("GitLab skipped is stored as %q, want skipped", stored)
	}
	got, pipelines := bucketsOfStatuses(stored, mapGitLabTestsPipelineStatus("success"), mapGitLabTestsPipelineStatus("canceled"))
	if want := (pipelineBuckets{1, 0, 1, 1, 0, 0}); got != want || pipelines != 3 {
		t.Errorf("GitLab skipped, success, canceled: buckets %v (pipelines %d), want %v and 3", got, pipelines, want)
	}
}
