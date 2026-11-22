package testops

import (
	"math"
	"testing"

	"github.com/google/uuid"
)

// A re-run (retry_count above 0) gives no queue sample, from queue_seconds or from started_at - queued_at: the writer
// stores the first attempt's creation and the latest attempt's start. The run stays counted (rerun_rate, duration) and
// stays one element of the pooled samples.

func rerunQueueRow(repoID uuid.UUID) PipelineRunRow {
	return PipelineRunRow{
		RepoID: repoID, Status: strPtr("success"), RetryCount: 1, OrgID: "org-1",
		QueuedAt: floatTimePtr(2026, 8, 27, 8, 59, 0), StartedAt: floatTime(2026, 8, 27, 11, 0, 0),
		FinishedAt: floatTimePtr(2026, 8, 27, 11, 30, 0), QueueSeconds: floatPtr(7260),
	}
}

func TestPipelineQueueSampleSkipsReRun(t *testing.T) {
	repoID := uuid.New()
	metrics := ComputePipelineMetrics(repoID, []PipelineRunRow{rerunQueueRow(repoID)}, "", nil)
	if len(metrics) != 1 {
		t.Fatalf("ComputePipelineMetrics = %d rows, want 1", len(metrics))
	}
	merged := finishRepoDay(t, rerunQueueRow(repoID))
	for name, got := range map[string]PipelineMetric{"Finish": metrics[0], "FinishRepoDay": *merged} {
		if got.AvgQueueSeconds != nil || got.P95QueueSeconds != nil {
			t.Errorf("%s: avg/p95 queue = %v/%v, want NULL/NULL (a day of re-runs only has no queue time, never 0)", name, showFloat(got.AvgQueueSeconds), showFloat(got.P95QueueSeconds))
		}
		if len(got.PooledSamples) != 1 || got.PooledSamples[0].Queue != nil {
			t.Errorf("%s: pooled queue samples = %v, want [nil]", name, queueSamples(got.PooledSamples))
		}
		if got.PipelinesCount != 1 || got.RerunRate != 1 || got.MedianDurationSeconds == nil || *got.MedianDurationSeconds != 1800 {
			t.Errorf("%s: pipelines %d, rerun_rate %v, median %v; want the run counted: 1, 1, 1800", name, got.PipelinesCount, got.RerunRate, showFloat(got.MedianDurationSeconds))
		}
	}
}

func TestPipelineQueueSampleKeepsFirstAttemptRuns(t *testing.T) {
	repoID := uuid.New()
	derived := rerunQueueRow(repoID)
	derived.QueueSeconds = nil
	control := PipelineRunRow{
		RepoID: repoID, Status: strPtr("success"), OrgID: "org-1",
		QueuedAt: floatTimePtr(2026, 8, 27, 11, 59, 0), StartedAt: floatTime(2026, 8, 27, 12, 0, 0),
		FinishedAt: floatTimePtr(2026, 8, 27, 12, 20, 0), QueueSeconds: floatPtr(60),
	}
	derivedControl := control
	derivedControl.QueueSeconds = nil
	got := finishRepoDay(t, rerunQueueRow(repoID), derived, control, derivedControl)
	if got.AvgQueueSeconds == nil || *got.AvgQueueSeconds != 60 || got.P95QueueSeconds == nil || *got.P95QueueSeconds != 60 {
		t.Errorf("avg/p95 queue = %v/%v, want 60/60 (the first-attempt runs only)", showFloat(got.AvgQueueSeconds), showFloat(got.P95QueueSeconds))
	}
	if math.Abs(got.RerunRate-0.5) > 1e-12 || got.PipelinesCount != 4 {
		t.Errorf("pipelines %d, rerun_rate %v; want 4 and 0.5", got.PipelinesCount, got.RerunRate)
	}
	if len(got.PooledSamples) != 4 || got.PooledSamples[0].Queue != nil || got.PooledSamples[1].Queue != nil ||
		got.PooledSamples[2].Queue == nil || *got.PooledSamples[2].Queue != 60 || got.PooledSamples[3].Queue == nil || *got.PooledSamples[3].Queue != 60 {
		t.Errorf("pooled queue samples = %v, want [nil nil 60 60]", queueSamples(got.PooledSamples))
	}
}

func showFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func queueSamples(samples []PooledSample) []any {
	out := make([]any, len(samples))
	for i, sample := range samples {
		out[i] = showFloat(sample.Queue)
	}
	return out
}
