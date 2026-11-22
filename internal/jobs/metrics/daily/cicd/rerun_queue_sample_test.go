package cicd

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A re-run (retry_count above 0) gives no queue sample: its queued_at is the first attempt's creation and its started_at
// the latest attempt's start, so started_at - queued_at holds the time between the attempts. Every other sample of the run
// stays, and the run stays one element of the pooled samples.

func rerunQueueRuns(repo uuid.UUID) (rerun, control PipelineRunRow) {
	at := func(hour, minute int) time.Time { return time.Date(2026, 8, 24, hour, minute, 0, 0, time.UTC) }
	ptrTime := func(value time.Time) *time.Time { return &value }
	rerun = PipelineRunRow{RepoID: repo, RunID: "rerun", Status: "success", RetryCount: 1,
		QueuedAt: ptrTime(at(8, 59)), StartedAt: at(11, 0), FinishedAt: ptrTime(at(11, 30))}
	control = PipelineRunRow{RepoID: repo, RunID: "first-attempt", Status: "success",
		QueuedAt: ptrTime(at(11, 59)), StartedAt: at(12, 0), FinishedAt: ptrTime(at(12, 20))}
	return rerun, control
}

func onlyMetric(t *testing.T, rows []CICDMetric) CICDMetric {
	t.Helper()
	if len(rows) != 1 {
		t.Fatalf("ComputeCICDMetricsDaily = %d rows, want 1", len(rows))
	}
	return rows[0]
}

func TestComputeCICDReRunGivesNoQueueSample(t *testing.T) {
	rerun, control := rerunQueueRuns(repoA)
	got := onlyMetric(t, ComputeCICDMetricsDaily(day, []PipelineRunRow{rerun, control}, computedAt))
	if got.AvgQueueMinutes == nil || *got.AvgQueueMinutes != 1 {
		t.Errorf("avg_queue_minutes = %v, want 1 (the first-attempt run alone; the re-run's 121 min is no sample)", show(got.AvgQueueMinutes))
	}
	if got.PipelinesCount != 2 || got.AvgDurationMinutes == nil || *got.AvgDurationMinutes != 25 {
		t.Errorf("pipelines %d, avg duration %v; want 2 runs and 25 min (the re-run keeps its duration sample)", got.PipelinesCount, show(got.AvgDurationMinutes))
	}
	if len(got.PooledQueueMinutes) != 2 || got.PooledQueueMinutes[0] != nil || got.PooledQueueMinutes[1] == nil || *got.PooledQueueMinutes[1] != 1 {
		t.Errorf("pooled queue samples = %v, want [nil 1] (one element per counted run)", derefAll(got.PooledQueueMinutes))
	}
}

// A repository-day of re-runs only has no queue time: NULL, never 0, and no queue sample in its pooled samples.
func TestComputeCICDDayOfReRunsOnlyHasNoQueueTime(t *testing.T) {
	rerun, _ := rerunQueueRuns(repoA)
	got := onlyMetric(t, ComputeCICDMetricsDaily(day, []PipelineRunRow{rerun}, computedAt))
	if got.AvgQueueMinutes != nil {
		t.Errorf("avg_queue_minutes = %v, want NULL", *got.AvgQueueMinutes)
	}
	if got.PipelinesCount != 1 || got.AvgDurationMinutes == nil || *got.AvgDurationMinutes != 30 {
		t.Errorf("pipelines %d, avg duration %v; want the day written with 1 run of 30 min", got.PipelinesCount, show(got.AvgDurationMinutes))
	}
	if len(got.PooledQueueMinutes) != 1 || got.PooledQueueMinutes[0] != nil {
		t.Errorf("pooled queue samples = %v, want [nil]", derefAll(got.PooledQueueMinutes))
	}
}

func derefAll(values []*float64) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = show(value)
	}
	return out
}

func show(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}
