package daily

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

func rerunOnlyPipe(repo uuid.UUID) *testops.PipelineMetric {
	median := 400.0
	return &testops.PipelineMetric{
		RepoID: repo, PipelinesCount: 1, RerunRate: 1.0, MedianDurationSeconds: &median, OrgID: "org",
	}
}

func TestQualityDragNoQueueSampleStoresNullQueue(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	row := computeQualityDrag(repo, day, rerunOnlyPipe(repo), measuredTest(repo), day)
	if row == nil {
		t.Fatal("nil row")
	}
	if row.QueueWaitHours != nil {
		t.Errorf("a group with no queue sample: queue_wait_hours = %v, want NULL", *row.QueueWaitHours)
	}
	if !strings.Contains(row.FactorsJSON, `"avg_queue_seconds": null`) {
		t.Errorf("a group with no queue sample: factors %s, want \"avg_queue_seconds\": null", row.FactorsJSON)
	}
	// retry = 1.0*1*400/3600 = 0.1111; flake = 0.25*4*0.25 = 0.25; drag = the known parts only
	if row.DragHours == nil || !approx(*row.DragHours, 0.3611) {
		t.Errorf("drag_hours = %v, want 0.3611 (the known parts)", row.DragHours)
	}
}

func TestQualityDragQueueSampleKeepsQueue(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	pipe := rerunOnlyPipe(repo)
	queue := 180.0
	pipe.AvgQueueSeconds = &queue
	row := computeQualityDrag(repo, day, pipe, measuredTest(repo), day)
	if row == nil || row.QueueWaitHours == nil || !approx(*row.QueueWaitHours, 0.05) {
		t.Fatalf("a group with a queue sample keeps queue_wait_hours 0.05: %+v", row)
	}
	if !strings.Contains(row.FactorsJSON, `"avg_queue_seconds": 180.0`) {
		t.Errorf("factors: %s", row.FactorsJSON)
	}
	if row.DragHours == nil || !approx(*row.DragHours, 0.4111) {
		t.Errorf("drag_hours = %v, want 0.4111", row.DragHours)
	}
}

func TestQualityDragMeasuredZeroQueueKeepsZero(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	pipe := rerunOnlyPipe(repo)
	queue := 0.0
	pipe.AvgQueueSeconds = &queue
	row := computeQualityDrag(repo, day, pipe, measuredTest(repo), day)
	if row == nil || row.QueueWaitHours == nil || *row.QueueWaitHours != 0 {
		t.Fatalf("a measured 0 queue stays 0, not NULL: %+v", row)
	}
	if !strings.Contains(row.FactorsJSON, `"avg_queue_seconds": 0.0`) {
		t.Errorf("factors: %s", row.FactorsJSON)
	}
}

func TestQualityDragNoPipelineRowKeepsZeroQueue(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	row := computeQualityDrag(repo, day, nil, measuredTest(repo), day)
	if row == nil || row.QueueWaitHours == nil || *row.QueueWaitHours != 0 {
		t.Fatalf("no pipeline row: 0 pipelines wait 0 hours, not NULL: %+v", row)
	}
	if !strings.Contains(row.FactorsJSON, `"avg_queue_seconds": 0.0`) {
		t.Errorf("factors: %s", row.FactorsJSON)
	}
}
