package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The pipeline writer names the four bucket columns after computed_at, then the pooled sample columns, and appends each
// count in the same order.
func TestWriteTestopsPipelineMetricsWritesTheFourBucketCounts(t *testing.T) {
	conn := &queryRecordingBatchConn{batch: &recordingBatch{}}
	metric := testops.PipelineMetric{
		RepoID: uuid.MustParse("00000000-0000-4000-8000-000000014019"), PipelinesCount: 20,
		SuccessCount: 5, FailureCount: 4, CancelledCount: 3,
		SkippedCount: 2, UnfinishedCount: 6, OtherCount: 0, UnfinishedStaleCount: 1, OrgID: "org-1",
	}
	day := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	if _, err := writeTestopsPipelineMetrics(context.Background(), conn, "org-1", day, day, []testops.PipelineMetric{metric}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(strings.Fields(conn.query), " "),
		"computed_at, skipped_count, unfinished_count, other_count, unfinished_stale_count, pooled_service_ids, pooled_durations, pooled_queues, pooled_samples_stored)") {
		t.Fatalf("INSERT does not name the four bucket columns, then the pooled columns: %s", conn.query)
	}
	if len(conn.batch.appended) != 1 {
		t.Fatalf("appended %d rows, want 1", len(conn.batch.appended))
	}
	row := conn.batch.appended[0]
	if want := 18 + 4 + 4; len(row) != want {
		t.Fatalf("row has %d values, want %d", len(row), want)
	}
	want := []uint32{2, 6, 0, 1}
	for index, value := range row[len(row)-8 : len(row)-4] {
		count, ok := value.(*uint32)
		if !ok || count == nil || *count != want[index] {
			t.Errorf("bucket value %d = %#v, want %d", index, value, want[index])
		}
	}
}
