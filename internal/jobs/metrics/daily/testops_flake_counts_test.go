package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

type queryRecordingBatchConn struct {
	batch *recordingBatch
	query string
}

func (conn *queryRecordingBatchConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.query = query
	return conn.batch, nil
}

// CHAOS-9737: the test metrics writer names flake_cases and distinct_cases in its INSERT and appends both counts
// in the same order, before the pooled and the report completeness columns, so the org series can sum them.
func TestWriteTestopsTestMetricsWritesFlakeAndDistinctCaseCounts(t *testing.T) {
	conn := &queryRecordingBatchConn{batch: &recordingBatch{}}
	metric := testops.TestMetric{
		RepoID: uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), TotalCases: 12, FlakeRate: 0.25,
		FlakeCases: 2, DistinctCases: 8, OrgID: "org-1",
	}
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if _, err := writeTestopsTestMetrics(context.Background(), conn, "org-1", day, day, []testops.TestMetric{metric}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(strings.Fields(conn.query), " "), "computed_at, flake_cases, distinct_cases, pooled_service_ids, pooled_durations, pooled_samples_stored, report_runs, partial_report_runs, unmarked_report_runs)") {
		t.Fatalf("INSERT does not name the two count columns, then the pooled columns: %s", conn.query)
	}
	if len(conn.batch.appended) != 1 {
		t.Fatalf("appended %d rows, want 1", len(conn.batch.appended))
	}
	row := conn.batch.appended[0]
	flake, distinct := row[len(row)-8].(*uint32), row[len(row)-7].(*uint32)
	if flake == nil || *flake != 2 || distinct == nil || *distinct != 8 {
		t.Fatalf("counts = %v/%v, want 2/8", flake, distinct)
	}
	if want := 19 + 2 + 3 + 3; len(row) != want {
		t.Fatalf("row has %d values, want %d", len(row), want)
	}
}
