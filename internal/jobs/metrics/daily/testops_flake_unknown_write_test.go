package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// An unknown flake day is written with a NULL flake_cases/distinct_cases pair,
// which the org series reads as an uncounted day; a counted day keeps its pair.
func TestWriteTestopsTestMetricsLeavesTheCountPairNullForAnUnknownFlakeDay(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		unknown bool
		want    bool // pair present
	}{"unknown": {unknown: true}, "counted": {want: true}} {
		conn := &queryRecordingBatchConn{batch: &recordingBatch{}}
		metric := testops.TestMetric{
			RepoID: uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), TotalCases: 12,
			FlakeCases: 2, DistinctCases: 8, OrgID: "org-1", FlakeUnknown: test.unknown,
		}
		if _, err := writeTestopsTestMetrics(context.Background(), conn, "org-1", day, day, []testops.TestMetric{metric}); err != nil {
			t.Fatal(err)
		}
		row := conn.batch.appended[0]
		flake, distinct := row[len(row)-2].(*uint32), row[len(row)-1].(*uint32)
		if (flake != nil) != test.want || (distinct != nil) != test.want {
			t.Errorf("%s: pair = %v/%v, want present=%v", name, flake, distinct, test.want)
		}
		if test.want && (*flake != 2 || *distinct != 8) {
			t.Errorf("%s: pair = %d/%d, want 2/8", name, *flake, *distinct)
		}
	}
}
