package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// An unknown flake day is written with a 0/0 flake_cases/distinct_cases pair,
// which adds nothing to the org series' sums; a NULL pair would blank the whole
// org day. A counted day keeps its pair.
func TestWriteTestopsTestMetricsWritesAZeroCountPairForAnUnknownFlakeDay(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		unknown      bool
		wantFlake    uint32
		wantDistinct uint32
	}{"unknown": {unknown: true}, "counted": {wantFlake: 2, wantDistinct: 8}} {
		conn := &queryRecordingBatchConn{batch: &recordingBatch{}}
		metric := testops.TestMetric{
			RepoID: uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), TotalCases: 12,
			FlakeCases: int(test.wantFlake), DistinctCases: int(test.wantDistinct), OrgID: "org-1", FlakeUnknown: test.unknown,
		}
		if _, err := writeTestopsTestMetrics(context.Background(), conn, "org-1", day, day, []testops.TestMetric{metric}); err != nil {
			t.Fatal(err)
		}
		row := conn.batch.appended[0]
		flake, distinct := row[len(row)-2].(*uint32), row[len(row)-1].(*uint32)
		if flake == nil || distinct == nil {
			t.Fatalf("%s: pair = %v/%v, want both non-NULL (a NULL pair blanks the org series)", name, flake, distinct)
		}
		if *flake != test.wantFlake || *distinct != test.wantDistinct {
			t.Errorf("%s: pair = %d/%d, want %d/%d", name, *flake, *distinct, test.wantFlake, test.wantDistinct)
		}
	}
}
