package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/filehotspots"
)

// TestWriteFileHotspotDailyDoesNotWrapChurnLOC30d is codex round 3's finding
// (P2): file_hotspot_daily.churn_loc_30d is UInt64 in production
// (migration 007_complexity_investment_issues.sql:43), NOT UInt32 like its
// sibling churn_commits_30d/cyclomatic_total columns. A uint32 cast on the
// Go side silently wraps any churn total >= 2^32 to a smaller (or zero)
// value BEFORE the row ever reaches ClickHouse -- the corruption happens in
// Go, so no amount of getting the ClickHouse column type right in a test
// fixture would catch it; only the argument writeFileHotspotDaily actually
// passes to batch.Append can prove this.
func TestWriteFileHotspotDailyDoesNotWrapChurnLOC30d(t *testing.T) {
	// A plain variable, not a const: Go evaluates a uint32(...) conversion
	// of a compile-time constant for representability and refuses to build
	// if it does not fit, which would defeat the point of this test.
	overflowingChurn := int(1<<32 + 12345) // exceeds MaxUint32 (4294967295)

	batch := &recordingBatch{}
	conn := &recordingBatchConn{batch: batch}
	rows := []fileHotspotDailyRow{
		{
			RepoID: uuid.New(),
			Day:    time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC),
			Metric: filehotspots.RiskMetric{
				FilePath:        "huge.py",
				ChurnLOC30d:     overflowingChurn,
				ChurnCommits30d: 2,
			},
			ComputedAt: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		},
	}

	written, err := writeFileHotspotDaily(context.Background(), conn, "org-1", rows)
	if err != nil {
		t.Fatal(err)
	}
	if written != 1 {
		t.Fatalf("written=%d, want 1", written)
	}
	if len(batch.appended) != 1 {
		t.Fatalf("appended %d rows, want 1", len(batch.appended))
	}
	// churn_loc_30d is the 4th positional argument (0-indexed 3) in the
	// INSERT column list: repo_id, day, file_path, churn_loc_30d, ...
	got, ok := batch.appended[0][3].(uint64)
	if !ok {
		t.Fatalf("churn_loc_30d argument type = %T, want uint64", batch.appended[0][3])
	}
	if got != uint64(overflowingChurn) {
		wrapped := uint32(uint64(overflowingChurn))
		t.Fatalf("churn_loc_30d = %d, want %d (a uint32 cast would silently wrap this to %d)",
			got, overflowingChurn, wrapped)
	}
}
