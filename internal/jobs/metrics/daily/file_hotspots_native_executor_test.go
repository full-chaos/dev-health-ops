package daily

import (
	"context"
	"errors"
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

// TestWriteFileMetricsDailyRejectsChurnOverflowInsteadOfWrapping is codex
// round 6's finding (P2): file_metrics_daily.churn IS UInt32 in production
// (migration 001_metrics_v2.sql), unlike churn_loc_30d above -- there is no
// wider column to cast to. Python's own insert would raise a DataError
// encoding a churn total >= 2^32 (clickhouse_connect refuses to narrow it
// silently); a bare uint32(...) conversion on the Go side would instead
// silently persist a wrapped (wrong) value with no error at all. Failing
// loudly here -- the family goes Refused and falls back to the Python
// bridge, which fails the same way -- is the fidelity-correct behavior:
// silent corruption is strictly worse than a visible, already-expected
// failure mode.
func TestWriteFileMetricsDailyRejectsChurnOverflowInsteadOfWrapping(t *testing.T) {
	overflowingChurn := int(1<<32 + 12345) // exceeds MaxUint32 (4294967295)

	batch := &recordingBatch{}
	conn := &recordingBatchConn{batch: batch}
	rows := []fileMetricsDailyRow{
		{
			RepoID: uuid.New(),
			Day:    time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC),
			Metric: filehotspots.FileMetric{
				Path:         "huge.py",
				Churn:        overflowingChurn,
				Contributors: 1,
				CommitsCount: 1,
			},
			ComputedAt: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		},
	}

	written, err := writeFileMetricsDaily(context.Background(), conn, "org-1", rows)
	if err == nil {
		t.Fatalf("expected an error for a churn total exceeding UInt32 range, got written=%d, appended=%v", written, batch.appended)
	}
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("error = %v, want it to wrap ErrInvalidState", err)
	}
	if len(batch.appended) != 0 {
		t.Fatalf("appended %d rows before failing, want 0 (no partial/corrupt write)", len(batch.appended))
	}
	if batch.sent {
		t.Fatal("batch.Send must never be called after a rejected row")
	}
}
