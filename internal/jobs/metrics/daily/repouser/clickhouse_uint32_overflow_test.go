package repouser

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/checkedcast"
)

// TestWriteResultRefusesUInt32OverflowInsteadOfWrapping is the red-first
// proof for the checked-cast fix: an identity whose accumulated LOC touched
// for one repo/day exceeds UInt32 range must make the write fail loud, never
// silently wrap into a small, plausible-looking number.
//
// Before the fix, writeUserMetrics narrowed row.LOCAdded with a bare
// uint32(...) conversion -- this exact case wrapped math.MaxUint32+1 to 0 and
// wrote it (and every later row in the batch) to ClickHouse without error.
func TestWriteResultRefusesUInt32OverflowInsteadOfWrapping(t *testing.T) {
	conn := &orgIDRecordingConn{}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	result := Result{
		UserMetrics: []UserMetric{{
			RepoID:      repoA,
			Day:         day,
			AuthorEmail: "alice@example.com",
			LOCAdded:    math.MaxUint32 + 1,
			ComputedAt:  time.Now().UTC(),
		}},
	}

	_, userRows, _, err := writer.WriteResult(context.Background(), result, "c6a38355-dad6-42e4-8cc9-4c712450827d")
	if err == nil {
		t.Fatalf("expected an error for loc_added=%d exceeding UInt32 range, got userRows=%d, no error", math.MaxUint32+1, userRows)
	}
	if !errors.Is(err, checkedcast.ErrOutOfRange) {
		t.Fatalf("error = %v, want it to wrap checkedcast.ErrOutOfRange", err)
	}
	if conn.userBatch != nil && len(conn.userBatch.appended) != 0 {
		t.Fatalf("appended %d rows before failing, want 0 (no partial/corrupt write)", len(conn.userBatch.appended))
	}
	if conn.userBatch != nil && conn.userBatch.sent {
		t.Fatal("batch.Send must never be called after a rejected row")
	}
}

// TestWriteResultRefusesNegativeInsteadOfWrapping covers the negative half
// of the same class: a bare uint32(-1) cast wraps to 4294967295, a plausible
// UInt32 value, not an error.
func TestWriteResultRefusesNegativeInsteadOfWrapping(t *testing.T) {
	conn := &orgIDRecordingConn{}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	result := Result{
		RepoMetrics: []RepoMetric{{
			RepoID:       repoA,
			Day:          day,
			CommitsCount: -1,
			ComputedAt:   time.Now().UTC(),
		}},
	}

	_, _, _, err = writer.WriteResult(context.Background(), result, "c6a38355-dad6-42e4-8cc9-4c712450827d")
	if err == nil {
		t.Fatal("expected an error for commits_count=-1, got nil")
	}
	if !errors.Is(err, checkedcast.ErrOutOfRange) {
		t.Fatalf("error = %v, want it to wrap checkedcast.ErrOutOfRange", err)
	}
}
