package providersync

import (
	"testing"
	"time"
)

func linearOwnershipTestRow(t *testing.T, team, project string, at time.Time) linearReferenceOwnershipRow {
	t.Helper()
	id := mustProjectID(t)(LinearProjectID(project))
	return linearReferenceOwnershipRow{
		OrgID: "org-1", Provider: "linear", TeamID: team, ProjectID: id, Source: "native",
		IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: at, UpdatedAt: at,
	}
}

// TestLinearOwnershipSnapshotRule pins the Linear writer on the shared rule:
// a fact still held keeps its first-seen valid_from (no new open row at each
// sync), a fact a COMPLETE run no longer holds is closed, and an incomplete
// run closes nothing.
func TestLinearOwnershipSnapshotRule(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t1.Add(time.Hour)
	kept := linearOwnershipTestRow(t, "linear:ENG", "p1", t0)
	lost := linearOwnershipTestRow(t, "linear:ENG", "p2", t0)
	open := []linearReferenceOwnershipRow{kept, lost}

	rows, closed := linearOwnershipSnapshot([]linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:ENG", "p1", t1)}, open, t1, true)
	if closed != 1 || len(rows) != 2 {
		t.Fatalf("complete run: rows=%d closed=%d, want 2 and 1", len(rows), closed)
	}
	if !rows[0].ValidFrom.Equal(t0) || rows[0].ValidTo != nil {
		t.Errorf("held fact: valid_from %v valid_to %v, want the first-seen %v and open", rows[0].ValidFrom, rows[0].ValidTo, t0)
	}
	if rows[1].ProjectID.String() != "p2" || rows[1].ValidTo == nil || !rows[1].ValidTo.Equal(t1) {
		t.Errorf("lost fact not closed at the run time: %+v", rows[1])
	}

	rows, closed = linearOwnershipSnapshot([]linearReferenceOwnershipRow{linearOwnershipTestRow(t, "linear:ENG", "p1", t2)}, open, t2, false)
	if closed != 0 || len(rows) != 1 || !rows[0].ValidFrom.Equal(t0) {
		t.Errorf("incomplete run: rows=%d closed=%d first=%v, want 1, 0 and first-seen valid_from", len(rows), closed, rows[0].ValidFrom)
	}
}
