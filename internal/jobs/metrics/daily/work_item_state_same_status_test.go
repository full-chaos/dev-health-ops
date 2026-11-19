package daily

import (
	"reflect"
	"testing"
	"time"
)

// Linear marks a duplicate with X -> Canceled at T1, then Canceled -> Duplicate (also canceled) at T2. When the
// history holds both, the second is a canceled -> canceled row. The item is canceled with a NULL completed_at, so its
// last segment ends at its last transition: the row moves that end from T1 to T2, and the T2 - T1 between the two
// moves counts as canceled time. Nothing else changes.
func TestSegmentWorkItemStatusesSameStatusCanceledRowEndsAtTheSecondMove(t *testing.T) {
	created := mustParseUTC(t, "2026-11-16T02:48:59Z")
	canceledAt := mustParseUTC(t, "2026-11-17T21:33:36.960Z")
	duplicateAt := mustParseUTC(t, "2026-11-17T21:33:37.811Z")
	transitions := []workItemStateTransition{
		{OccurredAt: canceledAt, FromStatus: "backlog", ToStatus: "canceled"},
		{OccurredAt: duplicateAt, FromStatus: "canceled", ToStatus: "canceled"},
	}
	want := []workItemStateSegment{
		{status: "backlog", start: created, end: canceledAt},
		{status: "canceled", start: canceledAt, end: duplicateAt},
	}
	for _, computedAt := range []time.Time{mustParseUTC(t, "2026-11-18T00:00:00Z"), mustParseUTC(t, "2026-11-20T00:00:00Z")} {
		got := segmentWorkItemStatuses(created, nil, "canceled", transitions, computedAt)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("computed at %s: segments %+v, want %+v", computedAt.Format(time.RFC3339), got, want)
		}
	}
}
