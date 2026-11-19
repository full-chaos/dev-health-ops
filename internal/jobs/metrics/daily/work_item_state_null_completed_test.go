package daily

import (
	"reflect"
	"testing"
	"time"
)

// A terminal item with no completed_at: its status or its last known transition is done or canceled. The last
// segment ends at the last transition, so two compute times give the same segments and the item gets no hours in a
// terminal status after it. An open item whose last transition is open still runs to the compute time.
func TestSegmentWorkItemStatusesNullCompletedAtTerminalEndsAtLastTransition(t *testing.T) {
	created := mustParseUTC(t, "2025-12-17T00:00:00Z")
	started := mustParseUTC(t, "2025-12-17T06:00:00Z")
	last := mustParseUTC(t, "2025-12-18T06:00:00Z")
	computedAt := []time.Time{mustParseUTC(t, "2025-12-18T12:00:00Z"), mustParseUTC(t, "2025-12-20T00:00:00Z")}

	for _, tc := range []struct {
		name, status, lastTo string
		wantEnd              func(at time.Time) time.Time
	}{
		{"canceled_last_canceled", "canceled", "canceled", func(time.Time) time.Time { return last }},
		{"done_last_done", "done", "done", func(time.Time) time.Time { return last }},
		{"in_progress_last_done", "in_progress", "done", func(time.Time) time.Time { return last }},
		{"in_progress_last_canceled", "in_progress", "canceled", func(time.Time) time.Time { return last }},
		{"canceled_last_in_review", "canceled", "in_review", func(time.Time) time.Time { return last }},
		{"in_progress_last_in_review", "in_progress", "in_review", func(at time.Time) time.Time { return at }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transitions := []workItemStateTransition{
				{OccurredAt: started, FromStatus: "todo", ToStatus: "in_progress"},
				{OccurredAt: last, FromStatus: "in_progress", ToStatus: tc.lastTo},
			}
			var got [][]workItemStateSegment
			for _, at := range computedAt {
				segments := segmentWorkItemStatuses(created, nil, tc.status, transitions, at)
				if len(segments) == 0 {
					t.Fatalf("computed at %s: no segments", at.Format(time.RFC3339))
				}
				end := segments[len(segments)-1]
				if !end.end.Equal(tc.wantEnd(at)) {
					t.Errorf("status %s, last to %s, completed_at NULL, computed at %s: last segment %s ends %s, want %s",
						tc.status, tc.lastTo, at.Format(time.RFC3339), end.status,
						end.end.Format(time.RFC3339), tc.wantEnd(at).Format(time.RFC3339))
				}
				got = append(got, segments)
			}
			if tc.wantEnd(computedAt[0]).Equal(last) && !reflect.DeepEqual(got[0], got[1]) {
				t.Errorf("status %s, last to %s, completed_at NULL: segments depend on the compute time: %v vs %v",
					tc.status, tc.lastTo, got[0], got[1])
			}
		})
	}
}
