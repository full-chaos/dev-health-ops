package daily

import (
	"reflect"
	"testing"
	"time"
)

func transitionPermutations(transitions []workItemStateTransition) [][]workItemStateTransition {
	if len(transitions) <= 1 {
		return [][]workItemStateTransition{append([]workItemStateTransition(nil), transitions...)}
	}
	var out [][]workItemStateTransition
	for i := range transitions {
		rest := make([]workItemStateTransition, 0, len(transitions)-1)
		rest = append(rest, transitions[:i]...)
		rest = append(rest, transitions[i+1:]...)
		for _, tail := range transitionPermutations(rest) {
			out = append(out, append([]workItemStateTransition{transitions[i]}, tail...))
		}
	}
	return out
}

// TestSegmentWorkItemStatusesSameInstantTransitionsAreOrderFree feeds every row
// order of transitions that share an occurred_at: the segments must not depend
// on the order the loader returned them in.
func TestSegmentWorkItemStatusesSameInstantTransitionsAreOrderFree(t *testing.T) {
	created := mustParseUTC(t, "2026-08-03T00:00:00Z")
	t1 := mustParseUTC(t, "2026-08-03T02:00:00Z")
	t2 := mustParseUTC(t, "2026-08-03T06:00:00Z")
	computedAt := mustParseUTC(t, "2026-08-04T00:00:00Z")
	seg := func(status string, start, end time.Time) string {
		return status + " " + start.Format("01-02T15") + ".." + end.Format("01-02T15")
	}
	for _, tc := range []struct {
		name        string
		status      string
		transitions []workItemStateTransition
		want        []string // nil = order-free only
	}{
		{"chain_at_one_instant", "done", []workItemStateTransition{
			{OccurredAt: t1, FromStatus: "todo", ToStatus: "in_progress"},
			{OccurredAt: t1, FromStatus: "in_progress", ToStatus: "done"},
		}, []string{seg("todo", created, t1), seg("done", t1, computedAt)}},
		{"three_step_chain_at_one_instant", "done", []workItemStateTransition{
			{OccurredAt: t1, FromStatus: "todo", ToStatus: "in_progress"},
			{OccurredAt: t1, FromStatus: "in_progress", ToStatus: "in_review"},
			{OccurredAt: t1, FromStatus: "in_review", ToStatus: "done"},
		}, []string{seg("todo", created, t1), seg("done", t1, computedAt)}},
		{"reopen_and_done_at_one_instant", "in_progress", []workItemStateTransition{
			{OccurredAt: t1, FromStatus: "todo", ToStatus: "in_progress"},
			{OccurredAt: t2, FromStatus: "in_progress", ToStatus: "done"},
			{OccurredAt: t2, FromStatus: "done", ToStatus: "in_progress"},
		}, []string{seg("todo", created, t1), seg("in_progress", t1, t2), seg("in_progress", t2, computedAt)}},
		{"cycle_at_first_instant", "in_progress", []workItemStateTransition{
			{OccurredAt: t1, FromStatus: "in_progress", ToStatus: "done"},
			{OccurredAt: t1, FromStatus: "done", ToStatus: "in_progress"},
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var first []string
			for i, order := range transitionPermutations(tc.transitions) {
				var got []string
				for _, segment := range segmentWorkItemStatuses(created, nil, tc.status, order, computedAt) {
					got = append(got, seg(segment.status, segment.start, segment.end))
				}
				if i == 0 {
					first = got
				} else if !reflect.DeepEqual(got, first) {
					t.Errorf("row order %d gives segments %v; row order 0 gives %v: same-instant transitions must not depend on row order", i, got, first)
				}
				if tc.want != nil && !reflect.DeepEqual(got, tc.want) {
					t.Errorf("row order %d gives segments %v, want %v", i, got, tc.want)
				}
			}
		})
	}
}
