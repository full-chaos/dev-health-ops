package workitemmetrics

import (
	"reflect"
	"testing"
	"time"
)

func transitionOrders(transitions []Transition) [][]Transition {
	if len(transitions) <= 1 {
		return [][]Transition{append([]Transition(nil), transitions...)}
	}
	var out [][]Transition
	for i := range transitions {
		rest := make([]Transition, 0, len(transitions)-1)
		rest = append(rest, transitions[:i]...)
		rest = append(rest, transitions[i+1:]...)
		for _, tail := range transitionOrders(rest) {
			out = append(out, append([]Transition{transitions[i]}, tail...))
		}
	}
	return out
}

// TestOrderTransitionsIsRowOrderFree: every row order of the same transitions gives one order; a same-instant chain
// follows the status in force, and a same-instant cycle sorts by (from_status, to_status).
func TestOrderTransitionsIsRowOrderFree(t *testing.T) {
	t1 := time.Date(2026, 8, 3, 2, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 8, 3, 6, 0, 0, 0, time.UTC)
	step := func(at time.Time, from, to string) Transition {
		return Transition{WorkItemID: "item", OccurredAt: at, FromStatus: from, ToStatus: to}
	}
	for _, tc := range []struct {
		name        string
		transitions []Transition
		want        []Transition
	}{
		{"chain_at_one_instant",
			[]Transition{step(t1, "todo", "in_progress"), step(t1, "in_progress", "done")},
			[]Transition{step(t1, "todo", "in_progress"), step(t1, "in_progress", "done")}},
		{"three_step_chain_at_one_instant",
			[]Transition{step(t1, "todo", "in_progress"), step(t1, "in_progress", "in_review"), step(t1, "in_review", "done")},
			[]Transition{step(t1, "todo", "in_progress"), step(t1, "in_progress", "in_review"), step(t1, "in_review", "done")}},
		{"reopen_and_done_after_a_start",
			[]Transition{step(t1, "todo", "in_progress"), step(t2, "in_progress", "done"), step(t2, "done", "in_progress")},
			[]Transition{step(t1, "todo", "in_progress"), step(t2, "in_progress", "done"), step(t2, "done", "in_progress")}},
		{"cycle_at_first_instant_sorts_by_string",
			[]Transition{step(t1, "in_progress", "done"), step(t1, "done", "in_progress")},
			[]Transition{step(t1, "done", "in_progress"), step(t1, "in_progress", "done")}},
		{"cycle_not_leaving_the_prior_status_sorts_by_string",
			[]Transition{step(t1, "backlog", "todo"), step(t2, "in_progress", "done"), step(t2, "done", "in_progress")},
			[]Transition{step(t1, "backlog", "todo"), step(t2, "done", "in_progress"), step(t2, "in_progress", "done")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i, order := range transitionOrders(tc.transitions) {
				if got := OrderTransitions(order); !reflect.DeepEqual(got, tc.want) {
					t.Errorf("row order %d: OrderTransitions gives %v, want %v", i, got, tc.want)
				}
			}
		})
	}
}
