package workitemmetrics

import (
	"fmt"
	"testing"
	"time"
)

func permuteTransitions(values []Transition) [][]Transition {
	if len(values) <= 1 {
		return [][]Transition{append([]Transition(nil), values...)}
	}
	var out [][]Transition
	for i := range values {
		rest := make([]Transition, 0, len(values)-1)
		rest = append(rest, values[:i]...)
		rest = append(rest, values[i+1:]...)
		for _, tail := range permuteTransitions(rest) {
			out = append(out, append([]Transition{values[i]}, tail...))
		}
	}
	return out
}

func TestFlowBreakdownSameInstantTransitionsAreOrderFree(t *testing.T) {
	start := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	at := func(hours int) time.Time { return start.Add(time.Duration(hours) * time.Hour) }
	tr := func(hours int, to string) Transition {
		return Transition{WorkItemID: "item-1", OccurredAt: at(hours), ToStatus: to}
	}
	cases := []struct {
		name  string
		fixed []Transition
		tie   []Transition
	}{
		{
			name:  "tie inside the window",
			fixed: []Transition{tr(0, "todo")},
			tie:   []Transition{tr(6, "blocked"), tr(6, "in_progress")},
		},
		{
			name: "tie at started_at seeds the first status",
			tie:  []Transition{tr(0, "blocked"), tr(0, "in_progress")},
		},
		{
			name:  "three-way tie inside the window",
			fixed: []Transition{tr(0, "in_progress")},
			tie:   []Transition{tr(8, "waiting"), tr(8, "in_progress"), tr(8, "review_requested")},
		},
		{
			name:  "tie before started_at seeds the first status",
			fixed: []Transition{tr(12, "done")},
			tie:   []Transition{tr(-2, "blocked"), tr(-2, "in_progress")},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var firstActive, firstWait float64
			var firstOrder []Transition
			for index, order := range permuteTransitions(tc.tie) {
				transitions := append(append([]Transition(nil), tc.fixed...), order...)
				active, wait := FlowBreakdown(start, end, transitions)
				if index == 0 {
					firstActive, firstWait, firstOrder = active, wait, order
					continue
				}
				if active != firstActive || wait != firstWait {
					t.Errorf("must not depend on row order: tie order %s gives active=%v wait=%v; tie order %s gives active=%v wait=%v",
						toStatuses(order), active, wait, toStatuses(firstOrder), firstActive, firstWait)
				}
			}
		})
	}
}

func toStatuses(values []Transition) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, value.ToStatus)
	}
	return fmt.Sprint(out)
}
