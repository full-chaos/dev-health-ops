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

func TestFlowBreakdownSameInstantTransitionsFollowTheStatusChain(t *testing.T) {
	start := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	tr := func(hours int, from, to string) Transition {
		return Transition{WorkItemID: "item-1", OccurredAt: start.Add(time.Duration(hours) * time.Hour), FromStatus: from, ToStatus: to}
	}
	cases := []struct {
		name       string
		fixed      []Transition
		tie        []Transition
		wantActive float64
		wantWait   float64
		chain      bool
	}{
		{
			name:       "chain todo->in_progress->blocked ends blocked",
			fixed:      []Transition{tr(0, "", "todo")},
			tie:        []Transition{tr(6, "in_progress", "blocked"), tr(6, "todo", "in_progress")},
			wantActive: 6, wantWait: 18, chain: true,
		},
		{
			name:       "chain at started_at seeds the chain's last status",
			tie:        []Transition{tr(0, "in_progress", "waiting"), tr(0, "", "in_progress")},
			wantActive: 0, wantWait: 24, chain: true,
		},
		{
			name:  "cycle with no chain head is deterministic",
			fixed: []Transition{tr(0, "", "todo")},
			tie:   []Transition{tr(6, "blocked", "in_progress"), tr(6, "in_progress", "blocked")},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var firstActive, firstWait float64
			for index, order := range permuteTransitions(tc.tie) {
				transitions := append(append([]Transition(nil), tc.fixed...), order...)
				active, wait := FlowBreakdown(start, end, transitions)
				if tc.chain && (active != tc.wantActive || wait != tc.wantWait) {
					t.Errorf("tie order %s: got active=%v wait=%v, want active=%v wait=%v",
						toStatuses(order), active, wait, tc.wantActive, tc.wantWait)
				}
				if index == 0 {
					firstActive, firstWait = active, wait
					continue
				}
				if active != firstActive || wait != firstWait {
					t.Errorf("must not depend on row order: tie order %s gives active=%v wait=%v; first order gives active=%v wait=%v",
						toStatuses(order), active, wait, firstActive, firstWait)
				}
			}
		})
	}
}
