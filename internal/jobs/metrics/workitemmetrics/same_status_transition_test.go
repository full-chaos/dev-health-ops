package workitemmetrics

import (
	"reflect"
	"testing"
	"time"
)

// Linear marks a duplicate with two moves: X -> Canceled, then Canceled ->
// Duplicate, which maps to canceled as well. When the history holds both, the
// second is a canceled -> canceled row. The daily work item family counts
// throughput, closures and cycle time from the item's own fields; only the
// flow breakdown reads transitions, so the row must change nothing here.
func TestDailyTripletIgnoresASameStatusCanceledTransition(t *testing.T) {
	day := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	at := func(value time.Time) *time.Time { return &value }
	canceledAt := day.Add(21*time.Hour + 33*time.Minute + 36*time.Second + 960*time.Millisecond)
	duplicateAt := canceledAt.Add(851 * time.Millisecond)
	items := []Item{
		{
			WorkItemID: "linear:ENG-1", Provider: "linear", Type: "task", Status: "canceled",
			CreatedAt: day.AddDate(0, 0, -1), StartedAt: at(day.Add(20 * time.Hour)), ClosedAt: at(canceledAt.Add(-10 * time.Millisecond)),
		},
		{
			WorkItemID: "linear:ENG-2", Provider: "linear", Type: "task", Status: "done",
			CreatedAt: day.AddDate(0, 0, -1), StartedAt: at(day.Add(20 * time.Hour)),
			CompletedAt: at(day.Add(23 * time.Hour)), ClosedAt: at(day.Add(23 * time.Hour)),
		},
	}
	base := []Transition{
		{WorkItemID: "linear:ENG-1", OccurredAt: day.Add(20 * time.Hour), FromStatus: "backlog", ToStatus: "in_progress"},
		{WorkItemID: "linear:ENG-1", OccurredAt: canceledAt, FromStatus: "in_progress", ToStatus: "canceled"},
		{WorkItemID: "linear:ENG-2", OccurredAt: day.Add(20 * time.Hour), FromStatus: "backlog", ToStatus: "in_progress"},
		{WorkItemID: "linear:ENG-2", OccurredAt: canceledAt, FromStatus: "in_progress", ToStatus: "canceled"},
		{WorkItemID: "linear:ENG-2", OccurredAt: day.Add(22*time.Hour + 7*time.Minute + 3*time.Second + 123*time.Millisecond), FromStatus: "canceled", ToStatus: "todo"},
		{WorkItemID: "linear:ENG-2", OccurredAt: day.Add(23 * time.Hour), FromStatus: "todo", ToStatus: "done"},
	}
	withRow := append(append([]Transition(nil), base...),
		Transition{WorkItemID: "linear:ENG-1", OccurredAt: duplicateAt, FromStatus: "canceled", ToStatus: "canceled"},
		Transition{WorkItemID: "linear:ENG-2", OccurredAt: duplicateAt, FromStatus: "canceled", ToStatus: "canceled"},
	)
	resolve := func(int) Attribution { return Attribution{WorkScopeID: "ENG", TeamID: "team-a", TeamName: "Team A"} }
	for _, computeDay := range []time.Time{day, day.AddDate(0, 0, 1)} {
		without := ComputeDailyTriplet(computeDay, items, base, resolve)
		with := ComputeDailyTriplet(computeDay, items, withRow, resolve)
		if computeDay.Equal(day) && len(without.MetricsDaily) == 0 {
			t.Fatalf("%s: no metrics rows", computeDay.Format(time.DateOnly))
		}
		if !reflect.DeepEqual(without, with) {
			t.Fatalf("%s: a canceled -> canceled row changed the triplet:\nwithout %+v\nwith    %+v", computeDay.Format(time.DateOnly), without, with)
		}
	}
	active, wait := FlowBreakdown(day.Add(20*time.Hour), day.Add(23*time.Hour), base[2:])
	activeWith, waitWith := FlowBreakdown(day.Add(20*time.Hour), day.Add(23*time.Hour), append(append([]Transition(nil), base[2:]...), withRow[len(withRow)-1]))
	if active != activeWith || wait != waitWith {
		t.Fatalf("flow breakdown: without (%v, %v) with (%v, %v)", active, wait, activeWith, waitWith)
	}
}
