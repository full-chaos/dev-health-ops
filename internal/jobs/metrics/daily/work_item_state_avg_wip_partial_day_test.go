package daily

import (
	"math"
	"testing"
	"time"
)

func avgWIPByStatusForRun(t *testing.T, computedAtValue string) map[string]float64 {
	t.Helper()
	return avgWIPByStatusForTransition(t, "2025-12-18T02:00:00Z", computedAtValue)
}

func avgWIPByStatusForTransition(t *testing.T, transitionAt, computedAtValue string) map[string]float64 {
	t.Helper()
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	item := workItemStateWorkItem{
		WorkItemID: "jira:ABC-1",
		Provider:   "jira",
		Status:     "in_progress",
		ProjectKey: "ABC",
		ProjectID:  "1",
		CreatedAt:  mustParseUTC(t, "2025-12-17T20:00:00Z"),
	}
	transitions := []workItemStateTransition{{
		WorkItemID: "jira:ABC-1",
		OccurredAt: mustParseUTC(t, transitionAt),
		FromStatus: "todo",
		ToStatus:   "in_progress",
	}}
	rows, _ := computeWorkItemStateDurationsForRepo(
		day, day, day.Add(24*time.Hour),
		[]workItemStateWorkItem{item}, transitions,
		map[string]workItemPrimaryAttribution{},
		mustParseUTC(t, computedAtValue), nil,
	)
	out := make(map[string]float64, len(rows))
	for _, row := range rows {
		out[row.Status] = row.AvgWIP
	}
	return out
}

// CHAOS-3493: a run at 12:00 on the target day has seen 12 hours of the day.
// todo holds 2 hours and in_progress holds 10 hours of those 12.
func TestWorkItemStateAvgWIPPartialDayDividesByCoveredHours(t *testing.T) {
	got := avgWIPByStatusForRun(t, "2025-12-18T12:00:00Z")
	want := map[string]float64{"todo": 2.0 / 12.0, "in_progress": 10.0 / 12.0}
	for status, w := range want {
		if math.Abs(got[status]-w) > 1e-9 {
			t.Fatalf("status %q avg_wip=%v, want %v (hours / 12 covered hours)", status, got[status], w)
		}
	}
}

// Control: a run after the day has ended covers all 24 hours.
func TestWorkItemStateAvgWIPFullDayDividesByTwentyFour(t *testing.T) {
	got := avgWIPByStatusForRun(t, "2025-12-19T00:30:00Z")
	want := map[string]float64{"todo": 2.0 / 24.0, "in_progress": 22.0 / 24.0}
	for status, w := range want {
		if math.Abs(got[status]-w) > 1e-9 {
			t.Fatalf("status %q avg_wip=%v, want %v (hours / 24)", status, got[status], w)
		}
	}
}

// computedAt one minute after the day start: the item has been in_progress
// for that minute only, so avg_wip is 1 and stays finite.
func TestWorkItemStateAvgWIPOneMinuteIntoDay(t *testing.T) {
	got := avgWIPByStatusForTransition(t, "2025-12-17T22:00:00Z", "2025-12-18T00:01:00Z")
	if len(got) != 1 || math.IsInf(got["in_progress"], 0) || math.Abs(got["in_progress"]-1.0) > 1e-9 {
		t.Fatalf("avg_wip by status = %v, want in_progress=1", got)
	}
}

// computedAt before the day start: no hours are observed, so no row exists and
// nothing divides by a non-positive span.
func TestWorkItemStateAvgWIPBeforeDayStartHasNoRows(t *testing.T) {
	got := avgWIPByStatusForTransition(t, "2025-12-17T22:00:00Z", "2025-12-17T12:00:00Z")
	if len(got) != 0 {
		t.Fatalf("rows before day start: %v", got)
	}
}

// computedAt exactly at the day end and long after it both observe 24 hours.
func TestWorkItemStateAvgWIPAtAndAfterDayEndUsesTwentyFour(t *testing.T) {
	for _, computedAt := range []string{"2025-12-19T00:00:00Z", "2025-12-25T00:00:00Z"} {
		got := avgWIPByStatusForRun(t, computedAt)
		if math.Abs(got["in_progress"]-22.0/24.0) > 1e-9 || math.Abs(got["todo"]-2.0/24.0) > 1e-9 {
			t.Fatalf("computedAt %s avg_wip=%v, want hours / 24", computedAt, got)
		}
	}
}
