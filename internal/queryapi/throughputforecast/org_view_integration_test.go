//go:build integration

package throughputforecast

import (
	"reflect"
	"testing"
	"time"
)

// CHAOS-8863: the argMax shape. One key of work_item_metrics_daily holds a legacy row that a later run retracted with
// the NEWEST row of the key (a row of zeros), an org row and a scope row. A read that groups by the key without
// row_view would merge the retraction with the org row, argMax would take the zeros, and the key would read as dead.
// The reads must take the org row, and a legacy-only day must read as before.
func TestThroughputReadsTakeTheOrgViewWithoutADeadKey(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)

	const org = "org-8863"
	today := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	older := today.AddDate(0, 0, -2)
	newer := today.AddDate(0, 0, -1)
	t1 := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed, items_started_unassigned,
		 items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio,
		 story_points_completed, computed_at, org_id, row_view)
		VALUES (?, 'jira', 'S', 't1', 'T', 0, ?, 0, 0, ?, 0, 0, 0, ?, ?, ?)`
	for _, row := range []struct {
		day            time.Time
		completed, wip uint32
		at             time.Time
		view           string
		what           string
	}{
		{older, 2, 1, t1, "", "legacy row of a day that was never recomputed"},
		{newer, 7, 5, t1, "", "legacy row of the recomputed day"},
		{newer, 4, 3, t2, "org", "org row of the recomputed day"},
		{newer, 6, 4, t2, "scope", "scope row of the recomputed day"},
		{newer, 0, 0, t3, "", "the retraction of the legacy row, the newest row of the day"},
	} {
		if err := admin.Exec(ctx, insert, row.day, row.completed, row.wip, row.at, org, row.view); err != nil {
			t.Fatalf("insert %s: %v", row.what, err)
		}
	}

	history, err := loadThroughputHistory(ctx, client, org, []string{"t1"}, nil, 4, today)
	if err != nil {
		t.Fatalf("loadThroughputHistory: %v", err)
	}
	if want := []int{2, 4}; !reflect.DeepEqual(history, want) {
		t.Fatalf("throughput history = %v, want %v (the org row of the recomputed day, the legacy row of the other)", history, want)
	}

	backlog, err := loadBacklog(ctx, client, org, []string{"t1"}, nil)
	if err != nil {
		t.Fatalf("loadBacklog: %v", err)
	}
	if backlog != 3 {
		t.Fatalf("backlog = %d, want 3 (the org row of the newest day; the zeros of the retraction add nothing)", backlog)
	}

	current, average, err := loadWorkItemOverlay(ctx, client, org, []string{"t1"}, nil, 4, today)
	if err != nil {
		t.Fatalf("loadWorkItemOverlay: %v", err)
	}
	if current != 3 {
		t.Fatalf("current WIP = %v, want 3 (the org row of the newest day)", current)
	}
	if want := 2.0; average != want {
		t.Fatalf("average WIP = %v, want %v (days: legacy 1, org 3)", average, want)
	}
}
