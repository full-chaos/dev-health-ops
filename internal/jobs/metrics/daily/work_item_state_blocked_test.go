package daily

import (
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// CHAOS-8493: the daily work_item_state family moves an item's hours to
// "blocked" for the span in which the item has an open blocker, and changes
// nothing for an item that has none.
//
// One item, created at 00:00, todo -> in_progress at 06:00, open all day.
// Without a blocker the day is todo 6h + in_progress 18h.
func blockedCase(t *testing.T) (day time.Time, items []workItemStateWorkItem, transitions []workItemStateTransition, computedAt time.Time) {
	t.Helper()
	day = mustParseUTC(t, "2025-12-18T00:00:00Z")
	item := workItemStateWorkItem{WorkItemID: "jira:OPS-2", Provider: "jira", Status: "in_progress", ProjectKey: "OPS", CreatedAt: day}
	transitions = []workItemStateTransition{
		{WorkItemID: item.WorkItemID, OccurredAt: mustParseUTC(t, "2025-12-18T06:00:00Z"), FromStatus: "todo", ToStatus: "in_progress"},
	}
	return day, []workItemStateWorkItem{item}, transitions, mustParseUTC(t, "2025-12-19T00:00:00Z")
}

func hoursByStatus(rows []workItemStateDailyRow) map[string]float64 {
	hours := map[string]float64{}
	for _, row := range rows {
		hours[row.Status] += row.DurationHours
	}
	return hours
}

func TestComputeWorkItemStateDurationsMovesHoursToBlockedWhileABlockerIsOpen(t *testing.T) {
	day, items, transitions, computedAt := blockedCase(t)
	at := func(clock string) time.Time { return mustParseUTC(t, "2025-12-18T"+clock+":00Z") }
	until := func(clock string) *time.Time { value := at(clock); return &value }

	for name, tc := range map[string]struct {
		blocked map[string][]workitemmetrics.BlockedInterval
		want    map[string]float64
	}{
		"no blocker: the day is as before": {
			nil, map[string]float64{"todo": 6, "in_progress": 18},
		},
		"a blocker of ANOTHER item changes nothing": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-9": {{Start: at("00:00")}}},
			map[string]float64{"todo": 6, "in_progress": 18},
		},
		"a blocker open from 10:00 to 14:00": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: at("10:00"), End: until("14:00")}}},
			map[string]float64{"todo": 6, "in_progress": 14, "blocked": 4},
		},
		"a blocker open over the status change": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: at("04:00"), End: until("08:00")}}},
			map[string]float64{"todo": 4, "in_progress": 16, "blocked": 4},
		},
		"a blocker that is still open": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: at("12:00")}}},
			map[string]float64{"todo": 6, "in_progress": 6, "blocked": 12},
		},
		"a blocker open for the whole day": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: mustParseUTC(t, "2025-12-01T00:00:00Z")}}},
			map[string]float64{"blocked": 24},
		},
		// A relation the provider no longer reports ends at the last time a
		// sync saw it. Here that is the NEXT day at 06:00: this day keeps
		// every blocked hour from 12:00 on.
		"a relation last seen on the next day": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: at("12:00"), End: func() *time.Time { v := mustParseUTC(t, "2025-12-19T06:00:00Z"); return &v }()}}},
			map[string]float64{"todo": 6, "in_progress": 6, "blocked": 12},
		},
		"a blocker completed before the day": {
			map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: mustParseUTC(t, "2025-12-01T00:00:00Z"), End: func() *time.Time { v := mustParseUTC(t, "2025-12-17T00:00:00Z"); return &v }()}}},
			map[string]float64{"todo": 6, "in_progress": 18},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rows, _ := computeWorkItemStateDurationsForRepo(day, day, day.Add(24*time.Hour), items, transitions, nil, computedAt, tc.blocked)
			got := hoursByStatus(rows)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("hours by status = %v, want %v", got, tc.want)
			}
			// The item's hours in the day do not change: they move.
			total := 0.0
			for _, hours := range got {
				total += hours
			}
			if total != 24 {
				t.Fatalf("the item contributes %v hours to the day, want 24", total)
			}
			for _, row := range rows {
				if row.Status == workitemmetrics.StatusBlocked && (row.ItemsTouched != 1 || row.Provider != "jira" || row.WorkScopeID != "OPS") {
					t.Fatalf("blocked row = %+v, want one jira item in scope OPS", row)
				}
			}
		})
	}
}

// A finished item is never blocked, whatever the relation says: its done
// segment keeps its hours.
func TestComputeWorkItemStateDurationsNeverBlocksATerminalSegment(t *testing.T) {
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	completed := mustParseUTC(t, "2025-12-18T12:00:00Z")
	item := workItemStateWorkItem{WorkItemID: "jira:OPS-2", Provider: "jira", Status: "done", ProjectKey: "OPS", CreatedAt: day, CompletedAt: &completed}
	transitions := []workItemStateTransition{
		{WorkItemID: item.WorkItemID, OccurredAt: mustParseUTC(t, "2025-12-18T06:00:00Z"), FromStatus: "in_progress", ToStatus: "done"},
	}
	blocked := map[string][]workitemmetrics.BlockedInterval{"jira:OPS-2": {{Start: day}}}
	rows, _ := computeWorkItemStateDurationsForRepo(day, day, day.Add(24*time.Hour), []workItemStateWorkItem{item}, transitions, nil, mustParseUTC(t, "2025-12-19T00:00:00Z"), blocked)
	want := map[string]float64{"blocked": 6, "done": 6}
	if got := hoursByStatus(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("hours by status = %v, want %v (in_progress 00:00-06:00 is blocked, done 06:00-12:00 is not)", got, want)
	}
}

// TestComputeWorkItemStateDurationRowsPersistsOneBlockedSnapshotPerProvider
// proves the detail source behind Blocked Work. The per-item row uses the same
// work-scope and primary-team resolution as the aggregate state rows for every
// supported provider. A later read can use a row's zero duration to remove an
// item from the blocked list without guessing from the aggregate.
func TestComputeWorkItemStateDurationRowsPersistsOneBlockedSnapshotPerProvider(t *testing.T) {
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")
	blockedStart := mustParseUTC(t, "2025-12-18T10:00:00Z")
	blockedEnd := mustParseUTC(t, "2025-12-18T14:00:00Z")

	cases := []struct {
		provider string
		itemID   string
		scope    string
		item     workItemStateWorkItem
	}{
		{
			provider: "jira", itemID: "jira:OPS-1", scope: "OPS",
			item: workItemStateWorkItem{WorkItemID: "jira:OPS-1", Provider: "jira", Status: "in_progress", ProjectKey: "OPS", CreatedAt: day},
		},
		{
			provider: "gitlab", itemID: "gitlab:group/project#1", scope: "group/project",
			item: workItemStateWorkItem{WorkItemID: "gitlab:group/project#1", Provider: "gitlab", Status: "in_progress", ProjectID: "group/project", CreatedAt: day},
		},
		{
			provider: "github", itemID: "github:owner/repo#1", scope: "owner/repo",
			item: workItemStateWorkItem{WorkItemID: "github:owner/repo#1", Provider: "github", Status: "in_progress", ProjectID: "owner/repo", CreatedAt: day},
		},
		{
			provider: "linear", itemID: "linear:ENG-1", scope: "linear-project",
			item: workItemStateWorkItem{WorkItemID: "linear:ENG-1", Provider: "linear", Status: "in_progress", ProjectID: "linear-project", CreatedAt: day},
		},
	}

	items := make([]workItemStateWorkItem, 0, len(cases))
	transitions := make([]workItemStateTransition, 0, len(cases))
	blocked := make(map[string][]workitemmetrics.BlockedInterval, len(cases))
	attributions := make(map[string]workItemPrimaryAttribution, len(cases))
	for _, tc := range cases {
		items = append(items, tc.item)
		transitions = append(transitions, workItemStateTransition{
			WorkItemID: tc.itemID,
			OccurredAt: mustParseUTC(t, "2025-12-18T06:00:00Z"),
			FromStatus: "todo",
			ToStatus:   "in_progress",
		})
		blocked[tc.itemID] = []workitemmetrics.BlockedInterval{{Start: blockedStart, End: &blockedEnd}}
		attributions[tc.itemID] = workItemPrimaryAttribution{TeamID: "team-" + tc.provider, TeamName: "Team " + tc.provider}
	}

	_, itemRows, missingAttribution := computeWorkItemStateDurationRowsForRepo(
		day, day, day.Add(24*time.Hour), items, transitions, attributions, computedAt, blocked,
	)
	if missingAttribution != 0 {
		t.Fatalf("missingAttribution=%d, want 0", missingAttribution)
	}
	if len(itemRows) != len(cases) {
		t.Fatalf("per-item rows=%d, want %d: %#v", len(itemRows), len(cases), itemRows)
	}

	byID := make(map[string]workItemBlockedDurationDailyRow, len(itemRows))
	for _, row := range itemRows {
		byID[row.WorkItemID] = row
	}
	for _, tc := range cases {
		row, ok := byID[tc.itemID]
		if !ok {
			t.Fatalf("no per-item row for %s", tc.itemID)
		}
		if row.Provider != tc.provider || row.WorkScopeID != tc.scope || row.TeamID != "team-"+tc.provider || row.TeamName != "Team "+tc.provider || row.DurationHours != 4 {
			t.Fatalf("per-item row for %s = %+v, want provider=%q scope=%q team=%q/%q duration=4", tc.itemID, row, tc.provider, tc.scope, "team-"+tc.provider, "Team "+tc.provider)
		}
	}
}

// TestComputeWorkItemStateDurationRowsWritesZeroAfterABlockerIsRemoved pins
// the freshness rule for the persisted list source. A recompute must append a
// row for a processed item even if it now has zero blocked hours: the table's
// replacing key then makes that zero supersede the old positive snapshot.
func TestComputeWorkItemStateDurationRowsWritesZeroAfterABlockerIsRemoved(t *testing.T) {
	day, items, transitions, computedAt := blockedCase(t)
	blockedStart := mustParseUTC(t, "2025-12-18T10:00:00Z")
	blockedEnd := mustParseUTC(t, "2025-12-18T14:00:00Z")
	blocked := map[string][]workitemmetrics.BlockedInterval{
		"jira:OPS-2": {{Start: blockedStart, End: &blockedEnd}},
	}

	_, oldRows, _ := computeWorkItemStateDurationRowsForRepo(
		day, day, day.Add(24*time.Hour), items, transitions, nil, computedAt, blocked,
	)
	_, newRows, _ := computeWorkItemStateDurationRowsForRepo(
		day, day, day.Add(24*time.Hour), items, transitions, nil, computedAt.Add(time.Second), nil,
	)
	if len(oldRows) != 1 || len(newRows) != 1 {
		t.Fatalf("blocked rows old=%#v new=%#v, want one snapshot each", oldRows, newRows)
	}
	if oldRows[0].DurationHours != 4 || newRows[0].DurationHours != 0 {
		t.Fatalf("duration snapshots = %v then %v, want 4 then 0", oldRows[0].DurationHours, newRows[0].DurationHours)
	}
	if oldRows[0].Provider != newRows[0].Provider || oldRows[0].WorkScopeID != newRows[0].WorkScopeID || oldRows[0].TeamID != newRows[0].TeamID || oldRows[0].WorkItemID != newRows[0].WorkItemID {
		t.Fatalf("recomputed row key changed: old=%+v new=%+v", oldRows[0], newRows[0])
	}
}
