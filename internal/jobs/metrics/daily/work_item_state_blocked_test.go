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

// The end lookup is a function of the relations alone: plain ids and
// external keys apart, sorted, no repeat, no empty key.
func TestWorkItemRelationEndLookupSplitsIdsFromExternalKeys(t *testing.T) {
	relations := []workitemmetrics.BlockingRelation{
		{SourceID: "jira:OPS-1", TargetID: "jira:OPS-2"},
		{SourceID: "gh:acme/api#7", TargetID: "extkey: ops-9 "},
		{SourceID: "gh:acme/api#7", TargetID: "extkey:OPS-9"},
		{SourceID: "gh:acme/api#8", TargetID: "extkey:"},
		{SourceID: "jira:OPS-2", TargetID: "jira:OPS-1"},
	}
	ids, keys := workItemRelationEndLookup(relations)
	wantIDs := []string{"gh:acme/api#7", "gh:acme/api#8", "jira:OPS-1", "jira:OPS-2"}
	wantKeys := []string{"OPS-9"}
	if !reflect.DeepEqual(ids, wantIDs) || !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("lookup = %v, %v; want %v, %v", ids, keys, wantIDs, wantKeys)
	}
	if ids, keys := workItemRelationEndLookup(nil); ids != nil || keys != nil {
		t.Fatalf("no relation: lookup = %v, %v, want nothing", ids, keys)
	}
}
