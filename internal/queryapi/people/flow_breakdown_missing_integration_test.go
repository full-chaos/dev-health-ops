//go:build integration

package people

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// Missing is not healthy: avg() over a window with no stored cycle time is unknown, not 0 hours. Today each scalar
// avg() of the flow breakdown is coalesced to 0, so a person with no cycle times in the window is served Active 0 h and
// Waiting 0 h, the same answer as a person whose items took no time. The control (stored cycle times) keeps today's
// values exactly.
func TestPersonFlowBreakdownServesNoMeasuredZeroForAWindowWithNoCycleTimes(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	start, end := day, day.AddDate(0, 0, 1)

	idle, err := fetchPersonFlowBreakdown(ctx, client, []string{f.Identity}, start, end, f.OrgA)
	if err != nil {
		t.Fatal(err)
	}
	if len(idle) != 0 {
		t.Errorf("a window with no stored cycle time: flow breakdown %+v, want no stage (avg over no rows is unknown, not 0 hours)", idle)
	}

	crossorg.Exec(ctx, t, admin, `
        INSERT INTO work_item_cycle_times (org_id, work_item_id, provider, day, assignee, type, status, created_at, cycle_time_hours, lead_time_hours, computed_at)
        VALUES (?, 'wi-1', 'jira', ?, ?, 'task', 'done', ?, 10, 15, now())`, f.OrgA, day, f.Identity, day)
	stored, err := fetchPersonFlowBreakdown(ctx, client, []string{f.Identity}, start, end, f.OrgA)
	if err != nil {
		t.Fatal(err)
	}
	// UNION ALL keeps no branch order: compare in stage order.
	sort.Slice(stored, func(i, j int) bool { return stored[i].Stage < stored[j].Stage })
	want := []FlowStageItem{{Stage: "Active", Value: 10, Unit: "hours"}, {Stage: "Waiting", Value: 5, Unit: "hours"}}
	if !reflect.DeepEqual(stored, want) {
		t.Errorf("control, stored cycle times: flow breakdown %+v, want %+v", stored, want)
	}
}

// Mixed window: the Waiting branch reads a subset of the Active branch's rows (it adds lead_time_hours IS NOT NULL), so a
// window can hold a stage with data and a stage without: an item with a cycle time and no lead time. Only the empty stage
// is omitted; the stage with data keeps its value.
func TestPersonFlowBreakdownOmitsOnlyTheEmptyStageOfAMixedWindow(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	start, end := day, day.AddDate(0, 0, 1)

	crossorg.Exec(ctx, t, admin, `
        INSERT INTO work_item_cycle_times (org_id, work_item_id, provider, day, assignee, type, status, created_at, cycle_time_hours, lead_time_hours, computed_at)
        VALUES (?, 'wi-1', 'jira', ?, ?, 'task', 'done', ?, 10, NULL, now())`, f.OrgA, day, f.Identity, day)
	got, err := fetchPersonFlowBreakdown(ctx, client, []string{f.Identity}, start, end, f.OrgA)
	if err != nil {
		t.Fatal(err)
	}
	want := []FlowStageItem{{Stage: "Active", Value: 10, Unit: "hours"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("a stage with data and a stage without: flow breakdown %+v, want %+v (only the empty Waiting stage omitted)", got, want)
	}
}
