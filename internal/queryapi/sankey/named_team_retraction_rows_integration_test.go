//go:build integration

package sankey

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestStateAndExpenseCountsOfANamedRetiredTeamGiveRetractionRowsNoWeight
// reads the two work item reads of the sankey for a team id the caller names,
// where that id was retired (package retractionseed).
//
// The status counts are a list: with retraction rows only it must be empty,
// where before the rule it held each status with 0 items. The expense counts
// are three sums in one row, to which a retraction row adds 0; they are read
// here to hold that.
func TestStateAndExpenseCountsOfANamedRetiredTeamGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const teamColumn = "ifNull(nullIf(team_id, ''), 'unassigned')"
	lastDay := store.Days[len(store.Days)-1]
	start, end := store.Days[0], lastDay.AddDate(0, 0, 1)
	type answers struct {
		Statuses []stateStatusCountRow
		Expense  []expenseCountsRow
	}
	read := func(org, teamID string, from time.Time) answers {
		t.Helper()
		filter, bindings := teamScopeFilter("team", []string{teamID}, teamColumn)
		var out answers
		var err error
		if out.Statuses, err = fetchStateStatusCounts(ctx, client, from, end, filter, bindings, org); err != nil {
			t.Fatalf("%s %s status counts: %v", org, teamID, err)
		}
		sort.Slice(out.Statuses, func(i, j int) bool { return out.Statuses[i].Status < out.Statuses[j].Status })
		if out.Expense, err = fetchExpenseCounts(ctx, client, from, end, filter, bindings, org); err != nil {
			t.Fatalf("%s %s expense counts: %v", org, teamID, err)
		}
		return out
	}

	none := read(retractionseed.ControlOrg, "no-such-team", start)
	if len(none.Statuses) != 0 {
		t.Fatalf("an id with no row = %+v, want no status", none)
	}
	for index, team := range retractionseed.Teams {
		// The control answer, from the seed: on its one measured day the
		// retired id touched 2+index items in each of two statuses, made
		// 1+index%2 new bugs and completed 1+index items, a quarter of them
		// bugs.
		control := read(retractionseed.ControlOrg, team.RetiredID, start)
		touched := float64(2 + index)
		wantStatuses := []stateStatusCountRow{{Status: "blocked", ItemsTouched: touched}, {Status: "in_progress", ItemsTouched: touched}}
		if !reflect.DeepEqual(control.Statuses, wantStatuses) {
			t.Fatalf("%s control %s statuses = %+v, want %+v", team.Provider, team.RetiredID, control.Statuses, wantStatuses)
		}
		if len(control.Expense) != 1 || control.Expense[0].NewBugs != float64(1+index%2) ||
			control.Expense[0].BugCompletedEstimate != 0.25*float64(1+index) {
			t.Fatalf("%s control %s expense = %+v", team.Provider, team.RetiredID, control.Expense)
		}
		if retracted := read(retractionseed.RetractedOrg, team.RetiredID, start); !reflect.DeepEqual(retracted, control) {
			t.Errorf("%s: the retraction rows changed the counts of %s:\n control   %+v\n retracted %+v",
				team.Provider, team.RetiredID, control, retracted)
		}
		// The window of the days computed again: retraction rows only.
		controlWindow := read(retractionseed.ControlOrg, team.RetiredID, store.Days[1])
		if len(controlWindow.Statuses) != 0 {
			t.Fatalf("%s control %s in the window of the days computed again = %+v, want no status",
				team.Provider, team.RetiredID, controlWindow)
		}
		if recomputed := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[1]); !reflect.DeepEqual(recomputed, controlWindow) {
			t.Errorf("%s: %s holds retraction rows only in the window and reads as %+v, want %+v",
				team.Provider, team.RetiredID, recomputed, controlWindow)
		}
	}
}
