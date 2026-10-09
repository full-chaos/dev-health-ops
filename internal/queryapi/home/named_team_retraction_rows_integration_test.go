//go:build integration

package home

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestThemeAllocationOfANamedRetiredTeamGivesRetractionRowsNoWeight reads the
// allocation by theme for a team id the caller names, where that id was
// retired (package retractionseed).
//
// With the measured day in the window the themes must be those of the control
// organization. With retraction rows only the read must give no theme: before
// the rule it listed the theme of each retracted key with an allocation of 0.
func TestThemeAllocationOfANamedRetiredTeamGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	end := store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	read := func(org, teamID string, from time.Time) []ReworkThemeAllocation {
		t.Helper()
		ids := []string{teamID}
		rows, err := fetchReworkThemeAllocation(ctx, client, from, end,
			scopeClauseMulti(ids, "team_id"), scopeBindingsMulti(ids), "", nil, org)
		if err != nil {
			t.Fatalf("%s %s: %v", org, teamID, err)
		}
		return rows
	}

	if none := read(retractionseed.ControlOrg, "no-such-team", store.Days[0]); len(none) != 0 {
		t.Fatalf("an id with no row = %+v, want no theme", none)
	}
	for index, team := range retractionseed.Teams {
		// The control answer, from the seed: on its one measured day the
		// retired id completed 2+index work items in one theme, merged
		// 1+index pull requests and churned 100*(1+index) lines.
		control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[0])
		if len(control) != 1 || control[0].Theme != "feature_delivery" || control[0].Allocation != float64(2+index) ||
			control[0].PRsMerged != int64(1+index) || control[0].ChurnLOC != int64(100*(1+index)) {
			t.Fatalf("%s control %s = %+v", team.Provider, team.RetiredID, control)
		}
		if retracted := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[0]); !reflect.DeepEqual(retracted, control) {
			t.Errorf("%s: the retraction rows changed the allocation of %s:\n control   %+v\n retracted %+v",
				team.Provider, team.RetiredID, control, retracted)
		}
		// The window of the days computed again: retraction rows only.
		if control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[1]); len(control) != 0 {
			t.Fatalf("%s control %s in the window of the days computed again = %+v, want no theme",
				team.Provider, team.RetiredID, control)
		}
		if recomputed := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[1]); len(recomputed) != 0 {
			t.Errorf("%s: %s holds retraction rows only in the window and reads as %+v, want no theme",
				team.Provider, team.RetiredID, recomputed)
		}
	}
}
