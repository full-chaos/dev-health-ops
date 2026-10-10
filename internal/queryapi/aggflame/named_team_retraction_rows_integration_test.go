//go:build integration

package aggflame

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestCycleBreakdownOfANamedRetiredTeamGivesRetractionRowsNoWeight reads the
// hours by status for a team id the caller names, where that id was retired
// (package retractionseed).
//
// With the rows of the measured day in the window, the statuses and hours
// must be those of the control organization, which holds the same
// measurements and no retraction row. With retraction rows only, the read
// must give no status: before the rule it listed each status with 0 hours,
// and the flame of a team that was not measured is not a flame of 0 hours.
func TestCycleBreakdownOfANamedRetiredTeamGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	lastDay := store.Days[len(store.Days)-1]
	start, end := store.Days[0], lastDay.AddDate(0, 0, 1)
	read := func(org, teamID string, from time.Time) []cycleBreakdownRow {
		t.Helper()
		rows, err := fetchCycleBreakdown(ctx, client, org, from, end, teamID, "", "")
		if err != nil {
			t.Fatalf("%s %s: %v", org, teamID, err)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Status < rows[j].Status })
		return rows
	}

	if none := read(retractionseed.ControlOrg, "no-such-team", start); len(none) != 0 {
		t.Fatalf("an id with no row = %+v, want no status", none)
	}
	for index, team := range retractionseed.Teams {
		// The control answer, from the seed: the one measured day of the
		// retired id, 6+2*index blocked hours and 12 hours in progress.
		control := read(retractionseed.ControlOrg, team.RetiredID, start)
		want := []cycleBreakdownRow{{Status: "blocked", TotalHours: 6 + 2*float64(index)}, {Status: "in_progress", TotalHours: 12}}
		if !reflect.DeepEqual(control, want) {
			t.Fatalf("%s control %s = %+v, want %+v", team.Provider, team.RetiredID, control, want)
		}
		if retracted := read(retractionseed.RetractedOrg, team.RetiredID, start); !reflect.DeepEqual(retracted, control) {
			t.Errorf("%s: the retraction rows changed the breakdown of %s:\n control   %+v\n retracted %+v",
				team.Provider, team.RetiredID, control, retracted)
		}
		// The window of the days computed again: the id holds retraction rows
		// only there, in the same organization.
		if recomputed := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[1]); len(recomputed) != 0 {
			t.Errorf("%s: %s holds retraction rows only in the window and reads as %+v, want no status",
				team.Provider, team.RetiredID, recomputed)
		}
		if control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[1]); len(control) != 0 {
			t.Fatalf("%s control %s in the window of the days computed again = %+v, want no status",
				team.Provider, team.RetiredID, control)
		}
	}
}

// TestAStatusThatComesBackAndAMeasuredZeroStatusStayInTheBreakdown holds two
// things the rule must NOT take for a retraction, for a retired team id the
// caller names (package retractionseed):
//
//   - a key that comes back: the retraction row of (day, status) is followed
//     by a newer measured row. Its hours count again.
//   - a measured status of 0 hours: three items touched it and no time was
//     spent. It is a measurement (items_touched is not 0), so the status is
//     listed, with 0 hours.
func TestAStatusThatComesBackAndAMeasuredZeroStatusStayInTheBreakdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org, retired = retractionseed.RetractedOrg, "ENG"
	later := store.NewComputedAt.Add(time.Hour)
	measure := func(status string, hours float64, touched uint32) {
		t.Helper()
		if err := store.Conn.Exec(ctx, `INSERT INTO work_item_state_durations_daily
(org_id, day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at)
VALUES (?, ?, 'jira', 'ENGPROJ', ?, 'Engineering', ?, ?, ?, 0, ?)`, org, store.Days[1], retired, status, hours, touched, later); err != nil {
			t.Fatal(err)
		}
	}
	measure("blocked", 5, 1) // comes back after its retraction row
	measure("review", 0, 3)  // measured: three items, no time

	end := store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	rows, err := fetchCycleBreakdown(ctx, client, org, store.Days[0], end, retired, "", "")
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Status < rows[j].Status })
	// blocked: 6 hours of the day that was not computed again + 5 that came
	// back; in_progress: the 12 hours of that first day; review: 0 hours.
	want := []cycleBreakdownRow{{Status: "blocked", TotalHours: 11}, {Status: "in_progress", TotalHours: 12}, {Status: "review", TotalHours: 0}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("breakdown = %+v, want %+v", rows, want)
	}
}
