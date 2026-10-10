//go:build integration

package cognitiveload

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestCognitiveLoadOfANamedRetiredTeamGivesRetractionRowsNoWeight reads the
// stored cognitive load of a team id the caller names, where that id was
// retired: its rows of the days computed again are retraction rows (package
// retractionseed).
//
// The days must be the days the id was measured on, as in the control
// organization, which holds the same measurements and no retraction row. A
// day whose newest row is a retraction row is a day with no data: before the
// rule it was a day with a load of 0 and no commit ratio.
func TestCognitiveLoadOfANamedRetiredTeamGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const layout = "2006-01-02"
	lastDay := store.Days[len(store.Days)-1]
	read := func(org, teamID string, from time.Time) []fullDayRow {
		t.Helper()
		rows, err := fetchTeamCognitiveLoad(ctx, client, org, teamID, from.Format(layout), lastDay.Format(layout))
		if err != nil {
			t.Fatalf("%s %s: %v", org, teamID, err)
		}
		return rows
	}

	if none := read(retractionseed.ControlOrg, "no-such-team", store.Days[0]); none != nil {
		t.Fatalf("an id with no row = %+v, want no day", none)
	}
	for index, team := range retractionseed.Teams {
		// The control answer, from the seed: the one measured day of the
		// retired id, with loads of 2+index, 3+index and 4, 2*(1+index) of 8
		// commits after hours and index%2 of 8 on a weekend.
		control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[0])
		if len(control) != 1 || !control[0].day.Equal(store.Days[0]) ||
			control[0].prInterruptionLoad != float64(2+index) || control[0].contextSpreadCount != float64(3+index) ||
			control[0].reviewRequestLoad != 4 || control[0].afterHoursCommitRatio == nil ||
			*control[0].afterHoursCommitRatio != float64(2*(1+index))/8 || control[0].weekendCommitRatio == nil ||
			*control[0].weekendCommitRatio != float64(index%2)/8 {
			t.Fatalf("%s control %s = %+v", team.Provider, team.RetiredID, control)
		}
		if retracted := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[0]); !reflect.DeepEqual(retracted, control) {
			t.Errorf("%s: the retraction rows changed the days of %s:\n control   %+v\n retracted %+v",
				team.Provider, team.RetiredID, control, retracted)
		}
		// The window of the days computed again: retraction rows only.
		if control := read(retractionseed.ControlOrg, team.RetiredID, store.Days[1]); control != nil {
			t.Fatalf("%s control %s in the window of the days computed again = %+v, want no day",
				team.Provider, team.RetiredID, control)
		}
		if recomputed := read(retractionseed.RetractedOrg, team.RetiredID, store.Days[1]); recomputed != nil {
			t.Errorf("%s: %s holds retraction rows only in the window and reads as %+v, want no day",
				team.Provider, team.RetiredID, recomputed)
		}
		// The keyed id is measured on those days: its days stay.
		if keyed := read(retractionseed.RetractedOrg, team.KeyedID, store.Days[1]); len(keyed) != len(store.Days)-1 {
			t.Errorf("%s: %s has %d days, want %d", team.Provider, team.KeyedID, len(keyed), len(store.Days)-1)
		}
	}
}

// TestTheRetractionDaysOfOneOrganizationTakeNoDayFromAnother holds the
// organization scope of the second read of the named-team path (the days
// whose newest row is a retraction row). Two organizations hold a team with
// the same id. In one the day is a retraction row; in the other the same day
// is a measured row, stored earlier. The read of the second organization must
// keep its measured day: a read of the retraction days across organizations
// would take the newer retraction row of the first for it and drop the day.
func TestTheRetractionDaysOfOneOrganizationTakeNoDayFromAnother(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const layout, retired = "2006-01-02", "ENG"
	measuredDay, lastDay := store.Days[1], store.Days[len(store.Days)-1]
	// The control organization measured the team on a day on which the other
	// organization holds a retraction row of the same team id. The measured
	// row is the OLDER of the two.
	if err := store.Conn.Exec(ctx, `INSERT INTO team_cognitive_load_daily
(org_id, team_id, day, pr_interruption_load, context_spread_count, review_request_load, contributing_repo_count, sample_author_count, computed_at)
VALUES (?, ?, ?, 5, 5, 5, 1, 2, ?)`, retractionseed.ControlOrg, retired, measuredDay, store.OldComputedAt); err != nil {
		t.Fatal(err)
	}
	read := func(org string) []time.Time {
		t.Helper()
		rows, err := fetchTeamCognitiveLoad(ctx, client, org, retired, store.Days[0].Format(layout), lastDay.Format(layout))
		if err != nil {
			t.Fatalf("%s: %v", org, err)
		}
		var days []time.Time
		for _, row := range rows {
			days = append(days, row.day.UTC())
		}
		return days
	}
	if got, want := read(retractionseed.ControlOrg), []time.Time{store.Days[0], measuredDay}; !reflect.DeepEqual(got, want) {
		t.Errorf("the organization with the measured day reads %v, want %v", got, want)
	}
	// The other organization is not changed by that row: its day stays out.
	if got, want := read(retractionseed.RetractedOrg), []time.Time{store.Days[0]}; !reflect.DeepEqual(got, want) {
		t.Errorf("the organization with the retraction row reads %v, want %v", got, want)
	}
}
