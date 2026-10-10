//go:build integration

package capacityforecast

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestCapacityInputsOfANamedRetiredTeamGiveRetractionRowsNoWeight reads the
// throughput history and the backlog for a team id the caller names, where
// that id was retired: its rows of the days computed again are retraction
// rows (package retractionseed).
//
// The history must hold the days the id was measured on and no other day: a
// day of retraction rows only is a day with no data, not a day on which the
// team completed 0 items, and the forecast counts its days. The control
// organization holds the same measurements and no retraction row.
//
// The backlog is the WIP on the newest day of each key. There the retraction
// row is the newest row, so the id has no backlog; the read must not fall back
// to the day before it, which is the value the control organization shows.
func TestCapacityInputsOfANamedRetiredTeamGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	today := time.Now().UTC()
	type answers struct {
		History []int
		Backlog int
	}
	read := func(org, teamID string) answers {
		t.Helper()
		var out answers
		var err error
		if out.History, err = loadThroughput(ctx, client, org, []string{teamID}, nil, 30, today); err != nil {
			t.Fatalf("%s %s throughput: %v", org, teamID, err)
		}
		if out.Backlog, err = loadBacklog(ctx, client, org, []string{teamID}, nil); err != nil {
			t.Fatalf("%s %s backlog: %v", org, teamID, err)
		}
		return out
	}

	none := read(retractionseed.ControlOrg, "no-such-team")
	if len(none.History) != 0 || none.Backlog != 0 {
		t.Fatalf("an id with no row = %+v, want no day and a backlog of 0", none)
	}

	lastDay := store.Days[len(store.Days)-1]
	for index, team := range retractionseed.Teams {
		// The control answers, from the seed: the retired id holds the rows of
		// the one day that was not computed again, with 1+index items
		// completed and a WIP of 3+index.
		control := read(retractionseed.ControlOrg, team.RetiredID)
		if want := (answers{History: []int{1 + index}, Backlog: 3 + index}); !reflect.DeepEqual(control, want) {
			t.Fatalf("%s control %s = %+v, want %+v", team.Provider, team.RetiredID, control, want)
		}

		retracted := read(retractionseed.RetractedOrg, team.RetiredID)
		if !reflect.DeepEqual(retracted.History, control.History) {
			t.Errorf("%s: the retraction rows changed the history of %s: control %v, retracted %v",
				team.Provider, team.RetiredID, control.History, retracted.History)
		}
		if retracted.Backlog != none.Backlog {
			t.Errorf("%s: backlog of %s = %d, want that of an id with no row (%d)",
				team.Provider, team.RetiredID, retracted.Backlog, none.Backlog)
		}

		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, lastDay, team, store.OldComputedAt, store.NewComputedAt)
		if only := read(retractionseed.RetractionOnlyOrg, team.RetiredID); !reflect.DeepEqual(only, none) {
			t.Errorf("%s: %s holds retraction rows only and reads as %+v, want no data %+v",
				team.Provider, team.RetiredID, only, none)
		}
	}
}

// TestAKeyThatComesBackAndAMeasuredZeroDayStayInTheCapacityHistory holds two things the rule must NOT take for a retraction, for a
// retired team id the caller names (package retractionseed):
//
//   - a key that comes back: the day's retraction row is followed by a newer
//     measured row. The day is in the history again, with its newer numbers.
//   - a measured day of 0: three items started, none completed, none in
//     progress. It is a measurement (a measure of the row is not 0), so the
//     day stays in the history with a throughput of 0.
//
// The other days of the id that were computed again hold retraction rows only
// and stay out.
func TestAKeyThatComesBackAndAMeasuredZeroDayStayInTheCapacityHistory(t *testing.T) {
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
	measure := func(day time.Time, started, completed, wip uint32) {
		t.Helper()
		if err := store.Conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at)
VALUES (?, ?, 'jira', 'ENGPROJ', ?, 'Engineering', ?, ?, ?, ?)`, org, day, retired, started, completed, wip, later); err != nil {
			t.Fatal(err)
		}
	}
	measure(store.Days[1], 1, 7, 2) // comes back after its retraction row
	measure(store.Days[2], 3, 0, 0) // a measured day with nothing completed and nothing in progress

	history, err := loadThroughput(ctx, client, org, []string{retired}, nil, 30, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 7, 0}; !reflect.DeepEqual(history, want) {
		t.Errorf("history = %v, want %v", history, want)
	}
}
