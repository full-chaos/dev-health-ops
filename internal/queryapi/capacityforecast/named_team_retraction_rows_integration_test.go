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
