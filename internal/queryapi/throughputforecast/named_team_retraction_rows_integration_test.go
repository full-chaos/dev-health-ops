//go:build integration

package throughputforecast

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// forecastInputs is what the readers of work_item_metrics_daily give the
// forecast for one named team.
type forecastInputs struct {
	// The per-day reads: one entry for each day with data.
	History    []int
	AverageWIP float64
	// The reads of the state on the newest day of each key.
	CurrentWIP         float64
	StaleP50, StaleP90 *float64
	Backlog            int
}

func (a forecastInputs) perDay() any {
	average := any(a.AverageWIP)
	if math.IsNaN(a.AverageWIP) {
		average = "no day"
	}
	return []any{a.History, average}
}

func (a forecastInputs) newestDay() any {
	return []any{a.CurrentWIP, deref(a.StaleP50), deref(a.StaleP90), a.Backlog}
}

func deref(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

// TestForecastInputsOfANamedRetiredTeamGiveRetractionRowsNoWeight reads the
// forecast inputs for a team id the caller names, where that id was retired:
// its rows of the days computed again are retraction rows (package
// retractionseed).
//
// The per-day reads must give the days the id was measured on and no other
// day: a day that holds only retraction rows is a day with no data, not a day
// with a throughput or a WIP of 0. The control organization holds the same
// measurements and no retraction row, so the two must agree.
//
// The newest-day reads must give what they give for an id with no row at all:
// the retraction row is the newest row of the key, so the id has no backlog
// now. They must NOT fall back to the day before the retraction; the control
// organization shows the value such a fallback would serve.
func TestForecastInputsOfANamedRetiredTeamGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	today := time.Now().UTC()
	const historyWeeks = 4
	read := func(org, teamID string) forecastInputs {
		t.Helper()
		var out forecastInputs
		var err error
		teams := []string{teamID}
		if out.History, err = loadThroughputHistory(ctx, client, org, teams, nil, historyWeeks, today); err != nil {
			t.Fatalf("%s %s history: %v", org, teamID, err)
		}
		if out.CurrentWIP, out.AverageWIP, err = loadWorkItemOverlay(ctx, client, org, teams, nil, historyWeeks, today); err != nil {
			t.Fatalf("%s %s overlay: %v", org, teamID, err)
		}
		if out.StaleP50, out.StaleP90, err = loadStaleWIP(ctx, client, org, teams, nil); err != nil {
			t.Fatalf("%s %s stale WIP: %v", org, teamID, err)
		}
		if out.Backlog, err = loadBacklog(ctx, client, org, teams, nil); err != nil {
			t.Fatalf("%s %s backlog: %v", org, teamID, err)
		}
		return out
	}

	// An id that holds no row: the answer "no data" of each read.
	none := read(retractionseed.ControlOrg, "no-such-team")
	if len(none.History) != 0 || !math.IsNaN(none.AverageWIP) || none.CurrentWIP != 0 ||
		none.StaleP50 != nil || none.StaleP90 != nil || none.Backlog != 0 {
		t.Fatalf("an id with no row = %+v, want no day, no age and 0", none)
	}

	lastDay := store.Days[len(store.Days)-1]
	for index, team := range retractionseed.Teams {
		// The control answers, from the seed: the retired id holds the rows of
		// the one day that was not computed again, with 1+index items
		// completed, a WIP of 3+index and WIP ages of 4 and 8 times 1+index.
		control := read(retractionseed.ControlOrg, team.RetiredID)
		wip, age := float64(3+index), 4*float64(index+1)
		want := forecastInputs{
			History: []int{1 + index}, AverageWIP: wip,
			CurrentWIP: wip, StaleP50: &age, StaleP90: ptr(2 * age), Backlog: 3 + index,
		}
		if !reflect.DeepEqual(control, want) {
			t.Fatalf("%s control %s = %+v, want %+v", team.Provider, team.RetiredID, control, want)
		}

		retracted := read(retractionseed.RetractedOrg, team.RetiredID)
		if !reflect.DeepEqual(retracted.perDay(), control.perDay()) {
			t.Errorf("%s: the retraction rows changed the per-day reads of %s:\n control   %v\n retracted %v",
				team.Provider, team.RetiredID, control.perDay(), retracted.perDay())
		}
		if !reflect.DeepEqual(retracted.newestDay(), none.newestDay()) {
			t.Errorf("%s: the newest-day reads of %s = %v, want those of an id with no row %v",
				team.Provider, team.RetiredID, retracted.newestDay(), none.newestDay())
		}

		// An organization in which the id holds retraction rows only.
		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, lastDay, team, store.OldComputedAt, store.NewComputedAt)
		only := read(retractionseed.RetractionOnlyOrg, team.RetiredID)
		if !reflect.DeepEqual(only.perDay(), none.perDay()) || !reflect.DeepEqual(only.newestDay(), none.newestDay()) {
			t.Errorf("%s: %s holds retraction rows only and reads as %v %v, want no data %v %v",
				team.Provider, team.RetiredID, only.perDay(), only.newestDay(), none.perDay(), none.newestDay())
		}
	}
}

func ptr(value float64) *float64 { return &value }
