//go:build integration

package remaining

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// jobInputs is what the recommendations loader and the capacity forecast job
// read from the team-keyed daily tables for ONE team id.
type jobInputs struct {
	// The per-day reads: one entry for each day with data.
	WIP, Throughput []float64
	AfterHours      float64
	AfterHoursKnown bool
	CycleTimes      []float64
	History         numerical.Throughput
	// The reads of the newest state of the team.
	RiskScore float64
	RiskKnown bool
	Severity  string
	Backlog   int
}

func (a jobInputs) perDay() any {
	return []any{a.WIP, a.Throughput, a.AfterHours, a.AfterHoursKnown, a.CycleTimes, a.History}
}

func (a jobInputs) newest() any {
	return []any{a.RiskScore, a.RiskKnown, a.Severity, a.Backlog}
}

// TestJobInputsOfANamedRetiredTeamGiveRetractionRowsNoWeight reads the inputs
// of the two jobs for a team id that was retired: its rows of the days
// computed again are retraction rows (package retractionseed).
//
// The per-day reads must give the days the id was measured on and no other
// day: a day of retraction rows only is not a day with a WIP, a throughput or
// an after-hours ratio of 0, and the jobs take means and counts over these
// days. The control organization holds the same measurements and no
// retraction row.
//
// The newest-state reads (the persisted risk score, the backlog) must give
// what they give for an id with no row: the retraction row is the newest row,
// so the id has no score and no backlog now. They must not fall back to the
// rows before it, which is what the control organization shows.
func TestJobInputsOfANamedRetiredTeamGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	capacity := &CapacityExecutor{conn: store.Conn}

	today := time.Now().UTC()
	lastDay := store.Days[len(store.Days)-1]
	start, end := store.Days[0], lastDay.AddDate(0, 0, 1)
	read := func(org, teamID string) jobInputs {
		t.Helper()
		loader, err := NewRecommendationsLoader(store.Conn, org)
		if err != nil {
			t.Fatalf("%s loader: %v", org, err)
		}
		var out jobInputs
		if out.WIP, out.Throughput, err = loader.loadWIPThroughput(ctx, teamID, start, end); err != nil {
			t.Fatalf("%s %s WIP and throughput: %v", org, teamID, err)
		}
		if out.AfterHours, out.AfterHoursKnown, out.CycleTimes, err = loader.loadSustainabilitySignals(ctx, teamID, start, end); err != nil {
			t.Fatalf("%s %s sustainability: %v", org, teamID, err)
		}
		if out.RiskScore, out.RiskKnown, out.Severity, err = loader.loadCompoundingRiskPersisted(ctx, teamID, start, end); err != nil {
			t.Fatalf("%s %s risk: %v", org, teamID, err)
		}
		target := capacityTarget{TeamID: &teamID}
		if out.History, err = capacity.loadThroughput(ctx, org, target, 30, today); err != nil {
			t.Fatalf("%s %s throughput history: %v", org, teamID, err)
		}
		if out.Backlog, err = capacity.loadBacklog(ctx, org, target); err != nil {
			t.Fatalf("%s %s backlog: %v", org, teamID, err)
		}
		return out
	}

	// An id that holds no row: the answer "no data" of each read.
	none := read(retractionseed.ControlOrg, "no-such-team")
	if len(none.WIP) != 0 || len(none.Throughput) != 0 || none.AfterHoursKnown || len(none.CycleTimes) != 0 ||
		none.History.DaysOfHistory != 0 || none.RiskKnown || none.Backlog != 0 {
		t.Fatalf("an id with no row = %+v, want no day, no score and a backlog of 0", none)
	}

	for index, team := range retractionseed.Teams {
		// The control answers, from the seed: on its one measured day the
		// retired id completed 1+index items with a WIP of 3+index and a
		// median cycle time of 8*(1+index) hours, made 2*(1+index) of 8
		// commits after hours, and held a risk score of 0.25+0.125*index.
		control := read(retractionseed.ControlOrg, team.RetiredID)
		want := jobInputs{
			WIP: []float64{float64(3 + index)}, Throughput: []float64{float64(1 + index)},
			AfterHours: float64(2*(1+index)) / 8, AfterHoursKnown: true,
			CycleTimes: []float64{8 * float64(1+index)},
			History:    numerical.Throughput{DailyThroughputs: []int{1 + index}, DaysOfHistory: 1},
			RiskScore:  0.25 + 0.125*float64(index), RiskKnown: true, Severity: "low",
			Backlog: 3 + index,
		}
		if !reflect.DeepEqual(control, want) {
			t.Fatalf("%s control %s = %+v, want %+v", team.Provider, team.RetiredID, control, want)
		}

		retracted := read(retractionseed.RetractedOrg, team.RetiredID)
		if !reflect.DeepEqual(retracted.perDay(), control.perDay()) {
			t.Errorf("%s: the retraction rows changed the per-day reads of %s:\n control   %v\n retracted %v",
				team.Provider, team.RetiredID, control.perDay(), retracted.perDay())
		}
		if !reflect.DeepEqual(retracted.newest(), none.newest()) {
			t.Errorf("%s: the newest-state reads of %s = %v, want those of an id with no row %v",
				team.Provider, team.RetiredID, retracted.newest(), none.newest())
		}

		// An organization in which the id holds retraction rows only.
		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, lastDay, team, store.OldComputedAt, store.NewComputedAt)
		only := read(retractionseed.RetractionOnlyOrg, team.RetiredID)
		if !reflect.DeepEqual(only.perDay(), none.perDay()) || !reflect.DeepEqual(only.newest(), none.newest()) {
			t.Errorf("%s: %s holds retraction rows only and reads as %v %v, want no data %v %v",
				team.Provider, team.RetiredID, only.perDay(), only.newest(), none.perDay(), none.newest())
		}
	}
}
