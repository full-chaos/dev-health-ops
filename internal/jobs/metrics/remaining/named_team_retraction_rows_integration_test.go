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

// TestARetractionOverOneScopeTakesNothingFromAnotherScopeOfTheDayInTheJobs
// holds the place of the rule against a roll-up in the two jobs: the rule is
// of the key the writer writes (provider, work scope, team, day), and the
// readers sum the keys of a day AFTER it. One team has two work scopes on one
// day. Both are measured; then a newer retraction row is stored over one
// scope. The day stays, with the numbers of the measured scope.
func TestARetractionOverOneScopeTakesNothingFromAnotherScopeOfTheDayInTheJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)

	const org, team = "c0c0c0c0-0000-4000-8000-0000000090b2", "jira:TWO"
	day := store.Days[1]
	measure := func(scope string, completed, wip uint32, cycle float64) {
		t.Helper()
		if err := store.Conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, cycle_time_p50_hours, computed_at)
VALUES (?, ?, 'jira', ?, ?, 'Two', 1, ?, ?, ?, ?)`, org, day, scope, team, completed, wip, cycle, store.OldComputedAt); err != nil {
			t.Fatal(err)
		}
	}
	measure("SCOPE-A", 3, 2, 10)
	measure("SCOPE-B", 5, 4, 20)
	if err := store.Conn.Exec(ctx, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, computed_at)
VALUES (?, ?, 'jira', 'SCOPE-A', ?, ?)`, org, day, team, store.NewComputedAt); err != nil {
		t.Fatal(err)
	}

	loader, err := NewRecommendationsLoader(store.Conn, org)
	if err != nil {
		t.Fatal(err)
	}
	start, end := store.Days[0], store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	wip, throughput, err := loader.loadWIPThroughput(ctx, team, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wip, []float64{4}) || !reflect.DeepEqual(throughput, []float64{5}) {
		t.Errorf("WIP %v and throughput %v, want [4] and [5]: the measured scope of the day", wip, throughput)
	}
	_, _, cycleTimes, err := loader.loadSustainabilitySignals(ctx, team, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cycleTimes, []float64{20}) {
		t.Errorf("cycle times %v, want [20]: the measured scope of the day", cycleTimes)
	}
	teamID := team
	history, err := (&CapacityExecutor{conn: store.Conn}).loadThroughput(ctx, org, capacityTarget{TeamID: &teamID}, 30, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if want := (numerical.Throughput{DailyThroughputs: []int{5}, DaysOfHistory: 1}); !reflect.DeepEqual(history, want) {
		t.Errorf("capacity history %+v, want %+v", history, want)
	}
}
