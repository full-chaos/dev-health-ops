package capacityforecast

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// A team that cannot finish in a year (CHAOS-8477, review of #3746): with a
// history of 0, 0, 0, 1 items a day and 500 items to do, every run reaches the
// simulation's horizon with items still open. The simulation records such a run
// at the horizon day. It is NOT done, so no share of the runs is done on that
// day: the curve must not say "100 % done" for it.
func TestARunThatReachesTheHorizonIsNotCountedAsDone(t *testing.T) {
	const runs = 1000
	days, err := numerical.MonteCarloForecastDays([]int{0, 0, 0, 1}, 500, runs, 7)
	if err != nil {
		t.Fatal(err)
	}
	histogram := numerical.NewHistogram(days)
	got := distributionToModel(&histogram, nil)
	if got == nil || len(got.Days) != 1 || got.Days[0].Value != 365 || got.Days[0].Count != runs {
		t.Fatalf("the fixture must give one bin at day 365 holding every run, got %+v", got)
	}
	if share := got.Days[0].CumulativeShare; share != 0 {
		t.Fatalf("cumulativeShare at the horizon = %v: no run was done, so the share done must be 0", share)
	}
	if got.Runs != runs {
		t.Errorf("runs = %d, want all %d runs (a run that is not done is still a run)", got.Runs, runs)
	}
}

// The same team, by the two new fields: every run is unfinished, and the
// horizon is the simulation's own.
func TestUnfinishedRunsAndTheHorizonAreServed(t *testing.T) {
	const runs = 1000
	days, err := numerical.MonteCarloForecastDays([]int{0, 0, 0, 1}, 500, runs, 7)
	if err != nil {
		t.Fatal(err)
	}
	histogram := numerical.NewHistogram(days)
	got := distributionToModel(&histogram, nil)
	if got.UnfinishedRuns == nil || *got.UnfinishedRuns != runs {
		t.Fatalf("unfinishedRuns = %v, want all %d runs", got.UnfinishedRuns, runs)
	}
	if got.HorizonDays != numerical.ForecastHorizonDays {
		t.Fatalf("horizonDays = %d, want the simulation's own horizon %d", got.HorizonDays, numerical.ForecastHorizonDays)
	}
	if served := got.HorizonDays; served != 365 {
		t.Fatalf("horizonDays = %d, want 365 (the value the SDL states)", served)
	}
	// The served horizon is the day the simulation stops at: no run is recorded later.
	for _, day := range days {
		if day > got.HorizonDays {
			t.Fatalf("a run is recorded at day %d, after the served horizon %d", day, got.HorizonDays)
		}
	}
}

// A team that finishes every run: the curve ends at exactly 1 and no run is
// unfinished.
func TestEveryRunFinishes_TheLastShareIsOneAndNoRunIsUnfinished(t *testing.T) {
	const runs = 1000
	days, err := numerical.MonteCarloForecastDays([]int{2, 3, 5, 1}, 60, runs, 7)
	if err != nil {
		t.Fatal(err)
	}
	histogram := numerical.NewHistogram(days)
	got := distributionToModel(&histogram, nil)
	if got == nil || len(got.Days) < 2 {
		t.Fatalf("the fixture must give several day bins, got %+v", got)
	}
	last := got.Days[len(got.Days)-1]
	if last.Value >= got.HorizonDays {
		t.Fatalf("the fixture must finish every run before the horizon; its last bin is day %d", last.Value)
	}
	if last.CumulativeShare != 1 {
		t.Errorf("last cumulativeShare = %v, want exactly 1 when every run finished", last.CumulativeShare)
	}
	if got.UnfinishedRuns == nil || *got.UnfinishedRuns != 0 {
		t.Errorf("unfinishedRuns = %v, want 0", got.UnfinishedRuns)
	}
	if got.Runs != runs {
		t.Errorf("runs = %d, want %d", got.Runs, runs)
	}
}

// Some runs finish and some reach the horizon: the share done stops at the last
// finished bin, the horizon bin keeps its count and adds nothing, and the runs
// are all the runs.
func TestSomeRunsReachTheHorizon_TheCurveStopsBelowOne(t *testing.T) {
	histogram := numerical.Histogram{Values: []int{100, 300, 365}, Counts: []int{10, 30, 60}}
	got := distributionToModel(&histogram, nil)
	if got == nil || len(got.Days) != 3 {
		t.Fatalf("got %+v", got)
	}
	want := []struct {
		value, count int
		share        float64
	}{{100, 10, 0.10}, {300, 30, 0.40}, {365, 60, 0.40}}
	for i, w := range want {
		bin := got.Days[i]
		if bin.Value != w.value || bin.Count != w.count || bin.CumulativeShare != w.share {
			t.Errorf("bin %d = %+v, want value %d count %d share %v", i, bin, w.value, w.count, w.share)
		}
	}
	if got.Runs != 100 || got.UnfinishedRuns == nil || *got.UnfinishedRuns != 60 {
		t.Errorf("runs %d unfinishedRuns %v, want 100 and 60", got.Runs, got.UnfinishedRuns)
	}
	// The share done and the unfinished runs are the whole: 0.40 + 60 / 100 = 1.
	if done := got.Days[2].CumulativeShare + float64(*got.UnfinishedRuns)/float64(got.Runs); done != 1 {
		t.Errorf("share done + share unfinished = %v, want 1", done)
	}
}

// The items mode has no horizon: an item count of 365 is an item count, its last
// share is 1, and with no days mode there is no unfinished-run count at all.
func TestTheItemsModeHasNoHorizon(t *testing.T) {
	items := numerical.Histogram{Values: []int{200, 365, 400}, Counts: []int{1, 2, 1}}
	got := distributionToModel(nil, &items)
	if got == nil || got.Days != nil || len(got.Items) != 3 {
		t.Fatalf("got %+v", got)
	}
	if got.Items[1].CumulativeShare != 0.75 || got.Items[2].CumulativeShare != 1 {
		t.Errorf("items shares = %v, %v; want 0.75 and 1", got.Items[1].CumulativeShare, got.Items[2].CumulativeShare)
	}
	if got.UnfinishedRuns != nil {
		t.Errorf("unfinishedRuns = %d with no days mode, want null", *got.UnfinishedRuns)
	}
	if got.HorizonDays != 365 {
		t.Errorf("horizonDays = %d, want 365", got.HorizonDays)
	}
	// Both modes: the items mode is still not cut at the horizon value.
	days := numerical.Histogram{Values: []int{10, 365}, Counts: []int{3, 1}}
	both := distributionToModel(&days, &items)
	if both.Items[2].CumulativeShare != 1 || both.Days[1].CumulativeShare != 0.75 || *both.UnfinishedRuns != 1 {
		t.Errorf("both modes: items last %v, days last %v, unfinished %d", both.Items[2].CumulativeShare, both.Days[1].CumulativeShare, *both.UnfinishedRuns)
	}
}
