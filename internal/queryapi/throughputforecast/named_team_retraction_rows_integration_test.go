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

// TestEstimateCoverageOfANamedRetiredTeamHasNoRule holds the one read of a
// named team that gets no rule, and the reason. In
// estimate_coverage_metrics_daily a MEASURED group whose items are all closed
// is stored with 0 in every count and no ratio, which is what a retraction
// row holds: no reader can drop one and keep the other. The read sums the
// newest day of each key, so a retraction row adds 0; what is left is that an
// id whose keys are all retracted answers "ratio not known" (as a team with
// an empty backlog does), where an id with no row answers a ratio of 0.
func TestEstimateCoverageOfANamedRetiredTeamHasNoRule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	type answer struct {
		Ratio                           any
		Estimated, Unestimated, Backlog int
	}
	read := func(org, teamID string) answer {
		t.Helper()
		coverage, err := loadEstimateCoverage(ctx, client, org, []string{teamID}, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", org, teamID, err)
		}
		return answer{deref(coverage.ratio), coverage.estimatedCount, coverage.unestimatedCount, coverage.backlogSize}
	}

	// A measured group with an empty backlog, stored as the writer stores it:
	// the counts are 0 and the ratio is not set.
	const emptyBacklogTeam = "team-with-an-empty-backlog"
	lastDay := store.Days[len(store.Days)-1]
	if err := store.Conn.Exec(ctx, `INSERT INTO estimate_coverage_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, estimated_count, unestimated_count, backlog_size, computed_at)
VALUES (?, ?, 'jira', 'ENGPROJ', ?, 'Empty', 0, 0, 0, ?)`,
		retractionseed.ControlOrg, lastDay, emptyBacklogTeam, store.NewComputedAt); err != nil {
		t.Fatalf("store the measured empty backlog: %v", err)
	}
	measuredEmpty := read(retractionseed.ControlOrg, emptyBacklogTeam)
	if want := (answer{Ratio: nil}); measuredEmpty != want {
		t.Fatalf("a measured empty backlog = %+v, want %+v", measuredEmpty, want)
	}
	if none, want := read(retractionseed.ControlOrg, "no-such-team"), (answer{Ratio: 0.0}); none != want {
		t.Fatalf("an id with no row = %+v, want %+v", none, want)
	}

	for index, team := range retractionseed.Teams {
		// The control answer, from the seed: on its one measured day the
		// retired id holds 3+index estimated and 3+index unestimated items.
		wip := 3 + index
		control := read(retractionseed.ControlOrg, team.RetiredID)
		if want := (answer{Ratio: 0.5, Estimated: wip, Unestimated: wip, Backlog: 2 * wip}); control != want {
			t.Fatalf("%s control %s = %+v, want %+v", team.Provider, team.RetiredID, control, want)
		}
		// The retraction row is the newest row of the key: it reads as the
		// measured empty backlog, and not as the day before it.
		if retracted := read(retractionseed.RetractedOrg, team.RetiredID); retracted != measuredEmpty {
			t.Errorf("%s: estimate coverage of %s = %+v, want that of a measured empty backlog %+v",
				team.Provider, team.RetiredID, retracted, measuredEmpty)
		}
	}
}

// TestAKeyThatComesBackAndAMeasuredZeroDayStayInTheForecastHistory holds two things the rule must NOT take for a retraction, for a
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
func TestAKeyThatComesBackAndAMeasuredZeroDayStayInTheForecastHistory(t *testing.T) {
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

	today := time.Now().UTC()
	history, err := loadThroughputHistory(ctx, client, org, []string{retired}, nil, 4, today)
	if err != nil {
		t.Fatal(err)
	}
	// The measured day that was not computed again (1 item), the day that
	// came back (7), the measured day of 0.
	if want := []int{1, 7, 0}; !reflect.DeepEqual(history, want) {
		t.Errorf("history = %v, want %v", history, want)
	}
	// The mean WIP over those three days: 3, 2 and 0.
	_, averageWIP, err := loadWorkItemOverlay(ctx, client, org, []string{retired}, nil, 4, today)
	if err != nil {
		t.Fatal(err)
	}
	if want := 5.0 / 3.0; math.Abs(averageWIP-want) > 1e-9 {
		t.Errorf("mean WIP = %v, want %v", averageWIP, want)
	}
}

// TestARetractionOverOneScopeTakesNothingFromAnotherScopeOfTheDayInTheForecast holds the place of the rule against a roll-up: the rule is of the
// key the writer writes (provider, work scope, team, day), and the reader
// sums the keys of a day AFTER it. One team has two work scopes on one day.
// Both are measured; then a newer retraction row is stored over one scope.
// The day stays, with the numbers of the measured scope: a retraction over
// one key takes nothing from another key of the same day.
func TestARetractionOverOneScopeTakesNothingFromAnotherScopeOfTheDayInTheForecast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org, team = "c0c0c0c0-0000-4000-8000-0000000090b1", "jira:TWO"
	day := store.Days[1]
	measure := func(scope string, completed, wip uint32) {
		t.Helper()
		if err := store.Conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at)
VALUES (?, ?, 'jira', ?, ?, 'Two', 1, ?, ?, ?)`, org, day, scope, team, completed, wip, store.OldComputedAt); err != nil {
			t.Fatal(err)
		}
	}
	measure("SCOPE-A", 3, 2)
	measure("SCOPE-B", 5, 4)
	if err := store.Conn.Exec(ctx, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, computed_at)
VALUES (?, ?, 'jira', 'SCOPE-A', ?, ?)`, org, day, team, store.NewComputedAt); err != nil {
		t.Fatal(err)
	}

	today := time.Now().UTC()
	history, err := loadThroughputHistory(ctx, client, org, []string{team}, nil, 4, today)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{5}; !reflect.DeepEqual(history, want) {
		t.Errorf("history = %v, want %v: the day with the measured scope", history, want)
	}
	_, averageWIP, err := loadWorkItemOverlay(ctx, client, org, []string{team}, nil, 4, today)
	if err != nil {
		t.Fatal(err)
	}
	if averageWIP != 4 {
		t.Errorf("mean WIP = %v, want 4: the WIP of the measured scope", averageWIP)
	}
}
