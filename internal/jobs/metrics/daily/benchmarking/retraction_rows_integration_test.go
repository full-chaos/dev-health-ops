//go:build integration

package benchmarking

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestMetricSeriesGiveRetractionRowsNoWeight reads the series the benchmarking
// job computes from work_item_metrics_daily, by team and for the whole
// organization, for two organizations that hold the same measurements. One of
// them also holds the old rows of the retired team ids and the retraction row
// over each (package retractionseed). The series must be the same: a
// retraction row holds 0 in defect_intro_rate, which is not a measured rate,
// and a retired team id has no series of the days computed again.
func TestMetricSeriesGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)

	// Inclusive day window: the days computed again.
	start, end := store.Days[1], store.Days[len(store.Days)-1]
	read := func(org string) map[string]map[string][]MetricPoint {
		t.Helper()
		loader, err := NewClickHouseLoader(store.Conn, org)
		if err != nil {
			t.Fatal(err)
		}
		answers := map[string]map[string][]MetricPoint{}
		for _, metric := range []string{"cycle_time_hours", "defect_intro_rate"} {
			for _, scope := range []string{ScopeTeam, ScopeGlobal} {
				series, err := loader.FetchMetricSeriesByScope(ctx, metric, start, end, scope)
				if err != nil {
					t.Fatalf("%s %s %s: %v", org, metric, scope, err)
				}
				answers[metric+"/"+scope] = series
			}
		}
		return answers
	}

	control := read(retractionseed.ControlOrg)

	// The control series, from the seed: four teams, six days. The defect
	// intro rate of the teams is 0.125, 0.25, 0.375 and 0.5 on every day, so
	// the organization mean is 0.3125. The cycle time is 8, 16, 24 and 32
	// hours, mean 20.
	wantTeams := map[string]float64{"jira:ENG": 0.125, "github:platform": 0.25, "gitlab:ops": 0.375, "linear:core": 0.5}
	teamSeries := control["defect_intro_rate/"+ScopeTeam]
	if len(teamSeries) != len(wantTeams) {
		t.Fatalf("control defect_intro_rate team series = %v, want the four keyed teams", teamSeries)
	}
	for team, want := range wantTeams {
		points := teamSeries[team]
		if len(points) != 6 {
			t.Fatalf("control series of %s = %+v, want six days", team, points)
		}
		for _, point := range points {
			if point.Value != want {
				t.Fatalf("control series of %s holds %v, want %v", team, point.Value, want)
			}
		}
	}
	for key, want := range map[string]float64{"defect_intro_rate/" + ScopeGlobal: 0.3125, "cycle_time_hours/" + ScopeGlobal: 20} {
		points := control[key]["global"]
		if len(points) != 6 {
			t.Fatalf("control %s = %+v, want six days", key, control[key])
		}
		for _, point := range points {
			if point.Value != want {
				t.Fatalf("control %s holds %v, want %v", key, point.Value, want)
			}
		}
	}

	retracted := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed a series:\n control   %+v\n retracted %+v", control, retracted)
	}
}
