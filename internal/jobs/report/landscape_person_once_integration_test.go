//go:build integration

package report

import (
	"context"
	"math"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A chart of a person's own landscape value that is not grouped by team counts
// each person once, whatever the number of teams the person is in.
//
// The store holds, on the migrated schema, for one day:
//
//   - person-a in team-1 and in team-2: the same raw values in both rows, on
//     two maps; different normalized coordinates in each team;
//   - person-b in team-1 only, on the same two maps, with an older version of
//     one row that holds other values;
//   - a retraction row (a retired team id, every measure 0) for person-c;
//   - a row of another organization.
//
// Each raw metric is read org-wide (as a scorecard, by day, by repository, and
// grouped by "identity", which the reader answers as one total), with a team
// filter, and grouped by team.
func TestALandscapeAverageNotGroupedByTeamCountsEachPersonOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	}()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const (
		org  = "org-person-once"
		day  = "2026-01-01"
		repo = "00000000-0000-0000-0000-000000000000"
	)
	// The raw values of the two people; a is in two teams.
	a := map[string]float64{"churn_loc_30d": 100, "delivery_units_30d": 8, "cycle_p50_30d_hours": 20, "wip_max_30d": 4, "x_raw": 1.5, "y_raw": 2.5}
	b := map[string]float64{"churn_loc_30d": 10, "delivery_units_30d": 2, "cycle_p50_30d_hours": 4, "wip_max_30d": 2, "x_raw": 0.5, "y_raw": 0.5}
	insert := func(orgID, identity, team, mapName string, raw map[string]float64, xNorm, yNorm float64, computedAt string) {
		t.Helper()
		if err := conn.Exec(ctx, `
INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
     churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, computed_at, org_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			repo, day, identity, team, mapName, raw["x_raw"], raw["y_raw"], xNorm, yNorm,
			uint64(raw["churn_loc_30d"]), uint32(raw["delivery_units_30d"]), raw["cycle_p50_30d_hours"], uint32(raw["wip_max_30d"]),
			computedAt, orgID); err != nil {
			t.Fatal(err)
		}
	}
	const newest, older = "2026-01-02 10:00:00", "2026-01-02 09:00:00"
	for _, mapName := range []string{"churn_throughput", "cycle_throughput"} {
		insert(org, "person-a", "team-1", mapName, a, 0.9, 0.8, newest)
		insert(org, "person-a", "team-2", mapName, a, 0.4, 0.3, newest)
		insert(org, "person-b", "team-1", mapName, b, 0.25, 0.25, newest)
	}
	insert(org, "person-b", "team-1", "churn_throughput", map[string]float64{"churn_loc_30d": 999, "delivery_units_30d": 999, "cycle_p50_30d_hours": 999, "wip_max_30d": 999, "x_raw": 999, "y_raw": 999}, 0.25, 0.25, older)
	insert(org, "person-c", "team-retired", "churn_throughput", map[string]float64{}, 0, 0, newest)
	insert("org-other", "person-a", "team-1", "churn_throughput", map[string]float64{"churn_loc_30d": 7777, "delivery_units_30d": 7777, "cycle_p50_30d_hours": 7777, "wip_max_30d": 7777, "x_raw": 7777, "y_raw": 7777}, 0.5, 0.5, newest)

	chart := func(metric, chartType, groupBy string, teams []string) map[string]float64 {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: chartType, Metric: metric, GroupBy: groupBy,
					FilterTeams: teams, TimeRangeStart: day, TimeRangeEnd: day, OrganizationID: org,
				}},
			}, nil
		})
		adapter, err := NewClickHouseQueryAdapter(loader, conn)
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
		if err != nil {
			t.Fatalf("%s %s by %q teams %v: %v", metric, chartType, groupBy, teams, err)
		}
		if len(result.Charts) != 1 {
			t.Fatalf("%s: %d chart(s)", metric, len(result.Charts))
		}
		points := map[string]float64{}
		for _, point := range result.Charts[0].DataPoints {
			points[point.X] = point.Y
		}
		return points
	}
	same := func(got, want float64) bool { return math.Abs(got-want) < 1e-9 }
	one := func(t *testing.T, what string, points map[string]float64, x string, want float64) {
		t.Helper()
		if len(points) != 1 {
			t.Fatalf("%s: %d point(s) %v, want one at %q", what, len(points), points, x)
		}
		got, ok := points[x]
		if !ok || !same(got, want) {
			t.Fatalf("%s: %v, want %q = %v", what, points, x, want)
		}
	}

	for metric := range a {
		t.Run(metric, func(t *testing.T) {
			eachPersonOnce := (a[metric] + b[metric]) / 2
			perRow := (2*a[metric] + b[metric]) / 3
			if same(eachPersonOnce, perRow) {
				t.Fatalf("the seed does not tell the two readings apart for %s", metric)
			}
			one(t, "org-wide scorecard", chart(metric, "scorecard", "", nil), "total", eachPersonOnce)
			one(t, "org-wide by day", chart(metric, "line", "day", nil), day, eachPersonOnce)
			one(t, "org-wide by repository", chart(metric, "bar", "repo", nil), repo, eachPersonOnce)
			one(t, "org-wide grouped by identity (one total)", chart(metric, "bar", "identity", nil), "total", eachPersonOnce)
			one(t, "both teams in the filter", chart(metric, "scorecard", "", []string{"team-1", "team-2"}), "total", eachPersonOnce)
			one(t, "team-1 in the filter", chart(metric, "scorecard", "", []string{"team-1"}), "total", eachPersonOnce)
			one(t, "team-2 in the filter", chart(metric, "scorecard", "", []string{"team-2"}), "total", a[metric])

			byTeam := chart(metric, "bar", "team", nil)
			if len(byTeam) != 2 || !same(byTeam["team-1"], eachPersonOnce) || !same(byTeam["team-2"], a[metric]) {
				t.Fatalf("grouped by team: %v, want team-1 = %v (two people) and team-2 = %v (person-a in that team's cohort) and no retired team", byTeam, eachPersonOnce, a[metric])
			}
		})
	}

	// The normalized coordinates are a rank inside one team: a person of two
	// teams has two values, and the reader keeps one value per (person, team),
	// the spread of the person over the teams. It makes no one number of the
	// person out of them.
	t.Run("the normalized coordinates keep one sample per person and team", func(t *testing.T) {
		one(t, "x_norm org-wide", chart("x_norm", "scorecard", "", nil), "total", (0.9+0.4+0.25)/3)
		one(t, "y_norm org-wide", chart("y_norm", "scorecard", "", nil), "total", (0.8+0.3+0.25)/3)
	})
}
