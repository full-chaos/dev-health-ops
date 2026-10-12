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
// each person once with the person's WHOLE value (ruling D5986): the person's
// user_metrics_daily rows of the 30 days, never a value rebuilt from the
// landscape's team rows, which hold the person's work in each OWNING team's
// repositories (CHAOS-9395) and so differ from team to team.
//
// The store holds, on the migrated schema, for one day:
//
//   - person-a: user rows in two repositories (whole churn 100) and landscape
//     points in team-1 (churn 60) and team-2 (churn 40), on two maps;
//   - person-b: one user row (whole churn 10), an older generation of it with
//     other values, a row 30 days before the day (outside the window), and
//     points in team-1;
//   - a retraction point (a retired team id, every measure 0) for person-c;
//   - a row and a point of another organization.
//
// Each raw metric is read org-wide (as a scorecard, by day, by repository, and
// grouped by "identity", which the reader answers as one total), with a team
// filter, and grouped by team (the team values, not the whole values).
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
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	userRow := func(orgID, repoID, identity, rowDay string, loc, delivery uint32, cycle float64, wip uint32, computedAt string) {
		t.Helper()
		exec(`INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, loc_touched, delivery_units, cycle_p50_hours, work_items_active, computed_at, org_id)
VALUES (toUUID(?), ?, ?, ?, 'unassigned', 'Unassigned', ?, ?, ?, ?, ?, ?)`,
			repoID, rowDay, identity, identity, loc, delivery, cycle, wip, computedAt, orgID)
	}
	var chartOf func(metric, chartType, groupBy string, teams, repos []string) map[string]float64
	const newest, older = "2026-01-02 10:00:00", "2026-01-02 09:00:00"
	repoOne, repoTwo := "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"
	userRow(org, repoOne, "person-a", day, 60, 5, 18, 3, newest)
	userRow(org, repoTwo, "person-a", day, 40, 3, 22, 4, newest)
	userRow(org, repoOne, "person-b", day, 999, 999, 999, 999, older)
	userRow(org, repoOne, "person-b", day, 10, 2, 4, 2, newest)
	userRow(org, repoOne, "person-b", "2025-12-02", 999, 999, 999, 999, newest)
	userRow("org-other", repoOne, "person-a", day, 7777, 7777, 7777, 7777, newest)
	// The whole values of the two people over the 30 days ending on the day.
	whole := map[string]map[string]float64{
		"person-a": {"churn_loc_30d": 100, "delivery_units_30d": 8, "cycle_p50_30d_hours": 20, "wip_max_30d": 4},
		"person-b": {"churn_loc_30d": 10, "delivery_units_30d": 2, "cycle_p50_30d_hours": 4, "wip_max_30d": 2},
	}

	point := func(orgID, identity, team, mapName string, churn, delivery uint64, cycle float64, wip uint32, xNorm, yNorm float64, computedAt string) {
		t.Helper()
		x := math.Log1p(float64(churn))
		if mapName == "cycle_throughput" {
			x = math.Log1p(cycle)
		}
		exec(`
INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
     churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, computed_at, org_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			repo, day, identity, team, mapName, x, float64(delivery), xNorm, yNorm,
			churn, uint32(delivery), cycle, wip, computedAt, orgID)
	}
	maps := []string{"churn_throughput", "cycle_throughput"}
	for _, mapName := range maps {
		point(org, "person-a", "team-1", mapName, 60, 5, 18, 3, 0.9, 0.8, newest)
		point(org, "person-a", "team-2", mapName, 40, 3, 22, 4, 0.4, 0.3, newest)
		point(org, "person-b", "team-1", mapName, 10, 2, 4, 2, 0.25, 0.25, newest)
	}
	point(org, "person-c", "team-retired", "churn_throughput", 0, 0, 0, 0, 0, 0, newest)
	point("org-other", "person-a", "team-1", "churn_throughput", 7777, 7777, 7777, 7777, 0.5, 0.5, newest)

	chart := func(metric, chartType, groupBy string, teams []string) map[string]float64 {
		t.Helper()
		return chartOf(metric, chartType, groupBy, teams, nil)
	}
	chartOf = func(metric, chartType, groupBy string, teams, repos []string) map[string]float64 {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: chartType, Metric: metric, GroupBy: groupBy,
					FilterTeams: teams, FilterRepos: repos, TimeRangeStart: day, TimeRangeEnd: day, OrganizationID: org,
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
		for _, dataPoint := range result.Charts[0].DataPoints {
			points[dataPoint.X] = dataPoint.Y
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

	// The x axis of a map from the whole values (icfinalize.LandscapeAxes).
	xOf := func(person, mapName string) float64 {
		if mapName == "churn_throughput" {
			return math.Log1p(whole[person]["churn_loc_30d"])
		}
		return math.Log1p(whole[person]["cycle_p50_30d_hours"])
	}
	wholeAverage := func(metric string, people ...string) float64 {
		var total float64
		var count int
		for _, person := range people {
			for _, mapName := range maps {
				switch metric {
				case "x_raw":
					total += xOf(person, mapName)
				case "y_raw":
					total += whole[person]["delivery_units_30d"]
				default:
					total += whole[person][metric]
				}
				count++
			}
		}
		return total / float64(count)
	}
	inRepoOne := map[string]map[string]float64{
		"person-a": {"churn_loc_30d": 60, "delivery_units_30d": 5, "cycle_p50_30d_hours": 18, "wip_max_30d": 3},
		"person-b": whole["person-b"],
	}
	// repoAverage is wholeAverage over given per-person values.
	repoAverage := func(metric string, values map[string]map[string]float64) float64 {
		var total float64
		var count int
		for _, person := range values {
			for _, mapName := range maps {
				switch metric {
				case "x_raw":
					if mapName == "churn_throughput" {
						total += math.Log1p(person["churn_loc_30d"])
					} else {
						total += math.Log1p(person["cycle_p50_30d_hours"])
					}
				case "y_raw":
					total += person["delivery_units_30d"]
				default:
					total += person[metric]
				}
				count++
			}
		}
		return total / float64(count)
	}
	// The team rows of each metric, one per (person, map).
	teamRows := map[string]map[string][]float64{
		"team-1": {"churn_loc_30d": {60, 10}, "delivery_units_30d": {5, 2}, "cycle_p50_30d_hours": {18, 4}, "wip_max_30d": {3, 2},
			"x_raw": {math.Log1p(60), math.Log1p(18), math.Log1p(10), math.Log1p(4)}, "y_raw": {5, 2}},
		"team-2": {"churn_loc_30d": {40}, "delivery_units_30d": {3}, "cycle_p50_30d_hours": {22}, "wip_max_30d": {4},
			"x_raw": {math.Log1p(40), math.Log1p(22)}, "y_raw": {3}},
	}
	mean := func(values []float64) float64 {
		var total float64
		for _, value := range values {
			total += value
		}
		return total / float64(len(values))
	}

	for _, metric := range []string{"churn_loc_30d", "delivery_units_30d", "cycle_p50_30d_hours", "wip_max_30d", "x_raw", "y_raw"} {
		t.Run(metric, func(t *testing.T) {
			both := wholeAverage(metric, "person-a", "person-b")
			oneValuePerPersonMax := mean([]float64{teamRows["team-1"][metric][0], teamRows["team-1"][metric][len(teamRows["team-1"][metric])-1]})
			if same(both, oneValuePerPersonMax) {
				t.Fatalf("the seed does not tell whole values from team rows for %s", metric)
			}
			one(t, "org-wide scorecard", chart(metric, "scorecard", "", nil), "total", both)
			one(t, "org-wide by day", chart(metric, "line", "day", nil), day, both)
			one(t, "org-wide by repository", chart(metric, "bar", "repo", nil), repo, both)
			one(t, "org-wide grouped by identity (one total)", chart(metric, "bar", "identity", nil), "total", both)
			one(t, "both teams in the filter", chart(metric, "scorecard", "", []string{"team-1", "team-2"}), "total", both)
			one(t, "team-1 in the filter", chart(metric, "scorecard", "", []string{"team-1"}), "total", both)
			one(t, "team-2 in the filter (person-a only, whole)", chart(metric, "scorecard", "", []string{"team-2"}), "total", wholeAverage(metric, "person-a"))
			// A landscape point holds no repository: a repository filter takes
			// each person's user rows of that repository.
			one(t, "repository one in the filter", chartOf(metric, "scorecard", "", nil, []string{repoOne}), "total",
				repoAverage(metric, map[string]map[string]float64{"person-a": inRepoOne["person-a"], "person-b": inRepoOne["person-b"]}))
			one(t, "repository two in the filter (person-a only)", chartOf(metric, "scorecard", "", nil, []string{repoTwo}), "total",
				repoAverage(metric, map[string]map[string]float64{"person-a": {"churn_loc_30d": 40, "delivery_units_30d": 3, "cycle_p50_30d_hours": 22, "wip_max_30d": 4}}))

			byTeam := chart(metric, "bar", "team", nil)
			if len(byTeam) != 2 || !same(byTeam["team-1"], mean(teamRows["team-1"][metric])) || !same(byTeam["team-2"], mean(teamRows["team-2"][metric])) {
				t.Fatalf("grouped by team: %v, want the TEAM values team-1 = %v and team-2 = %v and no retired team",
					byTeam, mean(teamRows["team-1"][metric]), mean(teamRows["team-2"][metric]))
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
