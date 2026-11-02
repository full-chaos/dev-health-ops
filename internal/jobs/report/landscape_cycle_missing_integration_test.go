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

// A landscape chart of the cycle reads a missing cycle as missing, not as a
// cycle of 0 hours. A person's day with no merged pull request (delivery_units
// less work_items_completed is 0) holds no cycle; a person with no such day in
// the window has no 30-day cycle, and a stored point with no cycle is no sample
// of a team's mean.
//
// One day, team-1, user rows and the points the compute stores for them:
//
//   - person-a: the day, one merged pull request of 18 h; the day before, none
//     (cycle 0 stored). Whole cycle 18.
//   - person-b: the day, none; two days before, two merged, median 4 h. Whole 4.
//   - person-d: the day, one merged pull request of 0 h. Whole 0.
//   - person-e: the day, none. No cycle: a churn point and no cycle-map point.
//
// In a second organization person-e is alone.
func TestALandscapeCycleChartLeavesOutAPersonWithNoCycle(t *testing.T) {
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
		org      = "org-cycle-missing"
		orgAlone = "org-cycle-alone"
		day      = "2026-01-01"
		repo     = "00000000-0000-0000-0000-000000000000"
		repoOne  = "00000000-0000-4000-8000-000000000001"
		at       = "2026-01-02 10:00:00"
	)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	userRow := func(orgID, identity, rowDay string, merged uint32, cycle float64) {
		t.Helper()
		exec(`INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, loc_touched, prs_merged, delivery_units, cycle_p50_hours, median_pr_cycle_hours, computed_at, org_id)
VALUES (toUUID(?), ?, ?, ?, 'unassigned', 'Unassigned', 10, ?, ?, ?, ?, ?, ?)`,
			repoOne, rowDay, identity, identity, merged, merged, cycle, cycle, at, orgID)
	}
	userRow(org, "person-a", day, 1, 18)
	userRow(org, "person-a", "2025-12-31", 0, 0)
	userRow(org, "person-b", day, 0, 0)
	userRow(org, "person-b", "2025-12-30", 2, 4)
	userRow(org, "person-d", day, 1, 0)
	userRow(org, "person-e", day, 0, 0)
	userRow(orgAlone, "person-e", day, 0, 0)

	point := func(orgID, identity, mapName string, cycle float64) {
		t.Helper()
		exec(`INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
     churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, computed_at, org_id)
VALUES (?, ?, ?, 'team-1', ?, ?, 1, 0.5, 0.5, 10, 1, ?, 0, ?, ?)`,
			repo, day, identity, mapName, math.Log1p(cycle), cycle, at, orgID)
	}
	// A point with no cycle: the column is left to its default (no 30-day cycle).
	pointWithNoCycle := func(orgID, identity string) {
		t.Helper()
		exec(`INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
     churn_loc_30d, delivery_units_30d, wip_max_30d, computed_at, org_id)
VALUES (?, ?, ?, 'team-1', 'churn_throughput', ?, 0, 0.5, 0.5, 10, 0, 0, ?, ?)`,
			repo, day, identity, math.Log1p(10), at, orgID)
	}
	for person, cycle := range map[string]float64{"person-a": 18, "person-b": 4, "person-d": 0} {
		point(org, person, "churn_throughput", cycle)
		point(org, person, "cycle_throughput", cycle)
	}
	pointWithNoCycle(org, "person-e")
	pointWithNoCycle(orgAlone, "person-e")

	chart := func(orgID, chartType, groupBy string) map[string]float64 {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: orgID},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: chartType, Metric: "cycle_p50_30d_hours", GroupBy: groupBy,
					TimeRangeStart: day, TimeRangeEnd: day, OrganizationID: orgID,
				}},
			}, nil
		})
		adapter, err := NewClickHouseQueryAdapter(loader, conn)
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
		if err != nil {
			t.Fatalf("%s by %q: %v", chartType, groupBy, err)
		}
		if len(result.Charts) != 1 {
			t.Fatalf("%d chart(s)", len(result.Charts))
		}
		points := map[string]float64{}
		for _, dataPoint := range result.Charts[0].DataPoints {
			points[dataPoint.X] = dataPoint.Y
		}
		return points
	}
	one := func(t *testing.T, what string, points map[string]float64, x string, want float64) {
		t.Helper()
		got, ok := points[x]
		if len(points) != 1 || !ok || math.Abs(got-want) > 1e-9 {
			t.Errorf("%s: %v, want %q = %v", what, points, x, want)
		}
	}

	t.Run("R1 the team mean leaves out a point with no cycle and keeps a cycle of 0", func(t *testing.T) {
		// Two rows (two maps) for each of a, b and d; e's one row has no cycle.
		one(t, "grouped by team", chart(org, "bar", "team"), "team-1", (18.0+4+0)/3)
	})
	t.Run("R2 a person's whole cycle reads only the days with a merged pull request", func(t *testing.T) {
		// Not grouped by team: one row per point, the person's whole value. a, b
		// and d count on two maps each; e has no whole cycle.
		one(t, "org-wide scorecard", chart(org, "scorecard", ""), "total", (18.0+4+0)/3)
	})
	t.Run("R3 a person with no merged pull request in the window has no cycle point", func(t *testing.T) {
		if points := chart(orgAlone, "scorecard", ""); len(points) != 0 {
			t.Errorf("whole-person chart of a person with no cycle = %v, want no point", points)
		}
		if points := chart(orgAlone, "bar", "team"); len(points) != 0 {
			t.Errorf("team chart of a team with no cycle = %v, want no point", points)
		}
	})
}
