//go:build integration

package report

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// teamKeyedTables are the source tables of metric_registry.json that hold a
// team id in their key and take retraction rows. The list is written out
// here, and not read from the reader's own rule, so the test does not follow
// the code it holds.
var teamKeyedTables = map[string]bool{
	"work_item_metrics_daily":         true,
	"work_item_state_durations_daily": true,
	"team_metrics_daily":              true,
	"investment_metrics_daily":        true,
	"issue_type_metrics_daily":        true,
}

// textColumns are the registry entries of those tables that name a text
// column. A text column has no mean, so no chart of it can be read.
var textColumns = map[string]bool{"investment_area": true, "issue_type_norm": true, "project_stream": true}

// TestReportChartsGiveRetractionRowsNoWeight charts EVERY averaged metric of
// metric_registry.json that reads a team-keyed daily table, as a
// scorecard, by team and by day, for two organizations that hold the same
// measurements. One of them also holds the old rows of the retired team ids
// and the retraction row over each (package retractionseed). The charts must
// be the same: most of these metrics are charted as a mean, where each
// retraction row is one more sample of 0, and a chart by team must not show a
// retired team id as a team of the days computed again.
func TestReportChartsGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)

	first, last := store.Days[1].Format("2006-01-02"), store.Days[len(store.Days)-1].Format("2006-01-02")
	var metricNames []string
	for name, definition := range supportedMetrics {
		// A metric that buildChartQuery sums is left out: a retraction row adds
		// 0 to a sum, and the sum of an integer column is not a value the
		// adapter can scan from a real ClickHouse (it expects a float).
		summed := strings.HasSuffix(name, "_count") || definition.Unit == "count"
		if teamKeyedTables[definition.SourceTable] && !summed && !textColumns[name] {
			metricNames = append(metricNames, name)
		}
	}
	sort.Strings(metricNames)
	if len(metricNames) < 15 {
		t.Fatalf("only %d registry metrics read a team-keyed daily table: %v", len(metricNames), metricNames)
	}
	shapes := []struct{ chartType, groupBy string }{{"scorecard", ""}, {"bar", "team"}, {"line", "day"}}

	read := func(org string) map[string][]DataPoint {
		t.Helper()
		answers := map[string][]DataPoint{}
		for _, metric := range metricNames {
			for _, shape := range shapes {
				// One chart for each report: a chart that fails names itself.
				chart := ChartSpec{
					ChartID: metric + "/" + shape.chartType, PlanID: "plan-1", ChartType: shape.chartType,
					Metric: metric, GroupBy: shape.groupBy, TimeRangeStart: first, TimeRangeEnd: last,
					OrganizationID: org,
				}
				loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
					return ReportDefinition{
						Plan:   Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
						Charts: []ChartSpec{chart},
					}, nil
				})
				adapter, err := NewClickHouseQueryAdapter(loader, store.Conn)
				if err != nil {
					t.Fatal(err)
				}
				result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
				if err != nil {
					t.Fatalf("%s chart %s: %v", org, chart.ChartID, err)
				}
				if len(result.Charts) != 1 {
					t.Fatalf("%s chart %s: %d results", org, chart.ChartID, len(result.Charts))
				}
				points := append([]DataPoint(nil), result.Charts[0].DataPoints...)
				sort.Slice(points, func(i, j int) bool { return points[i].X < points[j].X })
				answers[chart.ChartID] = points
			}
		}
		return answers
	}

	control := read(retractionseed.ControlOrg)
	if len(control) != len(metricNames)*len(shapes) {
		t.Fatalf("control gave %d charts, want %d", len(control), len(metricNames)*len(shapes))
	}

	// The control charts, from the seed (four teams, six days).
	// wip_congestion_ratio: the mean of 0.5, 0.75, 1.0 and 1.25.
	// after_hours_commit_ratio: each team's own ratio (2, 4, 6 and 8 of 8
	// commits), then the mean across the teams.
	for chart, want := range map[string]float64{
		"wip_congestion_ratio/scorecard":     0.875,
		"after_hours_commit_ratio/scorecard": 0.625,
	} {
		points := control[chart]
		if len(points) != 1 || points[0].Y != want {
			t.Fatalf("control %s = %+v, want one point of %v", chart, points, want)
		}
	}
	keyed := []string{"github:platform", "gitlab:ops", "jira:ENG", "linear:core"}
	for _, chart := range []string{"wip_congestion_ratio/bar", "after_hours_commit_ratio/bar", "churn_loc/bar", "lead_p50_hours/bar"} {
		var teams []string
		for _, point := range control[chart] {
			teams = append(teams, point.X)
		}
		if !reflect.DeepEqual(teams, keyed) {
			t.Fatalf("control %s teams = %v, want %v", chart, teams, keyed)
		}
	}

	retracted := read(retractionseed.RetractedOrg)
	for _, metric := range metricNames {
		for _, shape := range shapes {
			chart := metric + "/" + shape.chartType
			if !reflect.DeepEqual(control[chart], retracted[chart]) {
				t.Errorf("the retraction rows changed chart %s:\n control   %+v\n retracted %+v", chart, control[chart], retracted[chart])
			}
		}
	}
}
