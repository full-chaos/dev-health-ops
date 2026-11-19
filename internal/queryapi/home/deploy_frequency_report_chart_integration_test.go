//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/report"
)

type deployFrequencyLoader struct{ definition report.ReportDefinition }

func (l deployFrequencyLoader) Load(context.Context, report.QueryInput) (report.ReportDefinition, error) {
	return l.definition, nil
}

// The report chart of deployments_count (the New Report form label Deploy Frequency) is the number Home gives as
// deploy_freq for the same window: the sum of the newest row of each repository day, in the organization only.
//
//	repo A  day 1: 3 deployments (an older version said 40)   day 2: 5   day 3: 2
//	repo B  day 1: 4                                           day 3: 1
//	other organization, repo A's id, day 1: 500
func TestHomeDeployFrequencyEqualsTheReportChartOfDeploymentsCount(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES deploy_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	const orgID = "home-org-deploy-freq-equals-report"
	repoA, repoB := uuid.New(), uuid.New()
	day1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day2, day3, after := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2), day1.AddDate(0, 0, 3)
	older := time.Date(2026, 9, 5, 6, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	seed := func(org string, repo uuid.UUID, day, computedAt time.Time, deployments int) {
		t.Helper()
		if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO deploy_metrics_daily (repo_id, day, deployments_count, failed_deployments_count, computed_at, org_id) VALUES ('%s', '%s', %d, 0, '%s', '%s')`,
			repo, day.Format("2006-01-02"), deployments, computedAt.Format("2006-01-02 15:04:05"), org)); err != nil {
			t.Fatal(err)
		}
	}
	seed(orgID, repoA, day1, older, 40)
	seed(orgID, repoA, day1, newer, 3)
	seed(orgID, repoA, day2, newer, 5)
	seed(orgID, repoA, day3, newer, 2)
	seed(orgID, repoB, day1, newer, 4)
	seed(orgID, repoB, day3, newer, 1)
	seed("home-org-deploy-freq-equals-report-other", repoA, day1, newer, 500)

	spec := metricSpec{}
	for _, candidate := range metrics {
		if candidate.Metric == "deploy_freq" {
			spec = candidate
		}
	}
	if spec.Table != "deploy_metrics_daily" || spec.Column != "deployments_count" {
		t.Fatalf("deploy_freq reads %s.%s, want deploy_metrics_daily.deployments_count", spec.Table, spec.Column)
	}

	for _, tc := range []struct {
		name       string
		start, end time.Time
		days       []time.Time
		want       []float64
	}{
		{"one day, two repositories", day1, day2, []time.Time{day1}, []float64{7}},
		{"three days", day1, after, []time.Time{day1, day2, day3}, []float64{7, 5, 3}},
	} {
		home, err := computeMetricDelta(ctx, client, spec, tc.start, tc.end, tc.start.AddDate(0, 0, -7), tc.start, Filters{Scope: ScopeFilter{Level: "org"}}, orgID, teamScopeReadAsOf)
		if err != nil {
			t.Fatal(err)
		}
		var total float64
		for _, v := range tc.want {
			total += v
		}
		if !home.HasData || !closeTo(home.Value, total) || len(home.Spark) != len(tc.want) {
			t.Fatalf("%s: Home value %v hasData %v spark %+v, want %v over %d days", tc.name, home.Value, home.HasData, home.Spark, total, len(tc.want))
		}

		chart := func(id, chartType, groupBy string) report.ChartSpec {
			return report.ChartSpec{
				ChartID: id, PlanID: "plan-1", ChartType: chartType, Metric: "deployments_count", GroupBy: groupBy,
				TimeRangeStart: tc.start.Format("2006-01-02"), TimeRangeEnd: tc.end.AddDate(0, 0, -1).Format("2006-01-02"), OrganizationID: orgID,
			}
		}
		adapter, err := report.NewClickHouseQueryAdapter(deployFrequencyLoader{report.ReportDefinition{
			Plan:   report.Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: orgID},
			Charts: []report.ChartSpec{chart("by-day", "line", "day"), chart("total", "scorecard", "")},
		}}, admin)
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Query(ctx, report.QueryInput{ReportID: "report-1", RunID: "run-1"})
		if err != nil {
			t.Fatal(err)
		}
		points := map[string]map[string]float64{}
		for _, c := range result.Charts {
			points[c.Spec.ChartID] = map[string]float64{}
			for _, p := range c.DataPoints {
				points[c.Spec.ChartID][p.X] = p.Y
			}
		}
		if got := points["total"]["total"]; !closeTo(got, home.Value) {
			t.Errorf("%s: report total %v, Home %v", tc.name, got, home.Value)
		}
		if len(points["by-day"]) != len(tc.days) {
			t.Errorf("%s: report by-day %v, want %d points", tc.name, points["by-day"], len(tc.days))
		}
		for i, day := range tc.days {
			if got := points["by-day"][day.Format("2006-01-02")]; !closeTo(got, tc.want[i]) || !closeTo(home.Spark[i].Value, tc.want[i]) {
				t.Errorf("%s: %s report %v, Home spark %v, want %v", tc.name, day.Format("2006-01-02"), got, home.Spark[i].Value, tc.want[i])
			}
		}
	}
}
