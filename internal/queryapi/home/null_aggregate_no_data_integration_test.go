//go:build integration

package home

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// A window whose stored rows hold no value of a metric has no value: Home
// serves "no data", never a measured 0, and the day is not a point of the
// spark line. One case per kind of aggregate that can be undefined over
// stored rows, with rows from the real daily writers:
//
//	mean of a nullable column   review_latency (no reviewed pull request)
//	                            cycle_time     (no completed item)
//	ratio of summed counts      pr_rework_ratio (no reviewed pull request)
//
// A sum over the same rows is a stored 0 and stays a measured 0 (churn,
// throughput): missing and zero are different answers.
func TestHomeMetricOverRowsWithNoValueIsNoDataNotAMeasuredZero(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-null-aggregate"
	empty := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	valued := empty.AddDate(0, 0, 1)
	end := valued.AddDate(0, 0, 1)
	computedAt := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	repo := uuid.New()
	hours := func(v float64) *float64 { return &v }

	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	// Day one: a commit, no reviewed and no merged pull request, no line
	// touched. Day two: values for every metric.
	if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{
		{RepoID: repo, Day: empty, CommitsCount: 1, ComputedAt: computedAt},
		{RepoID: repo, Day: valued, CommitsCount: 1, TotalLOCTouched: 40, PRsMerged: 4, PRReworkRatio: 0.25,
			PRRework: &prrework.Counts{Merged: 4, Reviewed: 4, Rework: 1}, PRFirstReviewP50Hours: hours(3), ComputedAt: computedAt},
	}}, orgID); err != nil {
		t.Fatal(err)
	}
	workItems := func(day time.Time, row workitemmetrics.MetricsDailyRow) {
		t.Helper()
		row.Day, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName = day, "jira", "scope-1", "team-1", "Team One"
		if _, err := daily.WriteWorkItemMetricsDaily(ctx, admin, orgID, day, []workitemmetrics.MetricsDailyRow{row}, computedAt); err != nil {
			t.Fatal(err)
		}
	}
	// Day one: work in progress, nothing completed. Day two: 2 completed.
	workItems(empty, workitemmetrics.MetricsDailyRow{WIPCountEndOfDay: 3})
	workItems(valued, workitemmetrics.MetricsDailyRow{ItemsCompleted: 2, CycleTimeP50Hours: hours(48)})

	delta := func(metric string, start, stop time.Time) MetricDelta {
		t.Helper()
		for _, spec := range metrics {
			if spec.Metric != metric {
				continue
			}
			got, err := computeMetricDelta(ctx, client, spec, start, stop, start.AddDate(0, 0, -7), start, Filters{Scope: ScopeFilter{Level: "org"}}, orgID, teamScopeReadAsOf)
			if err != nil {
				t.Fatalf("%s: %v", metric, err)
			}
			return got
		}
		t.Fatalf("no Home metric %q", metric)
		return MetricDelta{}
	}

	for _, tc := range []struct {
		metric string
		// valuedValue is the metric's value in the window of both days.
		valuedValue float64
		// undefined: the metric has no value on day one. Otherwise day one is
		// a stored 0.
		undefined bool
	}{
		{"review_latency", 3, true},
		{"cycle_time", 2, true},       // 48 hours in days
		{"pr_rework_ratio", 25, true}, // percent
		{"churn", 40, false},
		{"throughput", 2, false},
	} {
		t.Run(tc.metric, func(t *testing.T) {
			dayOne := delta(tc.metric, empty, valued)
			if tc.undefined {
				if dayOne.HasData || dayOne.Value != 0 || len(dayOne.Spark) != 0 {
					t.Errorf("day one: value %v hasData %v spark %+v; want no data and no spark point", dayOne.Value, dayOne.HasData, dayOne.Spark)
				}
			} else if !dayOne.HasData || dayOne.Value != 0 || len(dayOne.Spark) != 1 || dayOne.Spark[0].Value != 0 {
				t.Errorf("day one: value %v hasData %v spark %+v; want a measured 0 with its spark point", dayOne.Value, dayOne.HasData, dayOne.Spark)
			}
			if dayOne.RateState != nil {
				t.Errorf("day one: rateState %s on a metric that has no state", *dayOne.RateState)
			}

			both := delta(tc.metric, empty, end)
			wantPoints := 2
			if tc.undefined {
				// The undefined day is left out of the mean and of the line.
				wantPoints = 1
			}
			if !both.HasData || !closeTo(both.Value, tc.valuedValue) || len(both.Spark) != wantPoints {
				t.Errorf("both days: value %v hasData %v spark %+v; want %v with data and %d spark point(s)", both.Value, both.HasData, both.Spark, tc.valuedValue, wantPoints)
			}
			// The comparison window of day two is day one.
			dayTwo := delta(tc.metric, valued, end)
			if dayTwo.HasPriorData == tc.undefined {
				t.Errorf("day two: hasPriorData %v; want %v (day one %s)", dayTwo.HasPriorData, !tc.undefined, map[bool]string{true: "has no value", false: "is a stored 0"}[tc.undefined])
			}
		})
	}
}
