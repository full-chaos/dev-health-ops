//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A report chart of the pull request rework ratio is the shared window rule
// over the counts of each bucket: reviewed pull requests only, the number Home
// gives for the same window. It is not the average of stored one-day values,
// and never the deprecated repo_metrics_daily.pr_rework_ratio column, which
// holds 0 where nothing was reviewed. Rows computed and written by the daily
// job's own compute and writer on the migrated schema:
//
//	repo N  day 1: 3 merged, no review data
//	        day 3: 3 merged, no review data
//	repo R  day 1: 4 merged: 2 reviewed (1 with changes requested), 2 with no review data
//	               (an older version of the day says 4 reviewed, 4 with changes requested)
//	        day 2: 8 merged: 3 reviewed (no changes requested), 5 with no review data
//	other organization, repo R's id, day 1: 10 reviewed, 10 with changes requested
func TestPRReworkRatioChartIsTheWindowRuleOverReviewedPullRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const org = "org-1"
	repoN, repoR := uuid.New(), uuid.New()
	day1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day2, day3 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2)
	older := day1.Add(80 * time.Hour)
	newer := older.Add(time.Hour)
	// Two versions of a day are stored below. A background merge would keep
	// only the newest one and hide a reader that does not select it.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES repo_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	unreviewed := prreworktest.PullRequest{}
	reviewed := prreworktest.PullRequest{Reviews: 2}
	rework := prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1}
	group := func(counts ...any) []prreworktest.PullRequest {
		var out []prreworktest.PullRequest
		for i := 0; i < len(counts); i += 2 {
			for n := 0; n < counts[i+1].(int); n++ {
				out = append(out, counts[i].(prreworktest.PullRequest))
			}
		}
		return out
	}
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day1, older, group(rework, 4))
	prreworktest.WriteDay(ctx, t, conn, org, repoN, "github", day1, newer, group(unreviewed, 3))
	prreworktest.WriteDay(ctx, t, conn, org, repoN, "github", day3, newer, group(unreviewed, 3))
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day1, newer, group(reviewed, 1, rework, 1, unreviewed, 2))
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day2, newer, group(reviewed, 3, unreviewed, 5))
	prreworktest.WriteDay(ctx, t, conn, "org-2", repoR, "github", day1, newer, group(rework, 10))

	chart := func(id, chartType, groupBy, start, end string) ChartSpec {
		return ChartSpec{
			ChartID: id, PlanID: "plan-1", ChartType: chartType, Metric: "pr_rework_ratio", GroupBy: groupBy,
			TimeRangeStart: start, TimeRangeEnd: end, OrganizationID: org,
		}
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
			Charts: []ChartSpec{
				chart("by-day", "line", "day", "2026-01-01", "2026-01-03"),
				chart("total", "scorecard", "", "2026-01-01", "2026-01-03"),
				chart("by-repo", "bar", "repo", "2026-01-01", "2026-01-03"),
				chart("day-three", "scorecard", "", "2026-01-03", "2026-01-03"),
			},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	points := map[string]map[string]float64{}
	for _, chart := range result.Charts {
		points[chart.Spec.ChartID] = map[string]float64{}
		for _, point := range chart.DataPoints {
			points[chart.Spec.ChartID][point.X] = point.Y
		}
	}
	want := map[string]map[string]float64{
		// Day 1: 1 of R's 2 reviewed pull requests (N has none reviewed, and
		// the older version of R's day is not read). Day 2: a measured 0. Day 3
		// has merged pull requests and no review data: no point, not a 0.
		"by-day": {"2026-01-01": 0.5, "2026-01-02": 0},
		// 1 of 5 reviewed pull requests in the three days: not the 0.25 mean
		// of the two day values, and not 1 of the 18 merged.
		"total": {"total": 0.2},
		// R: 1 of 5. N has no reviewed pull request: no point.
		"by-repo": {repoR.String(): 0.2},
		// A window of day 3 alone is unknown: the scorecard has no value.
		"day-three": {},
	}
	if len(result.Charts) != len(want) {
		t.Fatalf("%d charts, want %d", len(result.Charts), len(want))
	}
	for id, wantPoints := range want {
		got := points[id]
		if len(got) != len(wantPoints) {
			t.Errorf("chart %s points = %v, want %v", id, got, wantPoints)
			continue
		}
		for x, y := range wantPoints {
			if diff := got[x] - y; diff > 1e-12 || diff < -1e-12 {
				t.Errorf("chart %s point %s = %v, want %v (all points %v)", id, x, got[x], y, got)
			}
		}
	}
}
