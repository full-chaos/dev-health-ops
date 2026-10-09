//go:build integration

package analytics

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The PR_REWORK_RATIO measure of a timeseries bucket is the ratio of the
// bucket's summed counts over REVIEWED pull requests, and NULL for a bucket
// with none. Rows computed and written by the daily job's own compute and
// writer on the migrated schema, read by the compiled timeseries:
//
//	repo N  2026-01-05: 3 merged, no review data
//	repo R  2026-01-05: 4 merged: 2 reviewed (1 with changes requested), 2 with no review data
//	                    (an older version of the day says 4 reviewed, 4 with changes requested)
//	        2026-01-06: 8 merged: 3 reviewed (no changes requested), 5 with no review data
//	other organization, repo R's id, 2026-01-05: 10 reviewed, 10 with changes requested
func TestPRReworkRatioTimeseries_SeededRealClickHouse_ReviewedPullRequestsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "chaos-rework-timeseries"
	repoN, repoR := uuid.New(), uuid.New()
	day1 := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) // a Monday
	day2 := day1.AddDate(0, 0, 1)
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
	repeat := func(pullRequest prreworktest.PullRequest, count int) []prreworktest.PullRequest {
		out := make([]prreworktest.PullRequest, count)
		for i := range out {
			out[i] = pullRequest
		}
		return out
	}
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day1, older, repeat(rework, 4))
	prreworktest.WriteDay(ctx, t, conn, org, repoN, "github", day1, newer, repeat(unreviewed, 3))
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day1, newer, append(append(repeat(reviewed, 1), repeat(rework, 1)...), repeat(unreviewed, 2)...))
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day2, newer, append(repeat(reviewed, 3), repeat(unreviewed, 5)...))
	prreworktest.WriteDay(ctx, t, conn, org+"-OTHER-ORG", repoR, "github", day1, newer, repeat(rework, 10))

	run := func(interval BucketInterval) map[string]map[string]*float64 {
		t.Helper()
		q, err := CompileTimeseries(TimeseriesRequest{
			Dimension: DimensionRepo, Measure: MeasurePRReworkRatio, Interval: interval,
			StartDate: mustGraphQLDate("2026-01-05"), EndDate: mustGraphQLDate("2026-01-06"),
		}, org, queryTimeoutSecs, false, nil)
		if err != nil {
			t.Fatalf("CompileTimeseries: %v", err)
		}
		results, err := ExecuteTimeseries(ctx, client, q, "REPO", string(MeasurePRReworkRatio))
		if err != nil {
			t.Fatalf("ExecuteTimeseries: %v", err)
		}
		out := map[string]map[string]*float64{}
		for _, result := range results {
			out[result.DimensionValue] = map[string]*float64{}
			for _, bucket := range result.Buckets {
				out[result.DimensionValue][bucket.Date.Time().Format("2006-01-02")] = bucket.Value
			}
		}
		return out
	}
	check := func(name string, got *float64, present bool, want *float64) {
		t.Helper()
		switch {
		case !present:
			t.Errorf("%s: no bucket", name)
		case want == nil && got != nil:
			t.Errorf("%s = %v, want no value: no pull request of the bucket has review data", name, *got)
		case want != nil && got == nil:
			t.Errorf("%s has no value, want %v", name, *want)
		case want != nil && math.Abs(*got-*want) > 1e-12:
			t.Errorf("%s = %v, want %v", name, *got, *want)
		}
	}
	value := func(v float64) *float64 { return &v }

	byDay := run(BucketIntervalDay)
	got, present := byDay[repoR.String()]["2026-01-05"]
	check("repo R, 2026-01-05 (1 of 2 reviewed, not 1 of 4 merged)", got, present, value(0.5))
	got, present = byDay[repoR.String()]["2026-01-06"]
	check("repo R, 2026-01-06 (a measured 0)", got, present, value(0))
	got, present = byDay[repoN.String()]["2026-01-05"]
	check("repo N, 2026-01-05 (no review data)", got, present, nil)

	// One week bucket of the two days: 1 of 5 reviewed pull requests, not the
	// 0.25 mean of the day values and not 1 of the 12 merged.
	byWeek := run(BucketIntervalWeek)
	got, present = byWeek[repoR.String()]["2026-01-05"]
	check("repo R, the week", got, present, value(0.2))
	got, present = byWeek[repoN.String()]["2026-01-05"]
	check("repo N, the week", got, present, nil)
}
