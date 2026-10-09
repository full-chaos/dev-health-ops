//go:build integration

package quadrant

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

type pullRequestSeed struct {
	number     int
	createdAt  time.Time
	mergedAt   *time.Time
	lastSynced time.Time
}

// writePullRequests writes rows with the provider-sync writer's own INSERT
// statement and stored-version contract, the path a real sync takes into
// git_pull_requests.
func writePullRequests(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, repoID string, seeds []pullRequestSeed) {
	t.Helper()
	specs := providersync.StoredVersionSpecs()["git_pull_requests"]
	if len(specs) != 1 {
		t.Fatalf("provider sync has %d git_pull_requests writers, want 1", len(specs))
	}
	spec := specs[0]
	payload := map[string]any{"reviews_lookup": true}
	rows := make([]storedversion.Row, 0, len(seeds))
	for _, seed := range seeds {
		var mergedAt any
		state := "open"
		if seed.mergedAt != nil {
			mergedAt, state = *seed.mergedAt, "merged"
		}
		rows = append(rows, storedversion.Row{
			Values: []any{
				repoID, seed.number, nil, nil, state, "dev", nil, seed.createdAt, mergedAt,
				mergedAt, nil, nil, 1, 1, 1, nil, nil, 0, 0, 0, seed.lastSynced, nil, orgID,
			},
			Carry: spec.Carry(payload),
		})
	}
	if _, err := spec.Contract.Apply(ctx, admin, orgID, spec.Insert, rows); err != nil {
		t.Fatalf("stored-version contract: %v", err)
	}
	batch, err := admin.PrepareBatch(ctx, spec.Insert)
	if err != nil {
		t.Fatalf("prepare pull request batch: %v", err)
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		if err := batch.Append(row.Values...); err != nil {
			t.Fatalf("append pull request: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send pull requests: %v", err)
	}
}

// runDailyRepoMetrics runs the production daily repo/user family for one
// org and day: load the raw rows, compute, write repo_metrics_daily.
func runDailyRepoMetrics(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, repoID string, day time.Time) {
	t.Helper()
	loader, err := repouser.NewClickHouseLoader(admin)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	repo := uuid.MustParse(repoID)
	commits, prs, reviews, err := loader.LoadGitRows(ctx, orgID, []uuid.UUID{repo}, day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("load git rows: %v", err)
	}
	result := repouser.Compute(day, commits, prs, reviews, time.Now().UTC(),
		repouser.DefaultNormalizeIdentity, 1000, nil, nil, nil, nil, nil)
	if _, _, _, err := writer.WriteResult(ctx, result, orgID); err != nil {
		t.Fatalf("write daily repo metrics: %v", err)
	}
}

func at(day string, hour int) time.Time {
	parsed, err := time.Parse("2006-01-02", day)
	if err != nil {
		panic(err)
	}
	return parsed.Add(time.Duration(hour) * time.Hour)
}

func mergedHoursAfter(created time.Time, hours int) *time.Time {
	merged := created.Add(time.Duration(hours) * time.Hour)
	return &merged
}

// TestRepoCycleTimeWeekIsTheMedianOverEveryPullRequestMergedThatWeek pins
// the weekly repo cycle time to one weight per pull request. Week of Sunday
// 2026-09-13: five pull requests open since early August merge on Monday
// (912..1008 h), one merges on each of Tuesday..Friday (2, 4, 6, 8 h). The
// median over the nine pull requests is 912 h; the mean of the five daily
// medians repo_metrics_daily holds (960, 2, 4, 6, 8) is 196 h.
func TestRepoCycleTimeWeekIsTheMedianOverEveryPullRequestMergedThatWeek(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)

	synced := at("2026-10-05", 0)
	bulkMerge := at("2026-09-14", 10)
	var orgA []pullRequestSeed
	for i, created := range []string{"2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07"} {
		merged := bulkMerge
		orgA = append(orgA, pullRequestSeed{number: 100 + i, createdAt: at(created, 10), mergedAt: &merged, lastSynced: synced})
	}
	for i, day := range []string{"2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"} {
		created := at(day, 1)
		orgA = append(orgA, pullRequestSeed{number: 200 + i, createdAt: created, mergedAt: mergedHoursAfter(created, 2*(i+1)), lastSynced: synced})
	}
	// Week of 2026-09-20 has no merge in org A. Week of 2026-09-27 has one.
	weekThree := at("2026-09-28", 1)
	weekThreeMerge := weekThree.Add(10*time.Hour + 30*time.Minute)
	orgA = append(orgA,
		pullRequestSeed{number: 300, createdAt: weekThree, mergedAt: &weekThreeMerge, lastSynced: synced},
		pullRequestSeed{number: 301, createdAt: at("2026-09-21", 1), lastSynced: synced},
		// Opened inside both windows, merged after both: in neither.
		pullRequestSeed{number: 302, createdAt: at("2026-09-30", 1), mergedAt: mergedHoursAfter(at("2026-09-30", 1), 120), lastSynced: synced},
	)
	writePullRequests(ctx, t, admin, f.OrgA, f.RepoID, orgA)
	// A second sync of the Tuesday pull request: still one pull request.
	resynced := orgA[5]
	resynced.lastSynced = synced.Add(time.Hour)
	writePullRequests(ctx, t, admin, f.OrgA, f.RepoID, []pullRequestSeed{resynced})

	// Org B shares the repository id and merges slow pull requests in the
	// same week and in org A's empty week.
	var orgB []pullRequestSeed
	for i, day := range []string{"2026-09-15", "2026-09-16", "2026-09-16", "2026-09-22"} {
		created := at(day, 1).Add(-5000 * time.Hour)
		orgB = append(orgB, pullRequestSeed{number: 400 + i, createdAt: created, mergedAt: mergedHoursAfter(created, 5000), lastSynced: synced})
	}
	writePullRequests(ctx, t, admin, f.OrgB, f.RepoID, orgB)

	for day := at("2026-09-13", 0); day.Before(at("2026-10-04", 0)); day = day.Add(24 * time.Hour) {
		runDailyRepoMetrics(ctx, t, admin, f.OrgA, f.RepoID, day)
		runDailyRepoMetrics(ctx, t, admin, f.OrgB, f.RepoID, day)
	}
	var bulkDayMedian float64
	if err := admin.QueryRow(ctx, `
        SELECT median_pr_cycle_hours FROM repo_metrics_daily FINAL
        WHERE org_id = ? AND repo_id = ? AND day = '2026-09-14'`, f.OrgA, f.RepoID).Scan(&bulkDayMedian); err != nil {
		t.Fatalf("read bulk day repo_metrics_daily row: %v", err)
	}
	if bulkDayMedian != 960 {
		t.Fatalf("bulk day median_pr_cycle_hours = %v, want 960 (the old pull requests are real inputs)", bulkDayMedian)
	}

	spec := RepoMetrics["cycle_time"]
	weeks, err := fetchQuadrantMetric(ctx, client, spec, at("2026-09-13", 0), at("2026-10-04", 0), "week", f.OrgA, "")
	if err != nil {
		t.Fatalf("fetchQuadrantMetric week: %v", err)
	}
	want := []struct {
		bucket string
		hours  float64
	}{{"2026-09-13", 912}, {"2026-09-27", 10.5}}
	if len(weeks) != len(want) {
		t.Fatalf("week rows = %+v; want %d rows (the empty week of 2026-09-20 has no row, not 0)", weeks, len(want))
	}
	for i, w := range want {
		got := weeks[i]
		if got.Bucket.Format("2006-01-02") != w.bucket || got.EntityLabel != f.RepoName || got.Value != w.hours {
			t.Fatalf("week row %d = %+v; want bucket %s, %s, %v h", i, got, w.bucket, f.RepoName, w.hours)
		}
	}

	// September: ten pull requests, so the median is the mean of the two
	// middle values, (10.5 + 912) / 2.
	months, err := fetchQuadrantMetric(ctx, client, spec, at("2026-09-01", 0), at("2026-10-01", 0), "month", f.OrgA, "")
	if err != nil {
		t.Fatalf("fetchQuadrantMetric month: %v", err)
	}
	if len(months) != 1 || months[0].Bucket.Format("2006-01-02") != "2026-09-01" || months[0].Value != 461.25 {
		t.Fatalf("month rows = %+v; want one 2026-09-01 row of 461.25 h", months)
	}
}
