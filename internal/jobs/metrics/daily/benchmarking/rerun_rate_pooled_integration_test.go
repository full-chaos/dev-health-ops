//go:build integration

package benchmarking

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The rerun_rate series of a group is its reruns over its runs, not the mean of the stored repo-day rates: a repo-day
// weighs by its pipelines_count. A repo-day with no run (pipelines_count 0, also a retraction row of zeros) is in
// neither sum, and a day with no run in the group has no point. rerun_rate is not Nullable (chmigrate baseline
// head.json), so there is no NULL row to leave out.
func TestRerunRateSeriesIsRerunsOverRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	const (
		org   = "org-14118"
		repoA = "00000000-0000-4000-8000-000000014101"
		repoB = "00000000-0000-4000-8000-000000014102"
		repoC = "00000000-0000-4000-8000-000000014103"
	)
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	day2, day3, day4 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2), day1.AddDate(0, 0, 3)
	claimed := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	run := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)

	if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES ('team-a', generateUUIDv4(), 'team-a', ?, 'linear', 1, ?, ?)`, org, claimed, claimed); err != nil {
		t.Fatal(err)
	}
	for name, repo := range map[string]string{"acme/a": repoA, "acme/b": repoB, "acme/c": repoC} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, last_synced, org_id) VALUES (toUUID(?), ?, 'github', ?, ?)`,
			repo, name, claimed, org); err != nil {
			t.Fatal(err)
		}
		if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
    source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at)
VALUES (?, 'github', 'team-a', toUUID(?), ?, 'exact', 'inferred', 1, 10, 0, 1, ?, NULL, ?)`,
			org, repo, name, claimed, claimed); err != nil {
			t.Fatal(err)
		}
	}
	// reruns is the count behind the stored rate: rerun_rate = reruns / runs, unrounded.
	row := func(repo string, day time.Time, runs, reruns int, computedAt time.Time) {
		t.Helper()
		rate := 0.0
		if runs > 0 {
			rate = float64(reruns) / float64(runs)
		}
		if err := admin.Exec(ctx, `INSERT INTO testops_pipeline_metrics_daily
    (repo_id, day, pipelines_count, success_count, failure_count, cancelled_count, success_rate, failure_rate,
     cancel_rate, rerun_rate, team_id, org_id, computed_at)
VALUES (toUUID(?), ?, ?, ?, 0, 0, 1, 0, 0, ?, 'team-a', ?, ?)`,
			repo, day, uint32(runs), uint32(runs), rate, org, computedAt); err != nil {
			t.Fatal(err)
		}
	}
	// day1: 1/8 and 1/7 -> 2/15 (the mean of the rates is 0.1339).
	row(repoA, day1, 8, 1, older)
	row(repoB, day1, 7, 1, older)
	// day2: 1 run with 1 rerun against 99 runs with none -> 1/100 (the mean of the rates is 0.5).
	row(repoA, day2, 1, 1, older)
	row(repoB, day2, 99, 0, older)
	// day3: no run in the group.
	row(repoA, day3, 0, 0, older)
	row(repoB, day3, 0, 0, older)
	// day4: repoC's measurement is replaced by a newer row of zeros (a retraction): it counts in neither sum.
	row(repoC, day4, 10, 5, older)
	row(repoC, day4, 0, 0, newer)
	row(repoA, day4, 4, 1, older)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(run)
	want := map[string]float64{
		day1.Format(time.DateOnly): 2.0 / 15,
		day2.Format(time.DateOnly): 1.0 / 100,
		day4.Format(time.DateOnly): 0.25,
	}
	for _, scope := range []string{ScopeGlobal, ScopeTeam} {
		series, err := loader.FetchMetricSeriesByScope(ctx, "rerun_rate", day1, day4, scope)
		if err != nil {
			t.Fatalf("%s: %v", scope, err)
		}
		if len(series) != 1 {
			t.Fatalf("%s: %d series, want 1: %+v", scope, len(series), series)
		}
		for key, points := range series {
			if len(points) != len(want) {
				t.Errorf("%s %s: %+v, want %d points (no point on %s)", scope, key, points, len(want), day3.Format(time.DateOnly))
			}
			for _, point := range points {
				if expected, ok := want[point.Day.Format(time.DateOnly)]; !ok || math.Abs(point.Value-expected) > 1e-9 {
					t.Errorf("%s %s: %s = %v, want %v", scope, key, point.Day.Format(time.DateOnly), point.Value, expected)
				}
			}
		}
	}
	repoSeries, err := loader.FetchMetricSeriesByScope(ctx, "rerun_rate", day1, day1, ScopeRepo)
	if err != nil {
		t.Fatal(err)
	}
	if points := repoSeries[repoA]; len(points) != 1 || math.Abs(points[0].Value-0.125) > 1e-9 {
		t.Errorf("repo series of %s: %+v, want 1/8", repoA, repoSeries[repoA])
	}
}
