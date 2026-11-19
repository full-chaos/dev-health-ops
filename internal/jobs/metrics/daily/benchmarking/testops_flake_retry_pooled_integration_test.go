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

// The flake_rate and retry_dependency_rate series of a group are its flake (or retry-dependent) cases over its distinct
// cases, not the mean of the repo-day rates. A repo-day with a NULL rate, or NULL counts (a row stored before the
// counts existed), is in neither sum; a group with no counted repo-day has no point, never a 0.
func TestFlakeAndRetryDependencySeriesArePooledOverDistinctCases(t *testing.T) {
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
		org   = "org-14122"
		repoA = "00000000-0000-4000-8000-000000014211"
		repoB = "00000000-0000-4000-8000-000000014212"
	)
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	day2, day3, day4, day5 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2), day1.AddDate(0, 0, 3), day1.AddDate(0, 0, 4)
	claimed := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	run := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 8, 5, 1, 0, 0, 0, time.UTC)

	if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES ('team-a', generateUUIDv4(), 'team-a', ?, 'linear', 1, ?, ?)`, org, claimed, claimed); err != nil {
		t.Fatal(err)
	}
	for name, repo := range map[string]string{"acme/a": repoA, "acme/b": repoB} {
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
	// A nil count is a row stored before the counts existed; a nil rate is a day that measured no case.
	row := func(repo string, day time.Time, flakeRate, retryRate *float64, flakeCases, distinct *uint32) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at,
     flake_cases, distinct_cases)
VALUES (toUUID(?), ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, 1, 0, 'team-a', ?, ?, ?, ?)`,
			repo, day, flakeRate, retryRate, org, computed, flakeCases, distinct); err != nil {
			t.Fatal(err)
		}
	}
	f := func(v float64) *float64 { return &v }
	u := func(v uint32) *uint32 { return &v }
	// day1: 1 of 10 and 2 of 2 flaky -> 3/12 (the mean of the rates is 0.55); retry 2 of 10 and 0 of 2 -> 2/12.
	row(repoA, day1, f(0.1), f(0.2), u(1), u(10))
	row(repoB, day1, f(1), f(0), u(2), u(2))
	// day2: repoA measured no case (NULL rates, 0/0): in neither sum. repoB 1 of 4 flaky, 1 of 4 retry -> 0.25.
	row(repoA, day2, nil, nil, u(0), u(0))
	row(repoB, day2, f(0.25), f(0.25), u(1), u(4))
	// day3: repoA is a legacy row (rates, NULL counts): changes nothing. repoB 1 of 5 -> 0.2.
	row(repoA, day3, f(0.5), f(0.5), nil, nil)
	row(repoB, day3, f(0.2), f(0.2), u(1), u(5))
	// day4: only a legacy row, and only an unknown-day row: no point, never 0.
	row(repoA, day4, f(0.5), f(0.5), nil, nil)
	row(repoB, day4, nil, nil, u(0), u(0))
	// day5: measured rows with 0 distinct cases only (denominator 0): no point.
	row(repoA, day5, f(0), f(0), u(0), u(0))

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(run)
	want := map[string]map[string]float64{
		"flake_rate": {
			day1.Format(time.DateOnly): 3.0 / 12, day2.Format(time.DateOnly): 0.25, day3.Format(time.DateOnly): 0.2,
		},
		"retry_dependency_rate": {
			day1.Format(time.DateOnly): 2.0 / 12, day2.Format(time.DateOnly): 0.25, day3.Format(time.DateOnly): 0.2,
		},
	}
	for metric, expectedPoints := range want {
		for _, scope := range []string{ScopeGlobal, ScopeTeam} {
			series, err := loader.FetchMetricSeriesByScope(ctx, metric, day1, day5, scope)
			if err != nil {
				t.Fatalf("%s %s: %v", metric, scope, err)
			}
			if len(series) != 1 {
				t.Fatalf("%s %s: %d series, want 1: %+v", metric, scope, len(series), series)
			}
			for key, points := range series {
				if len(points) != len(expectedPoints) {
					t.Errorf("%s %s %s: %+v, want %d points (none on %s, %s, %s)", metric, scope, key, points,
						len(expectedPoints), day4.Format(time.DateOnly), day5.Format(time.DateOnly), "legacy-only")
				}
				for _, point := range points {
					if expected, ok := expectedPoints[point.Day.Format(time.DateOnly)]; !ok || math.Abs(point.Value-expected) > 1e-9 {
						t.Errorf("%s %s %s: %s = %v, want %v", metric, scope, key, point.Day.Format(time.DateOnly), point.Value, expected)
					}
				}
			}
		}
	}
}
