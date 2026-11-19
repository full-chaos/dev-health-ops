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

// The failure_recurrence_score series of a group is its recurrent failed names over its distinct failed names, not the
// mean of the repo-day scores. A repo-day without the counts (a row stored before they existed) is in neither sum; a
// group whose distinct failed names sum to 0 has no point, never a 0.
func TestFailureRecurrenceSeriesIsPooledOverDistinctFailedNames(t *testing.T) {
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
		org   = "org-14171"
		repoA = "00000000-0000-4000-8000-000000014171"
		repoB = "00000000-0000-4000-8000-000000014172"
	)
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	day2, day3, day4 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2), day1.AddDate(0, 0, 3)
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
	// A nil count is a row stored before the counts existed.
	row := func(repo string, day time.Time, score *float64, recurrent, distinct *uint32) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at,
     flake_cases, distinct_cases, recurrent_failed_names, distinct_failed_names)
VALUES (toUUID(?), ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, ?, 'team-a', ?, ?, 0, 1, ?, ?)`,
			repo, day, score, org, computed, recurrent, distinct); err != nil {
			t.Fatal(err)
		}
	}
	f := func(v float64) *float64 { return &v }
	u := func(v uint32) *uint32 { return &v }
	// day1: 1 of 1 and 0 of 50 recurrent -> 1/51 (the mean of the scores is 0.5).
	row(repoA, day1, f(1), u(1), u(1))
	row(repoB, day1, f(0), u(0), u(50))
	// day2: repoA is a legacy row (score, NULL counts): changes nothing. repoB 2 of 4 -> 0.5.
	row(repoA, day2, f(1), nil, nil)
	row(repoB, day2, f(0.5), u(2), u(4))
	// day3: only a legacy row: no point, never the stored score.
	row(repoA, day3, f(0.25), nil, nil)
	// day4: only a day with no failed name (0 of 0): no point, never 0.
	row(repoA, day4, f(0), u(0), u(0))

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(run)
	want := map[string]float64{day1.Format(time.DateOnly): 1.0 / 51, day2.Format(time.DateOnly): 0.5}
	for _, scope := range []string{ScopeGlobal, ScopeTeam} {
		series, err := loader.FetchMetricSeriesByScope(ctx, "failure_recurrence_score", day1, day4, scope)
		if err != nil {
			t.Fatalf("%s: %v", scope, err)
		}
		if len(series) != 1 {
			t.Fatalf("%s: %d series, want 1: %+v", scope, len(series), series)
		}
		for key, points := range series {
			if len(points) != len(want) {
				t.Errorf("%s %s: %+v, want %d points (none on %s legacy-only, %s no failed name)", scope, key, points,
					len(want), day3.Format(time.DateOnly), day4.Format(time.DateOnly))
			}
			for _, point := range points {
				if expected, ok := want[point.Day.Format(time.DateOnly)]; !ok || math.Abs(point.Value-expected) > 1e-9 {
					t.Errorf("%s %s: %s = %v, want %v", scope, key, point.Day.Format(time.DateOnly), point.Value, expected)
				}
			}
		}
	}
}
