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

// The pass_rate series of a group is its passed cases over its passed and failed cases, not the mean of the repo-day
// rates: a repo-day weighs by its executed cases. A repo-day with none (all skipped, stored pass_rate 0) is in neither
// sum, and a day with none in the group has no point, never a 0. The counts are UInt32 and not Nullable.
func TestTestPassRateSeriesIsPooledOverExecutedCases(t *testing.T) {
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
		org   = "org-14121"
		repoA = "00000000-0000-4000-8000-000000014111"
		repoB = "00000000-0000-4000-8000-000000014112"
		repoC = "00000000-0000-4000-8000-000000014113"
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
	row := func(repo string, day time.Time, passed, failed, skipped uint32, storedRate float64, computedAt time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at)
VALUES (toUUID(?), ?, ?, ?, ?, ?, 0, ?, 0, 0, 0, 1, 0, 'team-a', ?, ?)`,
			repo, day, passed+failed+skipped, passed, failed, skipped, storedRate, org, computedAt); err != nil {
			t.Fatal(err)
		}
	}
	// day1: 9 passed of 10 executed and 1 of 2 -> 10/12 (the mean of the rates is 0.7).
	row(repoA, day1, 9, 1, 0, 0.9, older)
	row(repoB, day1, 1, 1, 0, 0.5, older)
	// day2: repoA skipped 3 cases only (stored rate 0): in neither sum. repoB 4 of 4 -> 1.
	row(repoA, day2, 0, 0, 3, 0, older)
	row(repoB, day2, 4, 0, 0, 1, older)
	// day3: only skipped cases in the group: no point, never 0.
	row(repoA, day3, 0, 0, 2, 0, older)
	row(repoB, day3, 0, 0, 5, 0, older)
	// day4: repoC's measurement is replaced by a newer row of zeros: it counts in neither sum.
	row(repoC, day4, 5, 5, 0, 0.5, older)
	row(repoC, day4, 0, 0, 0, 0, newer)
	row(repoA, day4, 3, 1, 0, 0.75, older)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(run)
	want := map[string]float64{
		day1.Format(time.DateOnly): 10.0 / 12,
		day2.Format(time.DateOnly): 1,
		day4.Format(time.DateOnly): 0.75,
	}
	for _, scope := range []string{ScopeGlobal, ScopeTeam} {
		series, err := loader.FetchMetricSeriesByScope(ctx, "pass_rate", day1, day4, scope)
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
}
