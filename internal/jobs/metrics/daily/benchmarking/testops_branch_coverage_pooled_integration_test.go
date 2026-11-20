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

// The branch coverage series of a group is its covered branches over its total branches, times 100, not the mean of the
// repo-day percentages. A repo-day with a NULL count (a row stored before the counts existed) or no branches is in neither
// sum; a day with none left has no point, never 0. A retired day is not a sample.
func TestBranchCoverageSeriesIsPooledOverBranches(t *testing.T) {
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
		org   = "org-14181"
		repoA = "00000000-0000-4000-8000-000000014181"
		repoB = "00000000-0000-4000-8000-000000014182"
		repoC = "00000000-0000-4000-8000-000000014183"
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
	row := func(repo string, day time.Time, branchPct float64, total, covered *uint32, retracted uint8, computedAt time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily
    (repo_id, day, line_coverage_pct, branch_coverage_pct, branches_total, branches_covered, uncovered_files_count,
     coverage_regression_count, team_id, org_id, computed_at, retracted)
VALUES (toUUID(?), ?, 50, ?, ?, ?, 0, 0, 'team-a', ?, ?, ?)`,
			repo, day, branchPct, total, covered, org, computedAt, retracted); err != nil {
			t.Fatal(err)
		}
	}
	u := func(v uint32) *uint32 { return &v }
	// day1: 90/100 and 10/1000 -> 100/1100 (the mean of the pcts is 50).
	row(repoA, day1, 90, u(100), u(90), 0, older)
	row(repoB, day1, 1, u(1000), u(10), 0, older)
	// day2: repoA has a pct but NULL counts (a row stored before the counts existed): in neither sum. repoB 50/100.
	row(repoA, day2, 99, nil, nil, 0, older)
	row(repoB, day2, 50, u(100), u(50), 0, older)
	// day3: NULL counts and no branches only: no point, never 0.
	row(repoA, day3, 99, nil, u(5), 0, older)
	row(repoB, day3, 0, u(0), u(0), 0, older)
	// day4: repoC is retired by a newer marker row: in neither sum. repoA 8/10.
	row(repoC, day4, 100, u(1000), u(1000), 0, older)
	row(repoC, day4, 0, nil, nil, 1, newer)
	row(repoA, day4, 80, u(10), u(8), 0, older)

	loader, err := NewClickHouseLoader(admin, org)
	if err != nil {
		t.Fatal(err)
	}
	loader.SetOwnersAsOf(run)
	want := map[string]float64{
		day1.Format(time.DateOnly): 100 * 100.0 / 1100,
		day2.Format(time.DateOnly): 50,
		day4.Format(time.DateOnly): 80,
	}
	for _, scope := range []string{ScopeGlobal, ScopeTeam} {
		series, err := loader.FetchMetricSeriesByScope(ctx, "branch_coverage_pct", day1, day4, scope)
		if err != nil {
			t.Fatalf("%s: %v", scope, err)
		}
		if len(series) != 1 {
			t.Fatalf("%s: %d series, want 1: %+v", scope, len(series), series)
		}
		for _, points := range series {
			if len(points) != len(want) {
				t.Errorf("%s: %+v, want %d points (day3 has no value)", scope, points, len(want))
			}
			for _, point := range points {
				d := point.Day.Format(time.DateOnly)
				if expected, ok := want[d]; !ok || math.Abs(point.Value-expected) > 1e-9 {
					t.Errorf("%s %s: %v, want %v", scope, d, point.Value, expected)
				}
			}
		}
	}
}
