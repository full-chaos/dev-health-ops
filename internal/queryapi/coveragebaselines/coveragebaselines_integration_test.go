//go:build integration

package coveragebaselines

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8111 on a migrated ClickHouse: the 30 days before the end day, the
// newest version of a day (a newer NULL is "no value"), the seven-day rule,
// another org's rows, and the team scope by ownership.
func TestRealClickHouse_CoverageBaselines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const (
		org, other = "org-8111", "org-other"
		repoA      = "81110000-0000-4000-8000-00000000000a"
		repoB      = "81110000-0000-4000-8000-00000000000b"
	)
	end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 9, 2, 2, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	coverage := func(o, repo string, day time.Time, line, branch any, computed time.Time) {
		exec(`INSERT INTO testops_coverage_metrics_daily
			(repo_id, day, line_coverage_pct, branch_coverage_pct, uncovered_files_count, coverage_regression_count, org_id, computed_at)
			VALUES (?, ?, ?, ?, 0, 0, ?, ?)`, repo, day, line, branch, o, computed)
	}
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, repoA, "acme/alpha", "github", org, older, older)
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, repoB, "acme/beta", "gitlab", org, older, older)
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, repoA, "other/alpha", "github", other, older, older)

	// Repository A: a line value of 80 on each of the 30 days before the end day; a branch value on 3 of them.
	for back := 1; back <= 30; back++ {
		var branch any
		if back <= 3 {
			branch = 40.0
		}
		coverage(org, repoA, end.AddDate(0, 0, -back), 80.0, branch, newer)
		coverage(other, repoA, end.AddDate(0, 0, -back), 10.0, 10.0, newer) // another org, same keys
	}
	coverage(org, repoA, end.AddDate(0, 0, -5), 0.0, nil, older)                // an older version of a day: the newer 80 wins
	coverage(org, repoA, end.AddDate(0, 0, -9), nil, nil, newer.Add(time.Hour)) // a newer NULL: that day has no value
	coverage(org, repoA, end, 0.0, 0.0, newer)                                  // the end day itself: not in the window
	coverage(org, repoA, end.AddDate(0, 0, -31), 0.0, 0.0, newer)               // one day before the window
	// Repository B: six days only.
	for back := 1; back <= 6; back++ {
		coverage(org, repoB, end.AddDate(0, 0, -back), 50.0, 50.0, newer)
	}
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, "gitlab", "team-beta", repoB, "acme/beta", "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, nil, validFrom)

	all, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("%d rows, want the org's two repositories: %+v", len(all), all)
	}
	a, b := all[0], all[1]
	if a.RepoID != repoA || a.RepoName == nil || *a.RepoName != "acme/alpha" {
		t.Errorf("row 0 = %s %v, want repository A with the org's own name", a.RepoID, a.RepoName)
	}
	if a.LineBaselinePct == nil || *a.LineBaselinePct != 80 || a.LineDays != 29 {
		t.Errorf("A line = %v over %d days, want 80 over 29 (30 days less the day whose newest version is NULL)", show(a.LineBaselinePct), a.LineDays)
	}
	if a.BranchBaselinePct != nil || a.BranchDays != 3 {
		t.Errorf("A branch = %v over %d days, want no baseline over 3 days", show(a.BranchBaselinePct), a.BranchDays)
	}
	if b.RepoID != repoB || b.LineBaselinePct != nil || b.LineDays != 6 || b.BranchBaselinePct != nil || b.BranchDays != 6 {
		t.Errorf("B = %+v, want no baseline over 6 days", b)
	}

	owned, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{TeamIDs: []string{"team-beta"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 1 || owned[0].RepoID != repoB {
		t.Fatalf("team scope by ownership: %+v, want repository B only", owned)
	}
	byName, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{"acme/alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(byName) != 1 || byName[0].RepoID != repoA {
		t.Fatalf("repository scope by name: %+v, want repository A only", byName)
	}
	none, err := Resolve(ctx, client, "org-with-no-rows", graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if none == nil || len(none) != 0 {
		t.Fatalf("an org with no rows: %v, want an empty list", none)
	}
}
