//go:build integration

package coveragebaselines

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8541 on a migrated ClickHouse: the value of a day is the mean over the
// repositories that hold a value that day; the baseline is the mean of the day
// values; a day with no value is not a day; the seven-day rule; the newest
// version of a day; another org's rows; the scope by repository and by team
// ownership. The numbers are chosen so that the mean of the repository
// baselines (the sum the web must not make) is a different number.
func TestRealClickHouse_CoverageScopeBaseline(t *testing.T) {
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
		org, other = "org-8541", "org-other"
		repoA      = "85410000-0000-4000-8000-00000000000a"
		repoB      = "85410000-0000-4000-8000-00000000000b"
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

	// Repository A: a line value of 80 on each of the 30 days before the end day; a branch value of 40 on 3 of them.
	for back := 1; back <= 30; back++ {
		var branch any
		if back <= 3 {
			branch = 40.0
		}
		coverage(org, repoA, end.AddDate(0, 0, -back), 80.0, branch, newer)
		coverage(other, repoA, end.AddDate(0, 0, -back), 10.0, 10.0, newer) // another org, same keys
	}
	coverage(org, repoA, end.AddDate(0, 0, -4), 0.0, nil, older)                // an older version of a day: the newer 80 wins
	coverage(org, repoA, end.AddDate(0, 0, -5), nil, nil, newer.Add(time.Hour)) // a newer NULL on a day B has a value: the day is B's 50
	coverage(org, repoA, end.AddDate(0, 0, -9), nil, nil, newer.Add(time.Hour)) // a newer NULL on a day B has no row: the day has no value
	coverage(org, repoA, end, 0.0, 0.0, newer)                                  // the end day itself: not in the window
	coverage(org, repoA, end.AddDate(0, 0, -31), 0.0, 0.0, newer)               // one day before the window
	// Repository B: a line value of 50 and a branch value of 60 on six days only.
	for back := 1; back <= 6; back++ {
		coverage(org, repoB, end.AddDate(0, 0, -back), 50.0, 60.0, newer)
	}
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ownership := func(provider, team, repo, name string) {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			org, provider, team, repo, name, "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, nil, validFrom)
	}
	ownership("github", "team-alpha", repoA, "acme/alpha")
	ownership("gitlab", "team-beta", repoB, "acme/beta")

	near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) < 1e-9 }

	// The whole org. Line: days 1, 2, 3, 4, 6 are (80+50)/2 = 65; day 5 is 50
	// (A's newest version is NULL); day 9 has no value; the 23 other days are 80.
	// (5*65 + 50 + 23*80) / 29 = 2215 / 29. The mean of the repository baselines
	// would be 80 (B has six days: no baseline) or 65 (both means): neither is it.
	// Branch: days 1, 2, 3 are (40+60)/2 = 50; days 4, 5, 6 are 60: six days, so
	// no baseline.
	whole, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if !near(whole.LineBaselinePct, 2215.0/29.0) || whole.LineDays != 29 {
		t.Errorf("whole org line = %v over %d days, want %v over 29", show(whole.LineBaselinePct), whole.LineDays, 2215.0/29.0)
	}
	if whole.BranchBaselinePct != nil || whole.BranchDays != 6 {
		t.Errorf("whole org branch = %v over %d days, want no baseline over 6 days", show(whole.BranchBaselinePct), whole.BranchDays)
	}

	// Both repositories named (one by id, one by name) is the whole org.
	named, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{repoA, "acme/beta"}})
	if err != nil {
		t.Fatal(err)
	}
	if !near(named.LineBaselinePct, 2215.0/29.0) || named.LineDays != 29 || named.BranchBaselinePct != nil || named.BranchDays != 6 {
		t.Errorf("both repositories named = %+v, want the whole org's answer", named)
	}

	// One repository as the scope is that repository's own baseline: the same
	// value the per-repository read gives.
	alphaByTeam, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{TeamIDs: []string{"team-alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	alphaByName, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{"acme/alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	perRepo, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{"acme/alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(perRepo) != 1 || !near(perRepo[0].LineBaselinePct, 80) || perRepo[0].LineDays != 28 {
		t.Fatalf("per-repository read of A = %+v, want 80 over 28 days: nothing to compare with", perRepo)
	}
	for label, got := range map[string]*float64{"team that owns A": alphaByTeam.LineBaselinePct, "A by name": alphaByName.LineBaselinePct} {
		if !near(got, 80) {
			t.Errorf("%s: line = %v, want 80 (A's own baseline)", label, show(got))
		}
	}
	if alphaByTeam.LineDays != 28 || alphaByName.LineDays != 28 || alphaByTeam.BranchBaselinePct != nil || alphaByTeam.BranchDays != 3 {
		t.Errorf("A as the scope: by team %+v, by name %+v, want 28 line days and no branch baseline over 3 days", alphaByTeam, alphaByName)
	}

	// The team that owns B only: six days, so no baseline; the days are served.
	beta, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{TeamIDs: []string{"team-beta"}})
	if err != nil {
		t.Fatal(err)
	}
	if beta.LineBaselinePct != nil || beta.LineDays != 6 || beta.BranchBaselinePct != nil || beta.BranchDays != 6 {
		t.Errorf("team that owns B = %+v, want no baseline over 6 days", beta)
	}

	// Both arguments: the repositories named AND owned by the teams. A is not owned by team-beta.
	neither, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{"acme/alpha"}, TeamIDs: []string{"team-beta"}})
	if err != nil {
		t.Fatal(err)
	}
	if neither == nil || neither.LineBaselinePct != nil || neither.LineDays != 0 {
		t.Errorf("A named and team-beta: %+v, want no baseline over 0 days", neither)
	}

	none, err := ResolveScope(ctx, client, "org-with-no-rows", graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if none == nil || none.LineBaselinePct != nil || none.BranchBaselinePct != nil || none.LineDays != 0 || none.BranchDays != 0 {
		t.Fatalf("an org with no rows: %+v, want no baseline over 0 days", none)
	}
}
