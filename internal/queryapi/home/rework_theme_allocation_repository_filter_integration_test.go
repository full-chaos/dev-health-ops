//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

// CHAOS-9195 (D5932, D5933) with CHAOS-9150 (D5900): the Home tile "Investment by theme" is the
// COUNT over the canonical themes of the units (work_unit_investments; CHAOS-9622, D5999: one unit counts 1
// whatever its lines), narrowed by the scope
// like every other repository-keyed read: a team scope that also names a repository serves the
// REPOSITORY's number; a repository the team does not hold, or one that does not resolve,
// serves nothing (D5841); filters AND and never widen (D5844).
//
//	team-alpha holds repository A (a unit of 5 lines) and B (100 lines) by ownership; repository C (7 lines)
//	is held by nobody. All three units are 100% quality.
func TestInvestmentByThemeFollowsTheRepositoryFilterUnderATeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-9195-by-theme-filter"
	const (
		repoA = "11111111-1111-4111-8111-11111111aaaa"
		repoB = "22222222-2222-4222-8222-22222222bbbb"
		repoC = "33333333-3333-4333-8333-33333333cccc"
	)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for repo, name := range map[string]string{repoA: "acme/a", repoB: "acme/b", repoC: "acme/c"} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repo, name, org, synced, synced)
	}
	for repo, effort := range map[string]float64{repoA: 5, repoB: 100, repoC: 7} {
		seedThemeUnit(t, ctx, admin, org, "wu-"+repo, repo, day.Add(12*time.Hour), effort, map[string]float64{"quality.bugfix": 1}, "ok", 0)
	}
	for repo, name := range map[string]string{repoA: "acme/a", repoB: "acme/b"} {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-alpha', ?, ?, 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
			org, repo, name, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	}
	quality := func(f Filters) string {
		t.Helper()
		f.Time = TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))}
		resp, err := BuildResponse(ctx, client, nil, org, f, day.AddDate(0, 0, 3).Add(12*time.Hour))
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		for _, row := range resp.ReworkThemeAllocation {
			if row.Theme == "quality" {
				return fmt.Sprint(math.Round(row.Allocation))
			}
		}
		return "none"
	}
	team := ScopeFilter{Level: "team", IDs: []string{"team-alpha"}}
	for _, tc := range []struct {
		name string
		f    Filters
		want string
	}{
		{"the team alone: the repositories it holds", Filters{Scope: team}, "2"},
		{"team and a repository it holds: the repository's number", Filters{Scope: team, What: WhatFilter{Repos: []string{repoA}}}, "1"},
		{"team and that repository by name", Filters{Scope: team, What: WhatFilter{Repos: []string{"acme/a"}}}, "1"},
		{"team and a repository it does not hold: nothing", Filters{Scope: team, What: WhatFilter{Repos: []string{repoC}}}, "none"},
		{"team and a repository that resolves to nothing: nothing", Filters{Scope: team, What: WhatFilter{Repos: []string{"acme/missing"}}}, "none"},
		{"an organization scope and a repository: the repository's number", Filters{What: WhatFilter{Repos: []string{repoC}}}, "1"},
		{"a repo-level scope", Filters{Scope: ScopeFilter{Level: "repo", IDs: []string{repoB}}}, "1"},
		{"the organization: every unit", Filters{}, "3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := quality(tc.f); got != tc.want {
				t.Errorf("quality count = %s, want %s", got, tc.want)
			}
		})
	}
}
