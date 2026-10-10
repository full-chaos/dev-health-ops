//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// CHAOS-9150, D5900: rework by theme under a team scope that also names a repository
// serves the REPOSITORY's number; a repository the team does not hold, or one that
// does not resolve, serves nothing (D5841); the team alone serves the team's number
// as before; an organization scope with a named repository narrows too (D5844).
//
//	team-alpha holds repository A (allocation 5) and repository B (100) by ownership;
//	repository C (7) is held by nobody. All three rows carry team_id team-alpha, and
//	the team's own number reads that column (112), as the reference does.
func TestReworkThemeAllocationFollowsTheRepositoryFilterUnderATeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-9150-rework-theme"
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
	for repo, value := range map[string]uint32{repoA: 5, repoB: 100, repoC: 7} {
		exec(`INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
VALUES (?, ?, 'team-alpha', 'quality', 'general', ?, ?, 0, 0, 0, ?, ?)`, repo, day, value, value, synced, org)
	}
	for repo, name := range map[string]string{repoA: "acme/a", repoB: "acme/b"} {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-alpha', ?, ?, 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
			org, repo, name, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	}
	allocation := func(f Filters) string {
		t.Helper()
		f.Time = TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))}
		resp, err := BuildResponse(ctx, client, nil, org, f, day.AddDate(0, 0, 3).Add(12*time.Hour))
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		for _, row := range resp.ReworkThemeAllocation {
			if row.Theme == "quality" {
				return fmt.Sprint(row.Allocation)
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
		{"the team alone: the team's number (its team_id rows), as before", Filters{Scope: team}, "112"},
		{"team and a repository it holds: the repository's number", Filters{Scope: team, What: WhatFilter{Repos: []string{repoA}}}, "5"},
		{"team and that repository by name", Filters{Scope: team, What: WhatFilter{Repos: []string{"acme/a"}}}, "5"},
		{"team and a repository it does not hold: nothing", Filters{Scope: team, What: WhatFilter{Repos: []string{repoC}}}, "none"},
		{"team and a repository that resolves to nothing: nothing", Filters{Scope: team, What: WhatFilter{Repos: []string{"acme/missing"}}}, "none"},
		{"an organization scope and a repository: the repository's number", Filters{What: WhatFilter{Repos: []string{repoC}}}, "7"},
		{"a repo-level scope, as before", Filters{Scope: ScopeFilter{Level: "repo", IDs: []string{repoB}}}, "100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := allocation(tc.f); got != tc.want {
				t.Errorf("quality allocation = %s, want %s", got, tc.want)
			}
		})
	}
}
