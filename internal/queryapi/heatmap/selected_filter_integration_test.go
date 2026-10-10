//go:build integration

// CHAOS-9159: the heatmap takes selected teams and repositories beside its scope.
// Filters AND and never widen (D5844); a named repository that resolves to nothing
// leaves no cell (D5841); the reason of an empty answer is served.
package heatmap

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

const (
	selRepoOwned  = "00000000-0000-4000-8000-0000000000b1" // owned by team-alpha
	selRepoOther  = "00000000-0000-4000-8000-0000000000b2" // owned by nobody
	selRepoAbsent = "00000000-0000-4000-8000-0000000000bf" // not a repository of the org
)

func TestHeatmapSelectedRepositoriesAndTeamsNarrowAndSayWhy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	const org = "sel-org"
	at := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	for id, name := range map[string]string{selRepoOwned: "acme/owned", selRepoOther: "acme/other"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, name, org, synced, synced)
		// owned: 2 commits, other: 3 commits
		commits := 2
		if id == selRepoOther {
			commits = 3
		}
		for i := 0; i < commits; i++ {
			crossorg.Exec(ctx, t, admin, `INSERT INTO git_commits (org_id, repo_id, hash, message, author_name, author_email, author_when, committer_when, last_synced) VALUES (?, ?, ?, 'm', 'Dev', 'dev@x', ?, ?, now64(3))`,
				org, id, fmt.Sprintf("%s-%d", id, i), at, at)
		}
	}
	crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-alpha', ?, 'acme/owned', 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
		org, selRepoOwned, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	start, end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	base := Params{Type: "context_switch", Metric: "repo_touchpoints", StartDate: &start, EndDate: &end}
	text := func(v *string) string {
		if v == nil {
			return "null"
		}
		return *v
	}
	flag := func(v *bool) string {
		if v == nil {
			return "null"
		}
		return fmt.Sprint(*v)
	}
	for _, tc := range []struct {
		name        string
		mutate      func(*Params)
		wantCommits float64
		wantApplied string
		wantReason  string
	}{
		{"nothing selected: both repositories", func(*Params) {}, 5, "null", "null"},
		{"one repository", func(p *Params) { p.RepoRefs = []string{selRepoOwned} }, 2, "true", "null"},
		{"one repository by name", func(p *Params) { p.RepoRefs = []string{"acme/other"} }, 3, "true", "null"},
		{"the team alone: its repositories", func(p *Params) { p.TeamIDs = []string{"team-alpha"} }, 2, "null", "null"},
		{"team scope_id, as before", func(p *Params) { p.ScopeType, p.ScopeID = "team", "team-alpha" }, 2, "null", "null"},
		{"repo scope_id, as before", func(p *Params) { p.ScopeType, p.ScopeID = "repo", selRepoOther }, 3, "true", "null"},
		{"team scope_id AND repo_ids", func(p *Params) { p.ScopeType, p.ScopeID, p.RepoRefs = "team", "team-alpha", []string{selRepoOwned} }, 2, "true", "null"},
		{"team AND a repository it does not hold: nothing, and why", func(p *Params) {
			p.TeamIDs, p.RepoRefs = []string{"team-alpha"}, []string{selRepoOther}
		}, 0, "true", "repository_not_in_team"},
		{"repo scope_id of another repository AND the team: nothing", func(p *Params) {
			p.ScopeType, p.ScopeID, p.TeamIDs = "repo", selRepoOther, []string{"team-alpha"}
		}, 0, "true", "repository_not_in_team"},
		{"a repository that is not found: nothing, and why", func(p *Params) { p.RepoRefs = []string{selRepoAbsent} }, 0, "true", "repository_not_found"},
		{"an empty name is no repository", func(p *Params) { p.RepoRefs = []string{""} }, 5, "null", "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := base
			tc.mutate(&params)
			resp, err := BuildResponse(ctx, client, org, params)
			if err != nil {
				t.Fatal(err)
			}
			total := 0.0
			for _, cell := range resp.Cells {
				total += cell.Value
			}
			if total != tc.wantCommits {
				t.Errorf("commits = %v, want %v", total, tc.wantCommits)
			}
			if flag(resp.RepoFilterApplied) != tc.wantApplied || text(resp.FilterEmptyReason) != tc.wantReason {
				t.Errorf("repo_filter_applied = %s, filter_empty_reason = %s; want %s, %s", flag(resp.RepoFilterApplied), text(resp.FilterEmptyReason), tc.wantApplied, tc.wantReason)
			}
		})
	}
}
