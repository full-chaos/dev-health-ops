//go:build integration

// CHAOS-9159: the quadrant takes selected teams and repositories beside its grain.
// Filters AND and never widen (D5844); a named repository that resolves to nothing
// leaves no point (D5841); the reason of an empty answer is served; the team grain
// SAYS that a repository cannot narrow it.
package quadrant

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

const (
	selRepoOwned  = "00000000-0000-4000-8000-0000000000b1" // owned by team-alpha
	selRepoOther  = "00000000-0000-4000-8000-0000000000b2" // owned by nobody
	selRepoAbsent = "00000000-0000-4000-8000-0000000000bf" // not a repository of the org
)

func seedSelectedFilterWorld(ctx context.Context, t *testing.T) (conn any, run func(Params) *Response) {
	t.Helper()
	admin, client := crossorg.Start(ctx, t)
	const org = "sel-org"
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	for id, name := range map[string]string{selRepoOwned: "acme/owned", selRepoOther: "acme/other"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, name, org, synced, synced)
		crossorg.Exec(ctx, t, admin, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, prs_merged, computed_at) VALUES (?, ?, ?, 100, 5, now())`, org, id, day)
	}
	crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-alpha', ?, 'acme/owned', 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
		org, selRepoOwned, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	// Team-grain rows for two teams.
	for _, team := range []string{"team-alpha", "team-beta"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES (?, generateUUIDv4(), ?, [], [], [], 1, ?, ?, ?, 'github')`,
			team, team, synced, synced, org)
		crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, loc_touched, computed_at) VALUES (?, ?, ?, ?, ?, ?, 10, now())`,
			org, selRepoOther, day, team+"@x", team+"@x", team)
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, items_completed, wip_count_end_of_day, computed_at) VALUES (?, ?, 'github', 's', ?, 3, 2, now())`, org, day, team)
	}
	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	return nil, func(p Params) *Response {
		t.Helper()
		p.Bucket, p.RangeDays, p.EndDate = "week", 30, &end
		resp, err := BuildResponse(ctx, client, org, p)
		if err != nil {
			t.Fatalf("BuildResponse(%+v): %v", p, err)
		}
		return resp
	}
}

func entityIDs(resp *Response) []string {
	var ids []string
	for _, point := range resp.Points {
		ids = append(ids, point.EntityID)
	}
	sort.Strings(ids)
	return ids
}

func boolText(v *bool) string {
	if v == nil {
		return "null"
	}
	return fmt.Sprint(*v)
}

func textOf(v *string) string {
	if v == nil {
		return "null"
	}
	return *v
}

func TestQuadrantSelectedRepositoriesAndTeamsNarrowAndSayWhy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, run := seedSelectedFilterWorld(ctx, t)
	repoGrain := Params{Type: "churn_throughput", ScopeType: "repo"}
	for _, tc := range []struct {
		name        string
		params      Params
		wantPoints  []string
		wantApplied string
		wantReason  string
	}{
		{"nothing selected: both repositories, nothing said", repoGrain, []string{"acme/other", "acme/owned"}, "null", "null"},
		{"one repository", Params{Type: "churn_throughput", ScopeType: "repo", RepoRefs: []string{selRepoOwned}}, []string{"acme/owned"}, "true", "null"},
		{"one repository by name", Params{Type: "churn_throughput", ScopeType: "repo", RepoRefs: []string{"acme/other"}}, []string{"acme/other"}, "true", "null"},
		{"the team alone: its repositories", Params{Type: "churn_throughput", ScopeType: "repo", TeamIDs: []string{"team-alpha"}}, []string{"acme/owned"}, "null", "null"},
		{"team AND its repository", Params{Type: "churn_throughput", ScopeType: "repo", TeamIDs: []string{"team-alpha"}, RepoRefs: []string{selRepoOwned}}, []string{"acme/owned"}, "true", "null"},
		{"team AND a repository it does not hold: nothing, and why", Params{Type: "churn_throughput", ScopeType: "repo", TeamIDs: []string{"team-alpha"}, RepoRefs: []string{selRepoOther}}, nil, "true", "repository_not_in_team"},
		{"a repository that is not found: nothing, and why", Params{Type: "churn_throughput", ScopeType: "repo", RepoRefs: []string{selRepoAbsent}}, nil, "true", "repository_not_found"},
		{"a repository not found beside a team: not found", Params{Type: "churn_throughput", ScopeType: "repo", TeamIDs: []string{"team-alpha"}, RepoRefs: []string{selRepoAbsent}}, nil, "true", "repository_not_found"},
		{"an empty name is no repository", Params{Type: "churn_throughput", ScopeType: "repo", RepoRefs: []string{""}}, []string{"acme/other", "acme/owned"}, "null", "null"},
		{"a repository grain forced by the churn/throughput pair, from an org scope", Params{Type: "churn_throughput", ScopeType: "org", RepoRefs: []string{selRepoOwned}}, []string{"acme/owned"}, "true", "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := run(tc.params)
			got := entityIDs(resp)
			if fmt.Sprint(got) != fmt.Sprint(tc.wantPoints) {
				t.Errorf("points = %v, want %v", got, tc.wantPoints)
			}
			if boolText(resp.RepoFilterApplied) != tc.wantApplied || textOf(resp.FilterEmptyReason) != tc.wantReason {
				t.Errorf("repo_filter_applied = %s, filter_empty_reason = %s; want %s, %s", boolText(resp.RepoFilterApplied), textOf(resp.FilterEmptyReason), tc.wantApplied, tc.wantReason)
			}
		})
	}
}

func TestQuadrantTeamGrainIsNarrowedByTeamsAndSaysARepositoryCannotNarrowIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, run := seedSelectedFilterWorld(ctx, t)
	grain := func(teams, repos []string) Params {
		return Params{Type: "wip_throughput", ScopeType: "team", TeamIDs: teams, RepoRefs: repos}
	}
	for _, tc := range []struct {
		name        string
		params      Params
		wantPoints  []string
		wantApplied string
	}{
		{"nothing selected: both teams", grain(nil, nil), []string{"team-alpha", "team-beta"}, "null"},
		{"a selected team narrows the points", grain([]string{"team-alpha"}, nil), []string{"team-alpha"}, "null"},
		{"a repository cannot narrow the team grain, and it says so", grain(nil, []string{selRepoOwned}), []string{"team-alpha", "team-beta"}, "false"},
		{"a team narrows, a repository is said not to", grain([]string{"team-beta"}, []string{selRepoOwned}), []string{"team-beta"}, "false"},
		{"an unknown repository does not empty the team grain: it is not applied", grain(nil, []string{selRepoAbsent}), []string{"team-alpha", "team-beta"}, "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := run(tc.params)
			if got := entityIDs(resp); fmt.Sprint(got) != fmt.Sprint(tc.wantPoints) {
				t.Errorf("points = %v, want %v", got, tc.wantPoints)
			}
			if boolText(resp.RepoFilterApplied) != tc.wantApplied || textOf(resp.FilterEmptyReason) != "null" {
				t.Errorf("repo_filter_applied = %s, filter_empty_reason = %s; want %s, null", boolText(resp.RepoFilterApplied), textOf(resp.FilterEmptyReason), tc.wantApplied)
			}
		})
	}
}
