//go:build integration

package drilldown

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// CHAOS-9104 (D5844, D5841, D5855): the PR drilldown narrows by a team AND by
// named repositories; it never widens, and a named repository that resolves to
// nothing leaves no pull request. Real ClickHouse, the real builder. Team T owns
// A (github) and B (gitlab); C (github) is nobody's.
func TestPullRequestDrilldownNarrowsByTeamAndNamedRepositories(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	const org = "org-scope-narrows"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repos := map[string]struct {
		id       uuid.UUID
		provider string
		owned    bool
	}{
		"acme/a": {uuid.New(), "github", true},
		"acme/b": {uuid.New(), "gitlab", true},
		"acme/c": {uuid.New(), "github", false},
	}
	number := uint32(0)
	title := map[string]string{}
	for name, r := range repos {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			r.id, name, r.provider, org, at, at); err != nil {
			t.Fatal(err)
		}
		if r.owned {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
VALUES (?, ?, 'team-t', ?, ?, 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`, org, r.provider, r.id, name, at, at); err != nil {
				t.Fatal(err)
			}
		}
		number++
		title[name] = "pr of " + name
		if err := admin.Exec(ctx, `INSERT INTO git_pull_requests (org_id, repo_id, number, title, author_email, created_at, last_synced)
VALUES (?, ?, ?, ?, 'dev@example.com', ?, now64(3))`, org, r.id, number, title[name], time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	read := func(p PRParams) string {
		t.Helper()
		p.StartDay, p.EndDay, p.Limit = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), 50
		resp, err := BuildPRsResponse(ctx, reader, org, p)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, item := range resp.Items {
			if item.Title != nil {
				got = append(got, *item.Title)
			}
		}
		sort.Strings(got)
		return strings.Join(got, ", ")
	}
	teamScope := func(what ...string) PRParams {
		return PRParams{ScopeLevel: "team", ScopeIDs: []string{"team-t"}, WhatRepos: what}
	}
	for _, tc := range []struct {
		name string
		p    PRParams
		want string
	}{
		{"no filter", PRParams{}, "pr of acme/a, pr of acme/b, pr of acme/c"},
		{"team alone", teamScope(), "pr of acme/a, pr of acme/b"},
		{"team and an owned repository: that repository only", teamScope("acme/a"), "pr of acme/a"},
		{"team and an owned gitlab repository", teamScope("acme/b"), "pr of acme/b"},
		{"team and a repository it does not own: nothing", teamScope("acme/c"), ""},
		{"team and an unresolved repository: nothing", teamScope("acme/nothing"), ""},
		{"an unresolved repository alone: nothing", PRParams{WhatRepos: []string{"acme/nothing"}}, ""},
		{"a repository scope that resolves", PRParams{ScopeLevel: "repo", ScopeIDs: []string{"acme/c"}}, "pr of acme/c"},
		{"empty strings name no repository: the team's", teamScope("", ""), "pr of acme/a, pr of acme/b"},
		{"empty strings name no repository: no filter", PRParams{WhatRepos: []string{""}}, "pr of acme/a, pr of acme/b, pr of acme/c"},
	} {
		if got := read(tc.p); got != tc.want {
			t.Errorf("%s: pull requests = [%s], want [%s]", tc.name, got, tc.want)
		}
	}
}
