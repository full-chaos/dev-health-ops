//go:build integration

package drilldown

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// CHAOS-13090 R3 (acp): the REAL ceiling SQL on a real ClickHouse. The matcher resolves allowlist keys to repository
// ids; the ceiling term (ids bound as Array(String)) is ANDed with the team-scope ownership subquery on the UUID column
// pr.repo_id. A String list against a String-yielding subquery fails analysis with code 169 on a UUID column
// (teamscope.go, CHAOS-9183), so this cell is red if the ceiling and the team scope meet in the wrong type.
// Team T owns A and B; C is nobody's; the allowlist names A and C.
func TestPullRequestDrilldownCeilingWithTeamScopeSubquery(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	const org = "org-ceiling-13090"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repos := map[string]struct {
		id    uuid.UUID
		owned bool
	}{
		"acme/a": {uuid.New(), true},
		"acme/b": {uuid.New(), true},
		"acme/c": {uuid.New(), false},
	}
	number := uint32(0)
	for name, r := range repos {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			r.id, name, org, at, at); err != nil {
			t.Fatal(err)
		}
		if r.owned {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
VALUES (?, 'github', 'team-t', ?, ?, 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`, org, r.id, name, at, at); err != nil {
				t.Fatal(err)
			}
		}
		number++
		if err := admin.Exec(ctx, `INSERT INTO git_pull_requests (org_id, repo_id, number, title, author_email, created_at, last_synced)
VALUES (?, ?, ?, ?, 'dev@example.com', ?, now64(3))`, org, r.id, number, "pr of "+name, time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	read := func(allowed []string, p PRParams) string {
		t.Helper()
		ceiling, err := reader.ResolveCeiling(ctx, pepgate.Allowlist(org, allowed))
		if err != nil {
			t.Fatal(err)
		}
		p.Ceiling = ceiling
		p.StartDay, p.EndDay, p.Limit = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), 50
		resp, err := BuildPRsResponse(ctx, reader, org, p)
		if err != nil {
			t.Fatalf("real ClickHouse refused the ceiling statement: %v", err)
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
	team := func(what ...string) PRParams {
		return PRParams{ScopeLevel: "team", ScopeIDs: []string{"team-t"}, WhatRepos: what}
	}
	for _, tc := range []struct {
		name    string
		allowed []string
		p       PRParams
		want    string
	}{
		{"ceiling alone: allowed repositories only", []string{"acme/a", "acme/c"}, PRParams{}, "pr of acme/a, pr of acme/c"},
		{"ceiling and team subquery: the allowed owned repository", []string{"acme/a", "acme/c"}, team(), "pr of acme/a"},
		{"ceiling, team and a named allowed repository", []string{"acme/a", "acme/c"}, team("acme/a"), "pr of acme/a"},
		{"ceiling, team and a named repository outside the ceiling: nothing", []string{"acme/a", "acme/c"}, team("acme/b"), ""},
		{"ceiling with a key that names nothing: nothing", []string{"acme/none"}, team(), ""},
		{"a key in another case resolves", []string{"ACME/A"}, team(), "pr of acme/a"},
	} {
		if got := read(tc.allowed, tc.p); got != tc.want {
			t.Errorf("%s: pull requests = [%s], want [%s]", tc.name, got, tc.want)
		}
	}
}
