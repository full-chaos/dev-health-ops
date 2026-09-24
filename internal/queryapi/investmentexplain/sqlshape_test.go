package investmentexplain

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// teamScopeAsOf is the fixed instant every team-scope assertion in this
// file resolves ownership at, so an assertion states the instant it is
// about rather than depending on when the test ran.
var teamScopeAsOf = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// TestOrgIDScopesEveryFixedReadAtTheSameNestingDepthAsFinal pins the
// class ruling for the readers this package's sweep fixed:
// repos and user_metrics_daily are both ReplacingMergeTree (repos since
// migrations/clickhouse/000_raw_tables.sql + org_id via 024, sorting key
// (org_id, id) since 027; user_metrics_daily's team_id column is not part
// of its own sorting key since 096, so a raw read can surface a
// re-attributed row's stale team_id until the next merge). Every read
// below now dedups with FINAL, with org_id filtered in the SAME
// statement as that FINAL source -- never a separate, unfiltered
// subquery an outer WHERE would only narrow afterward.
//
// The check is depth-based (sqlshape.Depths, the same parenthesis-aware
// scan filteroptions's own equivalent guard uses), not a bare substring
// search: a parenthesised function call between the FINAL marker and the
// org predicate would false-positive a naive "(" search without changing
// nesting depth, which this test correctly ignores.
func TestOrgIDScopesEveryFixedReadAtTheSameNestingDepthAsFinal(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"

	cases := []struct {
		name   string
		marker string
		build  func(t *testing.T) string
	}{
		{
			name:   "resolveRepoID by uuid",
			marker: "FROM repos FINAL",
			build: func(t *testing.T) string {
				client := &capturingClient{}
				reader, err := NewReader(client)
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				if _, _, err := reader.resolveRepoID(context.Background(), "11111111-1111-4111-8111-111111111111", "org-1"); err != nil {
					t.Fatalf("resolveRepoID: %v", err)
				}
				return client.lastQuery
			},
		},
		{
			name:   "resolveRepoID by slug",
			marker: "FROM repos FINAL",
			build: func(t *testing.T) string {
				client := &capturingClient{}
				reader, err := NewReader(client)
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				if _, _, err := reader.resolveRepoID(context.Background(), "myorg/repo", "org-1"); err != nil {
					t.Fatalf("resolveRepoID: %v", err)
				}
				return client.lastQuery
			},
		},
		{
			name:   "FetchRepoScopes",
			marker: "FROM repos FINAL",
			build: func(t *testing.T) string {
				client := &capturingClient{}
				reader, err := NewReader(client)
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				if _, err := reader.FetchRepoScopes(context.Background(), "org-1", []string{"11111111-1111-4111-8111-111111111111"}); err != nil {
					t.Fatalf("FetchRepoScopes: %v", err)
				}
				return client.lastQuery
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query := tc.build(t)
			if strings.Contains(query, "LIMIT 1 BY") || strings.Contains(query, "argMax") {
				t.Fatalf("%s: contains a LIMIT-1-BY or argMax dedup subquery shape -- expected a plain FINAL read", tc.name)
			}

			depths := sqlshape.Depths(query)
			markerIdx := strings.Index(query, tc.marker)
			if markerIdx < 0 {
				t.Fatalf("%s: expected marker %q in query:\n%s", tc.name, tc.marker, query)
			}
			markerDepth := depths[markerIdx]

			rest := query[markerIdx+len(tc.marker):]
			orgIdx := strings.Index(rest, orgIDPredicate)
			if orgIdx < 0 {
				t.Fatalf("%s: expected %q after %q, got:\n%s", tc.name, orgIDPredicate, tc.marker, rest)
			}
			absoluteOrgIdx := markerIdx + len(tc.marker) + orgIdx
			orgDepth := depths[absoluteOrgIdx]

			if orgDepth != markerDepth {
				t.Fatalf("%s: org_id predicate sits at nesting depth %d but %q sits at depth %d -- "+
					"org_id must scope the SAME statement as the dedup source, not a subquery that "+
					"dedups the whole table before the tenant filter narrows it",
					tc.name, orgDepth, tc.marker, markerDepth)
			}
		})
	}
}

// assertOrgIDSameDepthAsMarkerNamed is the same depth check as
// TestOrgIDScopesEveryFixedReadAtTheSameNestingDepthAsFinal's own inline
// check, generalised for a condition string that carries a caller-chosen
// predicate (teamscope.RepoCondition's own uniquely-named org-id binding)
// instead of the package's plain org_id/{org_id:String} pair.
func assertOrgIDSameDepthAsMarkerNamed(t *testing.T, query, marker, predicate string) {
	t.Helper()
	depths := sqlshape.Depths(query)
	markerIdx := strings.Index(query, marker)
	if markerIdx < 0 {
		t.Fatalf("expected marker %q in query:\n%s", marker, query)
	}
	markerDepth := depths[markerIdx]

	rest := query[markerIdx+len(marker):]
	predIdx := strings.Index(rest, predicate)
	if predIdx < 0 {
		t.Fatalf("expected %q after %q, got:\n%s", predicate, marker, rest)
	}
	absolutePredIdx := markerIdx + len(marker) + predIdx
	predDepth := depths[absolutePredIdx]

	if predDepth != markerDepth {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- both must scope the SAME statement",
			predicate, predDepth, marker, markerDepth)
	}
}

// TestBreakdownFiltersScopeClauseTeamScopeNeverRoundTripsForRepoIDs is a
// regression pin: a repo-scoped breakdown filtered by team never asks the
// client to resolve or verify a repo-id LIST for that team as its own
// query. The prior shape did exactly that (one query to list every
// matching repo_id) -- a team whose true matching-repo count is large
// made that query's own result set large enough to exceed the read-only
// client's per-statement row ceiling, degrading the whole request instead
// of answering it. scopeClause building the condition from an already
// pushed-down TeamScopeCondition makes zero client round trips of its
// own: the team's repo_id membership is a condition inside the CALLER's
// eventual statement, never a standalone read of its own.
func TestBreakdownFiltersScopeClauseTeamScopeNeverRoundTripsForRepoIDs(t *testing.T) {
	teamCondition, teamBindings := teamscope.RepoCondition("org-1", "repo_id", []string{"team-x", "team-y"}, teamScopeAsOf)
	f := BreakdownFilters{TeamScopeCondition: teamCondition, TeamScopeBindings: teamBindings}
	scopeSQL, bindings := f.scopeClause()
	if !strings.Contains(scopeSQL, "repo_id IN (") || !strings.Contains(scopeSQL, teamscope.Marker) {
		t.Fatalf("scopeSQL missing the pushed-down team membership condition:\n%s", scopeSQL)
	}
	if strings.Contains(scopeSQL, "repo_id IN {scope_ids:Array(String)}") {
		t.Fatalf("scopeSQL carries an explicit-repo clause with no explicit repos requested:\n%s", scopeSQL)
	}
	var sawIDs bool
	for _, b := range bindings {
		if b.Name == teamscope.BindingTeamIDs {
			sawIDs = true
			if fmt.Sprint(b.Value) != "[team-x team-y]" {
				t.Fatalf("%s binding = %v, want [team-x team-y]", teamscope.BindingTeamIDs, b.Value)
			}
		}
	}
	if !sawIDs {
		t.Fatalf("bindings = %+v, want %s", bindings, teamscope.BindingTeamIDs)
	}
}

// TestBreakdownFiltersScopeClauseUnionsExplicitReposWithTeamScope pins
// the union semantics resolve_repo_filter_ids' own Python shape has: an
// explicit what.repos ref alongside a team scope means EITHER condition
// can match, not both required.
func TestBreakdownFiltersScopeClauseUnionsExplicitReposWithTeamScope(t *testing.T) {
	teamCondition, teamBindings := teamscope.RepoCondition("org-1", "repo_id", []string{"team-x"}, teamScopeAsOf)
	f := BreakdownFilters{RepoIDs: []string{"repo-1"}, TeamScopeCondition: teamCondition, TeamScopeBindings: teamBindings}
	scopeSQL, bindings := f.scopeClause()
	if !strings.HasPrefix(scopeSQL, " AND (repo_id IN {scope_ids:Array(String)} OR repo_id IN (") {
		t.Fatalf("scopeSQL does not OR the explicit-repo and team conditions together:\n%s", scopeSQL)
	}
	var sawScopeIDs, sawTeamIDs bool
	for _, b := range bindings {
		if b.Name == "scope_ids" {
			sawScopeIDs = true
			if fmt.Sprint(b.Value) != "[repo-1]" {
				t.Fatalf("scope_ids binding = %v, want [repo-1]", b.Value)
			}
		}
		if b.Name == teamscope.BindingTeamIDs {
			sawTeamIDs = true
		}
	}
	if !sawScopeIDs || !sawTeamIDs {
		t.Fatalf("bindings = %+v, want both scope_ids and %s", bindings, teamscope.BindingTeamIDs)
	}
}
