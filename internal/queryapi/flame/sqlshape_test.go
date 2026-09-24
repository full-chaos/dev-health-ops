package flame

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// TestOrgIDScopesEveryFlameReadAtTheSameNestingDepthAsFinal pins the class
// ruling this package holds to: every ReplacingMergeTree table this
// package reads (git_pull_requests, git_pull_request_reviews,
// work_item_cycle_times, deployments) is read FINAL, with org_id filtered
// in the SAME top-level statement as that FINAL source, never a separate
// subquery an outer WHERE only narrows afterward.
//
// The check is depth-based (sqlshape.Depths), not a bare substring search
// -- see quadrant/sqlshape_test.go's own doc comment for why.
func TestOrgIDScopesEveryFlameReadAtTheSameNestingDepthAsFinal(t *testing.T) {
	cases := []struct {
		name           string
		query          string
		marker         string
		orgIDPredicate string
	}{
		{"git_pull_requests", fetchPullRequestQuery, "FROM git_pull_requests FINAL", "org_id = {org_id:String}"},
		{"git_pull_request_reviews", fetchPullRequestReviewsQuery, "FROM git_pull_request_reviews FINAL", "org_id = {org_id:String}"},
		{"work_item_cycle_times", fetchIssueQuery, "FROM work_item_cycle_times FINAL", "org_id = {org_id:String}"},
		{"deployments", fetchDeploymentQuery, "FROM deployments FINAL", "org_id = {org_id:String}"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.query, "LIMIT 1 BY") {
				t.Fatalf("%s: contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read", tc.name)
			}

			depths := sqlshape.Depths(tc.query)
			markerIdx := strings.Index(tc.query, tc.marker)
			if markerIdx < 0 {
				t.Fatalf("%s: expected marker %q in query:\n%s", tc.name, tc.marker, tc.query)
			}
			markerDepth := depths[markerIdx]

			rest := tc.query[markerIdx+len(tc.marker):]
			orgIdx := strings.Index(rest, tc.orgIDPredicate)
			if orgIdx < 0 {
				t.Fatalf("%s: expected %q after %q, got:\n%s", tc.name, tc.orgIDPredicate, tc.marker, rest)
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

// TestFlameQueriesScopeRepoIDAndOtherPredicatesInsideTheWhereClause is a
// lighter shape guard: every non-org_id predicate this package's queries
// carry (repo_id/number/deployment_id/work_item_id/submitted_at) also
// appears, confirming none of them were accidentally left out of a query
// while porting.
func TestFlameQueriesScopeRepoIDAndOtherPredicatesInsideTheWhereClause(t *testing.T) {
	cases := []struct {
		name  string
		query string
		wants []string
	}{
		{"git_pull_requests", fetchPullRequestQuery, []string{"toString(repo_id) = {repo_id:String}", "number = {number:UInt32}"}},
		{"git_pull_request_reviews", fetchPullRequestReviewsQuery, []string{"toString(repo_id) = {repo_id:String}", "number = {number:UInt32}", "submitted_at IS NOT NULL", "ORDER BY submitted_at"}},
		{"work_item_cycle_times", fetchIssueQuery, []string{"work_item_id = {work_item_id:String}", "ORDER BY day DESC"}},
		{"deployments", fetchDeploymentQuery, []string{"toString(repo_id) = {repo_id:String}", "deployment_id = {deployment_id:String}"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, want := range tc.wants {
				if !strings.Contains(tc.query, want) {
					t.Fatalf("%s: query missing %q:\n%s", tc.name, want, tc.query)
				}
			}
		})
	}
}

// TestFetchIssueQueryOmitsTheDeadTeamAttributionJoin pins the deliberate
// simplification flame.go's own package doc comment describes: the
// reference fetch_issue (api/queries/flame.py) LEFT JOINs a
// team-attribution subquery whose result (team_id) is never read by
// _build_issue_flame_response -- this port's fetchIssueQuery carries no
// such join at all. A future edit that adds one back (e.g. by copying
// quadrant.go's primaryWorkItemTeamAttributionSource) must not do so
// silently.
func TestFetchIssueQueryOmitsTheDeadTeamAttributionJoin(t *testing.T) {
	if strings.Contains(fetchIssueQuery, "JOIN") {
		t.Fatalf("fetchIssueQuery contains a JOIN -- the team-attribution join is dead code the Python reference discards downstream and must not be ported:\n%s", fetchIssueQuery)
	}
	if strings.Contains(fetchIssueQuery, "team_id") || strings.Contains(fetchIssueQuery, "work_scope_id") {
		t.Fatalf("fetchIssueQuery selects team_id/work_scope_id -- neither is read by buildIssueFlameResponse, so selecting either is dead weight the Python reference's own query shape does not need reproduced:\n%s", fetchIssueQuery)
	}
}
