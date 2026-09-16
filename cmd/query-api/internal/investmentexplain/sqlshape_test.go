package investmentexplain

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

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
			name:   "resolveRepoIDsForTeams",
			marker: "FROM user_metrics_daily FINAL",
			build: func(t *testing.T) string {
				client := &capturingClient{}
				reader, err := NewReader(client)
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				if _, err := reader.resolveRepoIDsForTeams(context.Background(), []string{"team-a"}, "org-1"); err != nil {
					t.Fatalf("resolveRepoIDsForTeams: %v", err)
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
