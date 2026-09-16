package quadrant

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// TestOrgIDScopesEveryQuadrantMetricReadAtTheSameNestingDepthAsFinal pins
// the class ruling this package holds to: every daily-family table this package's
// MetricSpecs read is ReplacingMergeTree(computed_at) (see quadrant.go's
// package doc comment) and dedups with FINAL -- including the "repos"
// side of every RepoMetrics join, itself ReplacingMergeTree(last_synced)
// -- with org_id filtered in the SAME top-level statement as each FINAL
// source, never a separate subquery an outer WHERE only narrows
// afterward.
//
// The check is depth-based (sqlshape.Depths), not a bare substring
// search: a parenthesised function call between a FINAL marker and its
// org predicate would false-positive a naive "(" search without changing
// nesting depth, which this test correctly ignores.
func TestOrgIDScopesEveryQuadrantMetricReadAtTheSameNestingDepthAsFinal(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"

	cases := []struct {
		name    string
		spec    MetricSpec
		markers []string
	}{
		{"TeamMetrics churn (user_metrics_daily)", TeamMetrics["churn"], []string{"FROM user_metrics_daily FINAL"}},
		{"TeamMetrics throughput (work_item_metrics_daily)", TeamMetrics["throughput"], []string{"FROM work_item_metrics_daily FINAL"}},
		{"RepoMetrics churn (repo_metrics_daily + repos join)", RepoMetrics["churn"], []string{
			"FROM repo_metrics_daily FINAL",
			"INNER JOIN repos FINAL ON repos.id = m.repo_id AND repos.org_id",
		}},
		{"RepoMetrics wip (work_item_metrics_daily + repos join on work_scope_id)", RepoMetrics["wip"], []string{
			"FROM work_item_metrics_daily FINAL",
			"INNER JOIN repos FINAL ON repos.repo = m.work_scope_id AND repos.org_id",
		}},
		{"PersonMetrics throughput (work_item_user_metrics_daily)", PersonMetrics["throughput"], []string{"FROM work_item_user_metrics_daily FINAL"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query := quadrantMetricQuery(tc.spec, "week", "")
			if strings.Contains(query, "LIMIT 1 BY") {
				t.Fatalf("%s: contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read", tc.name)
			}

			depths := sqlshape.Depths(query)
			for _, marker := range tc.markers {
				markerIdx := strings.Index(query, marker)
				if markerIdx < 0 {
					t.Fatalf("%s: expected marker %q in query:\n%s", tc.name, marker, query)
				}
				markerDepth := depths[markerIdx]

				rest := query[markerIdx+len(marker):]
				orgIdx := strings.Index(rest, orgIDPredicate)
				if orgIdx < 0 {
					t.Fatalf("%s: expected %q after %q, got:\n%s", tc.name, orgIDPredicate, marker, rest)
				}
				absoluteOrgIdx := markerIdx + len(marker) + orgIdx
				orgDepth := depths[absoluteOrgIdx]

				if orgDepth != markerDepth {
					t.Fatalf("%s: org_id predicate sits at nesting depth %d but %q sits at depth %d -- "+
						"org_id must scope the SAME statement as the dedup source, not a subquery that "+
						"dedups the whole table before the tenant filter narrows it",
						tc.name, orgDepth, marker, markerDepth)
				}
			}
		})
	}
}

// TestOrgIDScopesResolvePersonIdentityAtTheSameNestingDepthAsFinal pins the
// same ruling for identity.go's resolvePersonIdentity: both UNION DISTINCT
// branches inside the "identities" CTE read a ReplacingMergeTree table
// FINAL, with org_id filtered inside the SAME parenthesised branch, not
// outside the WITH clause.
func TestOrgIDScopesResolvePersonIdentityAtTheSameNestingDepthAsFinal(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"

	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{}, nil
	}}
	if _, err := resolvePersonIdentity(context.Background(), client, "person-1", "org-1"); err != nil {
		t.Fatalf("resolvePersonIdentity: %v", err)
	}

	markers := []string{"FROM user_metrics_daily FINAL", "FROM work_item_user_metrics_daily FINAL"}
	depths := sqlshape.Depths(captured)
	for _, marker := range markers {
		markerIdx := strings.Index(captured, marker)
		if markerIdx < 0 {
			t.Fatalf("expected marker %q in query:\n%s", marker, captured)
		}
		markerDepth := depths[markerIdx]

		rest := captured[markerIdx+len(marker):]
		orgIdx := strings.Index(rest, orgIDPredicate)
		if orgIdx < 0 {
			t.Fatalf("expected %q after %q, got:\n%s", orgIDPredicate, marker, rest)
		}
		absoluteOrgIdx := markerIdx + len(marker) + orgIdx
		orgDepth := depths[absoluteOrgIdx]

		if orgDepth != markerDepth {
			t.Fatalf("org_id predicate sits at nesting depth %d but %q sits at depth %d",
				orgDepth, marker, markerDepth)
		}
	}
}
