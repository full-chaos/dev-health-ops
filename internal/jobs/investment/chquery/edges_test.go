package chquery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// capturingConn is the minimal `conn` fake this package's own doc comment
// says exists to let a test substitute a fake without a live server
// (chquery.go:44-46). It captures the rendered query text and then errors
// out immediately -- deliberately not implementing driver.Rows at all,
// since a SQL-shape test only needs the text that was about to be sent, not
// a real result set.
type capturingConn struct {
	query string
	args  []any
}

func (c *capturingConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.query = query
	c.args = args
	return nil, errors.New("capturingConn: shape test only, no rows")
}

// TestFetchWorkGraphEdgesQueryShapeHasArgMaxCollapse pins the CHAOS-4985
// follow-up fix (codex round 2 on #2186, P3) directly at the query-shape
// level -- mirroring cmd/query-api/internal/workgraph/workgraph_test.go's
// TestFetchDedupedEdgeRows_QueryShapeHasArgMaxCollapse for this package's
// own, previously-unguarded, work_graph_edges reader. A smoke test that the
// tupled argMax collapse is actually IN the rendered query text, so a
// future edit that reverts to five independent argMax calls fails a fast
// unit test rather than depending on ClickHouse's own tie-break behavior at
// runtime (measured elsewhere in this session to be an unreliable
// regression signal on its own).
func TestFetchWorkGraphEdgesQueryShapeHasArgMaxCollapse(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, _ = reader.FetchWorkGraphEdges(context.Background(), EdgeQueryOptions{OrganizationID: "org1"})

	sql := fake.query
	for _, want := range []string{
		// ONE tupled argMax over all five independently-varying columns,
		// NOT five separate argMax(col, last_synced) calls -- the separate
		// form can, under a last_synced tie, independently pick DIFFERENT
		// tied rows per column and assemble a hybrid that never existed.
		"argMax(tuple(repo_id, provider, provenance, confidence, evidence), last_synced) AS winner",
		"toString(winner.1) AS repo_id",
		"winner.2 AS provider",
		"winner.3 AS provenance",
		"winner.4 AS confidence",
		"winner.5 AS evidence",
		"any(edge_id) AS edge_id",
		"GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id",
		"ORDER BY org_id, source_type, source_id, edge_type, target_type, target_id",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("edges query missing %q:\n%s", want, sql)
		}
	}
	// The pre-fix shape -- five separate top-level argMax calls -- must not
	// be present. A regression back to that shape is exactly what this test
	// exists to catch fast.
	for _, mustNotContain := range []string{
		"argMax(repo_id, last_synced)",
		"argMax(provider, last_synced)",
		"argMax(provenance, last_synced)",
		"argMax(confidence, last_synced)",
		"argMax(evidence, last_synced)",
	} {
		if strings.Contains(sql, mustNotContain) {
			t.Fatalf("edges query contains the pre-fix independent-argMax shape %q -- regression to the hybrid-row defect:\n%s",
				mustNotContain, sql)
		}
	}
}

// TestFetchWorkGraphEdgesHeuristicFilterAppliesAfterTheCollapse pins the
// HAVING-to-WHERE change: the heuristic-exclusion predicate must apply to
// the EXTRACTED provenance (post-collapse), never fold into the
// pre-aggregation WHERE on the raw column, and must render as an outer
// WHERE now that the GROUP BY lives in the subquery.
func TestFetchWorkGraphEdgesHeuristicFilterAppliesAfterTheCollapse(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, _ = reader.FetchWorkGraphEdges(context.Background(), EdgeQueryOptions{OrganizationID: "org1"})

	sql := fake.query
	if strings.Contains(sql, "WHERE org_id = {org_id:String} AND provenance !=") {
		t.Fatalf("heuristic filter must not be folded into the pre-aggregation WHERE: %s", sql)
	}
	if !strings.Contains(sql, "WHERE provenance != {heuristic_provenance:String}") {
		t.Fatalf("expected the heuristic filter as an outer WHERE referencing the extracted provenance: %s", sql)
	}
	groupByIdx := strings.Index(sql, "GROUP BY")
	closeParenIdx := strings.Index(sql[groupByIdx:], ")")
	outerWhereIdx := strings.LastIndex(sql, "WHERE provenance !=")
	if groupByIdx < 0 || closeParenIdx < 0 || outerWhereIdx < 0 || outerWhereIdx < groupByIdx+closeParenIdx {
		t.Fatalf("outer heuristic filter must be rendered after the subquery closes: %s", sql)
	}
}
