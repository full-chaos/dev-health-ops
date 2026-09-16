package people

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// TestOrgIDScopesEveryPeopleSearchReadAtTheSameNestingDepthAsFinal pins
// the class ruling this package holds to (see searchPeopleQuery's own doc
// comment): both UNION ALL branches read a ReplacingMergeTree(computed_at)
// table FINAL, with org_id filtered inside the SAME branch, never a
// separate subquery an outer WHERE only narrows afterward. Depth-based
// (sqlshape.Depths), not a bare substring search, matching
// cmd/query-api/internal/quadrant/sqlshape_test.go's own convention: a
// parenthesised expression between a FINAL marker and its org predicate
// would false-positive a naive search without changing nesting depth.
func TestOrgIDScopesEveryPeopleSearchReadAtTheSameNestingDepthAsFinal(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"
	markers := []string{
		"FROM user_metrics_daily FINAL",
		"FROM work_item_user_metrics_daily FINAL",
	}

	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildSearchResponse(context.Background(), reader, "org-1", SearchParams{Query: "x", Limit: 20, Now: day(2024, 6, 15)}); err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}

	if strings.Contains(captured, "LIMIT 1 BY") {
		t.Fatalf("contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read:\n%s", captured)
	}

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
			t.Fatalf("org_id predicate sits at nesting depth %d but %q sits at depth %d -- "+
				"org_id must scope the SAME statement as the dedup source, not a subquery that "+
				"dedups the whole table before the tenant filter narrows it",
				orgDepth, marker, markerDepth)
		}
	}
}
