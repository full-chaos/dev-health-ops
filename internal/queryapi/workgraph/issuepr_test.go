package workgraph

import (
	"context"
	"strings"
	"testing"
)

// TestFetchLinkedIssueRowsFastPathQueryIsOneTupledArgMax pins the CHAOS-4924
// fix (codex round 1 on #2183, P2) directly at the query-shape level: a
// smoke test that the tupled argMax collapse is actually IN the rendered
// query text, so a future edit that reverts to three independent argMax
// calls fails a fast unit test rather than only a slow, coincidence-prone
// live one.
//
// Codex round 2 on #2183 (P3) explicitly asked for this: the live
// hybrid-row test (issuepr_integration_test.go) can pass against a REVERTED
// (pre-fix) query whenever ClickHouse's independent tie-breaks happen to
// agree across all three columns for the specific tie constructed there --
// an outcome ClickHouse permits but does not guarantee against. This test
// does not depend on ClickHouse's runtime tie-break behavior at all; it
// only checks the SQL text.
func TestFetchLinkedIssueRowsFastPathQueryIsOneTupledArgMax(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	_, err := fetchLinkedIssueRowsFastPath(context.Background(), client, "org1", "11111111-1111-1111-1111-111111111111", 12)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("want exactly 1 query, got %d", client.calls)
	}
	sql := client.statements[0]
	for _, want := range []string{
		// ONE tupled argMax over all three independently-varying columns,
		// NOT three separate argMax(col, version_rank) calls -- the
		// separate form can, under a version_rank tie, independently pick
		// DIFFERENT tied rows per column and assemble a hybrid that never
		// existed (codex round 1 on #2183/CHAOS-4924).
		"argMax(tuple(confidence, provenance, evidence), version_rank) AS winner",
		"toFloat64(winner.1)",
		"winner.2 AS provenance",
		"winner.3 AS evidence",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("fast-path query missing %q:\n%s", want, sql)
		}
	}
	// The pre-fix shape -- three separate top-level argMax calls -- must
	// not be present. A regression back to that shape is exactly what this
	// test exists to catch fast, without depending on ClickHouse's runtime
	// tie-break behavior (which the live hybrid-row test alone cannot rule
	// out, per its own doc comment).
	for _, mustNotContain := range []string{
		"argMax(confidence, version_rank)",
		"argMax(provenance, version_rank)",
		"argMax(evidence, version_rank)",
	} {
		if strings.Contains(sql, mustNotContain) {
			t.Fatalf("fast-path query contains the pre-fix independent-argMax shape %q -- regression to the hybrid-row defect:\n%s",
				mustNotContain, sql)
		}
	}
}
