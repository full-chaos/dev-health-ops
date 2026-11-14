package chwrite

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The reference columns of migration 149 are DEFAULTs of the row's evidence: the
// writer must leave them out, or a build would fail on a database without 149
// and could store keys that differ from its evidence.
func TestWriteInvestmentsLeavesTheReferenceColumnsToTheirDefault(t *testing.T) {
	conn := &shadowConn{}
	writer := newShadowWriter(t, conn)
	record := InvestmentRecord{WorkUnitID: "wu-1", StructuralEvidenceJSON: `{"issues":["i-1"],"prs":["p-1"]}`, ComputedAt: time.Now()}
	if _, err := writer.WriteInvestments(context.Background(), testOrgIDUnit, []InvestmentRecord{record}); err != nil {
		t.Fatal(err)
	}
	if len(conn.statements) != 1 {
		t.Fatalf("%d statements, want 1", len(conn.statements))
	}
	statement := conn.statements[0]
	if !strings.Contains(statement, "structural_evidence_json") {
		t.Fatalf("the insert does not name structural_evidence_json: %s", statement)
	}
	for _, column := range []string{"issue_refs", "pr_refs"} {
		if strings.Contains(statement, column) {
			t.Errorf("the insert names %s, a DEFAULT of the evidence: %s", column, statement)
		}
	}
	if got, want := columnCount(statement), len(conn.batches[0].rows[0]); got != want {
		t.Fatalf("the insert names %d columns and appends %d values", got, want)
	}
}

const testOrgIDUnit = "33333333-3333-4333-8333-333333333333"
