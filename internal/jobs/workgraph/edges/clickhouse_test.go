package edges

import (
	"strings"
	"testing"
)

// TestDependencyReadMatchesTheQueryPythonIssued asserts the Go read is the SAME
// read, against the query text PR1 froze from the deployed producer.
//
// Porting a query by eye is how a column gets dropped, a filter gets added, or a
// FINAL appears — none of which fails loudly. The frozen text makes it checkable
// instead of reviewable.
//
// The single permitted difference is the org binding: Python interpolates the id
// into the SQL (`_org_id_clause`, builder.py:167-171), this binds a named
// parameter. Same semantics, better mechanism, and the substitution below is
// what confines the comparison to that one difference.
func TestDependencyReadMatchesTheQueryPythonIssued(t *testing.T) {
	document := loadGolden(t)
	if len(document.Queries) == 0 {
		t.Fatal("the golden froze no query text")
	}
	frozen := document.Queries[0].Statement
	if !strings.Contains(frozen, "FROM work_item_dependencies") {
		t.Fatalf("first frozen query is not the dependency read: %q", frozen)
	}

	// Python's interpolated literal -> the named parameter this port binds.
	interpolated := "org_id = '" + document.OrgID + "'"
	if !strings.Contains(frozen, interpolated) {
		t.Fatalf("frozen query does not carry the interpolated org filter: %q", frozen)
	}
	expected := strings.Replace(frozen, interpolated, "org_id = {org_id:String}", 1)

	if got := collapseWhitespace(dependencyReadSQL); got != expected {
		t.Fatalf(
			"the Go read is not the read Python issued.\n  go:     %s\n  python: %s",
			got, expected,
		)
	}
}

// TestDependencyReadCarriesNoDedupOrOrdering pins the two absences that are
// deliberate and would each look like an improvement to add.
//
// FINAL/argMax: the table is a ReplacingMergeTree, so omitting them means
// unmerged duplicate versions reach the derivation and last-write-wins decides
// which timestamp survives (CHAOS-4788). Adding dedup here would change which
// edges this port produces relative to Python.
//
// ORDER BY: its absence is why row order is load-bearing. Adding one would make
// the port deterministic where Python is not — a divergence, not a fix.
func TestDependencyReadCarriesNoDedupOrOrdering(t *testing.T) {
	normalised := strings.ToUpper(collapseWhitespace(dependencyReadSQL))
	for _, forbidden := range []string{" FINAL", "ARGMAX", "ORDER BY", "GROUP BY", "LIMIT "} {
		if strings.Contains(normalised, forbidden) {
			t.Errorf(
				"the dependency read gained %q; Python has none, so this port would "+
					"read a different row set than the plane it is being compared against",
				strings.TrimSpace(forbidden),
			)
		}
	}
	// ...and the org filter must still be there.
	if !strings.Contains(normalised, "ORG_ID = {ORG_ID:STRING}") {
		t.Error("the dependency read lost its org filter")
	}
}

func collapseWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
