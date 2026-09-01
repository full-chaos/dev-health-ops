package workgraph

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// --- CHAOS-4655: tupled (node_type, node_id) match --------------------

// TestMembershipPairsLiteral_RendersATupleArrayAndEscapesQuotes locks the
// exact ClickHouse Array(Tuple(String, String)) parameter text
// batchResolveMembership's node_pairs binding sends -- a plain string
// binding whose VALUE is this literal (see that function's doc comment for
// why: dev-health-go@v0.6.1's clickhouse.Binding has no native tuple-array
// encoding). A node_type/node_id containing a single quote is a real input
// shape (arbitrary provider-sourced ids), not a hypothetical -- escaping it
// wrong would either corrupt the query or, worse, let one endpoint's id
// break out of its own tuple.
func TestMembershipPairsLiteral_RendersATupleArrayAndEscapesQuotes(t *testing.T) {
	pairs := []membershipKey{
		{nodeType: "issue", nodeID: "a"},
		{nodeType: "pr", nodeID: `o'brien`},
	}
	got := membershipPairsLiteral(pairs)
	want := `[('issue','a'),('pr','o\'brien')]`
	if got != want {
		t.Fatalf("membershipPairsLiteral() = %q, want %q", got, want)
	}
}

func TestMembershipPairsLiteral_Empty(t *testing.T) {
	if got := membershipPairsLiteral(nil); got != "[]" {
		t.Fatalf("membershipPairsLiteral(nil) = %q, want %q", got, "[]")
	}
}

// TestBatchResolveMembership_QueriesATupledMatch is CHAOS-4655's structural
// regression guard: it locks the WHERE clause shape (a tupled match, not
// two independent Array(String) IN predicates) and the binding set, so a
// future edit cannot silently reintroduce the cross-product shape while
// every other test (which only exercises small fixtures where the two
// shapes agree) stays green. The seeded-data red/green proof against a
// REAL ClickHouse engine -- where the two shapes actually DISAGREE on rows
// returned -- lives in the `integration`-tagged test in this package,
// which this fake-client test cannot substitute for (a fake only proves
// SQL TEXT changed, never that it executes correctly against the engine).
func TestBatchResolveMembership_QueriesATupledMatch(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"issue", "ep-1", "theme", "reliability"},
	}}}}

	var recorded []struct{ rows, endpoints int }
	previous := recordMembershipRowsPerEndpoint
	recordMembershipRowsPerEndpoint = func(_ context.Context, rowsReturned, endpointsRequested int) {
		recorded = append(recorded, struct{ rows, endpoints int }{rowsReturned, endpointsRequested})
	}
	t.Cleanup(func() { recordMembershipRowsPerEndpoint = previous })

	rows := []edgeEndpoint{{sourceType: "issue", sourceID: "ep-1", targetType: "pr", targetID: "ep-2"}}
	result, err := batchResolveMembership(context.Background(), client, "org1", rows, newFilterScope(nil, nil))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("got %d Query calls, want 1", len(client.statements))
	}
	stmt := client.statements[0]
	if !strings.Contains(stmt, "(m.node_type, m.node_id) IN {node_pairs:Array(Tuple(String, String))}") {
		t.Fatalf("query is not a tupled match: %s", stmt)
	}
	if strings.Contains(stmt, "node_types") || strings.Contains(stmt, "node_ids") {
		t.Fatalf("query still references the independent-IN binding names: %s", stmt)
	}

	pairsVal, ok := bindingValue(client.bindings[0], "node_pairs")
	if !ok {
		t.Fatalf("no node_pairs binding")
	}
	if want := "[('issue','ep-1'),('pr','ep-2')]"; pairsVal != want {
		t.Fatalf("node_pairs binding = %v, want %q", pairsVal, want)
	}
	if _, ok := bindingValue(client.bindings[0], "node_types"); ok {
		t.Fatalf("unexpected node_types binding still present")
	}
	if _, ok := bindingValue(client.bindings[0], "node_ids"); ok {
		t.Fatalf("unexpected node_ids binding still present")
	}

	if len(result) != 1 || result[membershipKey{nodeType: "issue", nodeID: "ep-1"}].dominantTheme != "reliability" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(recorded) != 1 || recorded[0].rows != 1 || recorded[0].endpoints != 2 {
		t.Fatalf("telemetry not recorded correctly: %+v", recorded)
	}
}

// TestCategoryKindCardinalityHasNoNewValue is CHAOS-4647's trip-wire
// (codex review round 2, adopted into the final ruling as the structural
// answer to "can a fan-out multiplier be undercounted again" -- no number
// can answer that, but a test that fails the moment the fan-out itself
// changes can).
//
// batchResolveMembership (membership.go) assumes work_unit_membership's
// category_kind has EXACTLY two values -- "theme" and "subcategory" --
// in two places that must move together:
//   - Go: the Next()/Scan() loop's switch statement (membership.go) only
//     recognizes those two case labels; a third value is silently
//     dropped (falls through, sets nothing on the map entry).
//   - Python: project_membership_records (the SINGLE authoritative
//     producer of work_unit_membership rows -- this module's own doc
//     comment) hardcodes exactly two loops, one per distribution
//     parameter, each stamping one literal category_kind value.
//
// This is also the exact fan-out CHAOS-4647's queryRouteMaxResultRows
// derivation depends on (query_route.go: 2 endpoints/edge x 2 rows/
// endpoint): a third category_kind would silently undercount that
// budget a third time, the same way rounds 1 and 2 of that review each
// undercounted the round before. Rather than trust a number to stay
// right, this test reads BOTH sides' actual source text and fails
// loudly the moment either one's set of category_kind literals drifts
// from {"theme", "subcategory"} OR the two sides disagree with each
// other -- forcing deliberate re-derivation of anything workload-sized
// that depends on this cardinality, instead of a silent live failure
// discovered the way this ticket's original bug was.
func TestCategoryKindCardinalityHasNoNewValue(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve workgraph package path")
	}
	packageDir := filepath.Dir(currentFile)
	repositoryRoot := filepath.Clean(filepath.Join(packageDir, "..", "..", "..", ".."))

	wantKinds := []string{"subcategory", "theme"} // sorted

	// Go side: membership.go's switch statement.
	membershipGoPath := filepath.Join(packageDir, "membership.go")
	goCaseRe := regexp.MustCompile(`case\s+"([a-z_]+)":\s*\n\s*entry\.dominant`)
	goKinds := mustFindSourceLiterals(t, membershipGoPath, goCaseRe,
		"membership.go's switch statement over category_kind")

	// Python side: project_membership_records, the single authoritative
	// producer (this module's own doc comment names it that).
	pythonPath := filepath.Join(repositoryRoot, "src", "dev_health_ops", "work_graph", "investment", "membership.py")
	pyLiteralRe := regexp.MustCompile(`category_kind="([a-z_]+)"`)
	pyKinds := mustFindSourceLiterals(t, pythonPath, pyLiteralRe,
		"membership.py's project_membership_records category_kind literals")

	// This check runs ONLY after both reads above already proved they
	// found the construct they were looking for (mustFindSourceLiterals
	// fails loudly and separately if either file is missing/unreadable
	// or the regex matched zero times -- see that helper's doc comment,
	// added per team-lead review: a source-reading check that finds
	// nothing must not silently agree with an empty expectation, and
	// this test's expectation is never empty). What's left to check here
	// is CONTENT, not whether the measurement happened at all.
	if !equalStringSlices(goKinds, wantKinds) {
		t.Fatalf("membership.go's switch statement recognizes category_kind values %v, want exactly %v -- "+
			"a category_kind case was added or removed without updating this trip-wire AND "+
			"query_route.go's queryRouteMaxResultRows derivation (workload fan-out: 2 endpoints/edge x N rows/endpoint)",
			goKinds, wantKinds)
	}
	if !equalStringSlices(pyKinds, wantKinds) {
		t.Fatalf("%s stamps category_kind literals %v, want exactly %v -- "+
			"a new distribution/category_kind was added to the producer without updating this trip-wire",
			pythonPath, pyKinds, wantKinds)
	}
}

// mustFindSourceLiterals reads path and extracts every regex capture
// group-1 match, failing loudly and SEPARATELY at each of the two
// distinct ways this kind of check can go wrong silently (team-lead
// review, CHAOS-4647: "an empty read is a claim about your path before
// it is a claim about the code" -- verification rule #4, "a measurement
// that did not happen must FAIL, loudly"):
//   - the file does not exist, moved, or is unreadable -- os.ReadFile's
//     error, surfaced with the exact path so the failure is actionable;
//   - the file exists and reads fine, but the regex matched ZERO times
//     (the construct it expects was renamed, reformatted, or moved to a
//     different file) -- this is NOT folded into the caller's "wrong
//     content" comparison below, because a same-length coincidence is
//     the only way an empty read could otherwise slip past a naive
//     equality check, and it must not be possible to construct one here
//     since this function's caller's wantKinds is never empty.
//
// Only once both of those are ruled out does the caller get to ask
// "and is the CONTENT right" -- which is a question about the code, not
// about whether this test's own inputs resolved.
func mustFindSourceLiterals(t *testing.T, path string, pattern *regexp.Regexp, what string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s (for %s): %v -- this trip-wire's own input did not resolve; that is a path failure, not evidence the checked-for construct is fine", path, what, err)
	}
	found := extractSortedUnique(pattern.FindAllStringSubmatch(string(src), -1))
	if len(found) == 0 {
		t.Fatalf("%s: read %s successfully (%d bytes) but the pattern matched ZERO times -- "+
			"the construct this trip-wire checks (%s) was renamed, reformatted, or moved; "+
			"this is NOT the same as finding zero category_kind values, it means the check itself needs updating",
			what, path, len(src), pattern.String())
	}
	return found
}

func extractSortedUnique(matches [][]string) []string {
	seen := map[string]struct{}{}
	for _, m := range matches {
		seen[m[1]] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
