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

// --- CHAOS-4655: pair-bound (node_type, node_id) match ------------------

// TestMembershipPairsLiteral_RendersHexPairs locks the exact ClickHouse
// Array(String) parameter text batchResolveMembership's node_pairs binding
// sends -- a plain string binding whose VALUE is this literal (see that
// function's doc comment for why: dev-health-go@v0.6.1's clickhouse.Binding
// has no native tuple-array encoding, and a quoted Array(Tuple(String,
// String)) literal was tried and proven broken against a real engine --
// see membership_integration_test.go's round-trip test). Each pair is
// hex(nodeType)+":"+hex(nodeID); hex digits need no quoting/escaping for
// any input, which is the entire point of this shape.
func TestMembershipPairsLiteral_RendersHexPairs(t *testing.T) {
	pairs := []membershipKey{
		{nodeType: "issue", nodeID: "a"},
		{nodeType: "pr", nodeID: `o'brien`},
	}
	got := membershipPairsLiteral(pairs)
	// hex("issue")=6973737565 hex("a")=61 hex("pr")=7072 hex("o'brien")=6f27627269656e
	want := `['6973737565:61','7072:6f27627269656e']`
	if got != want {
		t.Fatalf("membershipPairsLiteral() = %q, want %q", got, want)
	}
}

func TestMembershipPairsLiteral_Empty(t *testing.T) {
	if got := membershipPairsLiteral(nil); got != "[]" {
		t.Fatalf("membershipPairsLiteral(nil) = %q, want %q", got, "[]")
	}
}

// TestBatchResolveMembership_QueriesAPairBoundMatch is CHAOS-4655's
// structural regression guard: it locks the WHERE clause shape -- BOTH the
// sargable node_type/node_id IN prefilter (kept for primary-key index
// pruning; team-lead review, CHAOS-4655, 2026-09-01 -- see
// batchResolveMembership's doc comment) AND the hex+concat pair-exactness
// filter that removes the prefilter's cross-product over-fetch -- and the
// full binding set, so a future edit cannot silently drop either half
// while every other test (which only exercises small fixtures where the
// shapes agree on final RESULT rows) stays green. The seeded-data
// red/green proof against a REAL ClickHouse engine -- where the shapes
// actually DISAGREE on rows returned -- lives in the `integration`-tagged
// tests in this package, which this fake-client test cannot substitute for
// (a fake only proves SQL TEXT changed, never that it executes correctly,
// or efficiently, against the engine).
func TestBatchResolveMembership_QueriesAPairBoundMatch(t *testing.T) {
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
	if !strings.Contains(stmt, "m.node_type IN {node_types:Array(String)}") ||
		!strings.Contains(stmt, "m.node_id IN {node_ids:Array(String)}") {
		t.Fatalf("query is missing the sargable node_type/node_id prefilter: %s", stmt)
	}
	if !strings.Contains(stmt, "concat(lower(hex(m.node_type)), ':', lower(hex(m.node_id))) IN {node_pairs:Array(String)}") {
		t.Fatalf("query is missing the pair-exactness hex+concat filter: %s", stmt)
	}
	if strings.Contains(stmt, "Tuple") {
		t.Fatalf("query references a Tuple type (proven broken against a real engine, see membership.go's comment): %s", stmt)
	}

	typesVal, ok := bindingValue(client.bindings[0], "node_types")
	if !ok {
		t.Fatalf("no node_types binding")
	}
	if want := []string{"issue", "pr"}; !equalStringSlices(typesVal.([]string), want) {
		t.Fatalf("node_types binding = %v, want %v", typesVal, want)
	}
	idsVal, ok := bindingValue(client.bindings[0], "node_ids")
	if !ok {
		t.Fatalf("no node_ids binding")
	}
	if want := []string{"ep-1", "ep-2"}; !equalStringSlices(idsVal.([]string), want) {
		t.Fatalf("node_ids binding = %v, want %v", idsVal, want)
	}
	pairsVal, ok := bindingValue(client.bindings[0], "node_pairs")
	if !ok {
		t.Fatalf("no node_pairs binding")
	}
	// hex("issue")=6973737565 hex("ep-1")=65702d31 hex("pr")=7072 hex("ep-2")=65702d32
	if want := "['6973737565:65702d31','7072:65702d32']"; pairsVal != want {
		t.Fatalf("node_pairs binding = %v, want %q", pairsVal, want)
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
