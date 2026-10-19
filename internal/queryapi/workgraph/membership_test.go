package workgraph

import (
	"context"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
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
// structural regression guard: it locks the WHERE clause shape -- a tuple
// IN subquery that decodes node_pairs server-side and matches the RAW
// (node_type, node_id) columns against it, NOT independent IN lists and
// NOT a computed hex()/concat() match on the columns either (see
// batchResolveMembership's doc comment for why: the tuple-IN-subquery form
// is the only one that lets ClickHouse's primary-key index analysis use
// BOTH columns, confirmed via `EXPLAIN indexes=1`) -- so a future edit
// cannot silently reintroduce the cross-product shape, or regress back to
// an index-opaque column expression, while every other test (which only
// exercises small fixtures where the shapes agree on final RESULT rows)
// stays green. The seeded-data red/green proof against a REAL ClickHouse
// engine -- where the shapes actually DISAGREE on rows returned -- lives
// in the `integration`-tagged tests in this package, which this
// fake-client test cannot substitute for (a fake only proves SQL TEXT
// changed, never that it executes correctly, or efficiently, against the
// engine).
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
	if !strings.Contains(stmt, "(m.node_type, m.node_id) IN (") {
		t.Fatalf("query is missing the tuple-IN-subquery pair match: %s", stmt)
	}
	if !strings.Contains(stmt, "unhex(splitByChar(':', p)[1]), unhex(splitByChar(':', p)[2])") {
		t.Fatalf("query is missing the server-side hex decode of node_pairs: %s", stmt)
	}
	if !strings.Contains(stmt, "arrayJoin({node_pairs:Array(String)})") {
		t.Fatalf("query does not arrayJoin the node_pairs parameter: %s", stmt)
	}
	if strings.Contains(stmt, "node_types") || strings.Contains(stmt, "node_ids") || strings.Contains(stmt, "Tuple") || strings.Contains(stmt, "concat(") {
		t.Fatalf("query references a removed binding name, a Tuple-typed parameter, or the abandoned concat()-column form: %s", stmt)
	}

	// node_pairs is the ONLY node_type/node_id-related binding -- no plain
	// []string binding for either column exists anymore (that binding
	// shape is CHAOS-4745's exposure; see batchResolveMembership's doc
	// comment for the three escaping schemes that all failed for arbitrary
	// node_id content).
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

// TestBatchResolveMembership_RecordsTelemetryOnMissingMembershipTable is
// codex round 5's finding (CHAOS-4655, gpt-5.6-terra xhigh, P3): the
// rolling-deploy/pre-migration early return (work_unit_membership does not
// exist yet) bypassed recordMembershipRowsPerEndpoint entirely, so that
// window would silently produce NO histogram sample instead of an honest
// 0-rows-per-endpoint one. Confirmed RED before the fix (this early return
// skipped straight past the recording call); GREEN after.
func TestBatchResolveMembership_RecordsTelemetryOnMissingMembershipTable(t *testing.T) {
	client := &fakeClient{
		errs: []error{&chproto.Exception{Code: 60, Message: "Unknown table expression identifier 'work_unit_membership' in scope SELECT ..."}},
	}

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
	if len(result) != 0 {
		t.Fatalf("expected an empty result on a missing membership table, got: %+v", result)
	}
	if len(recorded) != 1 || recorded[0].rows != 0 || recorded[0].endpoints != 2 {
		t.Fatalf("expected one telemetry sample (0 rows / 2 endpoints), got: %+v", recorded)
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
