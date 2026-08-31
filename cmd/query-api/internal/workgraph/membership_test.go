package workgraph

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"testing"
)

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
	membershipGoSrc, err := os.ReadFile(membershipGoPath)
	if err != nil {
		t.Fatalf("read %s: %v", membershipGoPath, err)
	}
	goCaseRe := regexp.MustCompile(`case\s+"([a-z_]+)":\s*\n\s*entry\.dominant`)
	goKinds := extractSortedUnique(goCaseRe.FindAllStringSubmatch(string(membershipGoSrc), -1))
	if !equalStringSlices(goKinds, wantKinds) {
		t.Fatalf("membership.go's switch statement recognizes category_kind values %v, want exactly %v -- "+
			"a category_kind case was added or removed without updating this trip-wire AND "+
			"query_route.go's queryRouteMaxResultRows derivation (workload fan-out: 2 endpoints/edge x N rows/endpoint)",
			goKinds, wantKinds)
	}

	// Python side: project_membership_records, the single authoritative
	// producer (this module's own doc comment names it that).
	pythonPath := filepath.Join(repositoryRoot, "src", "dev_health_ops", "work_graph", "investment", "membership.py")
	pythonSrc, err := os.ReadFile(pythonPath)
	if err != nil {
		t.Fatalf("read %s: %v", pythonPath, err)
	}
	pyLiteralRe := regexp.MustCompile(`category_kind="([a-z_]+)"`)
	pyKinds := extractSortedUnique(pyLiteralRe.FindAllStringSubmatch(string(pythonSrc), -1))
	if !equalStringSlices(pyKinds, wantKinds) {
		t.Fatalf("%s stamps category_kind literals %v, want exactly %v -- "+
			"a new distribution/category_kind was added to the producer without updating this trip-wire",
			pythonPath, pyKinds, wantKinds)
	}
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
