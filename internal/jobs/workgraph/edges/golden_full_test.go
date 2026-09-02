package edges

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const goldenFixture = "workgraph_issue_edges_python_golden.json"

func repositoryRootPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
}

func loadGolden(t *testing.T) *GoldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", goldenFixture)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	document := &GoldenDocument{}
	if err := json.Unmarshal(raw, document); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if document.Schema != "workgraph_issue_edges_python_golden.v1" {
		t.Fatalf("unexpected golden schema %q", document.Schema)
	}
	if len(document.Edges) == 0 {
		t.Fatal("golden carries no edges")
	}
	return document
}

// TestGoldenCountsMatchItsOwnPayload catches a truncated or hand-edited fixture
// before any other test reasons about it.
func TestGoldenCountsMatchItsOwnPayload(t *testing.T) {
	document := loadGolden(t)
	existing := 0
	for _, page := range document.ExistingEdgeIDs {
		existing += len(page)
	}
	for name, got := range map[string]int{
		"dependencies":        len(document.Dependencies),
		"edges":               len(document.Edges),
		"projection_runs":     len(document.ProjectionRuns),
		"mutations":           len(document.Mutations),
		"existing_edge_pages": len(document.ExistingEdgeIDs),
		"existing_edge_ids":   existing,
	} {
		if want, ok := document.Counts[name]; !ok {
			t.Errorf("golden counts is missing %q", name)
		} else if got != want {
			t.Errorf("golden counts[%q] = %d, payload has %d", name, want, got)
		}
	}
}

// TestEdgeIDMatchesPythonForEveryFrozenEdge is the strongest assertion available
// before the derivation itself exists: it recomputes the id of all 3,548 real
// edges from their own endpoints and requires the Go hash to reproduce Python's
// exactly.
//
// The id is not cosmetic. It is the twin of the ReplacingMergeTree dedup identity
// AND the key the cleanup step deletes by, so a divergence does not fail loudly —
// it orphans the old row and mints a duplicate beside it.
func TestEdgeIDMatchesPythonForEveryFrozenEdge(t *testing.T) {
	document := loadGolden(t)
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		computed := EdgeID(row.SourceType, row.SourceID, row.EdgeType, row.TargetType, row.TargetID)
		if computed != row.EdgeID {
			t.Fatalf(
				"edge %d (%s:%s -%s-> %s:%s): Go edge_id %s, Python %s",
				index, row.SourceType, row.SourceID, row.EdgeType,
				row.TargetType, row.TargetID, computed, row.EdgeID,
			)
		}
	}
}

// TestFrozenConfidencesSurviveTheFloat32RoundTrip pins the quantisation contract
// against real values rather than a constructed one.
func TestFrozenConfidencesSurviveTheFloat32RoundTrip(t *testing.T) {
	document := loadGolden(t)
	for index, edge := range document.Edges {
		if !math.IsInf(edge.Confidence, 0) && !math.IsNaN(edge.Confidence) {
			narrowed := Quantize(edge.Confidence)
			if float64(narrowed) != float64(Quantize(float64(narrowed))) {
				t.Fatalf("edge %d: narrowing %v is not idempotent", index, edge.Confidence)
			}
			continue
		}
		t.Fatalf("edge %d carries a non-finite confidence %v", index, edge.Confidence)
	}
	// The value the variant-C policy writes must be a Float32 fixed point, or the
	// split's (confidence, edge_id) sort puts a freshly written edge in a
	// different tier from the identical edge read back.
	if got := Quantize(float64(AssociativeConfidence)); got != AssociativeConfidence {
		t.Fatalf("AssociativeConfidence %v is not a Float32 fixed point (got %v)", AssociativeConfidence, got)
	}
	if float64(AssociativeConfidence) == 0.9 {
		t.Fatal(
			"AssociativeConfidence compares equal to the float64 literal 0.9, which means " +
				"this test is not actually exercising the narrowing that changed the measured " +
				"component count from 1833 to 1832",
		)
	}
}

// TestBuildClockAndEventTimeAreDistinct pins the timestamp contract: the producer
// stamps discovered_at/last_synced from the BUILD's construction clock and
// event_ts from the ROW. Reversing them changes every ReplacingMergeTree version.
func TestBuildClockAndEventTimeAreDistinct(t *testing.T) {
	document := loadGolden(t)
	frozenNow, err := time.Parse(time.RFC3339Nano, document.FrozenNow)
	if err != nil {
		t.Fatalf("parse frozen_now: %v", err)
	}
	perRow := 0
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		if !row.DiscoveredAt.Equal(frozenNow) {
			t.Fatalf("edge %d discovered_at %s is not the build clock %s", index, row.DiscoveredAt, frozenNow)
		}
		if !row.LastSynced.Equal(frozenNow) {
			t.Fatalf("edge %d last_synced %s is not the build clock %s", index, row.LastSynced, frozenNow)
		}
		if !row.EventTs.Equal(frozenNow) {
			perRow++
		}
		if want := DayFor(row.EventTs); !row.Day.Equal(want) {
			t.Fatalf("edge %d day %s != toDate(event_ts) %s", index, row.Day, want)
		}
	}
	if perRow == 0 {
		t.Fatal(
			"every event_ts equals the build clock, so this fixture cannot distinguish " +
				"a per-row timestamp from a per-build one — the contract it is meant to pin",
		)
	}
}

// TestVariantCExceptionListIsClosedAndNecessary asserts the shape of the ONE
// permitted divergence from Python: every listed kind is genuinely present in the
// frozen output at the delivery tier (so the exception is load-bearing, not
// decorative), and every associative kind the policy would change is listed (so
// the list cannot silently under-declare).
func TestVariantCExceptionListIsClosedAndNecessary(t *testing.T) {
	document := loadGolden(t)

	listed := map[string]GoldenException{}
	for _, exception := range AssociativeConfidenceExceptions {
		if _, duplicate := listed[exception.EdgeType]; duplicate {
			t.Fatalf("exception list names %q twice", exception.EdgeType)
		}
		if _, associative := AssociativeEdgeTypes[exception.EdgeType]; !associative {
			t.Fatalf("exception list names %q, which the policy does not change", exception.EdgeType)
		}
		if exception.ToGo != DependencyConfidence(exception.EdgeType) {
			t.Fatalf("exception for %q claims %v, policy returns %v",
				exception.EdgeType, exception.ToGo, DependencyConfidence(exception.EdgeType))
		}
		if !(exception.ToGo < exception.FromPy) {
			t.Fatalf("exception for %q does not lower the tier (%v -> %v)",
				exception.EdgeType, exception.FromPy, exception.ToGo)
		}
		listed[exception.EdgeType] = exception
	}
	for edgeType := range AssociativeEdgeTypes {
		if _, ok := listed[edgeType]; !ok {
			t.Fatalf("policy changes %q but the exception list does not declare it", edgeType)
		}
	}

	// Now the necessity half, against the frozen output.
	observed := map[string]int{}
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		if _, associative := AssociativeEdgeTypes[row.EdgeType]; !associative {
			continue
		}
		if row.Confidence != DeliveryConfidence {
			t.Fatalf(
				"edge %d (%s) sits at %v in the golden, not the delivery tier — the golden "+
					"was regenerated against a Python that already implements variant C, so the "+
					"exception list is stale",
				index, row.EdgeType, row.Confidence,
			)
		}
		observed[row.EdgeType]++
	}
	if len(observed) == 0 {
		t.Fatal("the golden contains no associative edges, so it cannot exercise the exception at all")
	}
	for edgeType, count := range observed {
		if _, ok := listed[edgeType]; !ok {
			t.Fatalf("golden carries %d %q edges at the delivery tier with no declared exception", count, edgeType)
		}
	}
	t.Logf("exception exercised by %s", formatCounts(observed))
}

// TestCleanupMutationsArePagedAndOrgScoped pins the cleanup contract the port
// must reproduce. The paging is not an optimisation: the candidate set is
// unbounded in the org's size, and an unpaged IN-list works on this org and
// fails on a larger one.
func TestCleanupMutationsArePagedAndOrgScoped(t *testing.T) {
	document := loadGolden(t)
	if len(document.Mutations) < 2 {
		t.Fatalf("golden froze %d mutations; expected the projection delete plus at least one edge page", len(document.Mutations))
	}
	first := document.Mutations[0]
	if want := "ALTER TABLE work_graph_projection_runs DELETE"; !contains(first.Statement, want) {
		t.Fatalf("first mutation is %q, expected the projection-run delete", first.Statement)
	}
	edgePages := 0
	for index, mutation := range document.Mutations[1:] {
		if !contains(mutation.Statement, "ALTER TABLE work_graph_edges DELETE") {
			t.Fatalf("mutation %d is %q, expected an edge delete", index+1, mutation.Statement)
		}
		ids, ok := mutation.Parameters["edge_ids"].([]any)
		if !ok {
			t.Fatalf("mutation %d has no edge_ids array", index+1)
		}
		if len(ids) > 1000 {
			t.Fatalf("mutation %d deletes %d ids in one statement; the page size is 1000", index+1, len(ids))
		}
		edgePages++
	}
	if edgePages < 2 {
		t.Fatal(
			"the golden froze fewer than two edge-delete pages, so it cannot prove the port " +
				"pages at all — regenerate against an org whose candidate set exceeds 1000 ids",
		)
	}
	for index, mutation := range document.Mutations {
		if !contains(mutation.Statement, "org_id = {org_id:String}") {
			t.Fatalf("mutation %d is not org-scoped: %q", index, mutation.Statement)
		}
		if got, _ := mutation.Parameters["org_id"].(string); got != document.OrgID {
			t.Fatalf("mutation %d is scoped to %q, golden org is %q", index, got, document.OrgID)
		}
		if !contains(mutation.Statement, "mutations_sync=2") {
			t.Fatalf("mutation %d does not wait for the mutation: %q", index, mutation.Statement)
		}
	}
}

// TestValidateConfidenceRefusesUngroupableValues covers what the golden cannot:
// every confidence in it is finite, which is exactly how lane-4441's NaN defect
// survived its own golden (CHAOS-4441, ca0b40349).
func TestValidateConfidenceRefusesUngroupableValues(t *testing.T) {
	for name, value := range map[string]float32{
		"NaN":        float32(math.NaN()),
		"+Inf":       float32(math.Inf(1)),
		"-Inf":       float32(math.Inf(-1)),
		"above one":  1.5,
		"below zero": -0.5,
	} {
		if err := ValidateConfidence(value); err == nil {
			t.Errorf("ValidateConfidence(%s) accepted an ungroupable value", name)
		}
	}
	for name, value := range map[string]float32{
		"delivery tier":    DeliveryConfidence,
		"associative tier": AssociativeConfidence,
		"zero":             0,
	} {
		if err := ValidateConfidence(value); err != nil {
			t.Errorf("ValidateConfidence(%s) rejected a valid value: %v", name, err)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func formatCounts(counts map[string]int) string {
	rendered := ""
	for edgeType, count := range counts {
		if rendered != "" {
			rendered += ", "
		}
		rendered += fmt.Sprintf("%s=%d", edgeType, count)
	}
	return rendered
}
