package goapiproof

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The operations the running query-api registers, written out rather
// than loaded from the same place the code reads: this test's job is to
// fail when two artefacts DISAGREE, and a test that reads the code's own
// source of truth can only ever agree with itself.
//
// Fifteen at the 2026-09-07 measurement (enablement artifact
// 10-status-canary.json / go_api_operations.json); seventeen since
// CHAOS-4991 registered `pr` and CHAOS-5523 registered
// `featureFlagEvents` on 2026-09-09.
//
// Keeping this list correct by hand is exactly what failed: both
// operations were registered without an entry in operationSpecs, and
// nothing caught it until go-api-prove refused a live JOB 5 run.
// TestOperationSpecsCoverExactlyWhatQueryAPIRegisters (registered_set_test.go)
// is the guard that does not depend on anyone remembering; this list
// stays because the refusal tests below need a known-good base to
// perturb, and because a second, independent statement of the set is
// what makes a drift legible rather than merely detected.
var registeredOperations = []string{
	"capacityForecast", "capacityForecasts", "cognitiveLoad", "complexityTimeseries",
	"featureFlagEvents", "featureFlags", "flowMatrix", "hotspots",
	"investmentBreakdown", "investmentFull", "operatingReview", "pr",
	"reviewEdges", "throughputForecast",
	"workGraphArtifacts", "workGraphEdges", "workGraphFlow",
}

func TestAssertCoverageAcceptsTheRegisteredSet(t *testing.T) {
	if err := AssertCoverage(registeredOperations); err != nil {
		t.Fatalf("the committed table must cover every registered operation: %v", err)
	}
}

// D15/R4: an operation the running binary registers but this table cannot
// build a request for must FAIL the run, never be quietly skipped.
func TestAssertCoverageRefusesAnUncoveredOperation(t *testing.T) {
	err := AssertCoverage(append(append([]string(nil), registeredOperations...), "somethingBrandNew"))
	if err == nil {
		t.Fatal("an uncovered registered operation must fail, not be skipped")
	}
	if !strings.Contains(err.Error(), "somethingBrandNew") {
		t.Fatalf("the failure must NAME the uncovered operation, got %v", err)
	}
}

// The opposite direction: a stale entry here reads as coverage while
// proving nothing about a binary that no longer registers it.
func TestAssertCoverageRefusesAStaleEntry(t *testing.T) {
	err := AssertCoverage(registeredOperations[:len(registeredOperations)-1])
	if err == nil {
		t.Fatal("an operation covered here but not registered must fail")
	}
	if !strings.Contains(err.Error(), "workGraphFlow") {
		t.Fatalf("the failure must NAME the stale entry, got %v", err)
	}
}

func TestSpecForRefusesAnUnknownOperation(t *testing.T) {
	if _, err := SpecFor("noSuchOperation"); err == nil {
		t.Fatal("an unknown operation must be a refusal, never an empty request")
	}
}

// Every spec must actually build variables -- a nil Variables func would
// panic at request time, deep inside a run, instead of failing here.
func TestEverySpecBuildsVariables(t *testing.T) {
	window := DefaultWindow()
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		if spec.Variables == nil {
			t.Fatalf("%s has no Variables builder", operation)
		}
		variables := spec.Variables("70d529e0", window)
		if len(variables) == 0 {
			t.Fatalf("%s built an empty variables object", operation)
		}
		if _, err := json.Marshal(variables); err != nil {
			t.Fatalf("%s built variables that do not encode: %v", operation, err)
		}
	}
}

// Every windowed operation must actually USE the window it is handed --
// a spec that ignores it would silently query a different range than the
// operator asked for, and two runs with different --since flags would
// produce identical, indistinguishable receipts.
func TestWindowedSpecsUseTheWindow(t *testing.T) {
	base := DefaultWindow()
	moved := Window{
		SinceUTC:  "2025-01-01T00:00:00Z",
		UntilUTC:  "2025-02-01T00:00:00Z",
		SinceDate: "2025-01-01",
		UntilDate: "2025-02-01",
		WeekStart: "2025-01-06",
	}

	// The operations whose SDL input carries no date range at all. Named
	// explicitly so adding a windowed operation without wiring its window
	// fails this test rather than joining a silent allowlist.
	// featureFlagEvents and pr are windowless for the same reason
	// featureFlags is: they select current state or one stored row, not a
	// range. `pr` is additionally refused at run time (InstanceVariable),
	// but its Variables func is still checked here -- a spec that quietly
	// ignored the window would be wrong whether or not the run sends it.
	windowless := map[string]bool{
		"capacityForecast": true, "capacityForecasts": true,
		"featureFlagEvents": true, "featureFlags": true, "pr": true,
		"throughputForecast": true, "workGraphArtifacts": true, "workGraphEdges": true,
		"workGraphFlow": true,
	}

	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		encode := func(w Window) string {
			encoded, err := json.Marshal(spec.Variables("70d529e0", w))
			if err != nil {
				t.Fatalf("%s: %v", operation, err)
			}
			return string(encoded)
		}
		same := encode(base) == encode(moved)
		if windowless[operation] != same {
			if same {
				t.Fatalf("%s ignores the request window but is not declared windowless", operation)
			}
			t.Fatalf("%s is declared windowless but its request changed with the window", operation)
		}
	}
}

// Every declared volatile-field path must be rooted at `data.<operation>`
// -- an exclusion aimed at another operation's subtree could never match,
// and would sit here reading as coverage.
func TestVolatileExclusionsAreRootedAtTheirOperation(t *testing.T) {
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		for path, reason := range spec.Parity.VolatileFields {
			prefix := "data." + spec.ResponseRoot + "."
			if !strings.HasPrefix(path, prefix) {
				t.Fatalf("%s declares exclusion %q outside the subtree its document selects (want prefix %q)", operation, path, prefix)
			}
			if len(strings.TrimSpace(reason)) < 40 {
				t.Fatalf("%s exclusion %q has no written reason (got %q)", operation, path, reason)
			}
		}
	}
}

func TestWindowValidateRejectsAnIncompleteWindow(t *testing.T) {
	if err := DefaultWindow().Validate(); err != nil {
		t.Fatalf("the default window must be valid: %v", err)
	}
	incomplete := DefaultWindow()
	incomplete.WeekStart = ""
	if err := incomplete.Validate(); err == nil {
		t.Fatal("an empty window field must be refused, not sent as an empty string")
	}
}

// Every declared Tier-B entry must carry a written reason and sit inside
// the subtree its operation's REGISTERED DOCUMENT selects -- which is not
// always the operation's name. Checking against the name is what an
// earlier version of this test did, and it rejected every correct
// investment/flowMatrix path (those documents select `analytics`).
func TestTierBDeclarationsAreReasonedAndRooted(t *testing.T) {
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		for path, reason := range spec.Parity.FloatTierB {
			prefix := "data." + spec.ResponseRoot + "."
			if !strings.HasPrefix(path, prefix) {
				t.Errorf("%s declares Tier-B path %q outside the subtree its document selects (want prefix %q)", operation, path, prefix)
			}
			if len(strings.TrimSpace(reason)) < 20 {
				t.Errorf("%s Tier-B path %q has no written reason (got %q)", operation, path, reason)
			}
		}
	}
}

// Same, for baseline defects: every entry must name a ticket and at least
// one path, or it declares nothing and can only ever read as coverage.
func TestBaselineDefectDeclarationsNameATicketAndPaths(t *testing.T) {
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		for _, defect := range spec.Parity.BaselineDefects {
			if !strings.HasPrefix(defect.Ticket, "CHAOS-") {
				t.Errorf("%s declares a baseline defect with ticket %q -- a defect with no ticket is an opinion", operation, defect.Ticket)
			}
			if len(defect.Paths) == 0 {
				t.Errorf("%s baseline defect %s cites no field paths, so it excuses nothing", operation, defect.Ticket)
			}
			for _, path := range defect.Paths {
				prefix := "data." + spec.ResponseRoot
				if path != prefix && !strings.HasPrefix(path, prefix+".") {
					t.Errorf("%s baseline defect %s cites %q outside the subtree its document selects (want prefix %q)", operation, defect.Ticket, path, prefix)
				}
			}
		}
	}
}

// ResponseRoot is load-bearing for every declaration check above, so it
// must be present and must not be quietly assumed equal to the operation
// name -- three of the fifteen operations select a differently-named root.
func TestEverySpecDeclaresItsResponseRoot(t *testing.T) {
	sharedRoots := 0
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		if spec.ResponseRoot == "" {
			t.Errorf("%s declares no ResponseRoot, so its parity paths cannot be checked", operation)
		}
		if spec.ResponseRoot != operation {
			sharedRoots++
		}
	}
	// flowMatrix, investmentBreakdown and investmentFull all select
	// `analytics`. If this ever reads 0, either the documents changed or
	// someone "tidied" ResponseRoot into a copy of the operation name --
	// and the parity-path checks above would silently start passing for
	// paths that can never match.
	if sharedRoots != 3 {
		t.Fatalf("expected 3 operations whose response root differs from their name, got %d", sharedRoots)
	}
}

// RootNullable is a hand-kept mirror of the SDL, so it can drift from it.
// This derives the truth from contracts/graphql/v1/schema.graphql itself --
// a root declared without a trailing `!` is nullable -- and fails if the
// table disagrees.
//
// It exists because round 2's F4 was exactly this class in the other
// direction: the code assumed a fact about the schema (no root is nullable)
// that the schema did not support, and two of fifteen operations became
// unprovable as a result. A guard that reads the schema cannot make that
// assumption.
func TestResponseRootNullabilityMatchesTheSDL(t *testing.T) {
	sdl, err := os.ReadFile(filepath.Join(repoRootFromTest(t), "contracts", "graphql", "v1", "schema.graphql"))
	if err != nil {
		t.Fatalf("read SDL: %v", err)
	}

	checked := 0
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		pattern := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(spec.ResponseRoot) + `\([^)]*\)\s*:\s*([^\n]+)$`)
		match := pattern.FindSubmatch(sdl)
		if match == nil {
			t.Errorf("%s: root field %q not found in the SDL", operation, spec.ResponseRoot)
			continue
		}
		checked++
		declaration := strings.TrimSpace(string(match[1]))
		nullable := !strings.HasSuffix(declaration, "!")
		if spec.RootNullable != nullable {
			t.Errorf("%s: RootNullable=%v but the SDL declares %q (nullable=%v)",
				operation, spec.RootNullable, declaration, nullable)
		}
	}

	// Non-vacuity: every operation must have been resolved against the SDL.
	// A regex that stopped matching would otherwise leave this test silently
	// checking nothing.
	if checked != len(KnownOperations()) {
		t.Fatalf("only %d of %d roots were found in the SDL -- the match has stopped working",
			checked, len(KnownOperations()))
	}
	// And both answers must actually occur, or the comparison is trivial.
	nullableCount := 0
	for _, operation := range KnownOperations() {
		spec, _ := SpecFor(operation)
		if spec.RootNullable {
			nullableCount++
		}
	}
	if nullableCount == 0 || nullableCount == len(KnownOperations()) {
		t.Fatalf("all %d roots share one nullability: this test cannot discriminate", len(KnownOperations()))
	}
}

// repoRootFromTest walks up to the module root.
func repoRootFromTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the repository root")
		}
		dir = parent
	}
}
