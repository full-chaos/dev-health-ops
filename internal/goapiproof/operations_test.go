package goapiproof

import (
	"encoding/json"
	"strings"
	"testing"
)

// The fifteen operations the running query-api registered at the
// 2026-09-07 measurement (enablement artifact 10-status-canary.json /
// src/dev_health_ops/api/graphql/go_api_operations.json). Written out
// rather than loaded from that JSON on purpose: this test's job is to
// fail when the two DISAGREE, and a test that reads the same file the
// code reads can only ever agree with itself.
var registeredAt20260907 = []string{
	"capacityForecast", "capacityForecasts", "cognitiveLoad", "complexityTimeseries",
	"featureFlags", "flowMatrix", "hotspots", "investmentBreakdown", "investmentFull",
	"operatingReview", "reviewEdges", "throughputForecast",
	"workGraphArtifacts", "workGraphEdges", "workGraphFlow",
}

func TestAssertCoverageAcceptsTheRegisteredSet(t *testing.T) {
	if err := AssertCoverage(registeredAt20260907); err != nil {
		t.Fatalf("the committed table must cover every registered operation: %v", err)
	}
}

// D15/R4: an operation the running binary registers but this table cannot
// build a request for must FAIL the run, never be quietly skipped.
func TestAssertCoverageRefusesAnUncoveredOperation(t *testing.T) {
	err := AssertCoverage(append(append([]string(nil), registeredAt20260907...), "somethingBrandNew"))
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
	err := AssertCoverage(registeredAt20260907[:len(registeredAt20260907)-1])
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
	windowless := map[string]bool{
		"capacityForecast": true, "capacityForecasts": true, "featureFlags": true,
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
			prefix := "data." + operation + "."
			if !strings.HasPrefix(path, prefix) {
				t.Fatalf("%s declares exclusion %q outside its own subtree (want prefix %q)", operation, path, prefix)
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
