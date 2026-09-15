package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// D15/R4's output contract: a run that measured NOTHING must say so in
// explicit zeros, in both the human lines and the JSON report. "prove
// found no problems" and "prove ran no measurements" must not look alike.
func TestEmitReportCarriesExplicitZeros(t *testing.T) {
	dir := t.TempDir()
	reportPath := filepath.Join(dir, "report.json")

	f := flags{orgID: "70d529e0", window: goapiproof.DefaultWindow(), reportPath: reportPath}
	registry := goapiproof.RegistryView{SchemaDigest: "sha256:29d509cd", BuildIdentity: "b18e56fa7"}
	summary := goapiproof.Summary{
		Attempted:       15,
		Admitted:        0,
		Executed:        0,
		Refused:         15,
		ByTerminalState: map[string]int{},
		ByRefusalReason: map[string]int{goapiproof.RefusalNotRouted: 15},
	}
	outcomes := []goapiproof.Outcome{{
		Operation:     "featureFlags",
		Mode:          "python",
		RefusalReason: goapiproof.RefusalNotRouted,
		RefusalDetail: `mode="python" is not a Go-serving mode`,
	}}

	if err := emitReport(f, registry, outcomes, summary, nil, nil); err != nil {
		t.Fatalf("emitReport: %v", err)
	}

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode report: %v", err)
	}

	body := string(raw)
	// `executed` must be PRESENT with value 0, not omitted -- an omitted
	// zero is exactly the silence this contract forbids.
	if !strings.Contains(body, `"executed": 0`) {
		t.Fatalf("the report must carry an explicit executed=0:\n%s", body)
	}
	// admitted is the count that says whether ANYTHING satisfied the
	// preconditions. An omitted zero here would hide the difference between
	// "nothing was admissible" and "everything matched".
	if !strings.Contains(body, `"admitted": 0`) {
		t.Fatalf("the report must carry an explicit admitted=0:\n%s", body)
	}
	if !strings.Contains(body, goapiproof.RefusalNotRouted) {
		t.Fatalf("the report must name every refusal reason:\n%s", body)
	}
	if decoded["candidate_build"] != "b18e56fa7" {
		t.Fatalf("the report must record the build the receipts name, got %v", decoded["candidate_build"])
	}
	if decoded["stage"] != goapiproof.Stage {
		t.Fatalf("the report must record the stage, got %v", decoded["stage"])
	}
}

func TestSortedKeysIsDeterministic(t *testing.T) {
	got := sortedKeys(map[string]int{"b": 1, "a": 2, "c": 3})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// TestInstanceIDFlagParsesOperationEqualsValue pins -instance-id's
// operation=value shape: the mechanism the `pr` operationSpecs entry's own
// comment calls for ("a flag, or a row read from the org's own data") to
// let a real id reach Config.InstanceIDs without inventing one.
func TestInstanceIDFlagParsesOperationEqualsValue(t *testing.T) {
	m := instanceIDFlag{}
	if err := m.Set("pr=9f5c2e6a-real-pr-id"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := m["pr"]; got != "9f5c2e6a-real-pr-id" {
		t.Fatalf("m[%q] = %q, want %q", "pr", got, "9f5c2e6a-real-pr-id")
	}
	// Repeatable: a second -instance-id for a different operation adds,
	// never replaces.
	if err := m.Set("featureFlagEvents=irrelevant-for-this-operation"); err != nil {
		t.Fatalf("Set (second): %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("expected 2 entries after 2 distinct -instance-id flags, got %d: %v", len(m), m)
	}
}

// TestInstanceIDFlagRefusesTheShapesThatAreNotOperationEqualsValue guards
// the failure this flag exists to prevent: a malformed value silently
// becoming a no-op, or worse, an invented id nobody typed.
func TestInstanceIDFlagRefusesTheShapesThatAreNotOperationEqualsValue(t *testing.T) {
	for _, bad := range []string{"", "pr", "=9f5c2e6a", "pr=", "=", "pr:9f5c2e6a"} {
		m := instanceIDFlag{}
		if err := m.Set(bad); err == nil {
			t.Fatalf("Set(%q) = nil error, want a refusal (got map %v)", bad, m)
		}
	}
}

// TestInstanceIDFlagStringNeverPanicsOnTheZeroValue pins the shape flag's
// own package requires: String is called while building usage text before
// Set has ever run, and a nil map must not panic there.
func TestInstanceIDFlagStringNeverPanicsOnTheZeroValue(t *testing.T) {
	var m instanceIDFlag
	if got := m.String(); got != "" {
		t.Fatalf("String() on the zero value = %q, want empty", got)
	}
}
