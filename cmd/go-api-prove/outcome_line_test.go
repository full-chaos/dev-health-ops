package main

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// The terminal line for an executed measurement said only
// "(N findings, M outside ...)", so a receipt admitted because one leaf value
// differed and one admitted because a regression covered "Go returned no
// rows" printed the same line. It now names the shapes on both sides.
func TestTheOutcomeLineNamesWhatTheCitationCovered(t *testing.T) {
	leaf := goapiproof.Outcome{
		Operation: "hotspots", Mode: "primary", Route: "edge", TerminalState: goapiproof.TerminalStateMismatch,
		BaselineDefects: []string{"CHAOS-5447"}, CoveredByShape: map[string]int{"value": 384, "null": 7},
	}
	empty := leaf
	empty.CoveredByShape = nil
	empty.OutsideByShape = map[string]int{"length": 1}
	empty.DifferencesOutsideBaselineDefect = 1
	leafLine, emptyLine := executedOutcomeLine(leaf), executedOutcomeLine(empty)
	for _, c := range []struct{ line, want string }{
		{leafLine, "covered[null=7 value=384] outside[]"},
		{emptyLine, "covered[] outside[length=1]"},
	} {
		if !strings.HasSuffix(c.line, c.want) {
			t.Fatalf("line %q does not end with %q", c.line, c.want)
		}
	}
	if leafLine == emptyLine {
		t.Fatalf("two different coverages printed the same line: %q", leafLine)
	}
	t.Logf("leaf admission : %s", leafLine)
	t.Logf("empty result   : %s", emptyLine)
}

// An operation proven under a stochastic leaf class prints its own word, so
// its line never reads like one proven by equality or like a failed one.
func TestTheOutcomeLineNamesAStochasticLeafClassProof(t *testing.T) {
	classProven := goapiproof.Outcome{
		Operation: "capacityForecast", Mode: "canary", Route: "edge", TerminalState: goapiproof.TerminalStateMismatch,
		BaselineDefects: []string{goapiproof.StochasticLeafCitationPrefix + "ABC-123"},
		ProvenUnder:     goapiproof.ProvenUnderStochasticLeafClass,
		CoveredByShape:  map[string]int{goapiproof.ShapeStochasticLeaf: 2},
	}
	failed := classProven
	failed.ProvenUnder = ""
	failed.DifferencesOutsideBaselineDefect = 1
	classLine, failedLine := executedOutcomeLine(classProven), executedOutcomeLine(failed)
	if !strings.Contains(classLine, " mismatch PROVEN_UNDER=stochastic_leaf_class ") {
		t.Fatalf("class-proven line does not name the class: %q", classLine)
	}
	if strings.Contains(failedLine, "PROVEN_UNDER") {
		t.Fatalf("a line with no class proof names one: %q", failedLine)
	}
	t.Logf("class proven: %s", classLine)
}

// An operation proven by the go-only class prints its own verdict word, never
// a two-plane one, so a reader cannot mistake it for a match or a class proof.
func TestTheOutcomeLineNamesAGoOnlyProof(t *testing.T) {
	goOnly := goapiproof.Outcome{
		Operation: "capacityForecast", Mode: "canary", Route: "edge", TerminalState: goapiproof.TerminalStateMismatch,
		BaselineDefects: []string{goapiproof.GoOnlyCitationPrefix + "op=x"},
		ProvenUnder:     goapiproof.ProvenUnderGoOnly,
	}
	line := executedOutcomeLine(goOnly)
	if !strings.Contains(line, goapiproof.VerdictGoOnly+" (no two-plane baseline)") {
		t.Fatalf("go-only line does not carry its verdict word: %q", line)
	}
	if strings.Contains(line, " match ") || strings.Contains(line, "PROVEN_UNDER") || strings.Contains(line, " mismatch ") {
		t.Fatalf("go-only line reads like another proof: %q", line)
	}
	inert := goOnly
	inert.ProvenUnder = ""
	inert.DifferencesOutsideBaselineDefect = 1
	if strings.Contains(executedOutcomeLine(inert), goapiproof.VerdictGoOnly) {
		t.Fatal("an unproven measurement prints the go-only verdict")
	}
}

// A request measured under the declared-baseline-error class prints its own
// verdict word, never a two-plane one; without the class proof it does not.
func TestTheOutcomeLineNamesADeclaredBaselineErrorProof(t *testing.T) {
	measured := goapiproof.Outcome{
		Operation: "aiOpportunities", Mode: "canary", Route: "edge", TerminalState: goapiproof.TerminalStateUnsupported,
		ProvenUnder: goapiproof.ProvenUnderDeclaredBaselineError,
	}
	line := executedOutcomeLine(measured)
	if !strings.Contains(line, goapiproof.VerdictDeclaredBaselineError+" (declared Python failure)") {
		t.Fatalf("line does not carry its verdict word: %q", line)
	}
	if strings.Contains(line, " match ") || strings.Contains(line, "PROVEN_UNDER") || strings.Contains(line, goapiproof.VerdictGoOnly) {
		t.Fatalf("line reads like another proof: %q", line)
	}
	bare := measured
	bare.ProvenUnder = ""
	if strings.Contains(executedOutcomeLine(bare), goapiproof.VerdictDeclaredBaselineError) {
		t.Fatal("an unproven measurement prints the class verdict")
	}
}
