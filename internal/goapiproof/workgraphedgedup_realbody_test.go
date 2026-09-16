package goapiproof

import (
	"encoding/json"
	"os"
	"testing"
)

// This file exercises WorkGraphEdgeDedupShape through the SAME path
// go-api-prove's Runner drives a live comparison through -- SpecFor for
// the operation's own declared Options, DecodeSnapshot for each leg, and
// Compare -- against two real workGraphEdges response bodies captured
// from the candidate and baseline planes, rather than the hand-authored
// literals workgraphedgedup_test.go uses.
//
// The two planes format a whole-number field differently: the baseline
// (Python) body writes it as "1.0", the candidate (Go) body writes the
// same value as "1". workgraphedgedup_test.go's hand-authored fixtures
// never exercise that, because every literal in it spells the field the
// same way on both sides.

const (
	workGraphEdgesDedupBaselinePath  = "testdata/workgraphedges_dedup_baseline_3d15cee8.json"
	workGraphEdgesDedupCandidatePath = "testdata/workgraphedges_dedup_candidate_71b4c5e2.json"
)

// TestWorkGraphEdgeDedupShape_RealCapturedBodyIsFullyCovered pins the
// registered operation's declared duplicate-row citation against a real
// captured pair: the candidate carries 1000 distinct edgeIds, the
// baseline carries the same 1000 rows under only 740 distinct edgeIds
// (260 ids repeated by an unmerged physical duplicate), and every id the
// two pages share agrees field for field. Nothing under
// data.workGraphEdges.edges may be reported outside the citation, and the
// citation must be the one matched, not idle or stale.
func TestWorkGraphEdgeDedupShape_RealCapturedBodyIsFullyCovered(t *testing.T) {
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatalf("SpecFor(workGraphEdges): %v", err)
	}
	if len(spec.Parity.BaselineDefects) != 1 {
		t.Fatalf("workGraphEdges declares %d baseline defects, want exactly 1: %#v", len(spec.Parity.BaselineDefects), spec.Parity.BaselineDefects)
	}
	wantTicket := spec.Parity.BaselineDefects[0].Ticket

	baseline := snapshotFromFile(t, workGraphEdgesDedupBaselinePath)
	candidate := snapshotFromFile(t, workGraphEdgesDedupCandidatePath)

	result := Compare(baseline, candidate, spec.Parity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- covered %v outside %v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{wantTicket}) {
		t.Fatalf("matched = %v, want [%s] -- idle %v stale %v",
			result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}

// TestWorkGraphEdgeDedupShape_RealCapturedBodyWithAGenuineRegressionStaysUncovered
// keeps the shape's safety intent: a shared id whose content really
// differs must still leave the citation stale (or idle), never matched.
// Starting from the same real captured pair the coverage test above
// proves is otherwise fully explained, this mutates ONE shared, non-
// duplicated edge's own field in the candidate leg -- a real per-field
// regression hiding behind an id the two pages share -- and asserts the
// whole plan refuses again, exactly as it did (for the wrong reason)
// before the fix.
func TestWorkGraphEdgeDedupShape_RealCapturedBodyWithAGenuineRegressionStaysUncovered(t *testing.T) {
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatalf("SpecFor(workGraphEdges): %v", err)
	}

	baseline := snapshotFromFile(t, workGraphEdgesDedupBaselinePath)
	candidate := snapshotFromFile(t, mutateSharedEdgeDisplayName(t, workGraphEdgesDedupCandidatePath))

	result := Compare(baseline, candidate, spec.Parity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatal("outside = 0 -- a genuine per-field regression on a shared id must not be admitted by the duplicate-row shape")
	}
	if len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("matched = %v, want none -- a genuine regression must not read as the duplicate-row mechanism", result.BaselineDefectsMatched)
	}
}

// mutateSharedEdgeDisplayName writes a copy of the candidate fixture with
// its first edge's sourceDisplayName changed. That edge's id is present
// exactly once on each side (a shared, non-duplicated id), so the
// mutation is a real disagreement under rule 3, not a duplicate-group
// disagreement under rule 1.
func mutateSharedEdgeDisplayName(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	edges := body["data"].(map[string]any)["workGraphEdges"].(map[string]any)["edges"].([]any)
	first := edges[0].(map[string]any)
	original, _ := first["sourceDisplayName"].(string)
	first["sourceDisplayName"] = original + " -- mutated for the regression test"

	mutated, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("re-encode mutated fixture: %v", err)
	}
	out := t.TempDir() + "/mutated_candidate.json"
	if err := os.WriteFile(out, mutated, 0o600); err != nil {
		t.Fatalf("write mutated fixture: %v", err)
	}
	return out
}
