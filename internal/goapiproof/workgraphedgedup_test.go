package goapiproof

import (
	"strings"
	"testing"
)

// This file exercises WorkGraphEdgeDedupShape (workgraphedgedup.go)
// directly through small, self-contained edge lists -- the shape-specific
// admission that replaces a blanket "any difference under
// data.workGraphEdges.edges is covered" rule.

func workGraphEdgeDedupOptions() Options {
	return Options{BaselineDefects: []BaselineDefect{{
		Ticket:       "CHAOS-TEST-DEDUP",
		Reason:       "test fixture",
		Paths:        []string{"data.workGraphEdges.edges"},
		Intermittent: true, IntermittentReason: "test fixture",
		WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{
			EdgesListPath: "data.workGraphEdges.edges",
			IDField:       "edgeId",
		},
	}}}
}

func edgesBody(edges string) string {
	return `{"data":{"workGraphEdges":{"edges":[` + edges + `]}}}`
}

const (
	edgeA = `{"edgeId":"a","sourceId":"pr#1","confidence":1.0}`
	edgeB = `{"edgeId":"b","sourceId":"pr#2","confidence":1.0}`
	edgeC = `{"edgeId":"c","sourceId":"pr#3","confidence":1.0}`
)

// A baseline holding a content-identical duplicate physical row for one
// edge, against a candidate that already collapsed it, is fully admitted:
// the shift it produces on every later element is explained, not
// excused.
func TestWorkGraphEdgeDedupShape_ContentIdenticalDuplicateIsAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, edgesBody(edgeA+","+edgeA+","+edgeB))
	candidate := snapshotFromJSON(t, edgesBody(edgeA+","+edgeB+","+edgeC))

	result := Compare(baseline, candidate, workGraphEdgeDedupOptions())
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-DEDUP"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-DEDUP]", result.BaselineDefectsMatched)
	}
}

// Two rows sharing one edgeId that do NOT agree with each other are not a
// duplicate physical version of the same edge -- something else changed
// under that id, and nothing here may explain it away. Per this shape's own per-id verdict, this
// excludes ONLY id "a": id "b" is a genuine, well-behaved shared id
// elsewhere in the SAME comparison and is still admitted, so the ticket
// still reads as matched -- one id's own disagreement narrows admission
// to that id, it does not blind the shape to every other one.
func TestWorkGraphEdgeDedupShape_DuplicateGroupThatDisagreesIsNotAdmitted(t *testing.T) {
	edgeADifferentConfidence := `{"edgeId":"a","sourceId":"pr#1","confidence":0.5}`
	baseline := snapshotFromJSON(t, edgesBody(edgeA+","+edgeADifferentConfidence+","+edgeB))
	candidate := snapshotFromJSON(t, edgesBody(edgeA+","+edgeB+","+edgeC))

	result := Compare(baseline, candidate, workGraphEdgeDedupOptions())
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want at least 1 -- a duplicate group that disagrees must not be silently admitted: findings %+v", result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-DEDUP"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-DEDUP] -- id \"b\" is well-behaved and shared, so it must still be admitted: idle %v stale %v",
			result.BaselineDefectsMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
	for _, f := range result.Findings {
		if f.Kind != FindingMismatch {
			continue
		}
		if strings.Contains(f.Detail, `not admitted by the declared duplicate-row shape`) && !strings.Contains(f.Detail, `"a"`) {
			t.Errorf("excluded finding %s cites a different id than \"a\": %s", f.Path, f.Detail)
		}
	}
}

// The declared mechanism is one-sided: the candidate plane is the one
// that collapses duplicate versions. A repeated id on the candidate side
// is a different, unexplained condition.
func TestWorkGraphEdgeDedupShape_CandidateDuplicateIsNotAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, edgesBody(edgeA+","+edgeA+","+edgeB))
	candidate := snapshotFromJSON(t, edgesBody(edgeA+","+edgeA+","+edgeB))

	result := Compare(baseline, candidate, workGraphEdgeDedupOptions())
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("identical lists on both sides must match, got %q", result.TerminalState)
	}
	if len(result.StaleBaselineDefects) == 0 && len(result.IdleIntermittentBaselineDefects) == 0 {
		t.Fatalf("a comparison with no difference at all must report the entry stale or idle, got matched=%v", result.BaselineDefectsMatched)
	}
}

// A shared id (present, undedupped, on both sides) whose content actually
// disagrees is a real regression, not the duplicate-row mechanism -- the
// shape must leave the whole plan invalid rather than admit it because
// baseline also happens to carry an unrelated duplicate elsewhere.
func TestWorkGraphEdgeDedupShape_SharedIDContentDisagreementIsNotAdmitted(t *testing.T) {
	edgeCDifferentConfidence := `{"edgeId":"c","sourceId":"pr#3","confidence":0.25}`
	baseline := snapshotFromJSON(t, edgesBody(edgeA+","+edgeA+","+edgeCDifferentConfidence))
	candidate := snapshotFromJSON(t, edgesBody(edgeA+","+edgeB+","+edgeC))

	result := Compare(baseline, candidate, workGraphEdgeDedupOptions())
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want at least 1 -- a shared id's real content disagreement must not be admitted: findings %+v", result.Findings)
	}
}

// The registered declaration (operations.go) is wired the way this file's
// synthetic fixtures assume: same path, same id field, a shape actually
// set.
func TestWorkGraphEdgeDedupShape_RegisteredOnWorkGraphEdges(t *testing.T) {
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatalf("SpecFor(workGraphEdges): %v", err)
	}
	if len(spec.Parity.BaselineDefects) != 1 {
		t.Fatalf("want exactly one declared baseline defect, got %d", len(spec.Parity.BaselineDefects))
	}
	defect := spec.Parity.BaselineDefects[0]
	if defect.WorkGraphEdgeDedupShape == nil {
		t.Fatal("workGraphEdges' declared defect must set WorkGraphEdgeDedupShape")
	}
	if defect.WorkGraphEdgeDedupShape.EdgesListPath != "data.workGraphEdges.edges" {
		t.Fatalf("EdgesListPath = %q", defect.WorkGraphEdgeDedupShape.EdgesListPath)
	}
	if defect.WorkGraphEdgeDedupShape.IDField != "edgeId" {
		t.Fatalf("IDField = %q", defect.WorkGraphEdgeDedupShape.IDField)
	}
	if !defect.Intermittent {
		t.Fatal("the defect only exists while the source table holds an unmerged duplicate -- it must be declared Intermittent")
	}
}
