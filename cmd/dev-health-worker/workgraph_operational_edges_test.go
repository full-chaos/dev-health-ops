package main

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

func TestFlagGuardsAndOperationalIncidentStepNamesMatchDeclaredOrder(t *testing.T) {
	if _, err := newFlagGuardsEdgesPreStep(nil); err == nil {
		t.Fatal("newFlagGuardsEdgesPreStep(nil) should refuse a nil connection")
	}
	if _, err := newOperationalIncidentEdgesPreStep(nil); err == nil {
		t.Fatal("newOperationalIncidentEdgesPreStep(nil) should refuse a nil connection")
	}

	want := buildPreStepOrder()
	if len(want) != 3 || want[1] != "flag_guards_edges" || want[2] != "operational_incident_edges" {
		t.Fatalf("buildPreStepOrder() = %v, want [issue_pr_links flag_guards_edges operational_incident_edges]", want)
	}
}

// TestOperationalIncidentEdgesPreStepSkipsUnscopedClaims pins Python's own
// guard (`_build_operational_incident_edges`: `if not self.config.org_id:
// return 0`) -- the one sub-builder in this family that already refused
// unscoped on the Python side. An empty org must return a quiet zero-edges
// outcome, not an error and not a call into BuildOperationalIncidentEdges
// (whose own RequireOrganizationScope would instead FAIL the whole build,
// which is not what Python did for this specific case).
func TestOperationalIncidentEdgesPreStepSkipsUnscopedClaims(t *testing.T) {
	step := &operationalIncidentEdgesPreStep{conn: nil, now: time.Now}
	fragment, err := step.Run(context.Background(), workgraph.Claim{
		Request: workgraph.Request{OrganizationID: ""},
	})
	if err != nil {
		t.Fatalf("Run with an empty org should not error, got: %v", err)
	}
	if fragment["edges_written"] != 0 {
		t.Fatalf("fragment = %+v, want edges_written=0", fragment)
	}
}

func TestOperationalEdgesWindowForDefaultsMatchPython(t *testing.T) {
	// CORRECTED (codex round chaos-4924-pr-d-r1, P1): run_work_graph_build
	// (work_graph_tasks.py:121-124) always resolves to_date to now and
	// from_date to to_date-30d before BuildConfig is ever constructed, so an
	// absent scope value means "apply the default window", the same as
	// issue_pr_links/pr_commit_*, not "read the whole table unbounded".
	fixedNow := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	window, err := operationalEdgesWindowFor(nil, func() time.Time { return fixedNow })
	if err != nil {
		t.Fatalf("operationalEdgesWindowFor: %v", err)
	}
	// run_work_graph_build's own defaults (work_graph_tasks.py:102-103).
	if window.heuristicDaysWindow != 7 {
		t.Errorf("heuristicDaysWindow = %d, want 7", window.heuristicDaysWindow)
	}
	if window.heuristicConfidence != 0.3 {
		t.Errorf("heuristicConfidence = %v, want 0.3", window.heuristicConfidence)
	}
	wantTo := fixedNow
	wantFrom := fixedNow.AddDate(0, 0, -30)
	if !window.toDate.Equal(wantTo) {
		t.Errorf("toDate = %v, want %v", window.toDate, wantTo)
	}
	if !window.fromDate.Equal(wantFrom) {
		t.Errorf("fromDate = %v, want %v", window.fromDate, wantFrom)
	}
	if window.repoID != nil {
		t.Errorf("repoID should be nil by default, got %v", window.repoID)
	}
}

func TestOperationalEdgesWindowForParsesExplicitHeuristics(t *testing.T) {
	window, err := operationalEdgesWindowFor([]byte(`{"heuristic_window":14,"heuristic_confidence":0.5}`), time.Now)
	if err != nil {
		t.Fatalf("operationalEdgesWindowFor: %v", err)
	}
	if window.heuristicDaysWindow != 14 {
		t.Errorf("heuristicDaysWindow = %d, want 14", window.heuristicDaysWindow)
	}
	if window.heuristicConfidence != 0.5 {
		t.Errorf("heuristicConfidence = %v, want 0.5", window.heuristicConfidence)
	}
}

func TestOperationalEdgesWindowForRefusesNullScope(t *testing.T) {
	if _, err := operationalEdgesWindowFor([]byte(`null`), time.Now); err == nil {
		t.Fatal("a null scope must be refused, matching the bridge's own rejection")
	}
}

func TestOperationalEdgesWindowForRefusesUnsupportedField(t *testing.T) {
	if _, err := operationalEdgesWindowFor([]byte(`{"not_a_real_field":true}`), time.Now); err == nil {
		t.Fatal("an unsupported scope field must be refused before the bridge would reject it")
	}
}
