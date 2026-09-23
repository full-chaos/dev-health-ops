package goapiproof

import "testing"

// TestDeletedPythonBodyOperations_EveryRequestIsOverridden is the guard
// team-lead's design for CHAOS-6294 asked for: every corpus entry whose
// route is in DeletedPythonBodyOperations must never still declare a real
// baseline-equality comparison against the now-dead Python handler. It
// walks the COMMITTED, already-init()-ed corpus (restEndpointSpecs) and
// fails if any request under a deleted-body operation still breaks
// RequestBreaksDeletedBodyInvariant.
func TestDeletedPythonBodyOperations_EveryRequestIsOverridden(t *testing.T) {
	if len(DeletedPythonBodyOperations) == 0 {
		t.Fatal("DeletedPythonBodyOperations is empty -- this guard's premise no longer holds")
	}
	checked := 0
	for operation := range DeletedPythonBodyOperations {
		spec, ok := restEndpointSpecs[operation]
		if !ok {
			t.Fatalf("DeletedPythonBodyOperations names %q, which restEndpointSpecs does not declare", operation)
		}
		for _, req := range spec.Requests {
			checked++
			if RequestBreaksDeletedBodyInvariant(req) {
				t.Errorf("%s/%s still declares a live shape against the deleted Python body: WantCandidateStatus=%d WantBaselineStatus=%d BodyMode=%q",
					operation, req.Name, req.WantCandidateStatus, req.WantBaselineStatus, req.BodyMode)
			}
		}
	}
	if checked == 0 {
		t.Fatal("swept zero requests -- the sweep covered nothing")
	}
	t.Logf("checked %d requests across %d deleted-body operations", checked, len(DeletedPythonBodyOperations))
}

// TestRequestBreaksDeletedBodyInvariant_Cells proves the guard above can
// actually fail: one cell per way a Request can regress back to a live
// shape, each observed failing the OLD test (i.e. this predicate) before
// counting as covered -- the "observe every guard failing" rule.
func TestRequestBreaksDeletedBodyInvariant_Cells(t *testing.T) {
	for _, cell := range []struct {
		name string
		req  RESTRequest
		want bool
	}{
		{
			"correctly overridden: candidate_shape, sentinel baseline",
			RESTRequest{WantCandidateStatus: 200, WantBaselineStatus: 500, BodyMode: RESTBodyModeCandidateShape, StatusDivergenceReason: PythonBodyDeletedReason},
			false,
		},
		{
			"correctly overridden: status_only, sentinel baseline, non-200 candidate",
			RESTRequest{WantCandidateStatus: 422, WantBaselineStatus: 500, BodyMode: RESTBodyModeStatusOnly, StatusDivergenceReason: PythonBodyDeletedReason},
			false,
		},
		{
			"correctly left alone: framework-level 422/422 parity",
			RESTRequest{WantCandidateStatus: 422, WantBaselineStatus: 422, BodyMode: RESTBodyModeJSON},
			false,
		},
		{
			"regression: real happy path against a dead baseline",
			RESTRequest{WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeJSON},
			true,
		},
		{
			"regression: baseline still declared 200 even under status_only",
			RESTRequest{WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeStatusOnly},
			true,
		},
		{
			"regression: full JSON parity against a handler-computed 404/404 (not framework-level)",
			RESTRequest{WantCandidateStatus: 404, WantBaselineStatus: 404, BodyMode: RESTBodyModeJSON},
			true,
		},
		{
			"regression: JSON parity against an already-divergent pair",
			RESTRequest{WantCandidateStatus: 200, WantBaselineStatus: 503, BodyMode: RESTBodyModeJSON, StatusDivergenceReason: "stale"},
			true,
		},
	} {
		t.Run(cell.name, func(t *testing.T) {
			if got := RequestBreaksDeletedBodyInvariant(cell.req); got != cell.want {
				t.Fatalf("RequestBreaksDeletedBodyInvariant(%+v) = %v, want %v", cell.req, got, cell.want)
			}
		})
	}
}

// TestDeletedPythonBodyOperations_ArrayShapedSubsetIsWellFormed pins that
// deletedBodyArrayShapedOperations is a SUBSET of DeletedPythonBodyOperations
// (every array-shaped route is itself a deleted-body one -- there would be
// nothing to mark array-shaped otherwise) and that every request init()
// left in RESTBodyModeCandidateShape under one of those operations
// carries CandidateShapeArray true, never false -- a request under a
// NON-array-shaped operation must carry CandidateShapeArray false.
func TestDeletedPythonBodyOperations_ArrayShapedSubsetIsWellFormed(t *testing.T) {
	for operation := range deletedBodyArrayShapedOperations {
		if !DeletedPythonBodyOperations[operation] {
			t.Errorf("%s is in deletedBodyArrayShapedOperations but not DeletedPythonBodyOperations", operation)
		}
	}
	for operation := range DeletedPythonBodyOperations {
		spec, ok := restEndpointSpecs[operation]
		if !ok {
			t.Fatalf("SpecForREST(%s): no such operation", operation)
		}
		wantArray := deletedBodyArrayShapedOperations[operation]
		for _, req := range spec.Requests {
			if req.BodyMode != RESTBodyModeCandidateShape {
				continue
			}
			if req.CandidateShapeArray != wantArray {
				t.Errorf("%s/%s CandidateShapeArray = %v, want %v (operation array-shaped = %v)", operation, req.Name, req.CandidateShapeArray, wantArray, wantArray)
			}
		}
	}
}
