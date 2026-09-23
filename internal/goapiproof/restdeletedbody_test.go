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
			"correctly left alone: framework-level 422/422 parity, no IDBindings",
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
			"regression: a handler-computed 422/422 pair (IDBindings set, like flame's deployment_gap_entity_id_bound_422) is NOT exempted by status code alone",
			RESTRequest{WantCandidateStatus: 422, WantBaselineStatus: 422, BodyMode: RESTBodyModeJSON, IDBindings: []RESTIDBinding{{Producer: "deployment_gap_entity_id", QueryParam: "entity_id"}}},
			true,
		},
		{
			"regression: even a no-IDBindings 200/200 pair still breaks (the derivation only ever exempts an EQUAL 422/401/403/429 pair, and 200 always means a live happy path)",
			RESTRequest{WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeJSON},
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

// TestIsFrameworkValidatedEqualStatus_AgreesWithCommittedCorpus proves the
// derivation against the REAL committed corpus, not just hand-built
// cells: every entry init() left as a live equal-status comparison (proof
// it was judged framework-validated) must have empty IDBindings, and
// flame's own deployment_gap_entity_id_bound_422 -- the one entry the
// OLD, hand-maintained status-code whitelist misclassified -- must derive
// false when reconstructed with its declared (pre-override) shape.
//
// An entry init() left alone keeps WantCandidateStatus == WantBaselineStatus
// post-init (only an OVERRIDDEN entry's WantBaselineStatus ever changes),
// so "== still equal, at one of the four framework-validatable statuses"
// identifies exactly today's "whitelist members" from live corpus state,
// with no need to observe pre-init state.
func TestIsFrameworkValidatedEqualStatus_AgreesWithCommittedCorpus(t *testing.T) {
	checked := 0
	for operation, spec := range restEndpointSpecs {
		for _, req := range spec.Requests {
			if req.WantCandidateStatus != req.WantBaselineStatus || !frameworkValidatableEqualStatuses[req.WantBaselineStatus] {
				continue
			}
			checked++
			if len(req.IDBindings) != 0 {
				t.Errorf("%s/%s is a live %d/%d pair with IDBindings=%+v -- isFrameworkValidatedEqualStatus would derive false, so init() should have overridden it, not left it alone", operation, req.Name, req.WantCandidateStatus, req.WantBaselineStatus, req.IDBindings)
			}
			if !isFrameworkValidatedEqualStatus(req) {
				t.Errorf("%s/%s: isFrameworkValidatedEqualStatus = false for a request init() left alone", operation, req.Name)
			}
		}
	}
	if checked == 0 {
		t.Fatal("swept zero live equal-status framework-validatable requests -- the sweep covered nothing")
	}
	t.Logf("checked %d live framework-validated entries, all deriving true with empty IDBindings", checked)

	spec, err := SpecForREST("REST:GET:/api/v1/flame")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	var gap *RESTRequest
	for i := range spec.Requests {
		if spec.Requests[i].Name == "deployment_gap_entity_id_bound_422" {
			gap = &spec.Requests[i]
		}
	}
	if gap == nil {
		t.Fatal("flame corpus has no deployment_gap_entity_id_bound_422 entry")
	}
	if len(gap.IDBindings) == 0 {
		t.Fatal("deployment_gap_entity_id_bound_422 carries no IDBindings -- the derivation's whole premise for this entry no longer holds")
	}
	// Reconstruct the entry's DECLARED (pre-override) shape -- its live
	// WantBaselineStatus is 500 post-override, so testing the live struct
	// directly would trivially skip frameworkValidatableEqualStatuses'
	// membership check rather than exercise the derivation.
	declared := RESTRequest{WantCandidateStatus: 422, WantBaselineStatus: 422, BodyMode: RESTBodyModeJSON, IDBindings: gap.IDBindings}
	if isFrameworkValidatedEqualStatus(declared) {
		t.Error("deployment_gap_entity_id_bound_422's declared shape derives isFrameworkValidatedEqualStatus = true, want false -- this is the exact misclassification STEP 173 caught")
	}
}

// TestEqualStatusIDBindingJudgments_EqualsKnownSet pins that
// isFrameworkValidatedEqualStatus's soundness in the IDBindings!=empty
// direction is an ASSUMPTION -- true of every entry init() has ever
// judged by it, not a proven property --
// so this test asserts equalStatusIDBindingJudgments (populated by
// init()'s own pass over the PRE-override corpus, the only point that
// distinction survives) equals today's single known member, and fails
// loudly, by name, the moment a new entry is judged by that direction.
//
// A plain "any IDBindings-holding entry at a 422/401/403/429 status"
// sweep over the committed (post-init) corpus is NOT the right set here:
// it also matches flame's pr_gap_entity_id_bound_status_divergence and
// issue_gap_entity_id_bound_status_divergence, which declare a genuine,
// pre-existing, ALREADY-divergent status pair unrelated to the body
// deletion (restcorpus.go's own doc comments on both) -- they were never
// equal to begin with, so isFrameworkValidatedEqualStatus's risky
// direction is never consulted for them, and including them here would
// be a false positive that hides the real signal.
func TestEqualStatusIDBindingJudgments_EqualsKnownSet(t *testing.T) {
	knownToday := map[string]bool{
		"REST:GET:/api/v1/flame/deployment_gap_entity_id_bound_422": true,
	}

	if len(equalStatusIDBindingJudgments) == 0 {
		t.Fatal("equalStatusIDBindingJudgments is empty -- this pin's premise (the flame gap entry) no longer holds, or restdeletedbody.go's init() stopped populating it")
	}
	for _, entry := range equalStatusIDBindingJudgments {
		if !knownToday[entry] {
			t.Errorf(
				"%s is newly judged by isFrameworkValidatedEqualStatus's IDBindings!=empty=>NOT-framework-validated direction -- that direction is an ASSUMPTION, not a proven property: if this entry's own malformed field is DIFFERENT from the one IDBindings resolves, it is still framework-validated and this derivation wrongly overrides its declared baseline status to the deleted-body sentinel. Confirm this entry's own malformed field IS the bound one (like flame's deployment_gap_entity_id_bound_422), or make the derivation two-way before adding it to knownToday",
				entry,
			)
		}
	}
	for entry := range knownToday {
		stillPresent := false
		for _, g := range equalStatusIDBindingJudgments {
			if g == entry {
				stillPresent = true
				break
			}
		}
		if !stillPresent {
			t.Errorf("%s is in knownToday but init() no longer judges it -- update this test's knownToday set", entry)
		}
	}
}
