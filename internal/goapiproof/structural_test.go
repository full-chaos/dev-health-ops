package goapiproof

import (
	"context"
	"errors"
	"testing"
)

// CHAOS-5661 (JOB 7 step 7 run 2): flowMatrix's TEAM/REPO variants
// measured Go 3 nodes/6 edges vs Python 2/2 (TEAM) and Go 11/110 vs
// Python 9/54 (REPO), with ZERO node-id overlap -- Go keys nodes/edges by
// name, Python by UUID. Before this fix, the comparator paired elements
// POSITIONALLY, and the declared BaselineDefect only cites the "value"
// leaf (not "id"), so two elements with an EQUAL "value" and a wholly
// different "id" produced no finding the citation was even asked to
// cover -- the citation matched nothing, and the run refused with the
// misleading RefusalStaleBaselineDefect ("declared baseline defects
// covered no difference") instead of naming the real cause: the two legs
// are not describing the same entities at all.
//
// This is that exact shape, red-first: it fails on the pre-fix code by
// reporting RefusalStaleBaselineDefect (a citation "gone stale", inviting
// an operator to delete the ticket reference) instead of the true cause.
func TestStructuralRefusalOnDisjointNodeIdsThroughTheRealFlowMatrixVariants(t *testing.T) {
	// Same length (2 nodes, 1 edge) and EQUAL "value" fields on both
	// legs -- only "id" differs, and CHAOS-5426/CHAOS-5448's declared
	// citations name only "...nodes.value"/"...edges.value", never "id".
	// Every sub-request (the WORK_TYPE base request and the TEAM/REPO
	// variants) shares one fake edge body, so all three are driven
	// through the same disjoint-id shape at once.
	pythonBody := `{"data":{"analytics":{"flowMatrix":{
		"nodes":[{"id":"11111111-1111-1111-1111-111111111111","value":1},{"id":"22222222-2222-2222-2222-222222222222","value":2}],
		"edges":[{"id":"33333333-3333-3333-3333-333333333333","value":5}]
	}}}}`
	goBody := `{"data":{"analytics":{"flowMatrix":{
		"nodes":[{"id":"team-backend","value":1},{"id":"team-frontend","value":2}],
		"edges":[{"id":"team-backend->team-frontend","value":5}]
	}}}}`

	edge := &fakeEdge{goBody: goBody, pythonBody: pythonBody}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity
	runner.Documents = map[string]string{"flowMatrix": "query FlowMatrix { analytics { flowMatrix { nodes { id value } edges { id value } } } }"}
	runner.Registry.DocumentDigest = map[string]string{"flowMatrix": "5661aa00"}
	runner.Routing = map[string]RoutingRow{"flowMatrix": {Mode: "canary", CandidateBuild: runner.Registry.BuildIdentity}}

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("disjoint node ids must refuse every sub-request, got %v", err)
	}
	if len(outcomes) != 3 {
		t.Fatalf("expected 3 outcomes (base + TEAM + REPO), got %d: %+v", len(outcomes), outcomes)
	}
	for _, outcome := range outcomes {
		if outcome.RefusalReason == RefusalStaleBaselineDefect {
			t.Fatalf("variant %q: refused as a STALE declaration (%q) -- the real cause is that the two legs share no node id, and CHAOS-5661 rule 3 requires that text never stand in for this cause again",
				outcome.Variant, outcome.RefusalDetail)
		}
		if outcome.RefusalReason != RefusalLegsDoNotOverlap {
			t.Fatalf("variant %q: expected %s, got %s (%s)", outcome.Variant, RefusalLegsDoNotOverlap, outcome.RefusalReason, outcome.RefusalDetail)
		}
	}
}

// The same shape, driven directly through Compare (no Runner, no
// registered spec) so the underlying mechanism is pinned independently of
// flowMatrix's own declared citations ever changing shape.
func TestCompareRefusesOnDisjointListIds(t *testing.T) {
	baseline := `{"data":{"nodes":[{"id":"uuid-1","value":1},{"id":"uuid-2","value":2}]}}`
	candidate := `{"data":{"nodes":[{"id":"name-a","value":1},{"id":"name-b","value":2}]}}`
	// A citation broad enough to cover EVERY field under nodes (including
	// "id") is the worst case: pre-fix, this covers the "id" difference
	// as an ordinary leaf value difference and reports
	// differences_outside_baseline_defect=0 -- a MISMATCH that reads as
	// fully-cited enablement proof for two structurally unrelated lists.
	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-0000", Reason: "test", Paths: []string{"data.nodes"},
	}}}
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), opts)
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("expected StructuralRefusal=%s, got %q (terminal=%s outside=%d) -- pre-fix this reported terminal=%s outside=0, a false fully-cited proof",
			RefusalLegsDoNotOverlap, result.StructuralRefusal, result.TerminalState, result.DifferencesOutsideBaselineDefect, TerminalStateMismatch)
	}
	// A structural refusal computes NOTHING else: TerminalState stays the
	// zero value, never "match" and never a fully-cited "mismatch".
	if result.TerminalState != "" || result.DifferencesOutsideBaselineDefect != 0 || len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("a structural refusal must not also carry a verdict: %+v", result)
	}
}

// A list whose elements do not ALL carry "id" is never checked -- this is
// what keeps hotspots' rows and flowMatrix's own declared-citation
// {"value": N} shape (citation_shape_test.go) from ever being touched by
// this rule.
func TestCompareDoesNotRefuseListsWithoutAnIdField(t *testing.T) {
	baseline := `{"data":{"rows":[{"filePath":"a.go","churn":1},{"filePath":"b.go","churn":2}]}}`
	candidate := `{"data":{"rows":[{"filePath":"z.go","churn":9},{"filePath":"y.go","churn":8}]}}`
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), Options{})
	if result.StructuralRefusal != "" {
		t.Fatalf("a list with no \"id\" field must never trigger the overlap check, got %s (%s)", result.StructuralRefusal, result.StructuralDetail)
	}
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("the ordinary comparator must still run and report the value differences, got %s", result.TerminalState)
	}
}

// Partial id overlap (at least one id shared) is an ordinary comparison,
// never a structural refusal -- only a COMPLETELY disjoint id set refuses.
func TestCompareDoesNotRefuseOnPartialIdOverlap(t *testing.T) {
	baseline := `{"data":{"nodes":[{"id":"a","value":1},{"id":"b","value":2}]}}`
	candidate := `{"data":{"nodes":[{"id":"a","value":1},{"id":"c","value":3}]}}`
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), Options{})
	if result.StructuralRefusal != "" {
		t.Fatalf("a shared id (%q) must keep this an ordinary comparison, got %s (%s)", "a", result.StructuralRefusal, result.StructuralDetail)
	}
}

// CHAOS-5661 rule 2: an executed operation whose two legs are both empty
// result sets is not evidence of parity. Scoped to an operation that
// DECLARES a relaxation (see vacuousEmptyLegs's own doc comment for why
// it must not fire more broadly than that): workGraphEdges's own history
// is the precedent this generalises -- a declared defect over data that
// turns out to be vacuous must say so honestly rather than reading as a
// stale citation.
func TestCompareRefusesVacuousEmptyLegsWhenARelaxationIsDeclared(t *testing.T) {
	baseline := `{"data":{"flowMatrix":{"nodes":[],"edges":[]}}}`
	candidate := `{"data":{"flowMatrix":{"nodes":[],"edges":[]}}}`
	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-0000", Reason: "test", Paths: []string{"data.flowMatrix.nodes.value"},
	}}}
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), opts)
	if result.StructuralRefusal != RefusalVacuousEmptyLegs {
		t.Fatalf("expected %s, got %q (terminal=%s)", RefusalVacuousEmptyLegs, result.StructuralRefusal, result.TerminalState)
	}
}

// The regression this rule must never cause: admission.go is explicit
// that an empty list is a legitimate result ("this org has no feature
// flags"), and an operation that declares NO relaxation must keep
// reporting a real match on two empty legs -- refusing it would make the
// instrument unable to prove any operation over empty data.
// TestEmptyListRootIsStillAdmitted and
// TestCompareEnvelopeKeyExceptionIsTopLevelOnly pin the same rule through
// the full Runner and through EnvelopeKeys respectively; this pins it
// directly against Options{}.
func TestCompareDoesNotRefuseEmptyLegsWithNoDeclaredRelaxation(t *testing.T) {
	baseline := `{"data":{"featureFlags":[]}}`
	candidate := `{"data":{"featureFlags":[]}}`
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), Options{})
	if result.StructuralRefusal != "" {
		t.Fatalf("an operation with no declared relaxation must never refuse on vacuity, got %s (%s)", result.StructuralRefusal, result.StructuralDetail)
	}
	if !result.IsMatch() {
		t.Fatalf("two legitimately empty legs must still match, got %s", result.TerminalState)
	}
}

// throughputForecast/capacityForecast declare RootNullable and tolerate a
// null root as a real, TOLERATED empty result (operations.go). They also
// declare VolatileFields (throughputForecast additionally FloatTierB),
// but NOT BaselineDefects/OrderInsensitiveLists -- so two legs that both
// resolve to null must keep matching, never read as vacuous_empty_legs.
func TestCompareDoesNotRefuseANullableRootThatBothLegsReportAsNull(t *testing.T) {
	spec, err := SpecFor("throughputForecast")
	if err != nil {
		t.Fatalf("SpecFor(throughputForecast): %v", err)
	}
	if len(spec.Parity.BaselineDefects) != 0 || len(spec.Parity.OrderInsensitiveLists) != 0 {
		t.Fatalf("throughputForecast's declaration changed shape (%+v) -- this test assumes neither BaselineDefects nor OrderInsensitiveLists", spec.Parity)
	}
	baseline := `{"data":{"throughputForecast":null}}`
	candidate := `{"data":{"throughputForecast":null}}`
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), spec.Parity)
	if result.StructuralRefusal != "" {
		t.Fatalf("a nullable root both legs report as null is a real, tolerated match, not vacuous_empty_legs: got %s (%s)", result.StructuralRefusal, result.StructuralDetail)
	}
	if !result.IsMatch() {
		t.Fatalf("expected match, got %s", result.TerminalState)
	}
}

// A gross response-body size disagreement is its own structural signal,
// independent of the decoded value: two bodies this far apart in size are
// not the same shape of answer.
func TestCompareRefusesOnBodySizeDisagreement(t *testing.T) {
	small := `{"data":{"x":` + jsonRepeat("1", 200) + `}}`
	large := `{"data":{"x":` + jsonRepeat("1", 2000) + `}}`
	result := Compare(snapshotFromJSON(t, small), snapshotFromJSON(t, large), Options{})
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("expected %s, got %q", RefusalLegsDoNotOverlap, result.StructuralRefusal)
	}
}

// The measured shape behind this rule: GET /api/v1/work-units
// team_scoped, one work unit present in baseline and genuinely absent
// from candidate under an already-declared, already-admitted team-scope
// subset defect. Both legs' own `data` lists are the SAME length, so the
// one missing unit shows as a one-position SHIFT, never a length
// difference. `data`'s elements key on `unit_id`, never literal "id";
// nested `evidence` lists DO carry "id". Pre-fix, structuralAgreementFailure
// fell through to a raw positional walk, paired baseline's shifted-out
// unit against candidate's next (unrelated) unit, and refused on THEIR
// nested evidence lists sharing no "id" -- true, but for the wrong
// reason. With the fix, `data` is keyed by `unit_id`
// (TeamRepoSubsetShape.KeyFields, declared alongside a matching
// OrderInsensitiveLists entry so compareList also pairs by key rather
// than position), every unit sharing a key on both legs is compared
// correctly, the one baseline-only unit surfaces as a ShapePresence
// finding, and TeamRepoSubsetShape's own subset admission covers it: the
// comparison reaches an ADMITTED MISMATCH, never a structural refusal.
func TestCompareAlignsSubsetListByDeclaredIdentityInsteadOfPosition(t *testing.T) {
	baseline := `{"data":[
		{"unit_id":"u1","name":"first","evidence":[{"id":"e1"}]},
		{"unit_id":"u-extra","name":"baseline only unit","evidence":[{"id":"ABC-123"}]},
		{"unit_id":"u2","name":"second","evidence":[{"id":"e2"}]}
	]}`
	candidate := `{"data":[
		{"unit_id":"u1","name":"first","evidence":[{"id":"e1"}]},
		{"unit_id":"u2","name":"second","evidence":[{"id":"e2"}]}
	]}`
	const ticket = "CHAOS-0000"
	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{{
			Path: "data", KeyFields: []string{"unit_id"}, Reason: "test", Ticket: ticket,
		}},
		BaselineDefects: []BaselineDefect{{
			Ticket: ticket, Reason: "test", Paths: []string{"data"},
			TeamRepoSubsetShape: &TeamRepoSubsetShape{
				ListPath:    "data",
				KeyFields:   []string{"unit_id"},
				EqualLeaves: []string{"name", "evidence"},
			},
		}},
	}
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), opts)
	if result.StructuralRefusal != "" {
		t.Fatalf("expected no structural refusal once `data` aligns by unit_id, got %s (%s)", result.StructuralRefusal, result.StructuralDetail)
	}
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("a declared defect never promotes to match, expected %s, got %s", TerminalStateMismatch, result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("the one missing unit must be fully covered by the declared subset shape, got %d difference(s) outside it: %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if len(result.BaselineDefectsMatched) != 1 {
		t.Fatalf("expected the one declared defect to match the one baseline-only unit, matched=%v", result.BaselineDefectsMatched)
	}
}

// A list whose declared identity shares NOT ONE key between the two legs
// still refuses -- idOverlapFailure's own rule, generalized to a
// non-"id" identity rather than bypassed by one.
func TestCompareRefusesOnDisjointDeclaredIdentity(t *testing.T) {
	baseline := `{"data":[{"unit_id":"u1","name":"a"},{"unit_id":"u2","name":"b"}]}`
	candidate := `{"data":[{"unit_id":"u3","name":"c"},{"unit_id":"u4","name":"d"}]}`
	opts := Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-0000", Reason: "test", Paths: []string{"data"},
			TeamRepoSubsetShape: &TeamRepoSubsetShape{
				ListPath: "data", KeyFields: []string{"unit_id"}, EqualLeaves: []string{"name"},
			},
		}},
	}
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), opts)
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("expected %s, got %q (terminal=%s)", RefusalLegsDoNotOverlap, result.StructuralRefusal, result.TerminalState)
	}
}

// The nested "id" gate still fires on a truly disjoint NESTED list inside
// an element the outer identity correctly paired -- the fix changes
// ALIGNMENT, never idOverlapFailure's own disjoint-entities rule.
func TestCompareStillRefusesOnDisjointNestedIdsInsideACorrectlyPairedElement(t *testing.T) {
	baseline := `{"data":[{"unit_id":"u1","name":"first","evidence":[{"id":"e1"}]}]}`
	candidate := `{"data":[{"unit_id":"u1","name":"first","evidence":[{"id":"wholly-different"}]}]}`
	opts := Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-0000", Reason: "test", Paths: []string{"data"},
			TeamRepoSubsetShape: &TeamRepoSubsetShape{
				ListPath: "data", KeyFields: []string{"unit_id"}, EqualLeaves: []string{"name"},
			},
		}},
	}
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), opts)
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("the SAME unit_id's own nested evidence sharing no id must still refuse, got %s (terminal=%s)", result.StructuralRefusal, result.TerminalState)
	}
}

// jsonRepeat builds a JSON array of n copies of one element, so its
// caller controls the encoded body's byte length precisely without
// tripping the id-overlap check (plain numbers, not "id"-carrying
// objects).
func jsonRepeat(element string, n int) string {
	out := "["
	for i := 0; i < n; i++ {
		if i > 0 {
			out += ","
		}
		out += element
	}
	return out + "]"
}
