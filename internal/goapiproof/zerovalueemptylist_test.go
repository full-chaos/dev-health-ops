package goapiproof

import "testing"

// This file exercises ZeroValueEmptyListShape (zerovalueemptylist.go)
// directly, through small self-contained bodies, then against the real
// captured production evidence via the full explainParity wiring
// (restcorpus.go). Every isolated test disables exactly one of the
// shape's own two rules -- flipping that rule alone in
// zerovalueemptylist.go, or loosening the gate in classifyBaselineDefects
// (compare.go) to admit ShapePresence for ANY shaped defect regardless of
// which shape fired, turns the matching test red.

// zeroValueEmptyListOptions builds a declaration citing ListPath as an
// order-insensitive list (keyed by "id", matching explainDriverRankOrderInsensitive's
// own KeyFields) so a missing element reaches the comparator as its own
// per-key ShapePresence finding (compareListByKey, compare.go) rather than
// one whole-list ShapeLength finding.
func zeroValueEmptyListOptions(shape *ZeroValueEmptyListShape) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: shape.ListPath, KeyFields: []string{"id"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{
			{
				// Sibling blanket leaf entry, matching production's own
				// split (an unshaped value/delta_pct entry beside its
				// shaped list entries, restcorpus.go): isolates every test
				// below to the presence admission alone, so "outside" counts
				// the list finding only, never an incidental uncovered leaf
				// this fixture's own value change produces.
				Ticket: "CHAOS-TEST-ZVEL-VALUE", Reason: "test fixture",
				Paths:              []string{shape.ValuePath},
				Intermittent:       true,
				IntermittentReason: "test fixture",
			},
			{
				Ticket: "CHAOS-TEST-ZVEL", Reason: "test fixture",
				Paths:                   []string{shape.ListPath},
				Intermittent:            true,
				IntermittentReason:      "test fixture",
				ZeroValueEmptyListShape: shape,
			},
		},
	}
}

// TestZeroValueEmptyListShape_ValueCollapseWithEmptyListAdmitted is the
// isolated shape this fix exists to cover: baseline carries a non-zero
// value and one ranked entry, candidate's value collapsed to zero and its
// list came back empty -- the baseline-only entry is a consequence of the
// SAME collapse, admitted.
func TestZeroValueEmptyListShape_ValueCollapseWithEmptyListAdmitted(t *testing.T) {
	shape := &ZeroValueEmptyListShape{ValuePath: "data.value", ListPath: "data.drivers"}
	baseline := snapshotFromJSON(t, `{"data":{"value":11.87,"drivers":[{"id":"CHAOS","value":11.87}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":0,"drivers":[]}}`)

	result := Compare(baseline, candidate, zeroValueEmptyListOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a value collapse to zero with a now-empty candidate list must admit the baseline-only entry: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-ZVEL", "CHAOS-TEST-ZVEL-VALUE"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-ZVEL CHAOS-TEST-ZVEL-VALUE]", result.BaselineDefectsMatched)
	}
}

// TestZeroValueEmptyListShape_CandidateValueNonZeroRefused pins rule 1's
// candidate-zero half: the list still emptied, but the value never
// collapsed, so there is no declared value divergence for the list
// difference to be a consequence of.
func TestZeroValueEmptyListShape_CandidateValueNonZeroRefused(t *testing.T) {
	shape := &ZeroValueEmptyListShape{ValuePath: "data.value", ListPath: "data.drivers"}
	baseline := snapshotFromJSON(t, `{"data":{"value":11.87,"drivers":[{"id":"CHAOS","value":11.87}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":5,"drivers":[]}}`)

	result := Compare(baseline, candidate, zeroValueEmptyListOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a non-zero candidate value proves no value collapse, so the emptied list must stay outside: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestZeroValueEmptyListShape_BaselineValueZeroRefused pins rule 1's
// baseline-non-zero half: both planes already agree the value is zero for
// some unrelated reason, so a list difference under it is not this
// shape's own consequence.
func TestZeroValueEmptyListShape_BaselineValueZeroRefused(t *testing.T) {
	shape := &ZeroValueEmptyListShape{ValuePath: "data.value", ListPath: "data.drivers"}
	baseline := snapshotFromJSON(t, `{"data":{"value":0,"drivers":[{"id":"CHAOS","value":0}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":0,"drivers":[]}}`)

	result := Compare(baseline, candidate, zeroValueEmptyListOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- both planes already zero on the value proves no collapse occurred, so the list difference must stay outside: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestZeroValueEmptyListShape_NonEmptyCandidateListRefused pins rule 2:
// the value did collapse, but the candidate's own list is NOT empty (one
// shared entry survives), so the collapse did not fully explain what
// changed under this list and nothing is admitted -- the same "one bad
// element invalidates the plan" discipline TeamRepoSubsetShape already
// applies (teamreposubset.go).
func TestZeroValueEmptyListShape_NonEmptyCandidateListRefused(t *testing.T) {
	shape := &ZeroValueEmptyListShape{ValuePath: "data.value", ListPath: "data.drivers"}
	baseline := snapshotFromJSON(t, `{"data":{"value":11.87,"drivers":[{"id":"CHAOS","value":6},{"id":"OTHER","value":5.87}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":0,"drivers":[{"id":"CHAOS","value":6}]}}`)

	result := Compare(baseline, candidate, zeroValueEmptyListOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a non-empty candidate list must never be admitted as a value-collapse consequence: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestZeroValueEmptyListShape_ReverseDirectionNeverAdmitted pins the
// one-directional rule this shape's own doc comment states: a
// candidate-only entry ("absent in baseline") is never admitted, because
// a value collapsing to zero can only ever make a list NARROWER, never
// wider -- and, per rule 2, its presence also proves the candidate list
// is non-empty, refusing the admission every OTHER entry in this same
// comparison would otherwise get (the same mechanism
// TestZeroValueEmptyListShape_NonEmptyCandidateListRefused pins from the
// baseline-only side).
func TestZeroValueEmptyListShape_ReverseDirectionNeverAdmitted(t *testing.T) {
	shape := &ZeroValueEmptyListShape{ValuePath: "data.value", ListPath: "data.drivers"}
	baseline := snapshotFromJSON(t, `{"data":{"value":11.87,"drivers":[{"id":"CHAOS","value":11.87}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":0,"drivers":[{"id":"CHAOS","value":11.87},{"id":"NEW","value":1}]}}`)

	result := Compare(baseline, candidate, zeroValueEmptyListOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate-only entry must never be admitted, in either direction: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestZeroValueEmptyListShape_BlanketDeclarationAloneDoesNotAdmitPresence
// is the control this fix exists to close: an UNSHAPED declaration citing
// the very same ListPath, with no ZeroValueEmptyListShape set, is the
// pre-fix behaviour -- classifyBaselineDefects' own gate (compare.go)
// excludes ShapePresence from every unshaped citation's blanket "any leaf
// difference under Paths is covered" rule (leafDifference). This test
// must stay red if that gate is ever loosened to admit a structural
// finding for ANY shaped or unshaped defect without going through this
// shape's own admits().
func TestZeroValueEmptyListShape_BlanketDeclarationAloneDoesNotAdmitPresence(t *testing.T) {
	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.drivers", KeyFields: []string{"id"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-BLANKET", Reason: "test fixture, no shape set",
			Paths:              []string{"data.value", "data.drivers"},
			Intermittent:       true,
			IntermittentReason: "test fixture",
		}},
	}
	baseline := snapshotFromJSON(t, `{"data":{"value":11.87,"drivers":[{"id":"CHAOS","value":11.87}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":0,"drivers":[]}}`)

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- citing the list's path alone, with no shape, must never admit the presence finding: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestZeroValueEmptyListShape_RealBlockedWorkCaptureAdmitted replays the
// real captured production deployed-vs-deployed prove run (POST
// /api/v1/explain blocked_work_default): the candidate's blocked-only
// GROUP BY resolves to zero rows for the window (the status-filter
// divergence declared alongside this shape, restcorpus.go), so
// data.value/data.delta_pct collapse to 0 (covered by the sibling leaf
// declaration) and
// data.drivers/data.contributors come back empty against the baseline's
// one all-status entry each -- the two presence findings this lane exists
// to close. Falsifiable prediction: the next production run of this
// request shows zero differences outside every declaration, or (should
// the window's candidate value ever be non-zero instead) a plain,
// uncovered finding surfaces immediately rather than being silently
// swallowed.
func TestZeroValueEmptyListShape_RealBlockedWorkCaptureAdmitted(t *testing.T) {
	baseline := explainRESTSnapshot(t, `{"metric":"blocked_work","label":"Blocked Work","unit":"hours","value":1876.5770752777778,"delta_pct":-86.16173512669302,"drivers":[{"id":"CHAOS","label":"Fullchaos","value":11.8770700966948,"delta_pct":-80.37982323021122,"evidence_link":"/api/v1/drilldown/prs?metric=blocked_work&scope_type=org&scope_id=","display_name":"Fullchaos"}],"contributors":[{"id":"CHAOS","label":"Fullchaos","value":11.8770700966948,"delta_pct":0.0,"evidence_link":"/api/v1/drilldown/prs?metric=blocked_work&scope_type=org&scope_id=","display_name":"Fullchaos"}],"drilldown_links":{"prs":"/api/v1/drilldown/prs?metric=blocked_work","issues":"/api/v1/drilldown/issues?metric=blocked_work"}}`)
	candidate := explainRESTSnapshot(t, `{"metric":"blocked_work","label":"Blocked Work","unit":"hours","value":0,"delta_pct":0,"drivers":[],"contributors":[],"drilldown_links":{"issues":"/api/v1/drilldown/issues?metric=blocked_work","prs":"/api/v1/drilldown/prs?metric=blocked_work"}}`)

	result := Compare(baseline, candidate, explainParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- the real captured blocked_work_default divergence must be fully declared: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- the bodies genuinely differ, only every difference is declared", result.TerminalState)
	}
}
