package goapiproof

import "testing"

// This file exercises TeamRepoSubsetShape (teamreposubset.go) directly,
// through small, self-contained lists in both of its two manifestations:
// a whole-list ShapeLength finding (no OrderInsensitiveList declared for
// the path) and a per-key ShapePresence finding (one declared). Every
// test isolates exactly one of the shape's own rules -- disabling that
// rule alone in teamreposubset.go turns the matching test red.

// teamRepoSubsetLengthOptions builds a length-mode declaration: the list
// at shape.ListPath carries no OrderInsensitiveList entry, so a length
// difference reaches the comparator as one whole-list ShapeLength finding
// (compareList, compare.go).
func teamRepoSubsetLengthOptions(shape *TeamRepoSubsetShape) Options {
	return Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-SUBSET", Reason: "test fixture",
			Paths:               []string{shape.ListPath},
			Intermittent:        true,
			IntermittentReason:  "test fixture",
			TeamRepoSubsetShape: shape,
		}},
	}
}

// teamRepoSubsetPresenceOptions builds a presence-mode declaration: the
// list at shape.ListPath IS declared order-insensitive, so a missing
// element reaches the comparator as its own per-key ShapePresence finding
// (compareListByKey, compare.go) instead of one whole-list length finding.
func teamRepoSubsetPresenceOptions(shape *TeamRepoSubsetShape) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: shape.ListPath, KeyFields: shape.KeyFields, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-SUBSET", Reason: "test fixture",
			Paths:               []string{shape.ListPath},
			Intermittent:        true,
			IntermittentReason:  "test fixture",
			TeamRepoSubsetShape: shape,
		}},
	}
}

// TestTeamRepoSubsetShape_ProperSubsetEqualLeavesAdmitted is the
// post-fix regression this shape exists to cover: a candidate list whose
// every element also appears in baseline, under the same key, with every
// declared leaf equal, is a bounded subset and its length difference is
// admitted.
func TestTeamRepoSubsetShape_ProperSubsetEqualLeavesAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:    "data.items",
		KeyFields:   []string{"key"},
		EqualLeaves: []string{"label"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","label":"x"},{"key":"ABC-2","label":"y"}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","label":"x"}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a proper equal-leaf subset must be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-SUBSET"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-SUBSET]", result.BaselineDefectsMatched)
	}
}

// TestTeamRepoSubsetShape_EmptyCandidateRefused pins rule 2, and is the
// exact shape the CAPTURED pre-fix production bodies carry: an empty
// candidate list against a populated baseline (GET /api/v1/work-units'
// own team_scoped capture returned a bare `[]`; GET /api/v1/explain's
// own review_latency_team_scoped capture returned zero contributors/
// drivers and a zeroed data.value) certifies nothing about the claimed
// scope and must stay outside, never read as "a very small but valid
// team".
func TestTeamRepoSubsetShape_EmptyCandidateRefused(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:  "data.items",
		KeyFields: []string{"key"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1"},{"key":"ABC-2"}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- an empty candidate list must never be admitted as a subset: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_CandidateOnlyKeyStaysOutside pins rule 3 in
// isolation: the one matched key (ABC-1) agrees on every declared leaf,
// so the ONLY problem is ABC-2 existing on the candidate side and not
// baseline's -- a subset can never gain a key the wider list does not
// carry, so the whole plan is refused, not just that one key.
func TestTeamRepoSubsetShape_CandidateOnlyKeyStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:    "data.items",
		KeyFields:   []string{"key"},
		EqualLeaves: []string{"label"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","label":"x"}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","label":"x"},{"key":"ABC-2","label":"z"}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate-only key disproves the subset claim for the whole comparison: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_DifferingEqualLeafStaysOutside pins rule 4's
// EqualLeaves half in isolation: ABC-1 is present on both sides (no
// membership problem at all), but its own "label" leaf differs, which is
// a real, uncovered difference this shape must not paper over.
func TestTeamRepoSubsetShape_DifferingEqualLeafStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:    "data.items",
		KeyFields:   []string{"key"},
		EqualLeaves: []string{"label"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","label":"x"},{"key":"ABC-2","label":"y"}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","label":"CHANGED"}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a differing EqualLeaves field on a matched item must stay outside: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_BoundedLeafCandidateGreaterStaysOutside pins
// rule 4's BoundedLeaves half: a sum-type leaf may only shrink on the
// candidate side (baseline >= candidate); a candidate reading GREATER
// than baseline is the one direction this mechanism can never produce
// and must never be admitted.
func TestTeamRepoSubsetShape_BoundedLeafCandidateGreaterStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:        "data.items",
		KeyFields:       []string{"key"},
		BoundedLeaves:   []string{"value"},
		BoundedLeafKeys: []string{"ABC-1"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":5},{"key":"ABC-2","value":20}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":9}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a bounded leaf reading candidate > baseline must stay outside: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_BoundedLeafCandidateLessOrEqualAdmitted is
// BoundedLeafCandidateGreaterStaysOutside's positive twin: the same
// sum-type leaf shrinking (or staying equal) on the candidate side is
// exactly the claim this mechanism makes, and is admitted.
func TestTeamRepoSubsetShape_BoundedLeafCandidateLessOrEqualAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:        "data.items",
		KeyFields:       []string{"key"},
		BoundedLeaves:   []string{"value"},
		BoundedLeafKeys: []string{"ABC-1"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":9},{"key":"ABC-2","value":20}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":5}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a bounded leaf shrinking on the candidate side must be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_BoundedLeafKeysRestrictsWhichKeysAreBounded
// pins BoundedLeafKeys' own restriction: ABC-2 is NOT named in
// BoundedLeafKeys, so its own "value" leaf still needs to be EQUAL, not
// merely bounded, to admit -- a leaf is a sum-type aggregate only for the
// keys the declaration actually names, never for every key sharing the
// same leaf name.
func TestTeamRepoSubsetShape_BoundedLeafKeysRestrictsWhichKeysAreBounded(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:        "data.items",
		KeyFields:       []string{"key"},
		BoundedLeaves:   []string{"value"},
		BoundedLeafKeys: []string{"ABC-1"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":9},{"key":"ABC-2","value":20}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-2","value":5}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- ABC-2 is not a BoundedLeafKeys entry, so its shrunken value must still be treated as an EqualLeaves failure, not silently bounded: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_MissingKeyFieldRefusesWholePlan pins rule 1: an
// element missing a declared key field is the same vacuity failure
// orderInsensitiveKey's own callers refuse elsewhere in this package --
// nothing is admitted from a shape that cannot even read its own two
// lists.
func TestTeamRepoSubsetShape_MissingKeyFieldRefusesWholePlan(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:  "data.items",
		KeyFields: []string{"key"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1"},{"key":"ABC-2"}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"label":"no key field"}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetLengthOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate element missing the declared key field must refuse the whole plan: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_PresenceBaselineOnlyKeyAdmitted exercises the
// shape's OTHER manifestation: a list declared order-insensitive reports
// a missing element as its own per-key ShapePresence finding rather than
// one whole-list length finding, and this shape admits it the same way.
func TestTeamRepoSubsetShape_PresenceBaselineOnlyKeyAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:  "data.items",
		KeyFields: []string{"id"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":1},{"id":"ABC-2","value":2}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":1}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a baseline-only key (absent in candidate) must be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_PresenceCandidateOnlyKeyNeverAdmitted pins the
// one-directional rule this shape's own doc comment states: a
// candidate-only key ("absent in baseline") is never admitted, because a
// subset can only ever be missing elements the wider list has, never
// gain one it does not -- and, per rule 3, its presence also refuses the
// admission every OTHER key in this same comparison would otherwise get.
func TestTeamRepoSubsetShape_PresenceCandidateOnlyKeyNeverAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:  "data.items",
		KeyFields: []string{"id"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":1}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":1},{"id":"ABC-2","value":2}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate-only key must never be admitted, in either direction: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_PresenceBoundedLeafWithinBoundOnDeclaredKeyAdmitted
// closes the design gap a bounded leaf otherwise leaves: a BoundedLeaves
// leaf that stays within bound still differs numerically between the two
// sides (compareJSON emits its own ShapeValue finding for ABC-1's own
// "value", independent of the presence findings this same comparison
// also carries for ABC-3), and on a route with no sibling blanket
// citation over that leaf it must be admitted here or it is permanently
// outside every citation -- the bound this shape verifies would be
// inert.
func TestTeamRepoSubsetShape_PresenceBoundedLeafWithinBoundOnDeclaredKeyAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:        "data.items",
		KeyFields:       []string{"id"},
		BoundedLeaves:   []string{"value"},
		BoundedLeafKeys: []string{"ABC-1"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":9},{"id":"ABC-3","value":3}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":5}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- ABC-1's own bounded value finding and ABC-3's own absence must both be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_PresenceBoundedLeafOnUndeclaredKeyStaysOutside
// is the same fixture with ABC-2 (not in BoundedLeafKeys) narrowing its
// own "value" instead of ABC-1: that leaf needs EQUAL, not bounded, to
// admit for a key BoundedLeafKeys does not name, so its own ShapeValue
// finding stays outside even though the raw numbers moved the same
// direction as the admitted case above.
func TestTeamRepoSubsetShape_PresenceBoundedLeafOnUndeclaredKeyStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:        "data.items",
		KeyFields:       []string{"id"},
		BoundedLeaves:   []string{"value"},
		BoundedLeafKeys: []string{"ABC-1"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":9},{"id":"ABC-2","value":9}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":9},{"id":"ABC-2","value":5}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- ABC-2's own narrowed value is not a BoundedLeafKeys entry and must stay outside: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_PresenceBoundedLeafOutsideBoundStaysOutside is
// BoundedLeafWithinBoundOnDeclaredKeyAdmitted's negative twin: the SAME
// declared key's SAME leaf reading candidate > baseline is outside the
// bound and must stay outside, exactly like the length-mode
// BoundedLeafCandidateGreaterStaysOutside case above.
func TestTeamRepoSubsetShape_PresenceBoundedLeafOutsideBoundStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:        "data.items",
		KeyFields:       []string{"id"},
		BoundedLeaves:   []string{"value"},
		BoundedLeafKeys: []string{"ABC-1"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":5}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":9}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a bounded leaf reading candidate > baseline must stay outside even in presence mode: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestGate_OtherShapesNeverAdmitAStructuralFinding pins the gate this
// shape and LimitDisplacementShape share in classifyBaselineDefects
// (compare.go): a defect declaring some OTHER shape at a path that
// happens to carry a length difference must NOT have that finding reach
// the shape's own admits() at all -- the exception is narrow, by shape
// field, never by "some shape is set".
func TestGate_OtherShapesNeverAdmitAStructuralFinding(t *testing.T) {
	opts := Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-OTHER-SHAPE", Reason: "test fixture",
			Paths:               []string{"data.items"},
			Intermittent:        true,
			IntermittentReason:  "test fixture",
			KeyedDirectionShape: &KeyedDirectionShape{ListPath: "data.items", ValueField: "value", ValuePath: "data.items.value", KeyFields: []string{"key"}},
		}},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":1},{"key":"ABC-2","value":2}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":1}]}}`)

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a length finding must never reach a KeyedDirectionShape's own admits(): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_BoundedAllKeysCandidateGreaterStaysOutside pins
// BoundedLeavesAllKeys' own bound: a leaf named there must satisfy
// baseline >= candidate on EVERY matched key, with no BoundedLeafKeys
// restriction narrowing which keys the bound applies to -- ABC-2 is not
// named anywhere, yet its own value reading candidate > baseline must
// still stay outside, exactly like a BoundedLeafKeys-named key would.
func TestTeamRepoSubsetShape_BoundedAllKeysCandidateGreaterStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:             "data.items",
		KeyFields:            []string{"key"},
		BoundedLeavesAllKeys: []string{"value"},
	}
	// ABC-1 is identical on both sides (no finding of its own to leak
	// through); ABC-2 is not named in any BoundedLeafKeys list yet reads
	// candidate > baseline, which must still violate the bound and poison
	// the whole plan (the same one-bad-key rule rule 3 already states for
	// membership) -- so ABC-2's own value finding is the only one, and it
	// must stay outside.
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":9},{"key":"ABC-2","value":5}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":9},{"key":"ABC-2","value":9}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- ABC-2's own candidate > baseline value must stay outside even though it is not named in any BoundedLeafKeys list: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_BoundedAllKeysCandidateLessOrEqualAdmittedOnEveryKey
// is the positive twin: BOTH keys shrink (or stay equal) on the candidate
// side with no BoundedLeafKeys entry naming either one, and both are
// admitted -- the whole point of the opt-in is that every matched key
// gets the bounded rule, not only the ones a caller enumerates.
func TestTeamRepoSubsetShape_BoundedAllKeysCandidateLessOrEqualAdmittedOnEveryKey(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:             "data.items",
		KeyFields:            []string{"key"},
		BoundedLeavesAllKeys: []string{"value"},
	}
	// BOTH keys genuinely shrink on the candidate side, and NEITHER is
	// named in any BoundedLeafKeys list -- the whole point of the opt-in
	// is that every matched key gets the bounded rule, not only the ones
	// a caller enumerates.
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":9},{"key":"ABC-2","value":7}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","value":5},{"key":"ABC-2","value":3}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- every matched key shrinking on the candidate side must be admitted with no BoundedLeafKeys entry at all: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_BoundedAllKeysUndeclaredLeafNeverAdmitted pins
// the opt-in's own refusal: a leaf named in NEITHER EqualLeaves,
// BoundedLeaves nor BoundedLeavesAllKeys is not this shape's concern at
// all (rule 4's own "not checked by this shape at all" clause) -- its own
// ShapeValue finding stays outside, whichever direction it moved.
func TestTeamRepoSubsetShape_BoundedAllKeysUndeclaredLeafNeverAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:             "data.items",
		KeyFields:            []string{"id"},
		BoundedLeavesAllKeys: []string{"value"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":9,"other":1}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"ABC-1","value":5,"other":2}]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- \"other\" is named in no leaf list at all and must stay outside regardless of \"value\"'s own admitted bound: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_DisplacementCandidateOnlyAtOrBelowMinAdmitted
// pins the new opt-in rule-3 relaxation: both lists sit at the declared
// DisplacementLimit, and the one candidate-only key's own
// DisplacementValueField value ties the baseline list's own minimum --
// admitted (ties admitted, this shape's own doc comment).
func TestTeamRepoSubsetShape_DisplacementCandidateOnlyAtOrBelowMinAdmitted(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:               "data.items",
		KeyFields:              []string{"key"},
		DisplacementLimit:      2,
		DisplacementValueField: "effort.value",
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[
		{"key":"ABC-1","effort":{"value":10}},
		{"key":"ABC-2","effort":{"value":5}}
	]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[
		{"key":"ABC-1","effort":{"value":10}},
		{"key":"ABC-3","effort":{"value":5}}
	]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a candidate-only key at the baseline minimum, with both lists at the declared Limit, must be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_DisplacementCandidateOnlyAboveMinStaysOutside
// is the negative twin: the candidate-only key's own value is ABOVE the
// baseline list's own minimum, so it cannot be a row ranking below the
// baseline's own LIMIT cutoff -- refused. Both lists sit at the declared
// Limit (the precondition this test isolates), so ABC-2's own paired
// baseline-only absence is forced by the equal lengths too; refusing the
// whole plan (rule 3) leaves BOTH its own finding and the candidate-only
// one outside, exactly as an ordinary (non-displacement) candidate-only
// key already leaves a paired baseline-only key outside today.
func TestTeamRepoSubsetShape_DisplacementCandidateOnlyAboveMinStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:               "data.items",
		KeyFields:              []string{"key"},
		DisplacementLimit:      2,
		DisplacementValueField: "effort.value",
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[
		{"key":"ABC-1","effort":{"value":10}},
		{"key":"ABC-2","effort":{"value":5}}
	]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[
		{"key":"ABC-1","effort":{"value":10}},
		{"key":"ABC-3","effort":{"value":6}}
	]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 -- a candidate-only key above the baseline minimum refuses the whole plan, leaving its own finding and ABC-2's paired baseline-only absence both outside: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_DisplacementShortOfLimitStaysOutside pins the
// Limit precondition: the candidate-only key's own value sits below what
// would be the baseline minimum, but neither list reaches the declared
// DisplacementLimit -- there is no LIMIT boundary here for a
// rank-below-cutoff claim to mean anything, so the relaxation never
// activates and rule 3 refuses exactly as it did before this admission
// existed. Unequal list lengths (baseline's one key is also candidate's,
// so there is no paired baseline-only absence here, unlike the
// AboveMin case above) isolates the one candidate-only finding.
func TestTeamRepoSubsetShape_DisplacementShortOfLimitStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:               "data.items",
		KeyFields:              []string{"key"},
		DisplacementLimit:      5,
		DisplacementValueField: "effort.value",
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","effort":{"value":10}}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[
		{"key":"ABC-1","effort":{"value":10}},
		{"key":"ABC-3","effort":{"value":1}}
	]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- neither list reaches the declared DisplacementLimit, so the relaxation must not activate: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestTeamRepoSubsetShape_DisplacementUnconfiguredCandidateOnlyStaysOutside
// pins the opt-in default: a shape that never sets DisplacementLimit/
// DisplacementValueField keeps rule 3's own unconditional refusal, even
// when the candidate-only key's own value would satisfy the relaxed rule
// if it were configured. Same unequal-length shape as ShortOfLimit above,
// isolating the one candidate-only finding.
func TestTeamRepoSubsetShape_DisplacementUnconfiguredCandidateOnlyStaysOutside(t *testing.T) {
	shape := &TeamRepoSubsetShape{
		ListPath:  "data.items",
		KeyFields: []string{"key"},
	}
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"key":"ABC-1","effort":{"value":10}}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[
		{"key":"ABC-1","effort":{"value":10}},
		{"key":"ABC-3","effort":{"value":1}}
	]}}`)

	result := Compare(baseline, candidate, teamRepoSubsetPresenceOptions(shape))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- with DisplacementLimit/DisplacementValueField unset, a candidate-only key must stay outside regardless of its own value: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
