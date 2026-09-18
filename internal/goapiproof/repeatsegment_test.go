package goapiproof

import "testing"

// This file pins the ONE repeat-segment form FloatTierB/FloatExactLeaves/
// IntegerLeaves accept for a recursive tree of unbounded depth (see
// FloatTierB's own doc comment and matchRepeatDeclaration's): a declared
// segment ending in "+" matches one or more consecutive actual segments
// equal to its own base name.

// codeHotspotsBody builds a recursive root/children tree body at the
// given depth (0 = root only, no children), matching AggregatedFlameNode's
// own wire shape (schemas.py) closely enough for Compare to decode.
func codeHotspotsBody(depth int, leafValue int) string {
	node := `{"name":"leaf","value":` + jsonInt(leafValue) + `,"children":[]}`
	for i := 0; i < depth; i++ {
		node = `{"name":"dir","value":` + jsonInt(leafValue) + `,"children":[` + node + `]}`
	}
	return `{"mode":"code_hotspots","unit":"loc","root":` + node + `,"meta":{"window_start":"2026-01-01","window_end":"2026-01-07","filters":{},"notes":[],"approximation":{"used":false}}}`
}

// (a) RED FIRST: proves against the REAL registered corpus declaration
// (codeHotspotsParity, restcorpus.go), not a hand-built stand-in. a depth-4 code_hotspots body refuses as undeclared
// without the repeat declaration (NumericLeavesDeclared on, nothing
// declared), then is admitted once the repeat declaration is added --
// proving the repeat form, not luck, covers an unbounded depth.
func TestRepeatSegment_CodeHotspotsDepth4_RefusesUndeclaredThenAdmitted(t *testing.T) {
	baseline := restSnapshotFromJSON(t, codeHotspotsBody(4, 10))
	candidate := restSnapshotFromJSON(t, codeHotspotsBody(4, 11))

	bare := Options{NumericLeavesDeclared: true}
	bareResult := Compare(baseline, candidate, bare)
	found := map[string]bool{}
	for _, p := range bareResult.UndeclaredNumericLeaves {
		found[p] = true
	}
	if !found["data.root.value"] || !found["data.root.children.children.children.children.value"] {
		t.Fatalf("without any declaration, both depth-0 and depth-4 leaves must read as undeclared, got %v", bareResult.UndeclaredNumericLeaves)
	}

	result := Compare(baseline, candidate, codeHotspotsParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- an integer leaf must still catch a real difference at depth 4: findings %+v", result.TerminalState, result.Findings)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("with the repeat declaration, every depth must read as declared, got %v", result.UndeclaredNumericLeaves)
	}

	same := restSnapshotFromJSON(t, codeHotspotsBody(4, 10))
	if !Compare(same, same, codeHotspotsParity).IsMatch() {
		t.Fatal("identical depth-4 bodies must match under the repeat declaration")
	}
}

// (b) The repeat form must not match a different segment name, and must
// not match zero repetitions of its own base name.
func TestRepeatSegment_MatchRepeatingSegments_NameAndZeroRepeatBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		declared []string
		actual   []string
		want     bool
	}{
		{"one repeat matches", []string{"data", "root", "children+", "value"}, []string{"data", "root", "children", "value"}, true},
		{"four repeats match", []string{"data", "root", "children+", "value"}, []string{"data", "root", "children", "children", "children", "children", "value"}, true},
		{"zero repeats does not match", []string{"data", "root", "children+", "value"}, []string{"data", "root", "value"}, false},
		{"a different segment name does not match", []string{"data", "root", "children+", "value"}, []string{"data", "root", "items", "value"}, false},
		{"a different name among repeats does not match", []string{"data", "root", "children+", "value"}, []string{"data", "root", "children", "items", "value"}, false},
		{"trailing extra segment does not match", []string{"data", "root", "children+", "value"}, []string{"data", "root", "children", "value", "extra"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchRepeatingSegments(tc.declared, tc.actual)
			if got != tc.want {
				t.Fatalf("matchRepeatingSegments(%v, %v) = %v, want %v", tc.declared, tc.actual, got, tc.want)
			}
		})
	}
}

// (c) The validator rejects every malformed repeat marker shape: mid-
// segment, doubled, empty base, and marking the path's own last segment.
func TestRepeatSegment_ValidatorRejectsMalformedMarkers(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{"mid-segment plus", "data.root.chil+dren.value"},
		{"doubled trailing plus", "data.root.children++.value"},
		{"empty base", "data.root.+.value"},
		{"marks its own last segment", "data.root.children+"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRepeatMarker(tc.path); err == nil {
				t.Fatalf("validateRepeatMarker(%q) = nil, want an error", tc.path)
			}
		})
	}

	if err := validateRepeatMarker("data.root.children+.value"); err != nil {
		t.Fatalf("the one accepted form must validate clean, got %v", err)
	}

	// Wired through validateNumericLeaves too, over each of the three
	// tables.
	if err := validateNumericLeaves(Options{IntegerLeaves: map[string]string{"data.root.children+": "bad"}}); err == nil {
		t.Fatal("validateNumericLeaves must reject a malformed repeat marker in IntegerLeaves")
	}
	if err := validateNumericLeaves(Options{FloatTierB: map[string]string{"data.a++.value": "bad"}}); err == nil {
		t.Fatal("validateNumericLeaves must reject a malformed repeat marker in FloatTierB")
	}
}

// (d) cycle_breakdown and throughput's own fixed-depth declarations
// (added by this ticket) carry no repeat marker, and must therefore keep
// refusing a leaf one level deeper than they declare -- the repeat form
// existing elsewhere in this package must never loosen an unrelated,
// non-repeat declaration.
func TestRepeatSegment_FixedDepthDeclarationsStillRefuseOneLevelDeeper(t *testing.T) {
	deeperCycleBreakdown := `{"root":{"value":1,"children":[{"value":1,"children":[{"value":1,"children":[{"value":1,"children":[]}]}]}]}}`
	snap := restSnapshotFromJSON(t, deeperCycleBreakdown)
	result := Compare(snap, snap, Options{NumericLeavesDeclared: true, FloatTierB: cycleBreakdownFloats})
	if len(result.UndeclaredNumericLeaves) == 0 {
		t.Fatal("a cycle_breakdown tree one level deeper than the declared three fixed depths must report an undeclared leaf")
	}

	deeperThroughput := `{"root":{"value":1,"children":[{"value":1,"children":[{"value":1,"children":[{"value":1,"children":[]}]}]}]}}`
	snap2 := restSnapshotFromJSON(t, deeperThroughput)
	result2 := Compare(snap2, snap2, aggFlameThroughputParity)
	if len(result2.UndeclaredNumericLeaves) == 0 {
		t.Fatal("a throughput tree one level deeper than the declared three fixed depths must report an undeclared leaf")
	}
}
