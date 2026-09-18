package goapiproof

import (
	"fmt"
	"testing"
)

// This file exercises HeatmapAxisTieGroupShape (heatmapaxistiegroup.go):
// small synthetic bodies isolating each rule, and full Compare() runs
// against heatmapRepoTouchpointsAxisTieGroupDefect (restcorpus.go) for
// the four cases the class ruling names by exact finding count.

func heatmapAxisTieGroupTestShape() *HeatmapAxisTieGroupShape {
	return &HeatmapAxisTieGroupShape{
		CellsListPath: "data.cells",
		CellKeyFields: []string{"x", "y"},
		NameField:     "y",
		AxisListPath:  "data.axes.y",
	}
}

// heatmapAxisTieGroupBody reuses heatmapBoundaryBody (heatmapcellboundary_
// test.go) for the cells/axis JSON shape, decoded through DecodeRESTSnapshot
// the same way every other heatmap shape test in this package does.
func heatmapAxisTieGroupBody(t *testing.T, cellsJSON string, axisY []string) any {
	t.Helper()
	body := heatmapBoundaryBody(t, cellsJSON, axisY)
	snapshot, err := DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("decode synthetic body: %v", err)
	}
	return snapshot.Data
}

func heatmapAxisTieGroupSnapshot(t *testing.T, cellsJSON string, axisY []string) Snapshot {
	t.Helper()
	body := heatmapBoundaryBody(t, cellsJSON, axisY)
	snapshot, err := DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("decode synthetic body: %v", err)
	}
	return snapshot
}

// TestHeatmapAxisTieGroupShape_AdmitsASwapInsideAnEqualTotalGroup is the
// clean positive case (class ruling's own first example): repoA and
// repoB carry the exact SAME total (4) on BOTH legs, byte-identical
// cells, and only their own data.axes.y position swaps -- Python's own
// arbitrary tie-break placed repoB first, this port's name-ascending
// tiebreak places repoA first.
func TestHeatmapAxisTieGroupShape_AdmitsASwapInsideAnEqualTotalGroup(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisTieGroupBody(t, cells, []string{"repoB", "repoA"})
	candData := heatmapAxisTieGroupBody(t, cells, []string{"repoA", "repoB"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if !plan.valid {
		t.Fatal("plan should be valid: repoA/repoB tie at 4 on both legs, both axis lists a valid (tie-permitting) descending order")
	}
	if !plan.admits(Finding{Path: "$.data.axes.y[0]", Shape: ShapeValue}) {
		t.Error("data.axes.y[0] should be admitted: inside the verified tie group")
	}
	if !plan.admits(Finding{Path: "$.data.axes.y[1]", Shape: ShapeValue}) {
		t.Error("data.axes.y[1] should be admitted: inside the verified tie group")
	}
}

// TestHeatmapAxisTieGroupShape_RefusesASwapAcrossDifferentTotals pins
// rule 2: repoA (total 6) and repoB (total 4) never tie, so swapping
// their own axis positions -- with every cell value held identical
// across both legs -- produces a candidate list that is not a valid
// descending order of its own totals (rule 1's own sanity
// precondition). Whether the mechanism is "the whole plan refuses" or "a
// tie-group never forms," the position stays outside either way, which
// is what this test actually pins.
func TestHeatmapAxisTieGroupShape_RefusesASwapAcrossDifferentTotals(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 6) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisTieGroupBody(t, cells, []string{"repoA", "repoB"})
	candData := heatmapAxisTieGroupBody(t, cells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if plan.admits(Finding{Path: "$.data.axes.y[0]", Shape: ShapeValue}) {
		t.Error("data.axes.y[0] must stay outside: repoA/repoB totals (6, 4) never tie")
	}
	if plan.admits(Finding{Path: "$.data.axes.y[1]", Shape: ShapeValue}) {
		t.Error("data.axes.y[1] must stay outside: repoA/repoB totals (6, 4) never tie")
	}
}

// TestHeatmapAxisTieGroupShape_RefusesACandidateGroupNotInNameOrder pins
// rule 3: repoA/repoB genuinely tie at 4 on both legs, but the candidate
// leg's own order inside the tie group is NOT name ascending -- this
// shape certifies the ONE deterministic tiebreak this port actually
// applies, never "any permutation of a tied group is fine."
func TestHeatmapAxisTieGroupShape_RefusesACandidateGroupNotInNameOrder(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisTieGroupBody(t, cells, []string{"repoA", "repoB"})
	candData := heatmapAxisTieGroupBody(t, cells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if !plan.valid {
		t.Fatal("plan should be structurally valid: same name set, both axis lists a valid descending order of their own totals")
	}
	if plan.admits(Finding{Path: "$.data.axes.y[0]", Shape: ShapeValue}) {
		t.Error("data.axes.y[0] must stay outside: candidate's own tie-group order (repoB, repoA) is not name ascending")
	}
	if plan.admits(Finding{Path: "$.data.axes.y[1]", Shape: ShapeValue}) {
		t.Error("data.axes.y[1] must stay outside: candidate's own tie-group order (repoB, repoA) is not name ascending")
	}
}

// TestHeatmapAxisTieGroupShape_RefusesASetDifference pins rule 1: a
// name present on only one leg is a boundary-crossing case this shape
// never explains (that stays HeatmapAxisRepoOrderShape's/
// HeatmapCellBoundaryShape's own job) -- the whole plan refuses rather
// than guess at the remaining, name-identical positions.
func TestHeatmapAxisTieGroupShape_RefusesASetDifference(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4) + "," + heatmapBoundaryCell("w1", "repoC", 1)
	candCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisTieGroupBody(t, baseCells, []string{"repoA", "repoB", "repoC"})
	candData := heatmapAxisTieGroupBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: repoC is baseline-only, a presence difference this shape never explains")
	}
}

// TestHeatmapAxisTieGroupShape_StaysInertWhenATotalDiffersAcrossLegs
// isolates the shape's own composability guarantee: repoA's own total
// differs between legs (a value change some OTHER mechanism, verified
// or not, would have to explain), and merely coincides with repoB's
// candidate-side total -- rule 2's cross-leg equality check must refuse
// to treat that coincidence as a genuine tie.
func TestHeatmapAxisTieGroupShape_StaysInertWhenATotalDiffersAcrossLegs(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 8) + "," + heatmapBoundaryCell("w1", "repoB", 4)
	candCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisTieGroupBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisTieGroupBody(t, candCells, []string{"repoA", "repoB"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if !plan.valid {
		t.Fatal("plan should be structurally valid: same name set, both axis lists a valid descending order of their own totals")
	}
	if plan.admits(Finding{Path: "$.data.axes.y[1]", Shape: ShapeValue}) {
		t.Error("data.axes.y[1] must stay outside: repoA's own total (8 vs 4) differs across legs, so it can never join a verified tie group")
	}
}

// TestHeatmapAxisTieGroupShape_RefusesWhenAxisIsNotWeaklyDescending pins
// rule 1's own sanity precondition: an axis list this shape's model of
// `_axis_order`/`axisOrder` does not actually describe (here, ascending
// instead of descending) is refused outright rather than guessed at.
func TestHeatmapAxisTieGroupShape_RefusesWhenAxisIsNotWeaklyDescending(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 8)

	baseData := heatmapAxisTieGroupBody(t, cells, []string{"repoA", "repoB"})
	candData := heatmapAxisTieGroupBody(t, cells, []string{"repoA", "repoB"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: [repoA(4), repoB(8)] is not a descending order of its own totals")
	}
}

// TestHeatmapAxisTieGroupShape_RefusesWhenBaselinePositionsHoldDifferentNames
// pins rule 2's own "same position range" check directly, isolated from
// the cross-leg-total-differs guard: repoX's own total differs across
// legs (6 baseline, 2 candidate) -- correctly excluded from any group on
// its own -- but its rank RELATIVE to the tied repoB/repoC pair (total 4
// on both legs) flips: baseline ranks repoX above the tie (axis
// [repoX, repoB, repoC]), candidate ranks it below (axis [repoB, repoC,
// repoX]). The candidate's own tie-group position range ([0, 2)) sits
// over a DIFFERENT name pair on baseline at those same positions
// ({repoX, repoB}, not {repoB, repoC}), so the whole three-way reorder
// must stay outside even though repoB/repoC's own totals never moved.
func TestHeatmapAxisTieGroupShape_RefusesWhenBaselinePositionsHoldDifferentNames(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoX", 6) + "," + heatmapBoundaryCell("w1", "repoB", 4) + "," + heatmapBoundaryCell("w1", "repoC", 4)
	candCells := heatmapBoundaryCell("w1", "repoX", 2) + "," + heatmapBoundaryCell("w1", "repoB", 4) + "," + heatmapBoundaryCell("w1", "repoC", 4)

	baseData := heatmapAxisTieGroupBody(t, baseCells, []string{"repoX", "repoB", "repoC"})
	candData := heatmapAxisTieGroupBody(t, candCells, []string{"repoB", "repoC", "repoX"})

	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(), baseData, candData)
	if !plan.valid {
		t.Fatal("plan should be structurally valid: same name set, both axis lists a valid descending order of their own totals")
	}
	for _, idx := range []int{0, 1, 2} {
		if plan.admits(Finding{Path: fmt.Sprintf("$.data.axes.y[%d]", idx), Shape: ShapeValue}) {
			t.Errorf("data.axes.y[%d] must stay outside: candidate's own tie group at this position range does not occupy the same position range on baseline", idx)
		}
	}
}

// TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareAdmitsAPureTie runs
// the real registered heatmapRepoTouchpointsAxisTieGroupDefect (restcorpus.go)
// through Compare(), with exact finding counts (class ruling's own first
// case): repoA and repoB tie at 4 on both legs and only swap position;
// every other repository is byte-identical. The ONE data.axes.y mismatch
// pair (positions 0 and 1) must be fully admitted, leaving 0 differences
// outside the defect.
func TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareAdmitsAPureTie(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4) + "," + heatmapBoundaryCell("w1", "repoC", 1)

	baseline := heatmapAxisTieGroupSnapshot(t, cells, []string{"repoB", "repoA", "repoC"})
	candidate := heatmapAxisTieGroupSnapshot(t, cells, []string{"repoA", "repoB", "repoC"})

	result := Compare(baseline, candidate, heatmapRepoTouchpointsParity)

	if len(result.Findings) != 2 {
		t.Fatalf("findings = %d, want 2 (the two swapped axis positions): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 (both axis positions admitted by the tie-group defect): outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.OutsideByShape, result.Findings)
	}
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
}

// TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareRefusesADifferentTotalSwap
// is the class ruling's own second case: repoA (total 6) and repoB
// (total 4) never tie, so a swap between their positions stays outside.
func TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareRefusesADifferentTotalSwap(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 6) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseline := heatmapAxisTieGroupSnapshot(t, cells, []string{"repoA", "repoB"})
	candidate := heatmapAxisTieGroupSnapshot(t, cells, []string{"repoB", "repoA"})

	result := Compare(baseline, candidate, heatmapRepoTouchpointsParity)

	if len(result.Findings) != 2 {
		t.Fatalf("findings = %d, want 2 (the two swapped axis positions): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (repoA/repoB never tie, so neither position is admitted): outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.OutsideByShape, result.Findings)
	}
}

// TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareRefusesACandidateGroupNotInNameOrder
// is the class ruling's own third case: repoA/repoB tie at 4 on both
// legs, but the candidate's own order inside the group is not name
// ascending.
func TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareRefusesACandidateGroupNotInNameOrder(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseline := heatmapAxisTieGroupSnapshot(t, cells, []string{"repoA", "repoB"})
	candidate := heatmapAxisTieGroupSnapshot(t, cells, []string{"repoB", "repoA"})

	result := Compare(baseline, candidate, heatmapRepoTouchpointsParity)

	if len(result.Findings) != 2 {
		t.Fatalf("findings = %d, want 2 (the two swapped axis positions): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (candidate's own tie-group order is not name ascending, so neither position is admitted): outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.OutsideByShape, result.Findings)
	}
}

// TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareRefusesASetDifference
// is the class ruling's own fourth case: repoC is baseline-only, so the
// whole plan refuses -- 0 axis positions admitted, though the entrant/
// leaver's own data.cells findings are a separate shape's concern
// (heatmapDedupParity's KeyedDirectionShape/OrderInsensitiveList) not
// exercised by this synthetic body at all (repoC's own cell is simply
// absent from data.cells here, an unexplained structural difference this
// test does not assert on).
func TestHeatmapRepoTouchpointsAxisTieGroupDefect_CompareRefusesASetDifference(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4) + "," + heatmapBoundaryCell("w1", "repoC", 1)
	candCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseline := heatmapAxisTieGroupSnapshot(t, baseCells, []string{"repoA", "repoB", "repoC"})
	candidate := heatmapAxisTieGroupSnapshot(t, candCells, []string{"repoB", "repoA"})

	result := Compare(baseline, candidate, heatmapRepoTouchpointsParity)

	axisTicket := ""
	for _, d := range heatmapRepoTouchpointsParity.BaselineDefects {
		if d.HeatmapAxisTieGroupShape != nil {
			axisTicket = d.Ticket
		}
	}
	if axisTicket == "" {
		t.Fatal("heatmapRepoTouchpointsParity declares no HeatmapAxisTieGroupShape entry")
	}
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == axisTicket {
			t.Fatalf("matched = %v, must not include %s: repoC is a set difference, the whole plan must refuse", result.BaselineDefectsMatched, axisTicket)
		}
	}
}
