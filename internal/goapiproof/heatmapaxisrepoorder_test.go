package goapiproof

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// This file exercises HeatmapAxisRepoOrderShape (heatmapaxisrepoorder.go):
// small synthetic bodies for the individual rules, plus the real captured
// GET /api/v1/heatmap repo_touchpoints_team_scoped pair (STEP 97, prod
// rev 102) through the actual registered heatmapRepoTouchpointsTeamScopedParity.

func heatmapAxisOrderTestShape() *HeatmapAxisRepoOrderShape {
	return &HeatmapAxisRepoOrderShape{
		CellsListPath: "data.cells",
		CellKeyFields: []string{"x", "y"},
		RepoField:     "y",
		AxisListPath:  "data.axes.y",
	}
}

// heatmapAxisOrderBody reuses heatmapBoundaryCell/heatmapBoundaryBody
// (heatmapcellboundary_test.go) for the cell/body JSON shape.
func heatmapAxisOrderBody(t *testing.T, cellsJSON string, axisY []string) any {
	t.Helper()
	body := heatmapBoundaryBody(t, cellsJSON, axisY)
	snapshot, err := DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("decode synthetic body: %v", err)
	}
	return snapshot.Data
}

// TestHeatmapAxisRepoOrderShape_AdmitsAVerifiedTwoRepoSwap is the clean
// positive case: repoA's own two shared cells both agree on k=2, repoB is
// byte-identical on both legs, and the resulting axis swap (repoA moves
// from rank 1 to rank 2 as its own true, undoubled total falls below
// repoB's untouched one) is admitted whole-list.
func TestHeatmapAxisRepoOrderShape_AdmitsAVerifiedTwoRepoSwap(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 6)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if !plan.valid {
		t.Fatal("plan should be valid: repoA's k=2 verified by 2 agreeing cells, no tie in any of the three totals maps (base 8/6, cand 4/6, expected-base 8/6)")
	}
	admitted := plan.admits(Finding{Path: "$.data.axes.y[0]", Shape: ShapeValue})
	if !admitted {
		t.Error("data.axes.y[0] should be admitted")
	}
}

// TestHeatmapAxisRepoOrderShape_RefusesWithOnlyOneCorroboratingCell pins
// rule 2: a SINGLE differing shared cell, however clean its own ratio,
// never establishes a verified k on its own.
func TestHeatmapAxisRepoOrderShape_RefusesWithOnlyOneCorroboratingCell(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 6)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: repoA's own ratio is corroborated by only ONE shared cell, never enough to verify k")
	}
}

// TestHeatmapAxisRepoOrderShape_RefusesWhenAnEntrantOrLeaverIsPresent pins
// rule 1: this shape never explains a LIMIT-boundary crossing (that stays
// HeatmapCellBoundaryShape's own job) -- a repository present on only one
// leg refuses the whole plan outright.
func TestHeatmapAxisRepoOrderShape_RefusesWhenAnEntrantOrLeaverIsPresent(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 6) + "," + heatmapBoundaryCell("w1", "repoC", 1)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoB", "repoA", "repoC"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: repoC is baseline-only, a presence difference this shape never explains")
	}
}

// TestHeatmapAxisRepoOrderShape_RefusesWhenAnUnrelatedRepoAlsoDiffers pins
// the whole-list re-derivation's own safety: a genuine, UNVERIFIED
// difference on a repository besides the fanned-out one breaks the
// re-derived order's exact match against the observed baseline axis, so
// the plan must refuse rather than admit a partial explanation.
func TestHeatmapAxisRepoOrderShape_RefusesWhenAnUnrelatedRepoAlsoDiffers(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 9)
	// repoB's own total also differs (6 -> 9) with no verified k (only
	// repoA's ratio is corroborated) -- a real, unexplained regression
	// hiding next to the verified fan-out.
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoB", "repoA"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: repoB's own total differs with no verified k, so the re-derived expected-baseline order does not match the true (unexplained) observed one")
	}
}

// TestHeatmapAxisRepoOrderShape_RefusesOnATieInEitherLegsTotals is the
// team-lead-requested guard test: `_axis_order`'s/`axisOrder`'s own
// stable sort breaks a tie on row ENCOUNTER order, which this shape
// cannot verify from the two response bodies alone -- a tie anywhere in
// baseline's own totals, candidate's own totals, or the candidate-scaled-
// by-k expected-baseline totals must refuse the whole plan, never guess.
func TestHeatmapAxisRepoOrderShape_RefusesOnATieInEitherLegsTotals(t *testing.T) {
	// candidate's own totals: repoA=4, repoB=4 -- a genuine tie.
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: candidate's own totals tie (repoA=4, repoB=4) -- the tie-break is unverifiable from the wire alone")
	}
}

// TestHeatmapAxisRepoOrderShape_RefusesATieEvenWhenACoincidentalOrderWouldOtherwisePass
// isolates heatmapHasTie's own necessity from the whole-list re-derivation
// check alone: a tie whose candidate names happen to occupy an order the
// re-derivation would ALSO accept on its own (both legs' own axis already
// matches a name-ascending tie-break, purely by coincidence -- this
// shape's own model of `_axis_order`/`axisOrder` never uses name order as
// a tie-break, and has no way to know THIS run's real row-encounter order
// coincided with it) must still be refused. Without heatmapHasTie, this
// exact case would be wrongly admitted.
func TestHeatmapAxisRepoOrderShape_RefusesATieEvenWhenACoincidentalOrderWouldOtherwisePass(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	// baseline: repoA=8, repoB=4 -- no tie, unambiguous. candidate:
	// repoA=4, repoB=4 -- a genuine tie, whose two names happen to sit in
	// alphabetical order on the wire ([repoA, repoB]) on BOTH legs, which
	// coincides with what a name-ascending tie-break would also produce.
	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoA", "repoB"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: candidate's own totals tie (repoA=4, repoB=4) even though the observed axis coincidentally matches a name-ascending order")
	}
}

func heatmapAxisOrderSnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	return snapshot
}

const (
	heatmapAxisOrderBaselinePath  = "testdata/heatmap_repo_touchpoints_axis_order_baseline_01e6602f.json"
	heatmapAxisOrderCandidatePath = "testdata/heatmap_repo_touchpoints_axis_order_candidate_9ff6c313.json"
)

func heatmapAxisOrderTicket(t *testing.T) string {
	t.Helper()
	for _, d := range heatmapRepoTouchpointsTeamScopedParity.BaselineDefects {
		if d.HeatmapAxisRepoOrderShape != nil {
			return d.Ticket
		}
	}
	t.Fatal("heatmapRepoTouchpointsTeamScopedParity declares no HeatmapAxisRepoOrderShape entry")
	return ""
}

// TestHeatmapRepoTouchpointsTeamScopedParity_RealCapturedBodyRefusesOnATie
// runs the real captured pair through the actual registered
// heatmapRepoTouchpointsTeamScopedParity. Full derivation from the wire:
// full-chaos/dev-health-go's own 3 shared cells all agree on k=2 exactly
// (4/2, 2/1, 2/1) and its own 3 cells ARE its entire baseline total (8),
// so its candidate-side total (4) is EXACTLY equal to full-chaos/
// dev-health-web's own untouched total (also 4) -- a genuine tie in
// candidate's own totals, confirmed against real production data, not a
// synthetic corner case. heatmapDedupParity's own KeyedDirectionShape
// (inherited, direction-only, magnitude-unbounded) already admits the 3
// value findings; this shape's own axis rule 4 correctly refuses the 2
// remaining data.axes.y findings rather than guess at the tie-break, so
// they stay outside exactly as they did before this shape existed -- and
// the new declaration reads LIVE-UNEXPLAINED (its own Paths reach two
// real findings, admits neither), never idle, proving the citation is not
// a stale no-op.
func TestHeatmapRepoTouchpointsTeamScopedParity_RealCapturedBodyRefusesOnATie(t *testing.T) {
	baseline := heatmapAxisOrderSnapshotFromFile(t, heatmapAxisOrderBaselinePath)
	candidate := heatmapAxisOrderSnapshotFromFile(t, heatmapAxisOrderCandidatePath)

	result := Compare(baseline, candidate, heatmapRepoTouchpointsTeamScopedParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if len(result.Findings) != 5 {
		t.Fatalf("findings = %d, want 5 (3 value + 2 axis): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (the 2 axis findings, refused on the tie) -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	dedupTicket := ""
	for _, d := range heatmapDedupParity.BaselineDefects {
		if d.KeyedDirectionShape != nil {
			dedupTicket = d.Ticket
		}
	}
	if dedupTicket == "" {
		t.Fatal("heatmapDedupParity declares no KeyedDirectionShape entry")
	}
	sawDedupMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == dedupTicket {
			sawDedupMatched = true
		}
	}
	if !sawDedupMatched {
		t.Fatalf("matched = %v, want %s (the value cells) among them", result.BaselineDefectsMatched, dedupTicket)
	}
	axisTicket := heatmapAxisOrderTicket(t)
	sawAxisUnexplained := false
	for _, ticket := range result.LiveBaselineDefectsUnexplained {
		if ticket == axisTicket {
			sawAxisUnexplained = true
		}
	}
	if !sawAxisUnexplained {
		t.Fatalf("liveUnexplained = %v, want %s among them (the axis findings are live but the tie correctly refuses them): matched %v idle %v",
			result.LiveBaselineDefectsUnexplained, axisTicket, result.BaselineDefectsMatched, result.IdleIntermittentBaselineDefects)
	}
}

// TestHeatmapAxisRepoOrderShape_RealCapturedCellsWithoutTheTieAdmit takes
// the REAL captured cell data (the same 3 agreeing dev-health-go cells,
// k=2) but raises ONE of dev-health-web's own untouched cells (its own
// value 2, the largest of its three real cells) by exactly 1, both legs
// identically -- it is never one of the fanned-out cells -- so
// dev-health-web's own total rises from 4 to 5: enough to break the
// coincidental tie with dev-health-go's own candidate-side total (4)
// without inverting dev-health-go's own, much larger, baseline-side
// total (8) -- proving the shape WOULD admit this real mechanism's axis
// consequence once the tie is broken, isolating the tie itself (not some
// other flaw) as the reason TestHeatmapRepoTouchpointsTeamScopedParity_
// RealCapturedBodyRefusesOnATie stays outside.
func TestHeatmapAxisRepoOrderShape_RealCapturedCellsWithoutTheTieAdmit(t *testing.T) {
	baseline := heatmapAxisOrderSnapshotFromFile(t, heatmapAxisOrderBaselinePath)
	candidate := heatmapAxisOrderSnapshotFromFile(t, heatmapAxisOrderCandidatePath)

	raiseWebCell := func(cells []any) {
		for _, element := range cells {
			object := element.(map[string]any)
			value, ok := asFloat(object["value"])
			if object["y"] == "full-chaos/dev-health-web" && ok && value == 2 {
				object["value"] = json.Number("3")
				return
			}
		}
		t.Fatal("fixture carries no full-chaos/dev-health-web cell with value 2 to raise")
	}
	raiseWebCell(baseline.Data.(map[string]any)["cells"].([]any))
	raiseWebCell(candidate.Data.(map[string]any)["cells"].([]any))

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should now be valid: dev-health-web's own total (5) no longer ties dev-health-go's candidate-side total (4)")
	}
	for _, idx := range []int{4, 5} {
		if !plan.admits(Finding{Path: fmt.Sprintf("$.data.axes.y[%d]", idx), Shape: ShapeValue}) {
			t.Errorf("data.axes.y[%d] should be admitted once the tie is broken", idx)
		}
	}
}
