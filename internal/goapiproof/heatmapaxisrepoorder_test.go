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
// team-lead-requested guard test, narrowed to what rule 4 actually
// refuses on unaided: candidate's own totals tie (repoA=4, repoB=4), and
// the observed candidate axis ([repoB, repoA]) disagrees with
// `axisOrder`'s own deterministic name-ascending tie-break (which orders
// repoA before repoB) -- rule 3's whole-list re-derivation catches the
// mismatch on its own, so the plan refuses regardless.
func TestHeatmapAxisRepoOrderShape_RefusesOnATieInEitherLegsTotals(t *testing.T) {
	// candidate's own totals: repoA=4, repoB=4 -- a genuine tie.
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 4)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: candidate's own totals tie (repoA=4, repoB=4) and the observed order disagrees with axisOrder's own deterministic tie-break")
	}
}

// TestHeatmapAxisRepoOrderShape_AdmitsATieBrokenByAxisOrdersOwnDeterministicNameOrder
// replaces a former guard that assumed a tie purely in the candidate's
// own totals was as unverifiable as one in the baseline's -- it is not.
// `axisOrder`'s (heatmap/response.go) own default branch breaks a tie by
// name ascending, ALWAYS, a deterministic function of (name, total)
// alone (see HeatmapAxisTieGroupShape's own doc comment). repoFanned
// carries a verified k=2 (two agreeing shared cells), so its own
// baseline total (8) sits cleanly above repoEven's untouched one (4) --
// no tie on that leg at all -- but its own candidate-side total (4) ties
// repoEven's exactly, and "repoEven" sorts before "repoFanned", so the
// candidate's own axis swaps relative to the baseline's. Rule 3's
// whole-list re-derivation checks the candidate's observed order against
// `heatmapSortDescByTotal`'s SAME deterministic tie-break -- an exact
// match, never a guess -- so the plan admits both axis positions.
func TestHeatmapAxisRepoOrderShape_AdmitsATieBrokenByAxisOrdersOwnDeterministicNameOrder(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoFanned", 4) + "," + heatmapBoundaryCell("w2", "repoFanned", 4) + "," + heatmapBoundaryCell("w1", "repoEven", 4)
	candCells := heatmapBoundaryCell("w1", "repoFanned", 2) + "," + heatmapBoundaryCell("w2", "repoFanned", 2) + "," + heatmapBoundaryCell("w1", "repoEven", 4)

	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoFanned", "repoEven"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoEven", "repoFanned"})

	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if !plan.valid {
		t.Fatal("plan should be valid: repoFanned's k=2 verified by 2 agreeing cells, no tie in baseline (8/4) or expected-baseline (8/4) totals -- the tie sits only in the candidate's own totals (4/4), broken by axisOrder's own deterministic name-ascending rule exactly as observed")
	}
	for _, idx := range []int{0, 1} {
		if !plan.admits(Finding{Path: fmt.Sprintf("$.data.axes.y[%d]", idx), Shape: ShapeValue}) {
			t.Errorf("data.axes.y[%d] should be admitted", idx)
		}
	}
}

// A verified k=2 lifts repoEven1's baseline total (8) onto repoEven2's
// untouched one (8): a tie on the reference plane, whose order inside
// the tie is its own row encounter order. Either baseline order inside
// the run is admitted; the candidate (4 < 8) is exact.
func TestHeatmapAxisRepoOrderShape_AdmitsEitherBaselineOrderInsideAFanoutTie(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoEven1", 4) + "," + heatmapBoundaryCell("w2", "repoEven1", 4) + "," + heatmapBoundaryCell("w1", "repoEven2", 8)
	candCells := heatmapBoundaryCell("w1", "repoEven1", 2) + "," + heatmapBoundaryCell("w2", "repoEven1", 2) + "," + heatmapBoundaryCell("w1", "repoEven2", 8)
	for _, axisBase := range [][]string{{"repoEven1", "repoEven2"}, {"repoEven2", "repoEven1"}} {
		baseData := heatmapAxisOrderBody(t, baseCells, axisBase)
		candData := heatmapAxisOrderBody(t, candCells, []string{"repoEven2", "repoEven1"})
		plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
		if !plan.valid {
			t.Errorf("baseline axis %v: plan refused a baseline tie (8/8) produced by a verified k=2", axisBase)
		}
	}
}

// A repository placed outside its own tie run on the baseline axis is
// not an order inside a tie: repoLow (1) sits between the tied repoEven1/
// repoEven2 (8/8), so the run at positions [0,2) is not {Even1, Even2}.
func TestHeatmapAxisRepoOrderShape_RefusesARepositoryOutsideItsOwnTieRun(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoEven1", 4) + "," + heatmapBoundaryCell("w2", "repoEven1", 4) + "," + heatmapBoundaryCell("w1", "repoEven2", 8) + "," + heatmapBoundaryCell("w1", "repoLow", 1)
	candCells := heatmapBoundaryCell("w1", "repoEven1", 2) + "," + heatmapBoundaryCell("w2", "repoEven1", 2) + "," + heatmapBoundaryCell("w1", "repoEven2", 8) + "," + heatmapBoundaryCell("w1", "repoLow", 1)
	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoEven1", "repoLow", "repoEven2"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoEven2", "repoEven1", "repoLow"})
	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: repoLow sits inside the baseline's own 8/8 tie run")
	}
}

// A baseline tie whose run matches the observed totals but NOT the
// candidate totals scaled by k refuses: repoA and repoB tie at 8 on the
// baseline, but repoB carries no fan-out and its candidate total is 6,
// so the expected baseline run is {repoA}=8 then {repoB}=6.
func TestHeatmapAxisRepoOrderShape_RefusesATieTheMultiplierDoesNotProduce(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 8)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6)
	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoB", "repoA"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})
	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: repoB's baseline total (8) is not its candidate total (6) with no verified k")
	}
}

// The baseline axis must also follow the baseline's own observed
// totals: expected (candidate x k) totals tie repoA/repoB at 8, but the
// baseline's own repoB total is 7, so [repoB, repoA] is out of its own
// order.
func TestHeatmapAxisRepoOrderShape_RefusesABaselineAxisOutOfItsOwnTotalOrder(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 7)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 8)
	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoB", "repoA"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})
	plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData)
	if plan.valid {
		t.Error("plan must refuse: the baseline's own totals (8/7) order repoA first")
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
const (
	heatmapAxisOrderRecurringBaselinePath  = "testdata/heatmap_repo_touchpoints_axis_order_baseline_d5282133.json"
	heatmapAxisOrderRecurringCandidatePath = "testdata/heatmap_repo_touchpoints_axis_order_candidate_f86df743.json"
)

// TestHeatmapRepoTouchpointsTeamScopedParity_RecurringCapturedBodyAdmitsTheAxisSwap
// runs a SECOND real captured pair (a later production run than
// heatmapAxisOrderBaselinePath/heatmapAxisOrderCandidatePath's own STEP
// 97 capture) through the actual registered
// heatmapRepoTouchpointsTeamScopedParity. Same mechanism, same ticket,
// different repository: full-chaos/dev-health-web's own 3 shared cells
// all agree on k=2 exactly (2/1, 4/2, 2/1), and its own true (candidate-
// side) total (4) ties full-chaos/dev-health-go's own untouched total
// (also 4) -- but this time the CANDIDATE's own observed axis order
// ([..., "full-chaos/dev-health-go", "full-chaos/dev-health-web"]) IS
// exactly axisOrder's own deterministic name-ascending tie-break ("go" <
// "web"), so rule 3's whole-list re-derivation matches it exactly and
// the plan admits both axis findings -- unlike
// TestHeatmapRepoTouchpointsTeamScopedParity_RealCapturedBodyRefusesOnATie's
// own STEP 97 capture, whose candidate axis order does NOT match that
// tie-break and stays outside.
func TestHeatmapRepoTouchpointsTeamScopedParity_RecurringCapturedBodyAdmitsTheAxisSwap(t *testing.T) {
	baseline := heatmapAxisOrderSnapshotFromFile(t, heatmapAxisOrderRecurringBaselinePath)
	candidate := heatmapAxisOrderSnapshotFromFile(t, heatmapAxisOrderRecurringCandidatePath)

	result := Compare(baseline, candidate, heatmapRepoTouchpointsTeamScopedParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if len(result.Findings) != 5 {
		t.Fatalf("findings = %d, want 5 (3 value + 2 axis): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 (the 2 axis findings are now admitted) -- covered %v outside %v findings %+v",
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
	axisTicket := heatmapAxisOrderTicket(t)
	sawDedup := false
	sawAxis := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == dedupTicket {
			sawDedup = true
		}
		if ticket == axisTicket {
			sawAxis = true
		}
	}
	if !sawDedup || !sawAxis {
		t.Fatalf("matched = %v, want both %s (value cells) and %s (axis swap) among them", result.BaselineDefectsMatched, dedupTicket, axisTicket)
	}
}

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

// A real production pair (team_scoped) where TWO repositories fan out at
// a verified k=2 each (dev-health-web: 2/1, 4/2, 2/1; dev-health-go: 4/2,
// 2/1, 2/1), so their true totals tie at 4 on the candidate (name
// ascending: dev-health-go first) and their inflated totals tie at 8 on
// the baseline (reference encounter order: dev-health-web first).
const (
	heatmapFanoutTieBaselinePath  = "testdata/heatmap_repo_touchpoints_fanout_tie_team_scoped_baseline_234f5d43.json"
	heatmapFanoutTieCandidatePath = "testdata/heatmap_repo_touchpoints_fanout_tie_team_scoped_candidate_a0c7c3ee.json"
)

func TestHeatmapRepoTouchpointsTeamScopedParity_RealFanoutTieAdmitsTheAxisSwap(t *testing.T) {
	baseline := heatmapAxisOrderSnapshotFromFile(t, heatmapFanoutTieBaselinePath)
	candidate := heatmapAxisOrderSnapshotFromFile(t, heatmapFanoutTieCandidatePath)

	result := Compare(baseline, candidate, heatmapRepoTouchpointsTeamScopedParity)
	if len(result.Findings) != 8 {
		t.Fatalf("findings = %d, want 8 (6 value + 2 axis): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- covered %v outside %v findings %+v", result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	axisTicket := heatmapAxisOrderTicket(t)
	sawAxis := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == axisTicket {
			sawAxis = true
		}
	}
	if !sawAxis {
		t.Fatalf("matched = %v, want %s among them", result.BaselineDefectsMatched, axisTicket)
	}
}

// The same real pair with both legs' tied repositories swapped, so the
// candidate carries them name-DESCENDING: not axisOrder's rule, so both
// axis findings stay outside while the six value findings stay covered.
func TestHeatmapRepoTouchpointsTeamScopedParity_RealFanoutTieNameDescendingCandidateStaysOutside(t *testing.T) {
	baseline := heatmapAxisOrderSnapshotFromFile(t, heatmapFanoutTieBaselinePath)
	candidate := heatmapAxisOrderSnapshotFromFile(t, heatmapFanoutTieCandidatePath)
	axis := candidate.Data.(map[string]any)["axes"].(map[string]any)["y"].([]any)
	if axis[4] != "full-chaos/dev-health-go" || axis[5] != "full-chaos/dev-health-web" {
		t.Fatalf("fixture candidate axis[4:6] = %v, want [dev-health-go dev-health-web]", axis[4:6])
	}
	axis[4], axis[5] = axis[5], axis[4]
	baseAxis := baseline.Data.(map[string]any)["axes"].(map[string]any)["y"].([]any)
	if baseAxis[4] != "full-chaos/dev-health-web" || baseAxis[5] != "full-chaos/dev-health-go" {
		t.Fatalf("fixture baseline axis[4:6] = %v, want [dev-health-web dev-health-go]", baseAxis[4:6])
	}
	baseAxis[4], baseAxis[5] = baseAxis[5], baseAxis[4]

	result := Compare(baseline, candidate, heatmapRepoTouchpointsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if got, want := findingPathSet(result)[:2], []string{"$.data.axes.y[4]", "$.data.axes.y[5]"}; len(result.Findings) != 8 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("findings %v, want the two axis positions among 8", findingPathSet(result))
	}
}

// A name whose value moved without a verified multiplier refuses the
// plan even where the k-scaled totals put it inside a tie: repoC fans out
// at k=2, repoB's baseline cell is 3 against 2 with no verified k, and the
// candidate's name-ascending tie (A, B at 2) would otherwise explain the
// baseline order C, B, A.
func TestHeatmapRepoTouchpointsParity_UnexplainedValueInsideATieStaysOutside(t *testing.T) {
	base := []string{
		heatmapBoundaryCell("w1", "repoA", 2), heatmapBoundaryCell("w1", "repoB", 3),
		heatmapBoundaryCell("w1", "repoC", 20), heatmapBoundaryCell("w2", "repoC", 20),
	}
	cand := []string{
		heatmapBoundaryCell("w1", "repoA", 2), heatmapBoundaryCell("w1", "repoB", 2),
		heatmapBoundaryCell("w1", "repoC", 10), heatmapBoundaryCell("w2", "repoC", 10),
	}
	baseline, candidate := heatmapAxisSnapshotsFromCells(t, base, cand, []string{"repoC", "repoB", "repoA"}, []string{"repoC", "repoA", "repoB"})
	for _, opts := range []Options{heatmapRepoTouchpointsParity, heatmapRepoTouchpointsTeamScopedParity} {
		result := Compare(baseline, candidate, opts)
		if result.DifferencesOutsideBaselineDefect != 2 {
			t.Fatalf("outside = %d, want 2 (both axis positions): findings %v", result.DifferencesOutsideBaselineDefect, findingPathSet(result))
		}
	}

	// The same shape over hotspot_risk file keys: rc's two files carry
	// the verified k=2, rb:b.go moved 3 against 2 with none.
	base = []string{
		heatmapBoundaryCell("w1", "o/ra:a.go", 2), heatmapBoundaryCell("w1", "o/rb:b.go", 3),
		heatmapBoundaryCell("w1", "o/rc:c1.go", 20), heatmapBoundaryCell("w1", "o/rc:c2.go", 20),
	}
	cand = []string{
		heatmapBoundaryCell("w1", "o/ra:a.go", 2), heatmapBoundaryCell("w1", "o/rb:b.go", 2),
		heatmapBoundaryCell("w1", "o/rc:c1.go", 10), heatmapBoundaryCell("w1", "o/rc:c2.go", 10),
	}
	baseline, candidate = heatmapAxisSnapshotsFromCells(t, base, cand,
		[]string{"o/rc:c1.go", "o/rc:c2.go", "o/rb:b.go", "o/ra:a.go"},
		[]string{"o/rc:c1.go", "o/rc:c2.go", "o/ra:a.go", "o/rb:b.go"})
	for _, opts := range []Options{heatmapHotspotRiskParity, heatmapHotspotRiskTeamScopedParity} {
		result := Compare(baseline, candidate, opts)
		if result.DifferencesOutsideBaselineDefect != 2 {
			t.Fatalf("hotspot_risk: outside = %d, want 2 (both axis positions): findings %v", result.DifferencesOutsideBaselineDefect, findingPathSet(result))
		}
	}
}

func TestHeatmapCellExplained(t *testing.T) {
	for _, tc := range []struct {
		name       string
		base, cand float64
		k          int
		want       bool
	}{
		{"k=1 equal", 5, 5, 1, true},
		{"k=1 unexplained", 3, 2, 1, false},
		{"k=1 within float tolerance", 241.41495059667758, 241.41495059667756, 1, true},
		{"k=0 (no verified k) equal", 5, 5, 0, true},
		{"k=0 differs", 6, 5, 0, false},
		{"k=2 doubled", 8, 4, 2, true},
		{"k=2 tripled", 12, 4, 2, false},
		{"k=2 unchanged nonzero", 4, 4, 2, false},
		{"k=2 both zero", 0, 0, 2, true},
		{"k=2 candidate zero", 4, 0, 2, false},
		{"k=3 tripled", 12, 4, 3, true},
		{"k=2 candidate larger", 4, 8, 2, false},
	} {
		if got := heatmapCellExplained(tc.base, tc.cand, tc.k); got != tc.want {
			t.Errorf("%s: heatmapCellExplained(%v, %v, %d) = %t, want %t", tc.name, tc.base, tc.cand, tc.k, got, tc.want)
		}
	}
}

// A cell present on one leg only is not a multiplied cell: the plan
// refuses.
func TestHeatmapAxisRepoOrderShape_RefusesACellOnOneLegOnly(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w2", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 6) + "," + heatmapBoundaryCell("w3", "repoB", 1)
	candCells := heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6)
	baseData := heatmapAxisOrderBody(t, baseCells, []string{"repoA", "repoB"})
	candData := heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})
	if plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData); plan.valid {
		t.Error("plan must refuse: repoB's w3 cell exists on the baseline only")
	}
	candCells = heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6) + "," + heatmapBoundaryCell("w3", "repoB", 1) + "," + heatmapBoundaryCell("w4", "repoB", 1)
	candData = heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})
	if plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData); plan.valid {
		t.Error("plan must refuse: repoB's w4 cell exists on the candidate only")
	}
	candCells = heatmapBoundaryCell("w1", "repoA", 2) + "," + heatmapBoundaryCell("w2", "repoA", 2) + "," + heatmapBoundaryCell("w1", "repoB", 6) + "," + heatmapBoundaryCell("w4", "repoB", 1)
	candData = heatmapAxisOrderBody(t, candCells, []string{"repoB", "repoA"})
	if plan := buildHeatmapAxisRepoOrderPlan(heatmapAxisOrderTestShape(), baseData, candData); plan.valid {
		t.Error("plan must refuse: equal cell counts, different keys (w3 baseline, w4 candidate)")
	}
}
