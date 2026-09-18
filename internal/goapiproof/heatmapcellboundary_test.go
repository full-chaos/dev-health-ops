package goapiproof

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
)

// This file exercises HeatmapCellBoundaryShape (heatmapcellboundary.go)
// through two real captured GET /api/v1/heatmap hotspot_risk response
// pairs (a production deployed-vs-deployed prove run) plus small,
// self-contained synthetic bodies for the rules the real captures never
// individually isolate.

func heatmapBoundaryOptions(ticket string, limit int) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.cells", KeyFields: []string{"x", "y"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: ticket, Reason: "test fixture",
			Paths:        []string{"data.cells", "data.axes.y"},
			Intermittent: true, IntermittentReason: "test fixture",
			HeatmapCellBoundaryShape: &HeatmapCellBoundaryShape{
				CellsListPath: "data.cells",
				CellKeyFields: []string{"x", "y"},
				FileField:     "y",
				CellValuePath: "data.cells.value",
				AxisListPath:  "data.axes.y",
				Limit:         limit,
			},
		}},
	}
}

func heatmapBoundaryCell(x, y string, value float64) string {
	return fmt.Sprintf(`{"x":%q,"y":%q,"value":%s}`, x, y, jsonFloat(value))
}

func heatmapBoundaryBody(t *testing.T, cellsJSON string, axisY []string) string {
	t.Helper()
	axisJSON, err := json.Marshal(axisY)
	if err != nil {
		t.Fatalf("marshal axis: %v", err)
	}
	return fmt.Sprintf(`{"axes":{"x":[],"y":%s},"cells":[%s]}`, axisJSON, cellsJSON)
}

// heatmapHotspotRiskCellBoundaryTicket reads the ticket of
// heatmapHotspotRiskParity's own HeatmapCellBoundaryShape entry, rather
// than a literal copy that could drift out of sync with restcorpus.go.
func heatmapHotspotRiskCellBoundaryTicket(t *testing.T) string {
	t.Helper()
	for _, d := range heatmapHotspotRiskParity.BaselineDefects {
		if d.HeatmapCellBoundaryShape != nil {
			return d.Ticket
		}
	}
	t.Fatal("heatmapHotspotRiskParity declares no HeatmapCellBoundaryShape entry")
	return ""
}

func heatmapDirectionSnapshotFromFileT(t *testing.T, path string) Snapshot {
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

// TestHeatmapCellBoundaryShape_RealHotspotRiskOrgCaptureExactCounts pins
// buildHeatmapCellBoundaryPlan's own admission directly against the real
// captured pair (hotspot_risk_org): one repository
// (full-chaos/dev-health-ops) fans out at exactly k=2 across every one of
// its 24 differing shared cells (30 shared cells total, 6 already equal
// on both legs), and supplies every one of 30 baseline-only and 30
// candidate-only cells -- 20 distinct files on each leg (the route's own
// fixed top-20), 10 shared, 10 leaving, 10 entering.
func TestHeatmapCellBoundaryShape_RealHotspotRiskOrgCaptureExactCounts(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFileT(t, "testdata/heatmap_hotspot_risk_fanout_org_baseline_4b4fe21c.json")
	candidate := heatmapDirectionSnapshotFromFileT(t, "testdata/heatmap_hotspot_risk_fanout_org_candidate_b690c4c5.json")

	shape := &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: 20,
	}
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan is not valid against the real captured pair")
	}
	if len(plan.admittedBaselineOnlyCells) != 30 {
		t.Fatalf("admittedBaselineOnlyCells = %d, want 30: %v", len(plan.admittedBaselineOnlyCells), plan.admittedBaselineOnlyCells)
	}
	if len(plan.admittedCandidateOnlyCells) != 30 {
		t.Fatalf("admittedCandidateOnlyCells = %d, want 30: %v", len(plan.admittedCandidateOnlyCells), plan.admittedCandidateOnlyCells)
	}
	if len(plan.admittedSharedValueCells) != 24 {
		t.Fatalf("admittedSharedValueCells = %d, want 24: %v", len(plan.admittedSharedValueCells), plan.admittedSharedValueCells)
	}
}

// heatmapBoundaryAssertOnlyAxisRippleOutside asserts what the real
// fan-out captures leave outside: every one of their 19 data.axes.y
// findings. The captures predate this port's name-ascending tiebreak --
// their candidate lists one tied pair (the two
// mcp_investigation_result_response.v1.schema.json files) in the other
// order -- so the candidate axis is not axisOrder's own order and the
// axis admission gate (heatmapCandidateAxisInGoOrder) admits no
// position. Every one of the 84 cell-level findings (60 presence + 24
// value) is covered.
func heatmapBoundaryAssertOnlyAxisRippleOutside(t *testing.T, result Result) {
	t.Helper()
	if result.DifferencesOutsideBaselineDefect != 19 {
		t.Fatalf("outside = %d, want 19 (every axis finding) -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	if result.OutsideByShape["value"] != 19 || len(result.OutsideByShape) != 1 {
		t.Fatalf("outsideByShape = %v, want exactly {value: 19}", result.OutsideByShape)
	}
}

// TestHeatmapHotspotRiskParity_RealOrgCaptureLeavesOnlyAxisRippleOutside
// runs the real captured pair through the actual registered
// heatmapHotspotRiskParity (not a hand-built stand-in): every cell-level
// finding (60 presence, 24 value) is covered, the entry reads matched,
// and every axis finding stays outside (the gate: the candidate's tie
// order predates the name-ascending tiebreak).
func TestHeatmapHotspotRiskParity_RealOrgCaptureLeavesOnlyAxisRippleOutside(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFileT(t, "testdata/heatmap_hotspot_risk_fanout_org_baseline_4b4fe21c.json")
	candidate := heatmapDirectionSnapshotFromFileT(t, "testdata/heatmap_hotspot_risk_fanout_org_candidate_b690c4c5.json")

	result := Compare(baseline, candidate, heatmapHotspotRiskParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if len(result.Findings) != 103 {
		t.Fatalf("findings = %d, want 103 (60 presence + 24 value + 19 axis)", len(result.Findings))
	}
	heatmapBoundaryAssertOnlyAxisRippleOutside(t, result)
	wantTicket := heatmapHotspotRiskCellBoundaryTicket(t)
	foundMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			foundMatched = true
		}
	}
	if !foundMatched {
		t.Fatalf("matched = %v, want %s among them: idle %v stale %v", result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}

// TestHeatmapHotspotRiskTeamScopedParity_RealTeamScopedCaptureLeavesOnlyAxisRippleOutside
// runs the sibling team-scoped capture (same baseline, a different
// candidate leg) through heatmapHotspotRiskTeamScopedParity, proving the
// new entry composes correctly with the route's OWN team-scope Options
// value (inherited via its own append chain, restcorpus.go) and leaves
// the SAME honest axis-ripple residue.
func TestHeatmapHotspotRiskTeamScopedParity_RealTeamScopedCaptureLeavesOnlyAxisRippleOutside(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFileT(t, "testdata/heatmap_hotspot_risk_fanout_org_baseline_4b4fe21c.json")
	candidate := heatmapDirectionSnapshotFromFileT(t, "testdata/heatmap_hotspot_risk_fanout_team_scoped_candidate_2e8cfc1d.json")

	result := Compare(baseline, candidate, heatmapHotspotRiskTeamScopedParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	heatmapBoundaryAssertOnlyAxisRippleOutside(t, result)
}

// TestHeatmapCellBoundaryShape_SyntheticCompareLeavesCellsFullyCovered
// runs the SAME 20-file synthetic fixture (padding, repo-a's k=2
// evidence, one leaver, one entrant) through the actual Compare
// pipeline via heatmapBoundaryOptions, with the axis lists held
// identical on both legs (a trivial placeholder) so this test isolates
// data.cells' own presence and value wiring end to end -- every
// baseline-only, candidate-only and differing-shared cell finding is
// covered.
func TestHeatmapCellBoundaryShape_SyntheticCompareLeavesCellsFullyCovered(t *testing.T) {
	weeks := []string{"w1", "w2"}
	var baseCells, candCells []string
	for i := 0; i < 17; i++ {
		file := fmt.Sprintf("pad:file%d.go", i)
		baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], file, 10))
		candCells = append(candCells, heatmapBoundaryCell(weeks[0], file, 10))
	}
	for i := 0; i < 2; i++ {
		file := fmt.Sprintf("repo-a:shared%d.go", i)
		for _, week := range weeks {
			v := 10 + float64(i)
			candCells = append(candCells, heatmapBoundaryCell(week, file, v))
			baseCells = append(baseCells, heatmapBoundaryCell(week, file, v*2))
		}
	}
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], "repo-a:leaver.go", 4))
	candCells = append(candCells, heatmapBoundaryCell(weeks[0], "repo-b:entrant.go", 2))

	placeholderAxis := []string{"placeholder"}
	baseBody := heatmapBoundaryBody(t, joinJSON(baseCells), placeholderAxis)
	candBody := heatmapBoundaryBody(t, joinJSON(candCells), placeholderAxis)
	baseline, err := DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatal(err)
	}

	result := Compare(baseline, candidate, heatmapBoundaryOptions("CHAOS-TEST-BOUNDARY", 20))
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-BOUNDARY"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-BOUNDARY]", result.BaselineDefectsMatched)
	}
}

// TestHeatmapFileSetDifference pins heatmapFileSetDifference's own
// contract directly: every file in a but not in b, nothing else, and an
// empty a returns nil with no iteration.
func TestHeatmapFileSetDifference(t *testing.T) {
	a := map[string]float64{"x": 1, "y": 2, "shared": 3}
	b := map[string]float64{"shared": 3, "z": 4}
	got := heatmapFileSetDifference(a, b)
	sort.Strings(got)
	if !equalStrings(got, []string{"x", "y"}) {
		t.Fatalf("difference = %v, want [x y]", got)
	}
	if got := heatmapFileSetDifference(map[string]float64{}, b); got != nil {
		t.Fatalf("difference over an empty map = %v, want nil", got)
	}
	if got := heatmapFileSetDifference(a, a); len(got) != 0 {
		t.Fatalf("difference of a set from itself = %v, want none", got)
	}
}

// TestHeatmapCellBoundaryShape_NonPositiveLimitRefusesThePlan pins rule
// 3's own Limit guard directly: a shape declaring a zero Limit refuses
// the whole plan, never reaching heatmapFileSetDifference's own loop at
// all.
func TestHeatmapCellBoundaryShape_NonPositiveLimitRefusesThePlan(t *testing.T) {
	baseBody := heatmapBoundaryBody(t, "", []string{})
	candBody := heatmapBoundaryBody(t, "", []string{})
	baseline, err := DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatal(err)
	}

	shape := &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: 0,
	}
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if plan.valid {
		t.Fatal("plan should refuse a zero Limit")
	}
}

// TestHeatmapCellBoundaryShape_LoneSharedCellDerivesNoK is the synthetic
// guard for rule 1: a SINGLE shared, differing cell for a repository,
// even at a clean integer ratio, never establishes k on its own -- a lone
// ratio that happens to be whole could be an unrelated undercount. With
// no k, nothing for that repository is admitted: not the value diff, not
// the boundary crossing.
func TestHeatmapCellBoundaryShape_LoneSharedCellDerivesNoK(t *testing.T) {
	weeks := []string{"w1"}
	// 18 untouched padding files (identical both legs), repo-a's own ONE
	// shared cell at a clean 2x ratio (the case under test), plus a
	// SEPARATE leaver/entrant pair so rule 3's own boundary-crossing
	// precondition holds -- with no k ever established for that pair's
	// own repository (it derives no ratio of its own at all, being
	// presence-only), it is never admitted either way, and this test
	// asserts nothing about it.
	var baseCells, candCells []string
	for i := 0; i < 18; i++ {
		file := fmt.Sprintf("pad:file%d.go", i)
		baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], file, 10))
		candCells = append(candCells, heatmapBoundaryCell(weeks[0], file, 10))
	}
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], "repo-a:onecell.go", 20))
	candCells = append(candCells, heatmapBoundaryCell(weeks[0], "repo-a:onecell.go", 10))
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], "repo-a:leaver.go", 5))
	candCells = append(candCells, heatmapBoundaryCell(weeks[0], "repo-a:entrant.go", 5))

	shape := &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: 20,
	}
	baseBody := heatmapBoundaryBody(t, joinJSON(baseCells), []string{})
	candBody := heatmapBoundaryBody(t, joinJSON(candCells), []string{})
	baseline, err := DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatal(err)
	}

	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid (both legs carry exactly 20 files)")
	}
	if len(plan.admittedSharedValueCells) != 0 {
		t.Fatalf("admittedSharedValueCells = %v, want none -- a single shared cell must never establish k on its own", plan.admittedSharedValueCells)
	}
}

// TestHeatmapCellBoundaryShape_NonIntegerRatioDerivesNoK is the synthetic
// guard for rule 1's other half: two shared cells whose ratio is NOT an
// integer never establish k, even though there are two of them.
func TestHeatmapCellBoundaryShape_NonIntegerRatioDerivesNoK(t *testing.T) {
	weeks := []string{"w1", "w2"}
	// 18 untouched padding files, repo-b's own file with TWO shared cells
	// both at a 1.5x ratio (the case under test), plus a leaver/entrant
	// pair so rule 3's own boundary-crossing precondition holds.
	var baseCells, candCells []string
	for i := 0; i < 18; i++ {
		file := fmt.Sprintf("pad:file%d.go", i)
		baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], file, 10))
		candCells = append(candCells, heatmapBoundaryCell(weeks[0], file, 10))
	}
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], "repo-b:only.go", 15))
	candCells = append(candCells, heatmapBoundaryCell(weeks[0], "repo-b:only.go", 10))
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[1], "repo-b:only.go", 30))
	candCells = append(candCells, heatmapBoundaryCell(weeks[1], "repo-b:only.go", 20))
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], "repo-b:leaver.go", 5))
	candCells = append(candCells, heatmapBoundaryCell(weeks[0], "repo-b:entrant.go", 5))

	shape := &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: 20,
	}
	baseBody := heatmapBoundaryBody(t, joinJSON(baseCells), []string{})
	candBody := heatmapBoundaryBody(t, joinJSON(candCells), []string{})
	baseline, err := DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatal(err)
	}

	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid (both legs carry exactly 20 files)")
	}
	if len(plan.admittedSharedValueCells) != 0 {
		t.Fatalf("admittedSharedValueCells = %v, want none -- a 1.5x ratio is not an integer multiplier", plan.admittedSharedValueCells)
	}
}

// heatmapBoundarySyntheticPlan builds a 20-file (Limit=20) synthetic
// comparison exercising every rule at once: 17 untouched padding files
// shared and identical on both legs; repo-a's own 2-file, 2-week shared
// block at a clean, repeated 2x ratio (4 agreeing samples, comfortably
// past rule 1's minimum of 2), establishing k=2; ONE repo-a leaver
// (baseline-only, true value 2, at or under the candidate's own
// minimum) and ONE repo-b entrant (candidate-only, value 3) admitted by
// rule 4.
func heatmapBoundarySyntheticPlan(t *testing.T) (*HeatmapCellBoundaryShape, *heatmapCellBoundaryPlan) {
	t.Helper()
	weeks := []string{"w1", "w2"}
	var baseCells, candCells []string
	for i := 0; i < 17; i++ {
		file := fmt.Sprintf("pad:file%d.go", i)
		baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], file, 10))
		candCells = append(candCells, heatmapBoundaryCell(weeks[0], file, 10))
	}
	for i := 0; i < 2; i++ {
		file := fmt.Sprintf("repo-a:shared%d.go", i)
		for _, week := range weeks {
			v := 10 + float64(i)
			candCells = append(candCells, heatmapBoundaryCell(week, file, v))
			baseCells = append(baseCells, heatmapBoundaryCell(week, file, v*2))
		}
	}
	baseCells = append(baseCells, heatmapBoundaryCell(weeks[0], "repo-a:leaver.go", 4))
	candCells = append(candCells, heatmapBoundaryCell(weeks[0], "repo-b:entrant.go", 3))

	shape := &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: 20,
	}
	// Both axes are each plane's own sort of its own totals: the
	// candidate's is axisOrder's (ties by name), the baseline's keeps the
	// same order except two tied padding files swapped -- an order
	// Python's row-encounter tiebreak can produce.
	candTotals := map[string]float64{"repo-b:entrant.go": 3}
	baseTotals := map[string]float64{"repo-a:leaver.go": 4}
	for i := 0; i < 17; i++ {
		file := fmt.Sprintf("pad:file%d.go", i)
		candTotals[file], baseTotals[file] = 10, 10
	}
	for i := 0; i < 2; i++ {
		file := fmt.Sprintf("repo-a:shared%d.go", i)
		candTotals[file] = 2 * (10 + float64(i))
		baseTotals[file] = 4 * (10 + float64(i))
	}
	axisCand := heatmapSortDescByTotal(candTotals)
	axisBase := heatmapSortDescByTotal(baseTotals)
	axisBase[2], axisBase[3] = axisBase[3], axisBase[2]
	baseBody := heatmapBoundaryBody(t, joinJSON(baseCells), axisBase)
	candBody := heatmapBoundaryBody(t, joinJSON(candCells), axisCand)
	baseline, err := DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatal(err)
	}
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 20 distinct files")
	}
	return shape, plan
}

// TestHeatmapCellBoundaryShape_EntrantLeaverAdmitted pins rule 4's own
// admission on the synthetic 20-file fixture: the leaver's true value
// (4/k=2) sits under the candidate's own minimum (the entrant's own
// value, 3), so both are admitted.
func TestHeatmapCellBoundaryShape_EntrantLeaverAdmitted(t *testing.T) {
	_, plan := heatmapBoundarySyntheticPlan(t)
	if !plan.admittedFileNames["repo-a:leaver.go"] {
		t.Error("repo-a:leaver.go should be admitted as an entrant candidate under repo-a's verified k=2")
	}
	if !plan.admittedFileNames["repo-b:entrant.go"] {
		t.Error("repo-b:entrant.go should be admitted one-for-one against the leaver")
	}
}

// Axis admission runs only through rule 6b's whole-list identity: on
// the synthetic 20-file pair it holds and the leaver/entrant position is
// admitted; with the identity set aside, no position is admitted even
// though both names at the leaver/entrant position were admitted at the
// cell level.
func TestHeatmapCellBoundaryShape_AxisAdmissionOnlyThroughTheWholeListIdentity(t *testing.T) {
	_, plan := heatmapBoundarySyntheticPlan(t)
	if !plan.candidateAxisInGoOrder || !plan.axisIdentityHolds {
		t.Fatalf("fixture premise: gate %t, identity %t", plan.candidateAxisInGoOrder, plan.axisIdentityHolds)
	}
	if !plan.admittedFileNames["repo-a:leaver.go"] || !plan.admittedFileNames["repo-b:entrant.go"] {
		t.Fatal("fixture premise: the leaver and the entrant are cell-level admissions")
	}
	boundaryFinding := Finding{Kind: FindingMismatch, Path: "$.data.axes.y[19]", Detail: "repo-a:leaver.go != repo-b:entrant.go", Shape: ShapeValue}
	padFinding := Finding{Kind: FindingMismatch, Path: "$.data.axes.y[2]", Detail: "pad:file1.go != pad:file0.go", Shape: ShapeValue}
	for _, finding := range []Finding{boundaryFinding, padFinding} {
		if !plan.admits(finding) {
			t.Errorf("%s should be admitted while the whole-list identity holds", finding.Path)
		}
	}
	plan.axisIdentityHolds = false
	for _, finding := range []Finding{boundaryFinding, padFinding} {
		if plan.admits(finding) {
			t.Errorf("%s admitted without the whole-list identity", finding.Path)
		}
	}
}

func joinJSON(items []string) string {
	out := ""
	for i, item := range items {
		if i > 0 {
			out += ","
		}
		out += item
	}
	return out
}
