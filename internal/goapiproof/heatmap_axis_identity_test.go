package goapiproof

import (
	"fmt"
	"testing"
)

// This file exercises HeatmapCellBoundaryShape's own rule 6b -- the
// two-sided, tie-group-aware whole-list axis identity
// (heatmapcellboundary.go) -- through minimal synthetic fixtures plus a
// real captured GET /api/v1/heatmap hotspot_risk pair (a production
// deployed-vs-deployed prove run) and a fixture derived from it.

const (
	heatmapAxisIdentityBaselinePath           = "testdata/heatmap_hotspot_risk_pagecut_org_baseline_3747a3c5.json"
	heatmapAxisIdentityCandidatePath          = "testdata/heatmap_hotspot_risk_pagecut_org_candidate_a1926513.json"
	heatmapAxisIdentityNameOrderCandidatePath = "testdata/heatmap_hotspot_risk_pagecut_org_candidate_nameorder_e0905c95.json"
)

// heatmapAxisEntry is one file's own (baseline, candidate) cell value
// for heatmapAxisEntrantLeaverFixture.
type heatmapAxisEntry struct {
	file       string
	base, cand float64
}

// heatmapAxisEntrantLeaverFixture builds a hotspot_risk-shaped pair from
// explicit shared entries (present on both legs) plus exactly one
// baseline-only leaver and one candidate-only entrant -- for the tests
// that need a genuine boundary crossing (the union larger than Limit).
// Limit must equal len(shared)+1.
func heatmapAxisEntrantLeaverFixture(t *testing.T, shared []heatmapAxisEntry, leaverFile string, leaverBase float64, entrantFile string, entrantCand float64, axisBase, axisCand []string) (baseline, candidate Snapshot) {
	t.Helper()
	var baseCells, candCells []string
	for _, e := range shared {
		baseCells = append(baseCells, heatmapBoundaryCell("w1", e.file, e.base))
		candCells = append(candCells, heatmapBoundaryCell("w1", e.file, e.cand))
	}
	baseCells = append(baseCells, heatmapBoundaryCell("w1", leaverFile, leaverBase))
	candCells = append(candCells, heatmapBoundaryCell("w1", entrantFile, entrantCand))
	return heatmapAxisSnapshotsFromCells(t, baseCells, candCells, axisBase, axisCand)
}

func heatmapAxisSnapshotsFromCells(t *testing.T, baseCells, candCells []string, axisBase, axisCand []string) (baseline, candidate Snapshot) {
	t.Helper()
	baseBody := heatmapBoundaryBody(t, joinJSON(baseCells), axisBase)
	candBody := heatmapBoundaryBody(t, joinJSON(candCells), axisCand)
	var err error
	baseline, err = DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err = DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatal(err)
	}
	return baseline, candidate
}

func heatmapAxisShape(limit int) *HeatmapCellBoundaryShape {
	return &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: limit,
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsCleanRipple is
// the positive synthetic guard for rule 6b's own ripple case: an
// untouched file's rank moves purely because its touched (repo-a)
// neighbours' own ranks move around it, with no tie anywhere -- rules
// 1-5 alone cannot reach this axis position (neither name is a rule-4/
// rule-2 admission), rule 6b does.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsCleanRipple(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"repo-a:big.go", 20, 10},
		{"repo-a:mid.go", 16, 8},
		{"ops:untouched.go", 9, 9},
	}
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"repo-a:leaver.go", 0.02, "repo-b:entrant.go", 0.015,
		[]string{"repo-a:big.go", "repo-a:mid.go", "ops:untouched.go", "repo-a:leaver.go"},
		[]string{"repo-a:big.go", "ops:untouched.go", "repo-a:mid.go", "repo-b:entrant.go"},
	)
	shape := heatmapAxisShape(4)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 4 distinct files")
	}
	if !plan.axisIdentityHolds {
		t.Fatal("rule 6b should hold: no tie anywhere in the union, both identities reproduce their own target axis exactly")
	}
	if plan.admittedFileNames["ops:untouched.go"] {
		t.Fatal("test premise broken: ops:untouched.go must NOT be a rule-4/rule-2 admission on its own, or this test would not isolate rule 6b")
	}
	pos1 := Finding{Kind: FindingMismatch, Path: "$.data.axes.y[1]", Detail: "repo-a:mid.go != ops:untouched.go", Shape: ShapeValue}
	if !plan.admits(pos1) {
		t.Error("axis position 1 (mid.go/untouched.go swap) should be admitted by rule 6b")
	}
	pos2 := Finding{Kind: FindingMismatch, Path: "$.data.axes.y[2]", Detail: "ops:untouched.go != repo-a:mid.go", Shape: ShapeValue}
	if !plan.admits(pos2) {
		t.Error("axis position 2 (untouched.go/mid.go swap) should be admitted by rule 6b")
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsATieGroupInNameOrder
// is the positive synthetic guard for the tie-group mechanism itself:
// two untouched files ("grp:alpha.go"/"grp:beta.go") tie on the exact
// same total on BOTH legs. The candidate's own order inside the tie is
// name-ascending (this port's own deterministic tiebreak); the
// baseline's own order inside the SAME tie is the opposite (its own
// tiebreak is unverifiable, and rule 6b never constrains it) -- both
// identities still hold.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsATieGroupInNameOrder(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"repo-a:big.go", 20, 10},
		{"repo-a:mid.go", 16, 8},
		{"grp:alpha.go", 9, 9},
		{"grp:beta.go", 9, 9},
	}
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"repo-a:leaver.go", 0.002, "repo-x:entrant.go", 0.0015,
		[]string{"repo-a:big.go", "repo-a:mid.go", "grp:beta.go", "grp:alpha.go", "repo-a:leaver.go"},
		[]string{"repo-a:big.go", "grp:alpha.go", "grp:beta.go", "repo-a:mid.go", "repo-x:entrant.go"},
	)
	shape := heatmapAxisShape(5)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 5 distinct files")
	}
	if !plan.axisIdentityHolds {
		t.Fatal("rule 6b should hold: the tie group's own set is identical on both legs, candidate order is name-ascending, baseline order (reversed) is unconstrained")
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesWhenCandidateGroupNotNameAscending
// is the negative counterpart: the SAME tie group, but the candidate's
// own order inside it is REVERSED (beta before alpha) -- identity (i)
// refuses because this port's own deterministic tiebreak is
// name-ascending, never anything else.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesWhenCandidateGroupNotNameAscending(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"repo-a:big.go", 20, 10},
		{"repo-a:mid.go", 16, 8},
		{"grp:alpha.go", 9, 9},
		{"grp:beta.go", 9, 9},
	}
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"repo-a:leaver.go", 0.002, "repo-x:entrant.go", 0.0015,
		[]string{"repo-a:big.go", "repo-a:mid.go", "grp:alpha.go", "grp:beta.go", "repo-a:leaver.go"},
		[]string{"repo-a:big.go", "grp:beta.go", "grp:alpha.go", "repo-a:mid.go", "repo-x:entrant.go"},
	)
	shape := heatmapAxisShape(5)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 5 distinct files")
	}
	if plan.axisIdentityHolds {
		t.Fatal("rule 6b should refuse: the candidate's own order inside the tie group is beta-before-alpha, not name-ascending")
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnAStraddlingGroup
// is the negative synthetic guard for the straddle rule: a genuine,
// untouched 2-file tie group ("grp:b.go"/"grp:c.go", both value 5 on
// both legs) sits exactly where the Limit cut would otherwise fall
// between its own two members (a solo file at position 0, an unrelated
// baseline-only/candidate-only pair at position 4+) -- rule 6b cannot
// determine which ONE of the tied pair the cut would keep, and refuses.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnAStraddlingGroup(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"grp:a.go", 10, 10},
		{"grp:b.go", 5, 5},
		{"grp:c.go", 5, 5},
	}
	// leaver/entrant both untouched-repo (uncorroborated), same value 1
	// -- they tie with EACH OTHER too (t(leaver)=b(leaver)=1,
	// t(entrant)=b(entrant)=1), so the union's own t-groups are
	// [a](10), [b,c](5), [leaver,entrant](1) -- sizes 1,2,2, cumulative
	// 1,3,5. Limit=4 falls strictly inside the THIRD group (position 3
	// to 5): a straddle.
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"grp:leaver.go", 1, "grp:entrant.go", 1,
		[]string{"grp:a.go", "grp:b.go", "grp:c.go", "grp:leaver.go"},
		[]string{"grp:a.go", "grp:b.go", "grp:c.go", "grp:entrant.go"},
	)
	shape := heatmapAxisShape(4)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 4 distinct files")
	}
	if plan.axisIdentityHolds {
		t.Fatal("rule 6b should refuse: the {leaver, entrant} tie group straddles the Limit cut")
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsALegitimateCrossRepoBaselineTie
// is F5's own capture, reproduced directly: repo-a's own k=2 (established
// by p, q), x a THIRD repo-a file present on BOTH legs (true 5, baseline
// 10), and ops:u.go an untouched file present on both legs at 10 -- x's
// own INFLATED baseline value coincidentally equals u's own untouched
// value, a genuine tie in b(f) alone. t(f) carries no such tie (x's own
// true value, 5, is unrelated to u's, 10), so the two keys' own
// groupings are NOT the identical partition -- and neither identity
// needs them to be: identity (i) checks only the t(f) grouping against
// the candidate's own axis (no tie there, an exact match); identity (ii)
// checks only the b(f) grouping against the baseline's own axis, and
// admits the {x, u} tie regardless of which of the two names the
// baseline's own row happens to list first (TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsATieGroupInNameOrder
// already pins that order-independence directly, on a simpler fixture).
// A genuine regression touching this exact tie still stays outside,
// already covered generically: a baseline axis naming a file outside the
// tied group's own set
// (TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnANameInTheWrongGroup)
// or a tie straddling the Limit cut
// (TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnAStraddlingGroup)
// refuse regardless of which repository's own fan-out produced the tie.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsALegitimateCrossRepoBaselineTie(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"repo-a:p.go", 40, 20}, // establishes repo-a's own k=2 with q.go
		{"repo-a:q.go", 36, 18},
		{"repo-a:x.go", 10, 5}, // shared: true 5, baseline 10 (= u's own value)
		{"ops:u.go", 10, 10},   // untouched, shared
	}
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"repo-a:leaver.go", 0.6, "repo-b:entrant.go", 0.5,
		[]string{"repo-a:p.go", "repo-a:q.go", "repo-a:x.go", "ops:u.go", "repo-a:leaver.go"},
		[]string{"repo-a:p.go", "repo-a:q.go", "ops:u.go", "repo-a:x.go", "repo-b:entrant.go"},
	)
	shape := heatmapAxisShape(5)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 5 distinct files")
	}
	if !plan.axisIdentityHolds {
		t.Fatal("rule 6b should hold: x/u tie under b(f) alone is not a regression, and neither identity needs the two keys' own groupings to match")
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnANameInTheWrongGroup
// is the negative synthetic guard for identity (ii)'s own set check: the
// candidate's own axis is a clean, name-ascending reconstruction (so
// identity (i) holds), but the baseline's own axis names a file that is
// NOT a member of the tie group occupying that position range at all --
// identity (ii) refuses on the set mismatch, never guessing that a
// wrong name was "close enough".
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnANameInTheWrongGroup(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"grp:a.go", 10, 10},
		{"grp:b.go", 5, 5},
		{"grp:c.go", 5, 5},
	}
	// entrant (t=0.002) ranks ABOVE leaver (t=0.0015): identity (i)'s own
	// reconstruction correctly excludes leaver and includes entrant,
	// matching axisCandidate exactly, so identity (i) holds and identity
	// (ii) is the ONE check this fixture isolates.
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"grp:leaver.go", 0.0015, "grp:entrant.go", 0.002,
		[]string{"grp:a.go", "grp:b.go", "grp:zzz-not-a-member.go", "grp:leaver.go"},
		[]string{"grp:a.go", "grp:b.go", "grp:c.go", "grp:entrant.go"},
	)
	shape := heatmapAxisShape(4)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 4 distinct files")
	}
	if plan.axisIdentityHolds {
		t.Fatal("rule 6b should refuse: the baseline's own axis names a file outside the {b,c} tie group's own set")
	}
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnAMismatchedAxisLength
// is the synthetic guard for rule 6b's own axis-length precondition
// (heatmapAxisTwoSidedIdentityHolds: len(axisCandidate) != limit):
// nothing else in this file requires data.axes.y's own array length to
// agree with the route's own top-N cell-total count, so a candidate
// response whose axis array is SHORTER than its own file set (3 names
// for 4 distinct files) is a genuine, reachable malformed-body case,
// distinct from every other precondition this shape already checks.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesOnAMismatchedAxisLength(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"grp:a.go", 10, 10},
		{"grp:b.go", 8, 8},
		{"grp:c.go", 6, 6},
	}
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"grp:leaver.go", 0.002, "grp:entrant.go", 0.0015,
		[]string{"grp:a.go", "grp:b.go", "grp:c.go", "grp:leaver.go"},
		[]string{"grp:a.go", "grp:b.go", "grp:c.go", "grp:entrant.go"},
	)
	// Truncate the candidate's own axis array to 3 entries -- shorter
	// than its own 4 distinct files -- after the well-formed fixture is
	// built.
	candMap, ok := candidate.Data.(map[string]any)
	if !ok {
		t.Fatal("candidate.Data is not an object")
	}
	axes, ok := candMap["axes"].(map[string]any)
	if !ok {
		t.Fatal("candidate axes is not an object")
	}
	y, ok := axes["y"].([]any)
	if !ok || len(y) != 4 {
		t.Fatalf("candidate axes.y = %v (ok=%v), want 4 entries", y, ok)
	}
	axes["y"] = y[:3]

	shape := heatmapAxisShape(4)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs still carry exactly 4 distinct files (only the axis array itself is truncated)")
	}
	if plan.axisIdentityHolds {
		t.Fatal("rule 6b should refuse: the candidate's own axis array (3 entries) does not carry Limit (4) entries")
	}
}

// TestHeatmapAxisIdentityICandidateMatches_OvershootingGroupRefuses calls
// heatmapAxisIdentityICandidateMatches directly (bypassing
// buildHeatmapCellBoundaryPlan's own combined admission, where identity
// (ii) can independently refuse the SAME fixture and mask this function's
// own verdict): a straddling group's own accumulation overshoots limit,
// and the length check right after the loop is the ONLY thing standing
// between that overshoot and a coincidental prefix-truncation match --
// group two of the three trueGroups below has its own two names sorted
// so that truncating the overshot prefix to exactly `limit` entries
// would otherwise byte-match axisCandidate.
func TestHeatmapAxisIdentityICandidateMatches_OvershootingGroupRefuses(t *testing.T) {
	trueGroups := [][]string{
		{"a"},
		{"b", "c"},
		{"d", "z"}, // straddles limit=4 at position 3 (size 2, overshoots to 5)
	}
	axisCandidate := []string{"a", "b", "c", "d"} // matches the overshot prefix's own first 4 entries
	if heatmapAxisIdentityICandidateMatches(trueGroups, axisCandidate, 4) {
		t.Fatal("should refuse: the third group overshoots limit, and axisCandidate must not be judged against a truncated prefix")
	}
}

// TestHeatmapAxisIdentityIIBaselineMatches_OvershootingGroupRefuses is
// the same direct isolation for identity (ii)'s own length/bounds check.
func TestHeatmapAxisIdentityIIBaselineMatches_OvershootingGroupRefuses(t *testing.T) {
	baseGroups := [][]string{
		{"a"},
		{"b", "c"},
		{"d", "z"}, // straddles limit=4 the same way
	}
	axisBaseline := []string{"a", "b", "c", "d"}
	if heatmapAxisIdentityIIBaselineMatches(baseGroups, axisBaseline, 4) {
		t.Fatal("should refuse: the third group overshoots limit and len(axisBaseline)")
	}
}

// TestHeatmapHotspotRiskParity_RealPageCutCaptureAxisRippleStaysOutside
// runs a real captured pair through the actual registered
// heatmapHotspotRiskParity: the capture's own single tie group ("contracts/
// jsonschema/v1/mcp_investigation_result_response.v1.schema.json" and
// "internal/mcp/schemas/mcp_investigation_result_response.v1.schema.json",
// sharing the exact same hotspot_risk total on both legs) predates this
// port's own name-ascending tiebreak: the candidate's own order inside
// it is "internal/..." before "contracts/...", the OPPOSITE of
// name-ascending, so identity (i) refuses for exactly that reason, and
// the axis admission gate (heatmapCandidateAxisInGoOrder) refuses every
// axis position: the candidate axis is not axisOrder's own order. All 20
// axis findings stay outside; every cell-level finding stays covered.
func TestHeatmapHotspotRiskParity_RealPageCutCaptureAxisRippleStaysOutside(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFileT(t, heatmapAxisIdentityBaselinePath)
	candidate := heatmapDirectionSnapshotFromFileT(t, heatmapAxisIdentityCandidatePath)

	shape := heatmapHotspotRiskCellBoundaryShape(t)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid against the real captured pair")
	}
	if plan.axisIdentityHolds {
		t.Fatal("rule 6b should refuse on the real capture -- the candidate's own order inside its one tie group is not name-ascending")
	}

	result := Compare(baseline, candidate, heatmapHotspotRiskParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if len(result.Findings) != 98 {
		t.Fatalf("findings = %d, want 98", len(result.Findings))
	}
	if result.DifferencesOutsideBaselineDefect != 20 {
		t.Fatalf("outside = %d, want 20 (every axis finding: the capture predates the name-ascending tiebreak) -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if result.OutsideByShape["value"] != 20 || len(result.OutsideByShape) != 1 {
		t.Fatalf("outsideByShape = %v, want exactly {value: 20}", result.OutsideByShape)
	}
}

// TestHeatmapHotspotRiskParity_DerivedCaptureWithNameAscendingTieAdmitsFully
// runs the SAME captured baseline against a candidate that differs from
// the real capture in EXACTLY one way: the one tie group's own two names
// are reordered name-ascending (matching what this port's own live
// tiebreak produces today). Every cell is untouched. With that one
// change, both identities hold over the WHOLE axis -- including the
// ripple positions the real capture's own tie was blocking -- and every
// one of the 98 findings admits: 98 -> 0 outside.
func TestHeatmapHotspotRiskParity_DerivedCaptureWithNameAscendingTieAdmitsFully(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFileT(t, heatmapAxisIdentityBaselinePath)
	candidate := heatmapDirectionSnapshotFromFileT(t, heatmapAxisIdentityNameOrderCandidatePath)

	shape := heatmapHotspotRiskCellBoundaryShape(t)
	plan := buildHeatmapCellBoundaryPlan(shape, baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid against the derived captured pair")
	}
	if !plan.axisIdentityHolds {
		t.Fatal("rule 6b should hold once the tie group's own candidate order is name-ascending")
	}

	result := Compare(baseline, candidate, heatmapHotspotRiskParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if len(result.Findings) != 98 {
		t.Fatalf("findings = %d, want 98", len(result.Findings))
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// heatmapHotspotRiskCellBoundaryShape reads heatmapHotspotRiskParity's
// own declared HeatmapCellBoundaryShape entry directly, rather than a
// literal copy that could drift out of sync with restcorpus.go.
func heatmapHotspotRiskCellBoundaryShape(t *testing.T) *HeatmapCellBoundaryShape {
	t.Helper()
	for _, d := range heatmapHotspotRiskParity.BaselineDefects {
		if d.HeatmapCellBoundaryShape != nil {
			return d.HeatmapCellBoundaryShape
		}
	}
	t.Fatal("heatmapHotspotRiskParity declares no HeatmapCellBoundaryShape entry")
	return nil
}

// TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsACorroboratedLeaverEntrantRankShift
// pins rule 6b's own PROVENANCE LIMIT paragraph as a present fact: given
// a repository whose verified multiplier k (rule 1) is corroborated by
// TWO INDEPENDENT shared cells -- repo-a:file-1 (200 baseline / 100
// candidate) and repo-a:file-2 (100 baseline / 50 candidate), both
// exactly k=2 -- and a floor-consistent leaver/entrant pair rule 4
// already admits on its own (repo-a:leaver, true value 0.5, at or under
// candidate's own floor; repo-b:entrant, 0.6, at or under baseline's own
// floor), rule 6b propagates that SAME admission to the one axis
// position (position 1) where an untouched neighbour (repo-pad:file-00)
// outranks repo-a:file-2 on the candidate's own leg purely because
// file-2's own true value (50, below every pad file's 54-70) moved --
// never a position rule 4/1 did not already, independently, admit
// evidence for. This is STRUCTURAL corroboration (two independent
// agreeing ratios, not one), the same bar rule 1 has always required;
// it is not, and this shape never claims it to be, proof that this
// specific candidate body's numbers were produced by the repos-join
// mechanism rather than an unrelated regression that happens to produce
// the identical ratio -- see the type doc comment's own PROVENANCE LIMIT
// paragraph.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsACorroboratedLeaverEntrantRankShift(t *testing.T) {
	var baselineCells, candidateCells []string
	var padNames []string
	for i := 0; i < 17; i++ {
		name := fmt.Sprintf("repo-pad:file-%02d", i)
		value := float64(70 - i)
		padNames = append(padNames, name)
		baselineCells = append(baselineCells, heatmapBoundaryCell("w1", name, value))
		candidateCells = append(candidateCells, heatmapBoundaryCell("w1", name, value))
	}
	baselineCells = append(baselineCells,
		heatmapBoundaryCell("w1", "repo-a:file-1", 200),
		heatmapBoundaryCell("w1", "repo-a:file-2", 100),
		heatmapBoundaryCell("w1", "repo-a:leaver", 1),
	)
	candidateCells = append(candidateCells,
		heatmapBoundaryCell("w1", "repo-a:file-1", 100),
		heatmapBoundaryCell("w1", "repo-a:file-2", 50),
		heatmapBoundaryCell("w1", "repo-b:entrant", 0.6),
	)
	baselineAxis := append([]string{"repo-a:file-1", "repo-a:file-2"}, padNames...)
	baselineAxis = append(baselineAxis, "repo-a:leaver")
	candidateAxis := append([]string{"repo-a:file-1"}, padNames...)
	candidateAxis = append(candidateAxis, "repo-a:file-2", "repo-b:entrant")

	base := restSnapshotFromJSON(t, heatmapBoundaryBody(t, joinJSON(baselineCells), baselineAxis))
	cand := restSnapshotFromJSON(t, heatmapBoundaryBody(t, joinJSON(candidateCells), candidateAxis))

	shape := &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells", CellKeyFields: []string{"x", "y"}, FileField: "y",
		CellValuePath: "data.cells.value", AxisListPath: "data.axes.y", Limit: 20,
	}
	plan := buildHeatmapCellBoundaryPlan(shape, base.Data, cand.Data)
	if !plan.valid {
		t.Fatal("plan should be valid -- both legs reconstruct to exactly 20 distinct files")
	}
	if !plan.axisIdentityHolds {
		t.Fatal("rule 6b should hold: identity (i) matches candidate's own true-total order exactly, identity (ii) matches baseline's own re-inflated-total order exactly")
	}
	position1 := Finding{Path: "$.data.axes.y[1]", Shape: ShapeValue}
	if !plan.admits(position1) {
		t.Fatal("position 1 (repo-a:file-2 vs repo-pad:file-00) should be admitted: an untouched pad file's rank moved only because the corroborated leaver's own true value dropped below it")
	}

	result := Compare(base, cand, heatmapBoundaryOptions("CHAOS-TEST-RANKSHIFT", 20))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Rule 6b refuses when a shared file's value moved without its
// repository's verified multiplier, even where the k-scaled totals put
// it inside a tie: ops:y.go's baseline 9 against candidate 8 (no verified
// k for ops) would otherwise sit in the 8/8 run with ops:x.go.
func TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityRefusesAnUnexplainedValueInsideATie(t *testing.T) {
	shared := []heatmapAxisEntry{
		{"repo-a:big.go", 20, 10},
		{"repo-a:mid.go", 16, 8},
		{"ops:x.go", 8, 8},
		{"ops:y.go", 9, 8},
	}
	baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared,
		"repo-a:leaver.go", 0.002, "repo-x:entrant.go", 0.0015,
		[]string{"repo-a:big.go", "repo-a:mid.go", "ops:y.go", "ops:x.go", "repo-a:leaver.go"},
		[]string{"repo-a:big.go", "ops:x.go", "ops:y.go", "repo-a:mid.go", "repo-x:entrant.go"},
	)
	plan := buildHeatmapCellBoundaryPlan(heatmapAxisShape(5), baseline.Data, candidate.Data)
	if !plan.valid {
		t.Fatal("plan should be valid: both legs carry exactly 5 distinct files")
	}
	if plan.axisIdentityHolds {
		t.Fatal("rule 6b must refuse: ops:y.go's value moved with no verified multiplier for ops")
	}
	for _, idx := range []int{1, 2, 3} {
		if plan.admits(Finding{Kind: FindingMismatch, Path: fmt.Sprintf("$.data.axes.y[%d]", idx), Shape: ShapeValue}) {
			t.Errorf("axis position %d must stay outside", idx)
		}
	}
}
