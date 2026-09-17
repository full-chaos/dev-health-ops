package goapiproof

import (
	"encoding/json"
	"os"
	"testing"
)

// This file exercises CoverageShiftShape (coverageshift.go) directly
// through the registered CHAOS-4547 declaration on investmentFull: the
// SHAPE-SPECIFIC admission that replaces a blanket "any difference under
// sankey.coverage.teamCoverage/.repoCoverage is covered" rule. It reuses
// investmentfull_repojoin_fanout_shape_test.go's own fixture and helpers
// (loadSankeyCandidateCopy, sankeyOf, marshalBody, compareAsInvestmentFull):
// every baseline here starts as a byte-identical copy of the committed
// job5 candidate capture, so every field not deliberately mutated stays
// equal between the two legs.

// mutateCoverage scales both coverage ratios down by relativeDelta and
// returns their pre-mutation values.
func mutateCoverage(t *testing.T, body map[string]any, relativeDelta float64) (repoBefore, teamBefore float64) {
	t.Helper()
	coverage := sankeyOf(t, body)["coverage"].(map[string]any)
	repoBefore = coverage["repoCoverage"].(float64)
	teamBefore = coverage["teamCoverage"].(float64)
	coverage["repoCoverage"] = repoBefore * (1 - relativeDelta)
	coverage["teamCoverage"] = teamBefore * (1 - relativeDelta)
	return repoBefore, teamBefore
}

// A coverage-only shift with sankey nodes/edges untouched is exactly
// CHAOS-4547's argMax null-transition mechanism and is fully admitted.
// It is also, from the response bodies alone, indistinguishable from a
// downward-only supersession skew (CHAOS-5865): both mechanisms produce
// the identical observable shape (only the two coverage leaves differ,
// baseline strictly below candidate), so both legitimately match here --
// SupersessionSkewShape checks direction only and has no bound to
// exclude a 2% shift.
func TestCoverageShiftShape_CoverageOnlyShiftWithinBoundIsAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.02) // 2% relative shift, within the 5% bound

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-4547", "CHAOS-5865"}) {
		t.Fatalf("matched = %v, want [CHAOS-4547 CHAOS-5865] -- the repos-join fan-out must not also claim a coverage-only shift, but the direction-only supersession citation legitimately does", result.BaselineDefectsMatched)
	}
}

// A coverage shift accompanied by a real sankey node difference is NOT
// the argMax null-transition mechanism -- CHAOS-4547 must not match it,
// even though its cited paths still overlap the differing coverage
// leaves.
func TestCoverageShiftShape_CoverageShiftWithANodeDifferenceIsNotMatched(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.02)
	sankey := sankeyOf(t, baseline)
	found := false
	for _, n := range sankey["nodes"].([]any) {
		node := n.(map[string]any)
		if node["id"].(string) == "REPO:full-chaos/dev-health-ops" {
			node["value"] = node["value"].(float64) * 1.01 // unrelated drift, not a fan-out multiple
			found = true
			break
		}
	}
	if !found {
		t.Fatal("fixture no longer carries REPO:full-chaos/dev-health-ops")
	}

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == "CHAOS-4547" {
			t.Fatalf("matched = %v, must not include CHAOS-4547 -- a sankey node also differs, so this is not the argMax null-transition shape", result.BaselineDefectsMatched)
		}
	}
}

// Rule 2 must admit two REAL, INDEPENDENTLY-COMPUTED sankey subtrees --
// not loadSankeyCandidateCopy's own fixture (one decoded candidate
// capture, decoded a second time into the "baseline"), which every other
// test in this file uses and which can never distinguish byte equality
// from tiered equality: nodes/edges are the identical parsed float64
// values by construction there. testdata's job5 baseline/candidate pair
// are two SEPARATELY EXECUTED query results; diffed directly, their
// sankey nodes/edges values differ from each other by construction
// (measured ~1e-13, ClickHouse's own summation-order noise, the same
// shape this operation's own committed FloatTierB entry for
// repoCoverage's 1-ULP coverage measurement documents)
// while coverage.teamCoverage matches and coverage.repoCoverage differs
// by 1 ULP -- both far under floatTolerance, so neither the coverage
// leaves nor any sankey node/edge produces a Finding on this pair
// unmodified. The coverage leaves are shifted a further, clearly-citable
// 2% here so this exercises a real coverage-only divergence through the
// actual registered declaration (compareAsInvestmentFull ->
// classifyBaselineDefects), not buildCoverageShiftPlan called directly.
func TestCoverageShiftShape_RealIndependentlyComputedNodesAndEdgesAreAdmitted(t *testing.T) {
	const baselinePath = "testdata/investmentfull_baseline_job5_773418f7.json"
	const candidatePath = "testdata/investmentfull_candidate_job5_7ae7cb5b.json"

	candidateRaw, err := os.ReadFile(candidatePath)
	if err != nil {
		t.Fatalf("read %s: %v", candidatePath, err)
	}
	baselineRaw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("read %s: %v", baselinePath, err)
	}
	var baseline map[string]any
	if err := json.Unmarshal(baselineRaw, &baseline); err != nil {
		t.Fatalf("decode %s: %v", baselinePath, err)
	}
	mutateCoverage(t, baseline, 0.02) // 2% relative shift, within the 5% bound

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), string(candidateRaw))
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- rule 2 must admit two independently-computed real nodes/edges values within tolerance, findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-4547", "CHAOS-5865"}) {
		t.Fatalf("matched = %v, want [CHAOS-4547 CHAOS-5865] -- a byte-equality rule 2 could never admit this real pair's own ~1e-13 node/edge noise", result.BaselineDefectsMatched)
	}
}

// A shift larger than the bound is not admitted by CoverageShiftShape
// itself -- that shape draws a line, not a blank check. The comparison as
// a whole can still show outside=0, because the direction-only
// supersession citation (CHAOS-5865, no magnitude bound by design) also
// matches any downward-only coverage shift; this test asserts CHAOS-4547
// specifically stays out, not the whole-comparison outcome.
func TestCoverageShiftShape_ShiftBeyondBoundIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.2) // 20% relative shift, well past the 5% bound

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == "CHAOS-4547" {
			t.Fatalf("matched = %v, must not include CHAOS-4547 -- a shift past its own bound is not its shape", result.BaselineDefectsMatched)
		}
	}
}
