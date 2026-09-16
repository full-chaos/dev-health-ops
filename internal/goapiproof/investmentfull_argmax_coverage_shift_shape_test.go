package goapiproof

import "testing"

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
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-4547"}) {
		t.Fatalf("matched = %v, want [CHAOS-4547] -- the repos-join fan-out (CHAOS-4773) must not also claim a coverage-only shift", result.BaselineDefectsMatched)
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

// A shift larger than the bound is not admitted either -- the shape
// draws a line, not a blank check.
func TestCoverageShiftShape_ShiftBeyondBoundIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.2) // 20% relative shift, well past the 5% bound

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want at least 1 -- an out-of-bound coverage shift must not be silently admitted: findings %+v", result.Findings)
	}
}
