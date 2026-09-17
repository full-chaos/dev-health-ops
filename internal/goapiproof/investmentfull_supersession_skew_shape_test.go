package goapiproof

import (
	"encoding/json"
	"os"
	"testing"
)

// This file exercises SupersessionSkewShape (supersessionskew.go) both in
// isolation, against a scoped Options carrying only a synthetic
// declaration under a neutral ticket (this shape's own admission rules
// are what is under test, not any particular registered citation), and
// through the real registered investmentFull spec (compareAsInvestmentFull,
// investmentfull_repojoin_fanout_shape_test.go) in the one test that
// exercises the actual committed declaration. Every isolated test reuses
// that file's own fixture and helpers (loadSankeyCandidateCopy, sankeyOf,
// marshalBody): every baseline starts as a byte-identical copy of the
// committed job5 candidate capture, so every field not deliberately
// mutated stays equal between the two legs.

// supersessionSkewTestTicket is a neutral, made-up ticket for the
// synthetic declaration below -- this file is testing the SHAPE's own
// admission rules in isolation, not the real registered citation (which
// is exercised separately, through the real spec, by
// TestSupersessionSkewShape_RegisteredDeclarationAdmitsThroughTheRealSpec).
const supersessionSkewTestTicket = "ABC-123"

func supersessionSkewDefect() BaselineDefect {
	return BaselineDefect{
		Ticket: supersessionSkewTestTicket,
		Reason: "test fixture",
		Paths: []string{
			"data.analytics.sankey.coverage.teamCoverage",
			"data.analytics.sankey.coverage.repoCoverage",
		},
		Intermittent:       true,
		IntermittentReason: "test fixture",
		SupersessionSkewShape: &SupersessionSkewShape{
			TeamCoveragePath: "data.analytics.sankey.coverage.teamCoverage",
			RepoCoveragePath: "data.analytics.sankey.coverage.repoCoverage",
		},
	}
}

// A downward-only shift on both coverage leaves, with nothing else
// differing, is exactly this shape's mechanism and is fully admitted --
// at ANY magnitude, since the shape has no bound.
func TestSupersessionSkewShape_DownwardShiftIsAdmittedAtAnyMagnitude(t *testing.T) {
	opts := Options{BaselineDefects: []BaselineDefect{supersessionSkewDefect()}}
	for _, relativeDelta := range []float64{0.001, 0.02, 0.2, 0.6} {
		candidateJSON, baseline := loadSankeyCandidateCopy(t)
		mutateCoverage(t, baseline, relativeDelta)
		result := Compare(snapshotFromJSON(t, marshalBody(t, baseline)), snapshotFromJSON(t, candidateJSON), opts)
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("delta=%v: terminal = %q, want mismatch", relativeDelta, result.TerminalState)
		}
		if result.DifferencesOutsideBaselineDefect != 0 {
			t.Fatalf("delta=%v: outside = %d, want 0 -- findings %+v", relativeDelta, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
		if !equalStrings(result.BaselineDefectsMatched, []string{supersessionSkewTestTicket}) {
			t.Fatalf("delta=%v: matched = %v, want [%s]", relativeDelta, result.BaselineDefectsMatched, supersessionSkewTestTicket)
		}
	}
}

// An UPWARD shift (baseline above candidate) is the one direction this
// mechanism cannot produce -- excluding it from admission is the whole
// point of a direction-only shape, not an incidental side effect.
func TestSupersessionSkewShape_UpwardShiftIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, -0.02) // negative relativeDelta: baseline scaled UP

	opts := Options{BaselineDefects: []BaselineDefect{supersessionSkewDefect()}}
	result := Compare(snapshotFromJSON(t, marshalBody(t, baseline)), snapshotFromJSON(t, candidateJSON), opts)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want at least 1 -- an upward coverage shift must not be admitted: findings %+v", result.Findings)
	}
	if len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("matched = %v, want none", result.BaselineDefectsMatched)
	}
}

// Only ONE leaf moving down, the other unchanged, fails rule 2 on the
// unchanged leaf (not strictly less) and admits nothing -- the shape is
// whole-comparison, not per-leaf.
func TestSupersessionSkewShape_OnlyOneLeafMovingIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	coverage := sankeyOf(t, baseline)["coverage"].(map[string]any)
	coverage["repoCoverage"] = coverage["repoCoverage"].(float64) * 0.98 // moves
	// teamCoverage left untouched -- equal to candidate, so no Finding at
	// all is generated for it; repoCoverage alone still differs.

	opts := Options{BaselineDefects: []BaselineDefect{supersessionSkewDefect()}}
	result := Compare(snapshotFromJSON(t, marshalBody(t, baseline)), snapshotFromJSON(t, candidateJSON), opts)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want at least 1 -- a single-leaf shift is not this mechanism (it always moves both, since they share the same denominator): findings %+v", result.Findings)
	}
}

// A coverage shift accompanied by a real sankey node difference is NOT
// admitted -- the mechanism composes the same shared dedup source as
// sankey.nodes/.edges, so an unrelated Go regression under those paths
// must never ride along under this citation.
func TestSupersessionSkewShape_CoverageShiftWithANodeDifferenceIsNotAdmitted(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.02)
	sankey := sankeyOf(t, baseline)
	found := false
	for _, n := range sankey["nodes"].([]any) {
		node := n.(map[string]any)
		if node["id"].(string) == "REPO:full-chaos/dev-health-ops" {
			node["value"] = node["value"].(float64) * 1.01
			found = true
			break
		}
	}
	if !found {
		t.Fatal("fixture no longer carries REPO:full-chaos/dev-health-ops")
	}

	opts := Options{BaselineDefects: []BaselineDefect{supersessionSkewDefect()}}
	result := Compare(snapshotFromJSON(t, marshalBody(t, baseline)), snapshotFromJSON(t, candidateJSON), opts)
	if len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("matched = %v, want none -- a sankey node also differs, so this is not the supersession-only shape", result.BaselineDefectsMatched)
	}
}

// The documented blind spot: a same-direction Go regression that touches
// no other path is, from the response bodies alone, indistinguishable
// from a genuine supersession skew and IS admitted. This is not a bug --
// the shape's own doc comment states it as a known, accepted limit -- but
// it is pinned here so a future change cannot silently narrow or widen it
// without a test noticing.
func TestSupersessionSkewShape_SameDirectionRegressionIsTheAcceptedBlindSpot(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.005) // stands in for an unrelated Go-side over-count, not a real supersession skew

	opts := Options{BaselineDefects: []BaselineDefect{supersessionSkewDefect()}}
	result := Compare(snapshotFromJSON(t, marshalBody(t, baseline)), snapshotFromJSON(t, candidateJSON), opts)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- this scenario is indistinguishable from the real mechanism by design", result.DifferencesOutsideBaselineDefect)
	}
}

// Through the real registered investmentFull spec: a downward-only shift
// matches both CHAOS-4547 (within its own bound) and CHAOS-5865
// (unbounded) at once -- see
// investmentfull_argmax_coverage_shift_shape_test.go for that overlap
// pinned from CHAOS-4547's own side.
func TestSupersessionSkewShape_RegisteredDeclarationAdmitsThroughTheRealSpec(t *testing.T) {
	candidateJSON, baseline := loadSankeyCandidateCopy(t)
	mutateCoverage(t, baseline, 0.3) // past CHAOS-4547's own 5% bound

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), candidateJSON)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	foundOwn := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == "CHAOS-5865" {
			foundOwn = true
		}
	}
	if !foundOwn {
		t.Fatalf("matched = %v, want CHAOS-5865 present", result.BaselineDefectsMatched)
	}
}

// Rule 1 must hold against two REAL, INDEPENDENTLY-COMPUTED sankey
// subtrees, not loadSankeyCandidateCopy's own fixture (every test above):
// rule 1 requires that no OTHER path in the comparison differs, and a
// self-decoded copy can never exercise that -- nodes/edges are the
// identical parsed float64 values by construction there, so rule 1 is
// never actually asked to tolerate real engine noise elsewhere in the
// response. testdata's job5 baseline/candidate pair are two SEPARATELY
// EXECUTED query results: diffed directly, their sankey nodes/edges
// values differ from each other on their own (measured ~1e-13, the same
// ClickHouse summation-order noise this operation's own FloatTierB
// entries document), which the operation's own declared FloatTierB
// tolerance already absorbs -- rule
// 1 reads the comparison's mismatch list, which is already Tier-B
// filtered, so this real noise produces no Finding there and rule 1
// holds against it exactly as it must. The coverage leaves are shifted a
// further, clearly-citable downward amount so this exercises a real
// direction-only divergence through the actual registered declaration.
func TestSupersessionSkewShape_RealIndependentlyComputedNodesAndEdgesAreAdmitted(t *testing.T) {
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
	mutateCoverage(t, baseline, 0.3) // past CHAOS-4547's own 5% bound, downward-only

	result := compareAsInvestmentFull(t, marshalBody(t, baseline), string(candidateRaw))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- rule 1 must tolerate two independently-computed real nodes/edges values, findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	foundOwn := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == "CHAOS-5865" {
			foundOwn = true
		}
	}
	if !foundOwn {
		t.Fatalf("matched = %v, want CHAOS-5865 present", result.BaselineDefectsMatched)
	}
}
