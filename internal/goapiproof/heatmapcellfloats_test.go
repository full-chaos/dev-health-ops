package goapiproof

import "testing"

// This file pins heatmapHotspotRiskParity's and heatmapReviewWaitDensity
// Parity's own FloatTierB wiring (restcorpus.go) directly against the
// declared corpus Options an admissible request ACTUALLY carries -- not
// a hand-built stand-in. No admissible heatmap RESTRequest uses
// heatmapDedupParity directly: each of the four scenarios routes through
// its own per-scenario Options, each declaring its OWN leaf's type.
// Testing against the actual live Options is what makes this file a test
// that can fail: a copy of these captures run against an Options object
// nothing routes through would pass forever regardless of what the real
// heatmap routes do.
//
// Each capture below is single-metric (either hotspot_risk's own
// magnitude/cells or review_wait_density's own), so it demonstrates the
// same path, tolerance and declared reason its own scenario's Options
// carries.

// A ULP-scale difference at hotspot_risk's own magnitude (~100-200,
// toFloat64(sum(hotspot_score))) is exactly what a merged Float64
// ClickHouse aggregate's own thread-completion-order nondeterminism
// produces -- CHAOS-5451's class -- and must be tolerated: no finding at
// all, not merely one admitted by a shape unrelated to the actual cause.
func TestHeatmapHotspotRiskParity_TolerantOfULPNoiseAtHotspotMagnitude(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-09-06", "file.go", 146.65079792195135)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-09-06", "file.go", 146.65079792195132)))

	result := Compare(baseline, candidate, heatmapHotspotRiskParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- a 1-ULP difference at this magnitude is below float64's own precision floor, not a plane divergence: findings %+v", result.TerminalState, result.Findings)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %+v, want none -- Tier B must absorb this before classification ever runs", result.Findings)
	}
}

// The same ULP-scale noise at review_wait_density's own magnitude
// (~0.03-0.07, sum(dateDiff(...))/60.0) must be tolerated too: the
// tolerance's absolute floor (1e-9) governs here, not the relative term,
// and still sits many orders of magnitude above a 1-ULP shift at this
// size.
func TestHeatmapReviewWaitDensityParity_TolerantOfULPNoiseAtReviewWaitMagnitude(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("03", "Fri", 0.06666666666666667)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("03", "Fri", 0.06666666666666668)))

	result := Compare(baseline, candidate, heatmapReviewWaitDensityParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match: findings %+v", result.TerminalState, result.Findings)
	}
}

// Blind-spot pin (hotspot magnitude): a GENUINE difference at the same
// ~100-200 magnitude -- far above the tolerance's relative term -- must
// still surface. This is the case that fails if someone later loosens
// the tolerance past this route's own values.
func TestHeatmapHotspotRiskParity_StillCatchesGenuineDifferenceAtHotspotMagnitude(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-09-06", "file.go", 150.0)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("2026-09-06", "file.go", 150.0002)))

	result := Compare(baseline, candidate, heatmapHotspotRiskParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a 2e-4 difference at magnitude 150 is ~1300x the tolerance (1.5e-7) and must not be swallowed", result.TerminalState)
	}
}

// Blind-spot pin (review_wait_density magnitude): a genuine difference
// at the ~0.03-0.07 magnitude, far above the 1e-9 absolute floor, must
// still surface -- this is what proves the tolerance's floor, not a
// magnitude bound on the route's own values, is doing the work.
func TestHeatmapReviewWaitDensityParity_StillCatchesGenuineDifferenceAtReviewWaitMagnitude(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("03", "Fri", 0.06666666666666667)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("03", "Fri", 0.03333333333333333)))

	result := Compare(baseline, candidate, heatmapReviewWaitDensityParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
}

// TestHeatmapHotspotRiskParity_RealHotspotRiskULPCaptureIsNowAMatch
// replays a real captured production deployed-vs-deployed prove run (GET
// /api/v1/heatmap hotspot_risk_org, step74): 15 differing cells, every
// one a 1-2 ULP shift (1.4e-14 to 2.8e-14) at magnitudes 80-212, split
// roughly evenly between directions -- baseline>candidate on 7, the
// reverse on the other 8. Before this Tier B declaration existed,
// KeyedDirectionShape admitted the 7 direction-matched cells as if they
// were evidence of the declared repos-dedup mechanism (they are not --
// that mechanism is strictly one-directional, as the review_wait_density
// capture below shows) and left the other 8 as a false uncovered
// finding. This is the falsifiable prediction: the next production prove
// of this route comes back a clean match, because data.axes carries only
// label arrays (see heatmapDedupParity's own doc comment) and no other
// leaf in this response is numeric.
func TestHeatmapHotspotRiskParity_RealHotspotRiskULPCaptureIsNowAMatch(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFile(t, "testdata/heatmap_hotspot_risk_ulp_baseline_a2190a7b.json")
	candidate := heatmapDirectionSnapshotFromFile(t, "testdata/heatmap_hotspot_risk_ulp_candidate_cdb72b5c.json")

	result := Compare(baseline, candidate, heatmapHotspotRiskParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- all 15 real captured differences are ULP-scale noise Tier B must absorb: findings %+v", result.TerminalState, result.Findings)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %+v, want none", result.Findings)
	}
}

// TestHeatmapReviewWaitDensityParity_RealReviewWaitDensityCaptureUnchanged
// replays the SAME real captured production capture keyeddirection_test.go's
// own TestKeyedDirectionShape_RealHeatmapCapture already pins, but
// through the actual registered heatmapReviewWaitDensityParity (not the
// test-local heatmapCellsOptions stand-in), to prove this route's Tier B
// declaration does not touch this route's one live proof that the
// repos-dedup mechanism actually fires: both cells' real ~2x difference
// (0.0667 vs 0.0333) is seven orders of magnitude above the 1e-9 floor,
// so it still reaches classification and is still admitted by
// KeyedDirectionShape exactly as before Tier B existed.
func TestHeatmapReviewWaitDensityParity_RealReviewWaitDensityCaptureUnchanged(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFile(t, "testdata/heatmap_review_wait_direction_baseline_09956ee8.json")
	candidate := heatmapDirectionSnapshotFromFile(t, "testdata/heatmap_review_wait_direction_candidate_3b684469.json")

	result := Compare(baseline, candidate, heatmapReviewWaitDensityParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- Tier B must not swallow this route's real dedup evidence", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- both cells still admitted by KeyedDirectionShape, unchanged by Tier B: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-5803"}) {
		t.Fatalf("matched = %v, want [CHAOS-5803]", result.BaselineDefectsMatched)
	}
}
