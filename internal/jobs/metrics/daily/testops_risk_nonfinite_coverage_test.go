package daily

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// TestNonFiniteCoverageClassSweep is the class-level proof team-lead asked
// for after codex rounds 3 and 4 (2026-09-01): both findings were the SAME
// class -- Python float semantics on non-finite values (NaN/+Inf/-Inf/-0.0)
// diverging from Go's -- surfacing at two different sites
// (pythonFloatJSON's string formatting, clampUnit's comparison logic) that
// both trace back to the same reachable input: coverage_snapshots
// .line_coverage_pct is an unconstrained Nullable(Float64), so a non-finite
// value can reach every float this package derives from it.
//
// coveragePct/coverageDelta are the ONLY testops_risk inputs that can carry
// a non-finite value in production (pipeline/test metrics are ratios of
// finite integer counts or duration_seconds parsed off timestamps -- never
// NaN/Inf; see ComputePipelineMetrics/ComputeTestMetrics, neither of which
// touches coverage). Both feed computeReleaseConfidence exclusively --
// computeQualityDrag and computePipelineStability never read coverage at
// all (see their own doc comments) -- so this test sweeps
// computeReleaseConfidence's full chain from raw non-finite input to every
// output field/JSON string that value can reach:
//
//  1. coveragePct -> clampUnit(coveragePct/100.0) -> covFactor -> baseScore
//     -> score -> ConfidenceScore (all pyRound'd, all pythonFloatJSON'd)
//  2. coveragePct itself, verbatim, into factors_json's "coverage_pct" field
//  3. coverageDelta -> the `< -2.0` regression-penalty comparison
//  4. coverageDelta itself, verbatim, into factors_json's
//     "coverage_delta_pct" field
//
// Every expected value below was computed with a live python3 interpreter
// against the exact CPython operations compute_testops_risk.py performs
// (max/min for _clamp, `<` for the regression-penalty gate, round() and
// json.dumps() for output formatting) -- not hand-derived -- so a genuinely
// new divergence at any of these sites fails this test with the real
// CPython value on one side.
func TestNonFiniteCoverageClassSweep(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := day

	pipe := &testops.PipelineMetric{RepoID: repoID, SuccessRate: 1.0, OrgID: "org"}
	test := &testops.TestMetric{RepoID: repoID, PassRate: 1.0, OrgID: "org"}

	coveragePctCases := []struct {
		name string
		pct  float64
		// wantCovFactor is Python's `0.2 * max(0.0, min(1.0, pct/100.0))`,
		// verified live: NaN and +Inf both saturate to hi=1.0 (covFactor
		// 0.2); -Inf saturates to lo=0.0 (covFactor 0.0); -0.0 clamps to
		// positive 0.0 (covFactor 0.0) -- min(1.0,-0.0) keeps -0.0 (-0.0 <
		// 1.0 is True), then max(0.0,-0.0) keeps 0.0 (-0.0 > 0.0 is False).
		wantCovFactor float64
		// wantCoveragePctJSON is Python's round(pct, 2) then json.dumps.
		wantCoveragePctJSON string
	}{
		{"NaN", math.NaN(), 0.2, "NaN"},
		{"+Inf", math.Inf(1), 0.2, "Infinity"},
		{"-Inf", math.Inf(-1), 0.0, "-Infinity"},
		{"-0.0", math.Copysign(0, -1), 0.0, "-0.0"},
	}

	for _, tc := range coveragePctCases {
		t.Run("coveragePct/"+tc.name, func(t *testing.T) {
			pct := tc.pct
			cov := &testops.CoverageMetric{RepoID: repoID, LineCoveragePct: &pct, OrgID: "org"}

			row := computeReleaseConfidence(repoID, day, pipe, test, cov, computedAt)
			if row == nil {
				t.Fatal("computeReleaseConfidence returned nil")
			}
			if row.CoverageFactor != tc.wantCovFactor {
				t.Errorf("CoverageFactor = %v, want %v", row.CoverageFactor, tc.wantCovFactor)
			}
			// baseScore = 0.4*1.0 (pipeline) + 0.3*1.0 (test) + covFactor + 0.1*1.0 (flake, flakeRate=0)
			// = 0.7 + 0.1 + covFactor = 0.8 + covFactor, all finite regardless
			// of which coveragePct case ran, because clampUnit is a NaN/Inf
			// sink -- this is the whole point of the class fix: no non-finite
			// value survives past clampUnit into baseScore/score/ConfidenceScore.
			wantScore := pyRound(0.8+tc.wantCovFactor, 4)
			if row.ConfidenceScore != wantScore {
				t.Errorf("ConfidenceScore = %v, want %v (baseScore must be finite; a non-finite ConfidenceScore here would mean clampUnit stopped sinking non-finite input)", row.ConfidenceScore, wantScore)
			}
			if math.IsNaN(row.ConfidenceScore) || math.IsInf(row.ConfidenceScore, 0) {
				t.Errorf("ConfidenceScore is non-finite (%v) for coveragePct=%s -- clampUnit must always saturate to a finite bound", row.ConfidenceScore, tc.name)
			}
			wantCoveragePctField := `"coverage_pct": ` + tc.wantCoveragePctJSON
			if !strings.Contains(row.FactorsJSON, wantCoveragePctField) {
				t.Errorf("factors_json missing %q\ngot: %s", wantCoveragePctField, row.FactorsJSON)
			}
		})
	}

	coverageDeltaCases := []struct {
		name               string
		delta              float64
		wantRegressionAdds bool // Python: `if coverage_delta < -2.0: regression_penalty += 0.05`
		wantDeltaJSON      string
	}{
		{"NaN", math.NaN(), false, "NaN"},         // nan < -2.0 is False in both Go and Python
		{"+Inf", math.Inf(1), false, "Infinity"},  // +inf < -2.0 is False
		{"-Inf", math.Inf(-1), true, "-Infinity"}, // -inf < -2.0 is True
		{"-0.0", math.Copysign(0, -1), false, "-0.0"},
	}

	for _, tc := range coverageDeltaCases {
		t.Run("coverageDelta/"+tc.name, func(t *testing.T) {
			delta := tc.delta
			cov := &testops.CoverageMetric{RepoID: repoID, CoverageDeltaPct: &delta, OrgID: "org"}

			row := computeReleaseConfidence(repoID, day, pipe, test, cov, computedAt)
			if row == nil {
				t.Fatal("computeReleaseConfidence returned nil")
			}
			wantRegressionPenalty := 0.0
			if tc.wantRegressionAdds {
				wantRegressionPenalty = 0.05
			}
			if row.RegressionPenalty != wantRegressionPenalty {
				t.Errorf("RegressionPenalty = %v, want %v", row.RegressionPenalty, wantRegressionPenalty)
			}
			if math.IsNaN(row.ConfidenceScore) || math.IsInf(row.ConfidenceScore, 0) {
				t.Errorf("ConfidenceScore is non-finite (%v) for coverageDelta=%s", row.ConfidenceScore, tc.name)
			}
			wantDeltaField := `"coverage_delta_pct": ` + tc.wantDeltaJSON
			if !strings.Contains(row.FactorsJSON, wantDeltaField) {
				t.Errorf("factors_json missing %q\ngot: %s", wantDeltaField, row.FactorsJSON)
			}
		})
	}
}
