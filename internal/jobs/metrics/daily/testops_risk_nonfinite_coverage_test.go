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
// Round 5 (P2, EXECUTED) caught a THIRD site of the same class this test
// originally missed: compute_testops_risk.py's `X or 0.0` idiom
// (:55,58,133,136) is falsy for -0.0 too, not just for a missing value --
// `cov.line_coverage_pct or 0.0`, `cov.coverage_delta_pct or 0.0`,
// `pipe.median_duration_seconds or 0.0`, `pipe.avg_queue_seconds or 0.0`
// all silently normalize a genuine -0.0 reading to +0.0. This test's own
// FIRST version asserted `"-0.0"` as the expected JSON for these fields --
// wrong, because it derived that expectation from Go's (buggy) output
// instead of an independent live-python3 run of the ACTUAL expression
// Python evaluates (see pyOrZero's own doc comment for the corrected
// verification). Fixed here alongside pyOrZero itself.
//
// coveragePct/coverageDelta/medianDur/avgQueue are the ONLY testops_risk
// inputs that can carry a non-finite value in production (pipeline/test
// COUNT-based rates are ratios of finite integers, never NaN/Inf; see
// ComputePipelineMetrics/ComputeTestMetrics). coveragePct/coverageDelta
// feed computeReleaseConfidence only; medianDur/avgQueue feed
// computeQualityDrag only (never coverage -- see that function's own doc
// comment). computePipelineStability reads neither coverage NOR the "or
// 0.0"-normalized median/avg-queue fields (it reads median_duration_seconds
// directly via an explicit `is not None` check on BOTH planes, never `or
// 0.0` -- see compute_pipeline_stability's source), so it is out of this
// sweep's scope.
//
// Every expected value below was computed with a live python3 interpreter
// against the exact CPython operations compute_testops_risk.py performs
// (the `or 0.0` idiom, max/min for _clamp, `<` for the regression-penalty
// gate, round() and json.dumps() for output formatting) -- not
// hand-derived -- so a genuinely new divergence at any of these sites
// fails this test with the real CPython value on one side.
func TestNonFiniteCoverageClassSweep(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := day

	pipe := &testops.PipelineMetric{RepoID: repoID, SuccessRate: 1.0, OrgID: "org"}
	test := &testops.TestMetric{RepoID: repoID, PassRate: 1.0, OrgID: "org"}

	coveragePctCases := []struct {
		name string
		pct  float64
		// wantCovFactor is Python's `0.2 * max(0.0, min(1.0, (pct or 0.0)
		// /100.0))`. -0.0 goes through `or 0.0` FIRST (this function's own
		// pyOrZero call), landing on +0.0 before it ever reaches clampUnit
		// -- clampUnit(0.0/100.0) = 0.0, same end value as before this fix,
		// but for a different reason (this table exists to pin the reason,
		// not just the number).
		wantCovFactor float64
		// wantCoveragePctJSON is Python's round(pct or 0.0, 2) then json.dumps.
		wantCoveragePctJSON string
	}{
		{"NaN", math.NaN(), 0.2, "NaN"},
		{"+Inf", math.Inf(1), 0.2, "Infinity"},
		{"-Inf", math.Inf(-1), 0.0, "-Infinity"},
		{"-0.0", math.Copysign(0, -1), 0.0, "0.0"}, // `-0.0 or 0.0` == 0.0 in Python (bool(-0.0) is False)
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
		wantRegressionAdds bool // Python: `if (coverage_delta or 0.0) < -2.0: regression_penalty += 0.05`
		wantDeltaJSON      string
	}{
		{"NaN", math.NaN(), false, "NaN"},            // nan < -2.0 is False in both Go and Python
		{"+Inf", math.Inf(1), false, "Infinity"},     // +inf < -2.0 is False
		{"-Inf", math.Inf(-1), true, "-Infinity"},    // -inf < -2.0 is True
		{"-0.0", math.Copysign(0, -1), false, "0.0"}, // normalized to +0.0 before the comparison; 0.0 < -2.0 is False either way
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

// TestPyOrZeroMatchesPythonTruthyOrIdiom pins pyOrZero itself against a live
// python3 `value or 0.0` evaluation for every non-finite class plus the two
// finite zeros -- codex round 5's own EXECUTED finding (median_duration_
// seconds kept a -0.0 sign bit Go's row-9 sweep had not yet checked).
func TestPyOrZeroMatchesPythonTruthyOrIdiom(t *testing.T) {
	cases := []struct {
		name  string
		value float64
		want  float64
	}{
		{"positive zero unchanged", 0.0, 0.0},
		{"negative zero normalizes to positive zero", math.Copysign(0, -1), 0.0},
		{"NaN passes through (bool(nan) is True in Python)", math.NaN(), math.NaN()},
		{"+Inf passes through (truthy)", math.Inf(1), math.Inf(1)},
		{"-Inf passes through (truthy)", math.Inf(-1), math.Inf(-1)},
		{"ordinary negative value unchanged", -5.5, -5.5},
		{"ordinary positive value unchanged", 5.5, 5.5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pyOrZero(tc.value)
			if math.IsNaN(tc.want) {
				if !math.IsNaN(got) {
					t.Errorf("pyOrZero(%v) = %v, want NaN", tc.value, got)
				}
				return
			}
			if got != tc.want || math.Signbit(got) != math.Signbit(tc.want) {
				t.Errorf("pyOrZero(%v) = %v (signbit=%v), want %v (signbit=%v)",
					tc.value, got, math.Signbit(got), tc.want, math.Signbit(tc.want))
			}
		})
	}
}

// TestComputeQualityDragNegativeZeroDurationMatchesPython is round 5's own
// EXECUTED repro, independently re-verified rather than copied: a pipeline
// metric whose median_duration_seconds is a genuine -0.0 (reachable --
// ci_pipeline_runs.duration_seconds is unconstrained Nullable(Float64))
// must normalize to +0.0 in factors_json, matching Python's
// `pipe.median_duration_seconds or 0.0`, not retain its sign bit.
func TestComputeQualityDragNegativeZeroDurationMatchesPython(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	negZero := math.Copysign(0, -1)

	pipe := &testops.PipelineMetric{
		RepoID: repoID, OrgID: "org",
		MedianDurationSeconds: &negZero,
		AvgQueueSeconds:       &negZero,
		FailureCount:          1,
	}
	test := &testops.TestMetric{RepoID: repoID, OrgID: "org"}

	row := computeQualityDrag(repoID, day, pipe, test, day)
	if row == nil {
		t.Fatal("computeQualityDrag returned nil")
	}
	if math.Signbit(row.FailureReworkHours) {
		t.Errorf("FailureReworkHours retained a negative sign bit from -0.0 duration: %v", row.FailureReworkHours)
	}
	for _, want := range []string{`"median_duration_seconds": 0.0`, `"avg_queue_seconds": 0.0`} {
		if !strings.Contains(row.FactorsJSON, want) {
			t.Errorf("factors_json missing %q (Python's `pipe.median_duration_seconds or 0.0` / `pipe.avg_queue_seconds or 0.0` normalize -0.0 to +0.0)\ngot: %s", want, row.FactorsJSON)
		}
		if strings.Contains(row.FactorsJSON, strings.Replace(want, "0.0", "-0.0", 1)) {
			t.Errorf("factors_json retained a negative-zero sign bit Python's `or 0.0` idiom would have normalized away\ngot: %s", row.FactorsJSON)
		}
	}
}
