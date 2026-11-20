package daily

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/finite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

func unknownCoverageInputs(repo uuid.UUID) (*testops.PipelineMetric, *testops.TestMetric) {
	pipe := &testops.PipelineMetric{RepoID: repo, SuccessRate: 1.0, SuccessCount: 1, OrgID: "org"}
	test := &testops.TestMetric{RepoID: repo, PassRate: 0.9, PassedCount: 9, FailedCount: 1, DistinctCases: 1, OrgID: "org"}
	return pipe, test
}

func unknownCoverageDay() time.Time { return time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC) }

func coverageAt(repo uuid.UUID, pct float64) *testops.CoverageMetric {
	return &testops.CoverageMetric{RepoID: repo, LineCoveragePct: &pct, OrgID: "org"}
}

// Known factors: pipeline 1.0 (0.4), pass 0.9 (0.27), flake 0 (0.1). With coverage known at 0% the base is 0.77; with
// coverage unknown it is left out and the other weights are renormalized: 0.77 / 0.8 = 0.9625.
func TestReleaseConfidenceUnknownCoverageIsLeftOutAndRenormalized(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	pipe, test := unknownCoverageInputs(repo)
	cells := []struct {
		name      string
		cov       *testops.CoverageMetric
		wantScore float64
	}{
		{"0%", coverageAt(repo, 0), 0.77},
		{"NULL pct", &testops.CoverageMetric{RepoID: repo, OrgID: "org"}, 0.9625},
		{"no snapshot", nil, 0.9625},
	}
	for _, cell := range cells {
		row := computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, cell.cov, unknownCoverageDay())
		if row.ConfidenceScore != cell.wantScore || row.CoverageFactor != 0 {
			t.Errorf("%s: score=%v coverage_factor=%v, want %v and 0", cell.name, row.ConfidenceScore, row.CoverageFactor, cell.wantScore)
		}
	}
}

func TestReleaseConfidenceNaNCoverageIsUnknown(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	pipe, test := unknownCoverageInputs(repo)
	nan := computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, coverageAt(repo, math.NaN()), unknownCoverageDay())
	null := computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, &testops.CoverageMetric{RepoID: repo, OrgID: "org"}, unknownCoverageDay())
	if nan.ConfidenceScore != null.ConfidenceScore || nan.CoverageFactor != 0 {
		t.Errorf("NaN coverage: score=%v coverage_factor=%v, want the NULL-coverage score %v and 0", nan.ConfidenceScore, nan.CoverageFactor, null.ConfidenceScore)
	}
	if !strings.Contains(nan.FactorsJSON, `"coverage_pct": null`) || !strings.Contains(nan.FactorsJSON, `"coverage_delta_pct": null`) {
		t.Errorf("NaN coverage factors_json: %s", nan.FactorsJSON)
	}
}

func TestReleaseConfidenceFactorsJSONUnknownCoverageIsNull(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	pipe, test := unknownCoverageInputs(repo)
	drop := coverageAt(repo, 0)
	minusFive := -5.0
	drop.CoverageDeltaPct = &minusFive
	cells := []struct {
		name      string
		cov       *testops.CoverageMetric
		wantPct   string
		wantDelta string
	}{
		{"NULL pct", &testops.CoverageMetric{RepoID: repo, OrgID: "org"}, `"coverage_pct": null`, `"coverage_delta_pct": null`},
		{"no snapshot", nil, `"coverage_pct": null`, `"coverage_delta_pct": null`},
		{"0% no prior", coverageAt(repo, 0), `"coverage_pct": 0.0`, `"coverage_delta_pct": null`},
		{"0% after a drop of 5", drop, `"coverage_pct": 0.0`, `"coverage_delta_pct": -5.0`},
	}
	for _, cell := range cells {
		row := computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, cell.cov, unknownCoverageDay())
		if !strings.Contains(row.FactorsJSON, cell.wantPct) || !strings.Contains(row.FactorsJSON, cell.wantDelta) {
			t.Errorf("%s: factors_json %s, want %s and %s", cell.name, row.FactorsJSON, cell.wantPct, cell.wantDelta)
		}
	}
}

// A day with every factor known is the same row before and after the unknown-coverage rule.
func TestReleaseConfidenceAllFactorsKnownRowUnchanged(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	pipe := &testops.PipelineMetric{RepoID: repo, SuccessCount: 3, FailureCount: 1, OrgID: "org"}
	test := &testops.TestMetric{RepoID: repo, PassedCount: 9, FailedCount: 1, DistinctCases: 10, FlakeRate: 0.1, FailureRecurrence: 0.5, OrgID: "org"}
	cov := coverageAt(repo, 80)
	minusFive := -5.0
	cov.CoverageDeltaPct = &minusFive
	row := computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, cov, unknownCoverageDay())
	const wantFactors = `{"pipeline_success_rate": 0.75, "test_pass_rate": 0.9, "coverage_pct": 80.0, "flake_rate": 0.1, "failure_recurrence": 0.5, "coverage_delta_pct": -5.0, "base_score": 0.82, "flake_penalty": 0.1, "regression_penalty": 0.15}`
	if row.ConfidenceScore != 0.57 || row.PipelineSuccessFactor != 0.3 || row.TestPassFactor != 0.27 || row.CoverageFactor != 0.16 ||
		row.FlakePenalty != 0.1 || row.RegressionPenalty != 0.15 || row.FactorsJSON != wantFactors {
		t.Errorf("all-known row: score=%v factors %v/%v/%v penalties %v/%v json=%s",
			row.ConfidenceScore, row.PipelineSuccessFactor, row.TestPassFactor, row.CoverageFactor, row.FlakePenalty, row.RegressionPenalty, row.FactorsJSON)
	}
}

func coverageBoundaryCounts(field string) map[finite.Reason]uint64 {
	counts := map[finite.Reason]uint64{}
	for _, reason := range []finite.Reason{finite.ReasonNaN, finite.ReasonPositiveInf, finite.ReasonNegativeInf, finite.ReasonUndefinedInput} {
		counts[reason] = finite.Count("testops_release_confidence", field, reason)
	}
	return counts
}

// A non-finite coverage value is unknown and is also one trip of the finite-boundary counter for its field: exactly one,
// never zero and never one from the unknown check plus one from the JSON writer.
func TestReleaseConfidenceNonFiniteCoverageCountsOnceAtTheBoundary(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	pipe, test := unknownCoverageInputs(repo)
	classes := []struct {
		value  float64
		reason finite.Reason
	}{
		{math.NaN(), finite.ReasonNaN}, {math.Inf(1), finite.ReasonPositiveInf}, {math.Inf(-1), finite.ReasonNegativeInf},
	}
	for _, field := range []string{"coverage_pct", "coverage_delta_pct"} {
		for _, class := range classes {
			cov := coverageAt(repo, 50)
			if field == "coverage_pct" {
				cov.LineCoveragePct = &class.value
			} else {
				cov.CoverageDeltaPct = &class.value
			}
			before, beforeOther := coverageBoundaryCounts(field), coverageBoundaryCounts(otherCoverageField(field))
			computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, cov, unknownCoverageDay())
			after, afterOther := coverageBoundaryCounts(field), coverageBoundaryCounts(otherCoverageField(field))
			for reason, count := range after {
				want := before[reason]
				if reason == class.reason {
					want++
				}
				if count != want {
					t.Errorf("%s %s: counter[%s] moved %d -> %d, want %d", field, class.reason, reason, before[reason], count, want)
				}
				if afterOther[reason] != beforeOther[reason] {
					t.Errorf("%s %s: counter of %s [%s] moved %d -> %d, want no change", field, class.reason, otherCoverageField(field), reason, beforeOther[reason], afterOther[reason])
				}
			}
		}
	}
	finiteCov := coverageAt(repo, 50)
	minusFive := -5.0
	finiteCov.CoverageDeltaPct = &minusFive
	before := [2]map[finite.Reason]uint64{coverageBoundaryCounts("coverage_pct"), coverageBoundaryCounts("coverage_delta_pct")}
	computeReleaseConfidence(repo, unknownCoverageDay(), pipe, test, finiteCov, unknownCoverageDay())
	after := [2]map[finite.Reason]uint64{coverageBoundaryCounts("coverage_pct"), coverageBoundaryCounts("coverage_delta_pct")}
	for i := range before {
		for reason := range before[i] {
			if after[i][reason] != before[i][reason] {
				t.Errorf("finite coverage: counter %d [%s] moved %d -> %d, want no change", i, reason, before[i][reason], after[i][reason])
			}
		}
	}
}

func otherCoverageField(field string) string {
	if field == "coverage_pct" {
		return "coverage_delta_pct"
	}
	return "coverage_pct"
}
