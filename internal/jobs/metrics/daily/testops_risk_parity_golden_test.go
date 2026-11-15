package daily

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// TestTestopsRiskComputeMatchesFrozenPythonGolden is a FROZEN-golden test,
// not a live-dual-execution one: there is no Python compute for
// computeReleaseConfidence/computeQualityDrag/computePipelineStability left
// in this repository, so nothing can execute a live oracle at test time.
//
// The golden fixture (testdata/testops_risk_parity_golden.json) was
// captured by running the Python authority's own frozen oracle script
// (testdata/python_testops_risk_oracle.py -- reproduced verbatim as the
// input fixture below) against that revision's Python source, offline,
// ONCE, via a throwaway script never committed -- the same capture
// discipline internal/jobs/metrics/daily/icfinalize's and
// internal/jobs/metrics/daily/team_complexity_parity_golden_test.go's own
// goldens describe. The captured values are exactly what that oracle
// script's own fixture input produces.
//
// This proves the testops_risk family's FULL row output, field for field,
// for all three of its sub-outputs (release_confidence/quality_drag/
// pipeline_stability) against the Python authority, including factors_json.
//
// DIVERGENCE (pm ruling A, 2026-11-15, CHAOS-13841): the release_confidence row
// no longer matches Python in four fields. Python took the LAST (team, service)
// group's success rate; the Go job takes the repository's summed rate (and the
// repository's summed passed over passed plus failed, skipped out). The golden
// file is NOT regenerated: the Python values stay recorded, and
// riskGoldenDivergences lists each changed field with its Python value, its new
// value (computed from the fixture's raw rows, not by the function under test)
// and the reason.
func TestTestopsRiskComputeMatchesFrozenPythonGolden(t *testing.T) {
	var golden testopsRiskGoldenDocument
	data, err := os.ReadFile(filepath.Join("testdata", "testops_risk_parity_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode testops_risk_parity_golden.json: %v", err)
	}
	if len(golden.ReleaseConfidence) != 1 || len(golden.QualityDrag) != 1 || len(golden.PipelineStability) != 1 {
		t.Fatalf("golden has %d/%d/%d release_confidence/quality_drag/pipeline_stability rows, want 1/1/1",
			len(golden.ReleaseConfidence), len(golden.QualityDrag), len(golden.PipelineStability))
	}

	const orgID = "00000000-0000-4000-8000-000000000009"
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	day := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	riskDT := func(hour, minute, second int) time.Time {
		return time.Date(2026, 8, 15, hour, minute, second, 0, time.UTC)
	}
	teamA, svcA := riskStrPtr("team-a"), riskStrPtr("svc-a")
	teamB, svcB := riskStrPtr("team-b"), riskStrPtr("svc-b")

	pipelineRuns := []testops.PipelineRunRow{
		{
			RepoID: repoID, Status: riskStrPtr("success"),
			QueuedAt: riskTimePtr(riskDT(9, 0, 0)), StartedAt: riskDT(9, 1, 0), FinishedAt: riskTimePtr(riskDT(9, 11, 0)),
			RetryCount: 0, TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
		{
			RepoID: repoID, Status: riskStrPtr("failed"),
			QueuedAt: riskTimePtr(riskDT(10, 0, 0)), StartedAt: riskDT(10, 2, 0), FinishedAt: riskTimePtr(riskDT(10, 20, 0)),
			RetryCount: 1, TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
		{
			RepoID: repoID, Status: riskStrPtr("success"),
			QueuedAt: riskTimePtr(riskDT(11, 0, 0)), StartedAt: riskDT(11, 3, 0), FinishedAt: riskTimePtr(riskDT(11, 9, 0)),
			RetryCount: 0, TeamID: teamB, ServiceID: svcB, OrgID: orgID,
		},
	}
	suites := []testops.SuiteRow{
		{
			RepoID: repoID, RunID: "run-1", SuiteID: "suite-1",
			TotalCount: 10, PassedCount: 8, FailedCount: 2, QuarantinedCount: 1,
			DurationSeconds: riskFloatPtr(42.0), StartedAt: riskTimePtr(riskDT(9, 1, 0)), FinishedAt: riskTimePtr(riskDT(9, 5, 0)),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
		{
			RepoID: repoID, RunID: "run-2", SuiteID: "suite-2",
			TotalCount: 5, PassedCount: 3, FailedCount: 2,
			DurationSeconds: riskFloatPtr(88.0), StartedAt: riskTimePtr(riskDT(10, 2, 0)), FinishedAt: riskTimePtr(riskDT(10, 10, 0)),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
	}
	cases := []testops.CaseRow{
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "test_flaky", Status: riskStrPtr("passed"), RetryAttempt: 1},
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "test_flaky", Status: riskStrPtr("failed"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-2", SuiteID: "suite-2", CaseName: "test_recurrent_failure", Status: riskStrPtr("failed"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-2", SuiteID: "suite-2", CaseName: "test_stable", Status: riskStrPtr("passed"), RetryAttempt: 0},
	}
	historicalFailedNames := map[string]struct{}{"test_recurrent_failure": {}}

	coverage := []testops.CoverageSnapshotRow{
		{
			RepoID: repoID, RunID: "run-1", SnapshotID: "snap-1",
			LinesTotal: riskUintPtr(1000), LinesCovered: riskUintPtr(800),
			LineCoveragePct: riskFloatPtr(80.0), BranchCoveragePct: riskFloatPtr(70.0),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
	}
	priorCoverage := []testops.CoverageSnapshotRow{
		{
			RepoID: repoID, RunID: "run-0", SnapshotID: "snap-0",
			LinesTotal: riskUintPtr(1000), LinesCovered: riskUintPtr(850),
			LineCoveragePct: riskFloatPtr(85.0), BranchCoveragePct: riskFloatPtr(72.0),
			TeamID: teamA, ServiceID: svcA, OrgID: orgID,
		},
	}

	pipelineMetrics := testops.ComputePipelineMetrics(repoID, pipelineRuns, "", nil)
	testMetrics := testops.ComputeTestMetrics(repoID, suites, cases, historicalFailedNames, "", nil)
	coverageMetric := testops.ComputeCoverageMetric(repoID, coverage, priorCoverage, "", nil)

	var pipe *testops.PipelineMetric
	if n := len(pipelineMetrics); n > 0 {
		pipe = &pipelineMetrics[n-1]
	}
	var test *testops.TestMetric
	if len(testMetrics) > 0 {
		test = &testMetrics[0]
	}

	// The release confidence reads the repository's summed counts (CHAOS-13841, pm ruling A 11-15); the
	// quality drag and the stability rows keep the representatives and the group list, as before.
	confidencePipe, confidenceTest := repoSummedConfidenceInputs(pipe, pipelineMetrics, test, testMetrics)
	releaseConfidenceRow := computeReleaseConfidence(repoID, day, confidencePipe, confidenceTest, coverageMetric, computedAt)
	qualityDragRow := computeQualityDrag(repoID, day, pipe, test, computedAt)
	pipelineStabilityRow := computePipelineStability(repoID, day, pipelineMetrics, computedAt)

	if releaseConfidenceRow == nil {
		t.Fatal("computeReleaseConfidence returned nil")
	}
	// Python's recorded values are kept as the golden; the fields below are the DIVERGENCE list (the rate is the
	// repository's sum, not the last team group's). The new values come from the fixture's RAW rows, summed by
	// repository with skipped cases out of the pass-rate denominator, not from the function under test.
	wantRow := golden.ReleaseConfidence[0]
	for _, d := range riskGoldenDivergences(t, wantRow, pipelineRuns, suites, cases) {
		if d.pythonFragment != "" && !strings.Contains(wantRow.FactorsJSON, d.pythonFragment) {
			t.Fatalf("divergence %s: the Python golden no longer holds %q", d.field, d.pythonFragment)
		}
		d.apply(&wantRow)
	}
	if got := toReleaseConfidenceGoldenRow(*releaseConfidenceRow); !reflect.DeepEqual(got, wantRow) {
		t.Errorf("release_confidence mismatch:\n got  %+v\n want %+v", got, wantRow)
	}

	if qualityDragRow == nil {
		t.Fatal("computeQualityDrag returned nil")
	}
	if got, want := toQualityDragGoldenRow(t, *qualityDragRow), golden.QualityDrag[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("quality_drag mismatch:\n got  %+v\n want %+v", got, want)
	}

	if pipelineStabilityRow == nil {
		t.Fatal("computePipelineStability returned nil")
	}
	if got, want := toPipelineStabilityGoldenRow(*pipelineStabilityRow), golden.PipelineStability[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("pipeline_stability mismatch:\n got  %+v\n want %+v", got, want)
	}
}

type testopsRiskGoldenDocument struct {
	ReleaseConfidence []releaseConfidenceGoldenRow `json:"release_confidence"`
	QualityDrag       []qualityDragGoldenRow       `json:"quality_drag"`
	PipelineStability []pipelineStabilityGoldenRow `json:"pipeline_stability"`
}

type releaseConfidenceGoldenRow struct {
	RepoID                string  `json:"repo_id"`
	ConfidenceScore       float64 `json:"confidence_score"`
	PipelineSuccessFactor float64 `json:"pipeline_success_factor"`
	TestPassFactor        float64 `json:"test_pass_factor"`
	CoverageFactor        float64 `json:"coverage_factor"`
	FlakePenalty          float64 `json:"flake_penalty"`
	RegressionPenalty     float64 `json:"regression_penalty"`
	FactorsJSON           string  `json:"factors_json"`
	TeamID                *string `json:"team_id"`
	ServiceID             *string `json:"service_id"`
	OrgID                 string  `json:"org_id"`
}

func toReleaseConfidenceGoldenRow(row testopsReleaseConfidenceRow) releaseConfidenceGoldenRow {
	return releaseConfidenceGoldenRow{
		RepoID: row.RepoID.String(), ConfidenceScore: row.ConfidenceScore,
		PipelineSuccessFactor: row.PipelineSuccessFactor, TestPassFactor: row.TestPassFactor,
		CoverageFactor: row.CoverageFactor, FlakePenalty: row.FlakePenalty, RegressionPenalty: row.RegressionPenalty,
		FactorsJSON: row.FactorsJSON, TeamID: row.TeamID, ServiceID: row.ServiceID, OrgID: row.OrgID,
	}
}

type qualityDragGoldenRow struct {
	RepoID                  string  `json:"repo_id"`
	DragHours               float64 `json:"drag_hours"`
	FailureReworkHours      float64 `json:"failure_rework_hours"`
	FlakeInvestigationHours float64 `json:"flake_investigation_hours"`
	QueueWaitHours          float64 `json:"queue_wait_hours"`
	RetryOverheadHours      float64 `json:"retry_overhead_hours"`
	FactorsJSON             string  `json:"factors_json"`
	TeamID                  *string `json:"team_id"`
	ServiceID               *string `json:"service_id"`
	OrgID                   string  `json:"org_id"`
}

// toQualityDragGoldenRow dereferences testopsQualityDragRow's *float64
// fields (nullable -- see testops_risk_native_clickhouse.go's own doc
// comment on that struct: a non-finite intermediate persists as SQL NULL
// rather than a computed float). The frozen golden corpus behind this test
// has no zero-denominator/NaN/overflow inputs, so every one of these fields
// is expected to be non-nil; a nil here is a genuine regression, not
// silently skipped or compared as a zero value.
func toQualityDragGoldenRow(t *testing.T, row testopsQualityDragRow) qualityDragGoldenRow {
	t.Helper()
	return qualityDragGoldenRow{
		RepoID:                  row.RepoID.String(),
		DragHours:               riskMustFloat(t, "DragHours", row.DragHours),
		FailureReworkHours:      riskMustFloat(t, "FailureReworkHours", row.FailureReworkHours),
		FlakeInvestigationHours: riskMustFloat(t, "FlakeInvestigationHours", row.FlakeInvestigationHours),
		QueueWaitHours:          riskMustFloat(t, "QueueWaitHours", row.QueueWaitHours),
		RetryOverheadHours:      riskMustFloat(t, "RetryOverheadHours", row.RetryOverheadHours),
		FactorsJSON:             row.FactorsJSON, TeamID: row.TeamID, ServiceID: row.ServiceID, OrgID: row.OrgID,
	}
}

func riskMustFloat(t *testing.T, field string, ptr *float64) float64 {
	t.Helper()
	if ptr == nil {
		t.Fatalf("%s is nil -- the frozen golden corpus has no non-finite inputs, so the nullable-on-non-finite boundary must not have fired here", field)
	}
	return *ptr
}

type pipelineStabilityGoldenRow struct {
	RepoID                    string   `json:"repo_id"`
	StabilityIndex            float64  `json:"stability_index"`
	SuccessRate7d             float64  `json:"success_rate_7d"`
	SuccessRateTrend          float64  `json:"success_rate_trend"`
	FailureClusteringScore    float64  `json:"failure_clustering_score"`
	MedianRecoveryTimeSeconds *float64 `json:"median_recovery_time_seconds"`
	TeamID                    *string  `json:"team_id"`
	ServiceID                 *string  `json:"service_id"`
	OrgID                     string   `json:"org_id"`
}

func toPipelineStabilityGoldenRow(row testopsPipelineStabilityRow) pipelineStabilityGoldenRow {
	return pipelineStabilityGoldenRow{
		RepoID: row.RepoID.String(), StabilityIndex: row.StabilityIndex, SuccessRate7d: row.SuccessRate7d,
		SuccessRateTrend: row.SuccessRateTrend, FailureClusteringScore: row.FailureClusteringScore,
		MedianRecoveryTimeSeconds: row.MedianRecoveryTimeSeconds, TeamID: row.TeamID, ServiceID: row.ServiceID, OrgID: row.OrgID,
	}
}

func riskStrPtr(v string) *string        { return &v }
func riskFloatPtr(v float64) *float64    { return &v }
func riskUintPtr(v uint32) *uint32       { return &v }
func riskTimePtr(v time.Time) *time.Time { return &v }

// riskDivergence is one recorded difference between the frozen Python golden and the Go output after the pm ruling
// (11-15): a repository's rate is its summed counts over its denominator, never one team group's rate.
type riskDivergence struct {
	field          string
	python, now    string
	reason         string
	pythonFragment string
	apply          func(*releaseConfidenceGoldenRow)
}

func riskGoldenDivergences(t *testing.T, golden releaseConfidenceGoldenRow, runs []testops.PipelineRunRow, suites []testops.SuiteRow, cases []testops.CaseRow) []riskDivergence {
	t.Helper()
	round4 := func(v float64) float64 { return math.Round(v*1e4) / 1e4 }
	success := 0
	for _, run := range runs {
		if run.Status != nil && *run.Status == "success" {
			success++
		}
	}
	pipelineRate := float64(success) / float64(len(runs))
	passed, failed := 0, 0
	for _, suite := range suites {
		passed += int(suite.PassedCount)
		failed += int(suite.FailedCount)
	}
	passRate := float64(passed) / float64(passed+failed)
	statuses := map[string]map[string]bool{}
	for _, c := range cases {
		if statuses[c.CaseName] == nil {
			statuses[c.CaseName] = map[string]bool{}
		}
		statuses[c.CaseName][*c.Status] = true
	}
	flaky := 0
	for _, seen := range statuses {
		if seen["passed"] && seen["failed"] {
			flaky++
		}
	}
	flakeRate := float64(flaky) / float64(len(statuses))
	base := 0.4*pipelineRate + 0.3*passRate + golden.CoverageFactor + 0.1*(1-flakeRate)
	score := round4(math.Max(0, math.Min(1, base-golden.FlakePenalty-golden.RegressionPenalty)))
	if round4(passRate) != 0.7333 {
		t.Fatalf("raw pass rate %v: the fixture changed, recompute the divergence list", passRate)
	}
	return []riskDivergence{
		{"pipeline_success_factor", "0.4", fmt.Sprint(round4(0.4 * pipelineRate)), "last group's rate (1/1) replaced by the repository's 2/3", "",
			func(r *releaseConfidenceGoldenRow) { r.PipelineSuccessFactor = round4(0.4 * pipelineRate) }},
		{"factors_json.pipeline_success_rate", "1.0", fmt.Sprint(round4(pipelineRate)), "same", `"pipeline_success_rate": 1.0`,
			func(r *releaseConfidenceGoldenRow) {
				r.FactorsJSON = strings.Replace(r.FactorsJSON, `"pipeline_success_rate": 1.0`, fmt.Sprintf(`"pipeline_success_rate": %v`, round4(pipelineRate)), 1)
			}},
		{"factors_json.base_score", "0.8467", fmt.Sprint(round4(base)), "follows the pipeline factor", `"base_score": 0.8467`,
			func(r *releaseConfidenceGoldenRow) {
				r.FactorsJSON = strings.Replace(r.FactorsJSON, `"base_score": 0.8467`, fmt.Sprintf(`"base_score": %v`, round4(base)), 1)
			}},
		{"confidence_score", "0.5967", fmt.Sprint(score), "follows the pipeline factor", "",
			func(r *releaseConfidenceGoldenRow) { r.ConfidenceScore = score }},
	}
}
