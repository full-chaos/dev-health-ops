package daily

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

func unmeasuredPipe(repo uuid.UUID) *testops.PipelineMetric {
	median, queue := 3600.0, 0.0
	return &testops.PipelineMetric{
		RepoID: repo, PipelinesCount: 2, SuccessCount: 4, MedianDurationSeconds: &median, AvgQueueSeconds: &queue, OrgID: "org",
	}
}

func measuredTest(repo uuid.UUID) *testops.TestMetric {
	return &testops.TestMetric{
		RepoID: repo, TotalCases: 4, PassedCount: 3, FailedCount: 1, DistinctCases: 4, FlakeCases: 1,
		FlakeRate: 0.25, FailureRecurrence: 0.5, OrgID: "org",
	}
}

func unmeasuredTest(repo uuid.UUID) *testops.TestMetric {
	return &testops.TestMetric{RepoID: repo, TotalCases: 4, PassedCount: 3, FailedCount: 1, OrgID: "org"}
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestReleaseConfidenceUnmeasuredDayLeavesFlakeFactorsOut(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repoA, repoB := uuid.New(), uuid.New()
	measured := computeReleaseConfidence(repoA, day, unmeasuredPipe(repoA), measuredTest(repoA), nil, day)
	unmeasured := computeReleaseConfidence(repoB, day, unmeasuredPipe(repoB), unmeasuredTest(repoB), nil, day)
	if measured == nil || unmeasured == nil {
		t.Fatal("nil row")
	}
	// No coverage row: coverage is unknown, so every base below is divided by 0.8.
	// measured: base (0.4+0.225+0.1*(1-0.25))/0.8=0.875; flake penalty 0.1, recurrence 0.5>0.3 adds 0.1 -> 0.675
	if !approx(measured.ConfidenceScore, 0.675) || !approx(measured.FlakePenalty, 0.1) || !approx(measured.RegressionPenalty, 0.1) {
		t.Errorf("measured day: score=%v flake_penalty=%v regression=%v", measured.ConfidenceScore, measured.FlakePenalty, measured.RegressionPenalty)
	}
	if !strings.Contains(measured.FactorsJSON, `"flake_rate": 0.25`) {
		t.Errorf("measured factors: %s", measured.FactorsJSON)
	}
	// unmeasured: flake term left out (adds 0), no penalties: (0.4+0.225)/0.8 = 0.78125 -> 0.7812, never 0.725/0.8
	if !approx(unmeasured.ConfidenceScore, 0.7812) || unmeasured.FlakePenalty != 0 || unmeasured.RegressionPenalty != 0 {
		t.Errorf("unmeasured day: score=%v flake_penalty=%v regression=%v want 0.7812/0/0", unmeasured.ConfidenceScore, unmeasured.FlakePenalty, unmeasured.RegressionPenalty)
	}
	for _, want := range []string{`"flake_rate": null`, `"failure_recurrence": null`} {
		if !strings.Contains(unmeasured.FactorsJSON, want) {
			t.Errorf("unmeasured factors missing %s: %s", want, unmeasured.FactorsJSON)
		}
	}
}

func TestReleaseConfidenceAllUnmeasuredHasNoFlakeFactor(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	row := computeReleaseConfidence(repo, day, unmeasuredPipe(repo), unmeasuredTest(repo), nil, day)
	if row == nil {
		t.Fatal("nil row")
	}
	if strings.Contains(row.FactorsJSON, `"flake_rate": 0`) || !strings.Contains(row.FactorsJSON, `"flake_rate": null`) {
		t.Errorf("factors: %s", row.FactorsJSON)
	}
	if !strings.Contains(row.FactorsJSON, `"base_score": 0.7812`) {
		t.Errorf("base_score must carry no flake credit: %s", row.FactorsJSON)
	}
}

func TestReleaseConfidenceMeasuredZeroFlakeKeepsCredit(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	test := &testops.TestMetric{RepoID: repo, TotalCases: 4, PassedCount: 3, FailedCount: 1, DistinctCases: 4, OrgID: "org"}
	row := computeReleaseConfidence(repo, day, unmeasuredPipe(repo), test, nil, day)
	// (0.4+0.225+0.1)/0.8 (coverage unknown) = 0.90625 -> 0.9062
	if row == nil || !approx(row.ConfidenceScore, 0.9062) || !strings.Contains(row.FactorsJSON, `"flake_rate": 0.0`) {
		t.Errorf("a measured 0 keeps its credit: %+v", row)
	}
}

func TestReleaseConfidenceNoTestRowUnchanged(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	row := computeReleaseConfidence(repo, day, unmeasuredPipe(repo), nil, nil, day)
	if row == nil || !approx(row.ConfidenceScore, 0.625) || !strings.Contains(row.FactorsJSON, `"flake_rate": 0.0`) {
		t.Errorf("test==nil keeps its flake credit ((0.4+0.1)/0.8, coverage unknown): %+v", row)
	}
}

func TestQualityDragUnmeasuredDayFlakeHoursNull(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repoA, repoB := uuid.New(), uuid.New()
	pipeA, pipeB := unmeasuredPipe(repoA), unmeasuredPipe(repoB)
	pipeA.FailureCount, pipeB.FailureCount = 1, 1
	measured := computeQualityDrag(repoA, day, pipeA, measuredTest(repoA), day)
	unmeasured := computeQualityDrag(repoB, day, pipeB, unmeasuredTest(repoB), day)
	if measured == nil || unmeasured == nil {
		t.Fatal("nil row")
	}
	// flake hours = 0.25 * 4 * 0.25 = 0.25; failure rework = 1*3600/3600 = 1
	if measured.FlakeInvestigationHours == nil || !approx(*measured.FlakeInvestigationHours, 0.25) || !approx(*measured.DragHours, 1.25) {
		t.Errorf("measured: flake=%v drag=%v", measured.FlakeInvestigationHours, measured.DragHours)
	}
	if unmeasured.FlakeInvestigationHours != nil {
		t.Errorf("unmeasured flake hours must be NULL, got %v", *unmeasured.FlakeInvestigationHours)
	}
	if unmeasured.DragHours == nil || !approx(*unmeasured.DragHours, 1.0) {
		t.Errorf("unmeasured drag must be the sum of known parts (1.0): %v", unmeasured.DragHours)
	}
	if !strings.Contains(unmeasured.FactorsJSON, `"flake_rate": null`) {
		t.Errorf("factors: %s", unmeasured.FactorsJSON)
	}
}

// A measured day keeps main's addition order, so its rounded drag_hours is the same bits as before:
// for these inputs the order flake-last rounds one unit apart in the 4th decimal.
func TestQualityDragMeasuredDayKeepsMainAdditionOrder(t *testing.T) {
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	repo := uuid.New()
	median, queue := 3852.7, 5780.6
	pipe := &testops.PipelineMetric{
		RepoID: repo, PipelinesCount: 6, FailureCount: 6, RerunRate: 0.9, MedianDurationSeconds: &median, AvgQueueSeconds: &queue, OrgID: "org",
	}
	test := &testops.TestMetric{RepoID: repo, TotalCases: 244, DistinctCases: 244, FlakeRate: 0.82, OrgID: "org"}
	row := computeQualityDrag(repo, day, pipe, test, day)
	if row == nil || row.DragHours == nil {
		t.Fatal("nil row")
	}
	failure := 6 * median / 3600.0
	flake := 0.82 * float64(244) * 0.25
	wait := 6 * queue / 3600.0
	retry := 0.9 * 6 * median / 3600.0
	mainOrder := failure + flake + wait + retry
	if want := pyRound(mainOrder, 4); *row.DragHours != want {
		t.Errorf("drag_hours = %v, want main's order %v", *row.DragHours, want)
	}
}
