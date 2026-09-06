package cicd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	repoA = uuid.MustParse("00000000-0000-4000-8000-00000000000a")
	repoB = uuid.MustParse("00000000-0000-4000-8000-00000000000b")

	day        = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computedAt = time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
)

func dt(hour, minute, dayOfMonth int) time.Time {
	return time.Date(2026, 8, dayOfMonth, hour, minute, 0, 0, time.UTC)
}

func dtPtr(hour, minute, dayOfMonth int) *time.Time {
	value := dt(hour, minute, dayOfMonth)
	return &value
}

// fixturePipelineRuns mirrors the PIPELINE_RUNS corpus the (now-deleted,
// CHAOS-5312/CHAOS-3092: cicd's Python compute is gone)
// generate_daily_cicd_python_golden.py generator used to produce
// tests/fixtures/daily_cicd_python_golden.json verbatim -- keep this in sync
// with that frozen golden; this file's exhaustive decode against it is the
// only thing left to catch a drift between them.
func fixturePipelineRuns() []PipelineRunRow {
	return []PipelineRunRow{
		{
			RepoID: repoA, RunID: "a-success", Status: "  Success  ",
			QueuedAt: dtPtr(9, 55, 24), StartedAt: dt(10, 0, 24), FinishedAt: dtPtr(10, 10, 24),
		},
		{
			RepoID: repoA, RunID: "a-failed", Status: "failed",
			QueuedAt: dtPtr(11, 50, 24), StartedAt: dt(12, 0, 24), FinishedAt: dtPtr(12, 30, 24),
		},
		{
			RepoID: repoA, RunID: "a-out-of-window", Status: "success",
			QueuedAt: dtPtr(23, 0, 23), StartedAt: dt(23, 30, 23), FinishedAt: dtPtr(0, 30, 24),
		},
		{
			RepoID: repoA, RunID: "a-no-queue", Status: "succeeded",
			QueuedAt: nil, StartedAt: dt(13, 0, 24), FinishedAt: dtPtr(13, 5, 24),
		},
		{
			RepoID: repoA, RunID: "a-clock-skew", Status: "passed",
			QueuedAt: dtPtr(13, 55, 24), StartedAt: dt(14, 0, 24), FinishedAt: dtPtr(13, 59, 24),
		},
		{
			RepoID: repoB, RunID: "b-running", Status: "running",
			QueuedAt: nil, StartedAt: dt(15, 0, 24), FinishedAt: nil,
		},
	}
}

// TestComputeMatchesFrozenPythonGolden is a fast, readable smoke check on the
// hand-picked values -- see TestComputeMatchesFrozenGoldenExhaustively
// (golden_full_test.go) for the row-complete guard against the same frozen
// file.
func TestComputeMatchesFrozenPythonGolden(t *testing.T) {
	records := ComputeCICDMetricsDaily(day, fixturePipelineRuns(), computedAt)
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2 (repo C never appears -- zero rows means no record)", len(records))
	}

	repoARecord := records[0]
	if repoARecord.RepoID != repoA {
		t.Fatalf("records[0].RepoID = %s, want repo A", repoARecord.RepoID)
	}
	if repoARecord.PipelinesCount != 4 {
		t.Errorf("repo A PipelinesCount = %d, want 4 (out-of-window run excluded)", repoARecord.PipelinesCount)
	}
	if repoARecord.SuccessRate != 0.75 {
		t.Errorf("repo A SuccessRate = %v, want 0.75", repoARecord.SuccessRate)
	}
	if repoARecord.AvgDurationMinutes == nil || *repoARecord.AvgDurationMinutes != 15.0 {
		t.Errorf("repo A AvgDurationMinutes = %v, want 15.0", repoARecord.AvgDurationMinutes)
	}
	if repoARecord.P90DurationMinutes == nil || *repoARecord.P90DurationMinutes != 26.0 {
		t.Errorf("repo A P90DurationMinutes = %v, want 26.0", repoARecord.P90DurationMinutes)
	}
	if repoARecord.AvgQueueMinutes == nil || *repoARecord.AvgQueueMinutes != (20.0/3.0) {
		t.Errorf("repo A AvgQueueMinutes = %v, want %v", repoARecord.AvgQueueMinutes, 20.0/3.0)
	}

	repoBRecord := records[1]
	if repoBRecord.RepoID != repoB {
		t.Fatalf("records[1].RepoID = %s, want repo B", repoBRecord.RepoID)
	}
	if repoBRecord.PipelinesCount != 1 || repoBRecord.SuccessRate != 0.0 {
		t.Errorf("repo B PipelinesCount=%d SuccessRate=%v, want 1/0.0", repoBRecord.PipelinesCount, repoBRecord.SuccessRate)
	}
	if repoBRecord.AvgDurationMinutes != nil || repoBRecord.P90DurationMinutes != nil || repoBRecord.AvgQueueMinutes != nil {
		t.Errorf("repo B duration/queue fields must all be nil (no finished_at/queued_at ever set)")
	}
}

// --- Exhaustive golden decode (mirrors repouser/golden_full_test.go) ---

type goldenRecord struct {
	RepoID             string   `json:"repo_id"`
	Day                string   `json:"day"`
	PipelinesCount     int      `json:"pipelines_count"`
	SuccessRate        float64  `json:"success_rate"`
	AvgDurationMinutes *float64 `json:"avg_duration_minutes"`
	P90DurationMinutes *float64 `json:"p90_duration_minutes"`
	AvgQueueMinutes    *float64 `json:"avg_queue_minutes"`
	ComputedAt         string   `json:"computed_at"`
}

type goldenDocument struct {
	Records []goldenRecord `json:"records"`
}

func toGoldenRecord(metric CICDMetric) goldenRecord {
	return goldenRecord{
		RepoID:             metric.RepoID.String(),
		Day:                metric.Day.Format("2006-01-02"),
		PipelinesCount:     metric.PipelinesCount,
		SuccessRate:        metric.SuccessRate,
		AvgDurationMinutes: metric.AvgDurationMinutes,
		P90DurationMinutes: metric.P90DurationMinutes,
		AvgQueueMinutes:    metric.AvgQueueMinutes,
		// Python's isoformat() renders +00:00; Go's RFC3339 renders Z --
		// normalize both to the same instant comparison rather than a string
		// comparison to sidestep that harmless representation difference.
		ComputedAt: metric.ComputedAt.UTC().Format(time.RFC3339),
	}
}

func repositoryRootPath(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

// TestComputeMatchesFrozenGoldenExhaustively decodes the ENTIRE frozen golden
// file (every row, every field) and compares it against
// ComputeCICDMetricsDaily's live output, rather than the hand-picked subset
// TestComputeMatchesFrozenPythonGolden asserts.
func TestComputeMatchesFrozenGoldenExhaustively(t *testing.T) {
	repoRoot := repositoryRootPath(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "tests", "fixtures", "daily_cicd_python_golden.json"))
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var golden goldenDocument
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}

	records := ComputeCICDMetricsDaily(day, fixturePipelineRuns(), computedAt)
	live := make([]goldenRecord, len(records))
	for i, record := range records {
		live[i] = toGoldenRecord(record)
	}

	// Normalize Python's isoformat computed_at the same way toGoldenRecord
	// does for Go's, so the comparison is instant-equal, not string-equal.
	for i := range golden.Records {
		parsed, err := time.Parse(time.RFC3339, golden.Records[i].ComputedAt)
		if err != nil {
			t.Fatalf("parse golden computed_at %q: %v", golden.Records[i].ComputedAt, err)
		}
		golden.Records[i].ComputedAt = parsed.UTC().Format(time.RFC3339)
	}

	sortGoldenRecords(golden.Records)
	sortGoldenRecords(live)

	if !reflect.DeepEqual(golden.Records, live) {
		t.Errorf("cicd_metrics_daily mismatch:\nfrozen: %+v\nlive:   %+v", golden.Records, live)
	}
}

func sortGoldenRecords(records []goldenRecord) {
	sort.Slice(records, func(i, j int) bool { return records[i].RepoID < records[j].RepoID })
}
