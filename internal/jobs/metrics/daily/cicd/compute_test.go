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
// with that frozen golden for repo A; this file's exhaustive decode against it
// is the only thing left to catch a drift between them. The golden's repo B
// row is a still-running run, which the Python rule counted on its start day
// and the finish-day rule counts on no day: the decode compares repo A only.
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
			QueuedAt: dtPtr(23, 0, 24), StartedAt: dt(23, 30, 24), FinishedAt: dtPtr(0, 30, 25),
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
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1 (a still-running run counts on no day; zero rows means no record)", len(records))
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
}

// A run counts on the UTC day it finishes: a run that crosses midnight counts
// once, on the day it ends, and its duration still spans start to finish.
func TestComputeCountsRunOnFinishDay(t *testing.T) {
	runs := []PipelineRunRow{
		{RepoID: repoA, RunID: "a", Status: "success", StartedAt: dt(10, 0, 24), FinishedAt: dtPtr(10, 30, 24)},
		{RepoID: repoA, RunID: "b", Status: "success", QueuedAt: dtPtr(23, 40, 24), StartedAt: dt(23, 50, 24), FinishedAt: dtPtr(0, 20, 25)},
	}
	onStart := ComputeCICDMetricsDaily(day, runs, computedAt)
	if len(onStart) != 1 || onStart[0].PipelinesCount != 1 {
		t.Fatalf("day 24: %+v, want one record counting run a only", onStart)
	}
	onFinish := ComputeCICDMetricsDaily(day.AddDate(0, 0, 1), runs, computedAt)
	if len(onFinish) != 1 || onFinish[0].PipelinesCount != 1 {
		t.Fatalf("day 25: %+v, want one record counting run b", onFinish)
	}
	if got := onFinish[0].AvgDurationMinutes; got == nil || *got != 30 {
		t.Errorf("run b duration = %v, want 30", got)
	}
	if got := onFinish[0].AvgQueueMinutes; got == nil || *got != 10 {
		t.Errorf("run b queue = %v, want 10", got)
	}
	running := []PipelineRunRow{{RepoID: repoB, RunID: "r", Status: "running", StartedAt: dt(15, 0, 24)}}
	if got := ComputeCICDMetricsDaily(day, running, computedAt); len(got) != 0 {
		t.Errorf("a still-running run counted: %+v", got)
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

	repoAOnly := golden.Records[:0]
	for _, record := range golden.Records {
		if record.RepoID == repoA.String() {
			repoAOnly = append(repoAOnly, record)
		}
	}
	golden.Records = repoAOnly
	sortGoldenRecords(golden.Records)
	sortGoldenRecords(live)

	if !reflect.DeepEqual(golden.Records, live) {
		t.Errorf("cicd_metrics_daily mismatch:\nfrozen: %+v\nlive:   %+v", golden.Records, live)
	}
}

func sortGoldenRecords(records []goldenRecord) {
	sort.Slice(records, func(i, j int) bool { return records[i].RepoID < records[j].RepoID })
}
