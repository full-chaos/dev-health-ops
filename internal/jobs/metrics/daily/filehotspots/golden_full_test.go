package filehotspots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// TestComputeMatchesFrozenPythonGolden decodes the frozen golden file
// (tests/fixtures/file_hotspots_python_golden.json, originally generated
// from REAL production Python before compute_file_hotspots/
// compute_file_risk_hotspots were deleted, CHAOS-5234/CHAOS-3092 -- see
// this repo's own history for the generator that produced it) and asserts
// ComputeFileHotspots/ComputeFileRiskHotspots reproduce it exactly for both
// repositories in the fixture -- including the AGGREGATE_STATS_MARKER
// skip, the empty-file_path skip, cross-repo isolation, the complexity-only
// union case, and both the present/absent blame_concentration branches.
// The live-Python rot guard this file used to have a counterpart for
// (golden_rot_guard_test.go) was retired in the same PR that deleted the
// Python compute it compared against -- this frozen-bits test is now the
// only regression guard, per chris's ruling that the frozen golden + this
// test are the ongoing contract once Python is out of the loop.
func TestComputeMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)

	windowStats := goldenWindowStats()
	repoA := uuid.MustParse("00000000-0000-4000-8000-0000000000fa")
	repoB := uuid.MustParse("00000000-0000-4000-8000-0000000000fb")

	assertFileMetricsMatch(t, golden.FileMetricsRepoA, ComputeFileHotspots(repoA, windowStats))
	assertFileMetricsMatch(t, golden.FileMetricsRepoB, ComputeFileHotspots(repoB, windowStats))

	complexityMap := map[string]ComplexitySnapshot{
		"shared.py":       {CyclomaticTotal: 24, CyclomaticAvg: 3.0},
		"idle_complex.py": {CyclomaticTotal: 80, CyclomaticAvg: 4.0},
	}
	blameMap := map[string]float64{"shared.py": 0.75}

	assertRiskMetricsMatch(t, golden.RiskHotspotsRepoA, ComputeFileRiskHotspots(repoA, windowStats, complexityMap, blameMap))
	assertRiskMetricsMatch(t, golden.RiskHotspotsRepoB, ComputeFileRiskHotspots(repoB, windowStats, nil, nil))
}

// TestComputeFileRiskHotspotsReturnsEmptyNotNilSlice is codex round 8's
// finding: a repo with neither churn nor a complexity snapshot must return
// an empty, non-nil slice -- matching Python's `return []` (hotspots.py:174)
// literally, not merely behaviorally (no current caller distinguishes nil
// from empty, but this removes any doubt).
func TestComputeFileRiskHotspotsReturnsEmptyNotNilSlice(t *testing.T) {
	repoWithNoData := uuid.MustParse("00000000-0000-4000-8000-00000000dead")
	result := ComputeFileRiskHotspots(repoWithNoData, nil, nil, nil)
	if result == nil {
		t.Fatal("ComputeFileRiskHotspots returned nil for an empty input, want a non-nil empty slice")
	}
	if len(result) != 0 {
		t.Fatalf("ComputeFileRiskHotspots returned %d rows for an empty input, want 0", len(result))
	}
}

// goldenWindowStats mirrors generate_file_hotspots_python_golden.py's
// _window_stats() exactly -- same repo ids, hashes, authors, days, and byte
// counts -- so this test and the frozen fixture describe the same input.
func goldenWindowStats() []repouser.CommitStatRow {
	repoA := uuid.MustParse("00000000-0000-4000-8000-0000000000fa")
	repoB := uuid.MustParse("00000000-0000-4000-8000-0000000000fb")
	day := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	priorDay := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	return []repouser.CommitStatRow{
		{RepoID: repoA, CommitHash: "c1", AuthorEmail: "alice@example.com", AuthorName: "Alice", CommitterWhen: priorDay, FilePath: "shared.py", Additions: 40, Deletions: 10},
		{RepoID: repoA, CommitHash: "c2", AuthorEmail: "bob@example.com", AuthorName: "Bob", CommitterWhen: day, FilePath: "shared.py", Additions: 5, Deletions: 5},
		{RepoID: repoA, CommitHash: "c3", AuthorEmail: "alice@example.com", AuthorName: "Alice", CommitterWhen: day, FilePath: "solo.py", Additions: 3, Deletions: 1},
		{RepoID: repoA, CommitHash: "c4", AuthorEmail: "backfill@example.com", AuthorName: "Backfill Bot", CommitterWhen: day, FilePath: "__AGGREGATE__", Additions: 9999, Deletions: 9999},
		{RepoID: repoA, CommitHash: "c5", AuthorEmail: "carol@example.com", AuthorName: "Carol", CommitterWhen: day, FilePath: "", Additions: 1, Deletions: 1},
		{RepoID: repoA, CommitHash: "c6", AuthorEmail: " ", AuthorName: "Wendy", CommitterWhen: day, FilePath: "ws_email.py", Additions: 5, Deletions: 0},
		{RepoID: repoA, CommitHash: "c7", AuthorEmail: "Wendy", AuthorName: "Wendy", CommitterWhen: day, FilePath: "ws_email.py", Additions: 5, Deletions: 0},
		{RepoID: repoB, CommitHash: "b1", AuthorEmail: "dave@example.com", AuthorName: "Dave", CommitterWhen: day, FilePath: "other_repo.py", Additions: 500, Deletions: 500},
	}
}

type goldenDocument struct {
	FileMetricsRepoA  []goldenFileMetric `json:"file_metrics_repo_a"`
	FileMetricsRepoB  []goldenFileMetric `json:"file_metrics_repo_b"`
	RiskHotspotsRepoA []goldenRiskMetric `json:"risk_hotspots_repo_a"`
	RiskHotspotsRepoB []goldenRiskMetric `json:"risk_hotspots_repo_b"`
}

type goldenFileMetric struct {
	Path         string  `json:"path"`
	Churn        int     `json:"churn"`
	Contributors int     `json:"contributors"`
	CommitsCount int     `json:"commits_count"`
	HotspotScore float64 `json:"hotspot_score"`
}

type goldenRiskMetric struct {
	FilePath           string   `json:"file_path"`
	ChurnLOC30d        int      `json:"churn_loc_30d"`
	ChurnCommits30d    int      `json:"churn_commits_30d"`
	CyclomaticTotal    int      `json:"cyclomatic_total"`
	CyclomaticAvg      float64  `json:"cyclomatic_avg"`
	BlameConcentration *float64 `json:"blame_concentration"`
	RiskScore          float64  `json:"risk_score"`
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	root := repositoryRootPath(t)
	raw, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "file_hotspots_python_golden.json"))
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var golden goldenDocument
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}
	return golden
}

// floatTolerance absorbs the Float64 last-bit noise a Go reimplementation of
// a floating-point formula can accumulate against Python's, WITHOUT masking
// a real fidelity bug: 1e-9 is far tighter than any of this fixture's score
// magnitudes (0.4-3.4) while still exceeding IEEE-754 double rounding noise
// from a handful of arithmetic operations in a different order.
const floatTolerance = 1e-9

func assertFileMetricsMatch(t *testing.T, golden []goldenFileMetric, live []FileMetric) {
	t.Helper()
	if len(golden) != len(live) {
		t.Fatalf("row count mismatch: golden=%d live=%d\ngolden=%+v\nlive=%+v", len(golden), len(live), golden, live)
	}
	liveByPath := make(map[string]FileMetric, len(live))
	for _, row := range live {
		liveByPath[row.Path] = row
	}
	for _, want := range golden {
		got, ok := liveByPath[want.Path]
		if !ok {
			t.Fatalf("path %q present in golden but not in live output", want.Path)
		}
		if got.Churn != want.Churn || got.Contributors != want.Contributors || got.CommitsCount != want.CommitsCount {
			t.Errorf("path %q: got %+v, want churn=%d contributors=%d commits_count=%d", want.Path, got, want.Churn, want.Contributors, want.CommitsCount)
		}
		if !floatsClose(got.HotspotScore, want.HotspotScore) {
			t.Errorf("path %q: hotspot_score got %v want %v", want.Path, got.HotspotScore, want.HotspotScore)
		}
	}
}

func assertRiskMetricsMatch(t *testing.T, golden []goldenRiskMetric, live []RiskMetric) {
	t.Helper()
	if len(golden) != len(live) {
		t.Fatalf("row count mismatch: golden=%d live=%d\ngolden=%+v\nlive=%+v", len(golden), len(live), golden, live)
	}
	liveByPath := make(map[string]RiskMetric, len(live))
	for _, row := range live {
		liveByPath[row.FilePath] = row
	}
	for _, want := range golden {
		got, ok := liveByPath[want.FilePath]
		if !ok {
			t.Fatalf("path %q present in golden but not in live output", want.FilePath)
		}
		if got.ChurnLOC30d != want.ChurnLOC30d || got.ChurnCommits30d != want.ChurnCommits30d {
			t.Errorf("path %q: got %+v, want churn_loc_30d=%d churn_commits_30d=%d", want.FilePath, got, want.ChurnLOC30d, want.ChurnCommits30d)
		}
		if got.CyclomaticTotal != want.CyclomaticTotal || !floatsClose(got.CyclomaticAvg, want.CyclomaticAvg) {
			t.Errorf("path %q: got cyclomatic_total=%d cyclomatic_avg=%v, want %d/%v", want.FilePath, got.CyclomaticTotal, got.CyclomaticAvg, want.CyclomaticTotal, want.CyclomaticAvg)
		}
		if (got.BlameConcentration == nil) != (want.BlameConcentration == nil) {
			t.Errorf("path %q: blame_concentration nil-ness mismatch: got=%v want=%v", want.FilePath, got.BlameConcentration, want.BlameConcentration)
		} else if got.BlameConcentration != nil && !floatsClose(*got.BlameConcentration, *want.BlameConcentration) {
			t.Errorf("path %q: blame_concentration got %v want %v", want.FilePath, *got.BlameConcentration, *want.BlameConcentration)
		}
		if !floatsClose(got.RiskScore, want.RiskScore) {
			t.Errorf("path %q: risk_score got %v want %v", want.FilePath, got.RiskScore, want.RiskScore)
		}
	}
}

func floatsClose(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= floatTolerance
}

// repositoryRootPath walks up from this package to the checkout root (the
// directory containing go.mod), matching repouser's identically-named
// helper.
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
