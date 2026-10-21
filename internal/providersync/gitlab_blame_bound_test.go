package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// accumulatingGitLabBlameCoverage is the durable coverage as production reads
// it: the paths whose rows reached git_blame.
type accumulatingGitLabBlameCoverage struct {
	mu    sync.Mutex
	paths []string
}

func (coverage *accumulatingGitLabBlameCoverage) BlamedPaths(context.Context, Claim, string) ([]string, error) {
	coverage.mu.Lock()
	defer coverage.mu.Unlock()
	return append([]string(nil), coverage.paths...), nil
}

func (coverage *accumulatingGitLabBlameCoverage) record(batch CompleteRouteBatch) {
	coverage.mu.Lock()
	defer coverage.mu.Unlock()
	seen := map[string]bool{}
	for _, raw := range batch.Effects[0].Rows {
		var row gitBlameRow
		_ = json.Unmarshal(raw, &row)
		if !seen[row.Path] {
			seen[row.Path] = true
			coverage.paths = append(coverage.paths, row.Path)
		}
	}
}

func runGitLabBlameBound(
	t *testing.T, files int, coverage GitLabBlameCoverage, limits *gitLabBlameLimits,
) (CompleteRouteBatch, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	client := gitLabRepositoryClient(t, fakehttp.Client(&gitLabBlameDoer{t: t, fileCount: files}), "https://gitlab.example")
	batch, err := (GitLabBlameRouteHandler{Coverage: coverage, writeLimits: limits}).Collect(
		context.Background(), nativeTestClaim("gitlab", "blame"), providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch, &logs
}

func warnCount(logs *bytes.Buffer, text string) int {
	count := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, text) {
			count++
		}
	}
	return count
}

// Each fixture file has 2 blame lines = 2 rows. The unit stops BEFORE the file
// that would cross the row bound, the bound is inclusive, and the refused file
// is never added to the batch.
func TestGitLabBlameStopsBeforeAFileThatCrossesTheRowBound(t *testing.T) {
	for _, tc := range []struct {
		maxRows       int
		wantRows      int
		wantRemaining int
		wantStatus    string
		wantStopWarn  int
	}{
		{4, 4, 1, "partial", 1},
		{5, 4, 1, "partial", 1},
		{6, 6, 0, "complete", 0},
	} {
		built := 0
		limits := defaultGitLabBlameLimits()
		limits.maxRows, limits.rowsBuilt = tc.maxRows, &built
		batch, logs := runGitLabBlameBound(t, 3, staticGitLabBlameCoverage{}, &limits)
		if len(batch.Effects[0].Rows) != tc.wantRows || built != tc.wantRows ||
			batch.Result["remaining_paths"] != tc.wantRemaining || batch.Result["inventory_status"] != tc.wantStatus {
			t.Fatalf("maxRows=%d rows=%d built=%d result=%v", tc.maxRows, len(batch.Effects[0].Rows), built, batch.Result)
		}
		if got := warnCount(logs, "stopped early"); got != tc.wantStopWarn {
			t.Fatalf("maxRows=%d stop WARN lines=%d want %d: %s", tc.maxRows, got, tc.wantStopWarn, logs.String())
		}
		if tc.wantStopWarn == 1 && (!strings.Contains(logs.String(), "class=rows") || !strings.Contains(logs.String(), "paths_deferred=1")) {
			t.Fatalf("stop WARN lacks its class or count: %s", logs.String())
		}
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, "level=WARN") && (strings.Contains(line, "src/file") || strings.Contains(line, "acme")) {
				t.Fatalf("a path or the repository reached a WARN: %s", line)
			}
		}
	}
}

func TestGitLabBlameStopsBeforeAFileThatCrossesThePayloadBound(t *testing.T) {
	probe, _ := runGitLabBlameBound(t, 1, staticGitLabBlameCoverage{}, nil)
	perFile := 0
	for _, raw := range probe.Effects[0].Rows {
		perFile += len(raw)
	}
	for _, tc := range []struct{ bound, wantFiles int }{{perFile * 2, 2}, {perFile*2 - 1, 1}, {perFile * 3, 3}} {
		limits := defaultGitLabBlameLimits()
		limits.maxPayloadBytes = tc.bound
		batch, logs := runGitLabBlameBound(t, 3, staticGitLabBlameCoverage{}, &limits)
		if got := len(batch.Effects[0].Rows); got != tc.wantFiles*2 {
			t.Fatalf("bound=%d rows=%d want %d files", tc.bound, got, tc.wantFiles)
		}
		if tc.wantFiles < 3 && !strings.Contains(logs.String(), "class=bytes") {
			t.Fatalf("bound=%d: no bytes stop WARN: %s", tc.bound, logs.String())
		}
	}
}

// A file that alone is above the bound never blocks the unit: it stays a
// retryable path failure, counted, with one WARN per file and no rows.
func TestGitLabBlameFileAloneAboveTheBoundStaysRetryableAndNeverBlocks(t *testing.T) {
	limits := defaultGitLabBlameLimits()
	limits.maxRows = 1
	batch, logs := runGitLabBlameBound(t, 3, staticGitLabBlameCoverage{}, &limits)
	if len(batch.Effects[0].Rows) != 0 || batch.Result["retryable_path_failures"] != 3 || batch.Result["remaining_paths"] != 3 {
		t.Fatalf("rows=%d result=%v", len(batch.Effects[0].Rows), batch.Result)
	}
	if got := warnCount(logs, "above the write bound kept retryable"); got != 3 {
		t.Fatalf("WARN lines=%d want 3: %s", got, logs.String())
	}
}

// The early stop loses nothing: coverage is what git_blame holds, so a later
// run selects exactly the paths the stopped run left. Over runs the blamed set
// is every file and no file is blamed twice.
func TestGitLabBlameEarlyStopCarriesTheRemainingPathsToTheNextRunsWithoutLossOrRepeat(t *testing.T) {
	coverage := &accumulatingGitLabBlameCoverage{}
	limits := defaultGitLabBlameLimits()
	limits.maxRows = 4
	perPath := map[string]int{}
	runs := 0
	for ; runs < 10; runs++ {
		batch, _ := runGitLabBlameBound(t, 5, coverage, &limits)
		if got := len(batch.Effects[0].Rows); got > limits.maxRows {
			t.Fatalf("run %d wrote %d rows above the bound %d", runs+1, got, limits.maxRows)
		}
		for _, raw := range batch.Effects[0].Rows {
			var row gitBlameRow
			_ = json.Unmarshal(raw, &row)
			perPath[row.Path]++
		}
		coverage.record(batch)
		if batch.Result["remaining_paths"] == 0 && batch.Result["inventory_status"] == "complete" {
			runs++
			break
		}
	}
	if runs < 3 {
		t.Fatalf("finished in %d runs: 5 files of 2 rows at 4 rows a run need 3", runs)
	}
	if len(perPath) != 5 {
		t.Fatalf("blamed %d paths, want all 5: %v", len(perPath), perPath)
	}
	for path, rows := range perPath {
		if rows != 2 {
			t.Fatalf("%s was written %d rows, want its 2 lines exactly once", path, rows)
		}
	}
}

// A file above the row bound is refused from its line count alone: none of its
// rows is built or marshalled (r1 P2: a 2 MiB response of empty lines is ~700,000
// rows and 1.2 GB of transient allocation before the old check).
func TestGitLabBlameRefusesAFileAboveTheRowBoundBeforeBuildingAnyRow(t *testing.T) {
	built, materialised := 0, 0
	limits := defaultGitLabBlameLimits()
	limits.maxRows = 3 // each fixture file has 2 lines: the second file would make 4
	limits.rowsBuilt, limits.rowsMaterialised = &built, &materialised
	batch, _ := runGitLabBlameBound(t, 3, staticGitLabBlameCoverage{}, &limits)
	if built != 2 || materialised != 2 || len(batch.Effects[0].Rows) != 2 || batch.Result["remaining_paths"] != 2 {
		t.Fatalf("built=%d materialised=%d rows=%d result=%v: only the first file may ever be built", built, materialised, len(batch.Effects[0].Rows), batch.Result)
	}
}
