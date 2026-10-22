package providersync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type filesInventoryCoverage struct {
	staticGitHubBlameCoverage
	result GitHubBlameFilesInventoryResult
	err    error
	calls  int
}

func (coverage *filesInventoryCoverage) FilesInventory(
	context.Context, Claim, string, time.Duration,
) (GitHubBlameFilesInventoryResult, error) {
	coverage.calls++
	return coverage.result, coverage.err
}

var filesInventoryNow = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func collectWithFilesInventory(
	t *testing.T, doer *gitHubBigTreeDoer, coverage GitHubBlameCoverage, recovery string,
) (CompleteRouteBatch, error) {
	t.Helper()
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	claim := nativeTestClaim("github", "blame")
	limits := defaultGitHubBlameLimits()
	limits.now = func() time.Time { return filesInventoryNow }
	return collectGitHubBlame(
		context.Background(), claim, client, filesInventoryNow, coverage, nil,
		gitHubBlameMaxFiles, false, recovery, limits,
	)
}

// The blame unit takes its file list from the banked files inventory and makes
// no tree request at all: the failing walk is not run.
func TestGitHubBlameUsesTheBankedFilesInventoryWithoutWalking(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 5, bigDir: -1, truncatedDir: -1}
	coverage := &filesInventoryCoverage{result: GitHubBlameFilesInventoryResult{
		Paths: doer.allPaths(), NewestSyncedAt: filesInventoryNow.Add(-time.Hour),
	}}
	batch, err := collectWithFilesInventory(t, doer, coverage, "")
	if err != nil {
		t.Fatal(err)
	}
	if doer.counts["recursive"] != 0 || doer.counts["dir"] != 0 {
		t.Fatalf("tree requests recursive=%d dir=%d, want none", doer.counts["recursive"], doer.counts["dir"])
	}
	if got := len(progressPaths(t, batch)); got != 5 || coverage.calls != 1 {
		t.Fatalf("blamed=%d calls=%d, want 5 and 1", got, coverage.calls)
	}
	if !strings.Contains(logs.String(), "github blame file list source") ||
		!strings.Contains(logs.String(), "source=files_inventory") {
		t.Fatalf("logs=%q, want the source line", logs.String())
	}
}

// Every case that cannot use the inventory falls back to today's walk.
func TestGitHubBlameFallsBackToTheTreeWhenTheFilesInventoryCannotBeUsed(t *testing.T) {
	paths := (&gitHubBigTreeDoer{dirs: 1, filesPerDir: 3}).allPaths()
	cases := []struct {
		name     string
		coverage func() *filesInventoryCoverage
		reason   string
	}{
		{"reader error", func() *filesInventoryCoverage {
			return &filesInventoryCoverage{err: errors.New("clickhouse down")}
		}, "reader_error"},
		{"no rows", func() *filesInventoryCoverage { return &filesInventoryCoverage{} }, "no_inventory"},
		{"older than the age bound", func() *filesInventoryCoverage {
			return &filesInventoryCoverage{result: GitHubBlameFilesInventoryResult{
				Paths: paths, NewestSyncedAt: filesInventoryNow.Add(-gitHubBlameFilesInventoryMaxAge - time.Minute),
			}}
		}, "stale_inventory"},
		{"synced in the future", func() *filesInventoryCoverage {
			return &filesInventoryCoverage{result: GitHubBlameFilesInventoryResult{
				Paths: paths, NewestSyncedAt: filesInventoryNow.Add(time.Hour),
			}}
		}, "stale_inventory"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			logs := blameCaptureSlog(t)
			doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, bigDir: -1, truncatedDir: -1}
			batch, err := collectWithFilesInventory(t, doer, testCase.coverage(), "")
			if err != nil {
				t.Fatal(err)
			}
			if doer.counts["recursive"] != 1 || len(progressPaths(t, batch)) != 3 {
				t.Fatalf("recursive=%d blamed=%d, want the tree path used", doer.counts["recursive"], len(progressPaths(t, batch)))
			}
			if !strings.Contains(logs.String(), "source=tree") || !strings.Contains(logs.String(), "fallback_reason="+testCase.reason) {
				t.Fatalf("logs=%q, want source=tree and reason %s", logs.String(), testCase.reason)
			}
		})
	}
}

// A coverage without the inventory reader keeps today's behaviour.
func TestGitHubBlameWithoutAnInventoryReaderWalksTheTree(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, bigDir: -1, truncatedDir: -1}
	if _, err := collectWithFilesInventory(t, doer, staticGitHubBlameCoverage{}, ""); err != nil {
		t.Fatal(err)
	}
	if doer.counts["recursive"] != 1 {
		t.Fatalf("recursive=%d, want 1", doer.counts["recursive"])
	}
}

// A recovery run replays the exact paths of a crashed run: it never trusts a
// list that may differ from the tree.
func TestGitHubBlameRecoveryDoesNotConsultTheFilesInventory(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, bigDir: -1, truncatedDir: -1}
	coverage := &filesInventoryCoverage{
		staticGitHubBlameCoverage: staticGitHubBlameCoverage{state: GitHubBlameProgressState{
			InFlightOutcomes: map[string]string{bigTreePath(0, 0): gitHubBlameOutcomeRows},
		}},
		result: GitHubBlameFilesInventoryResult{
			Paths: []string{"other.go"}, NewestSyncedAt: filesInventoryNow.Add(-time.Hour),
		},
	}
	claim := nativeTestClaim("github", "blame")
	if _, err := collectWithFilesInventory(t, doer, coverage, claim.GenerationKey()); err != nil {
		t.Fatal(err)
	}
	if coverage.calls != 0 || doer.counts["recursive"] != 1 {
		t.Fatalf("calls=%d recursive=%d, want the reader unused and the tree read", coverage.calls, doer.counts["recursive"])
	}
}

// A path the inventory still lists but the tree no longer has is asked of
// GitHub once per run, answered with no ranges, recorded 'empty' and counted in
// the unit's result line; it does not fail the unit.
func TestGitHubBlameInventoryPathMissingFromTheTreeIsCountedAndDoesNotFailTheUnit(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{
		t: t, dirs: 1, filesPerDir: 2, bigDir: -1, truncatedDir: -1,
		emptyPaths: map[string]bool{"deleted/gone.go": true},
	}
	coverage := &filesInventoryCoverage{result: GitHubBlameFilesInventoryResult{
		Paths: append(doer.allPaths(), "deleted/gone.go"), NewestSyncedAt: filesInventoryNow.Add(-time.Hour),
	}}
	batch, err := collectWithFilesInventory(t, doer, coverage, "")
	if err != nil || len(progressPaths(t, batch)) != 2 || len(batch.Effects[0].Rows) != 3 {
		t.Fatalf("err=%v with rows=%d progress=%d, want 2 with rows and 3 progress rows (one empty)", err, len(progressPaths(t, batch)), len(batch.Effects[0].Rows))
	}
	if !strings.Contains(logs.String(), "paths_empty=1") || !strings.Contains(logs.String(), "paths_listed=3") {
		t.Fatalf("logs=%q, want paths_empty=1 paths_listed=3", logs.String())
	}
	if doer.counts["blame"] != 3 {
		t.Fatalf("blame requests=%d, want one per listed path", doer.counts["blame"])
	}
}
