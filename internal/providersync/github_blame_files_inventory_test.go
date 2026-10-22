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

// truncatedTree is a repository whose recursive listing GitHub marks truncated:
// the case the banked inventory may replace the directory walk.
func truncatedTree(t *testing.T, filesPerDir int) *gitHubBigTreeDoer {
	return &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: filesPerDir, recursive: "truncated", bigDir: -1, truncatedDir: -1}
}

func freshInventory(doer *gitHubBigTreeDoer, extra ...string) GitHubBlameFilesInventoryResult {
	return GitHubBlameFilesInventoryResult{
		Paths: append(doer.allPaths(), extra...), NewestSyncedAt: filesInventoryNow.Add(-time.Hour),
	}
}

// When the recursive listing cannot answer, the unit takes its file list from
// the banked files inventory and walks no directory: only the root listing that
// proves the list covers the tree is requested.
func TestGitHubBlameUsesTheBankedFilesInventoryInsteadOfWalking(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := truncatedTree(t, 5)
	coverage := &filesInventoryCoverage{result: freshInventory(doer)}
	batch, err := collectWithFilesInventory(t, doer, coverage, "")
	if err != nil {
		t.Fatal(err)
	}
	if doer.counts["recursive"] != 1 || doer.counts["dir"] != 1 {
		t.Fatalf("recursive=%d dir=%d, want 1 and only the root listing", doer.counts["recursive"], doer.counts["dir"])
	}
	if got := len(progressPaths(t, batch)); got != 5 || coverage.calls != 1 {
		t.Fatalf("blamed=%d calls=%d, want 5 and 1", got, coverage.calls)
	}
	for _, want := range []string{"github blame file list source", "source=files_inventory", "paths_listed=5"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs=%q, want %q", logs.String(), want)
		}
	}
}

// A repository whose recursive listing is complete always uses its own tree:
// the banked list is never asked.
func TestGitHubBlameDoesNotConsultTheFilesInventoryWhenTheRecursiveListingIsComplete(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, bigDir: -1, truncatedDir: -1}
	coverage := &filesInventoryCoverage{result: freshInventory(doer)}
	batch, err := collectWithFilesInventory(t, doer, coverage, "")
	if err != nil || len(progressPaths(t, batch)) != 3 || coverage.calls != 0 {
		t.Fatalf("err=%v blamed=%d calls=%d, want the tree used and the reader unused", err, len(progressPaths(t, batch)), coverage.calls)
	}
}

// A partial inventory (the sorted prefix of an unfinished sync) lacks whole
// top-level entries of the tree: it is refused and the tree is walked.
func TestGitHubBlameRefusesAnInventoryThatMissesATopLevelEntry(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := truncatedTree(t, 3)
	coverage := &filesInventoryCoverage{result: GitHubBlameFilesInventoryResult{
		Paths: []string{"README.md"}, NewestSyncedAt: filesInventoryNow.Add(-time.Minute),
	}}
	batch, err := collectWithFilesInventory(t, doer, coverage, "")
	if err != nil {
		t.Fatal(err)
	}
	if doer.counts["dir"] < 2 || len(progressPaths(t, batch)) != 3 {
		t.Fatalf("dir=%d blamed=%d, want the walk used and all 3 tree paths blamed", doer.counts["dir"], len(progressPaths(t, batch)))
	}
	// The fixture's tree holds the top-level directory pkg; the list lacks it.
	for _, want := range []string{
		"fallback_reason=top_level_uncovered", "uncovered_top_level=1", "uncovered_first=[pkg]", "inventory_age_seconds=60",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs=%q, want %q", logs.String(), want)
		}
	}
	if strings.Contains(logs.String(), "source=files_inventory") {
		t.Fatalf("logs=%q, a refused inventory must not be logged as the source", logs.String())
	}
}

// Every case that cannot use the inventory falls back to the walk.
func TestGitHubBlameFallsBackToTheWalkWhenTheFilesInventoryCannotBeUsed(t *testing.T) {
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
			doer := truncatedTree(t, 3)
			batch, err := collectWithFilesInventory(t, doer, testCase.coverage(), "")
			if err != nil {
				t.Fatal(err)
			}
			if doer.counts["dir"] < 2 || len(progressPaths(t, batch)) != 3 {
				t.Fatalf("dir=%d blamed=%d, want the walk used", doer.counts["dir"], len(progressPaths(t, batch)))
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
	doer := truncatedTree(t, 3)
	if _, err := collectWithFilesInventory(t, doer, staticGitHubBlameCoverage{}, ""); err != nil {
		t.Fatal(err)
	}
	if doer.counts["dir"] < 2 {
		t.Fatalf("dir=%d, want the walk", doer.counts["dir"])
	}
}

// A recovery run replays the exact paths of a crashed run: it never trusts a
// list that may differ from the tree.
func TestGitHubBlameRecoveryDoesNotConsultTheFilesInventory(t *testing.T) {
	blameCaptureSlog(t)
	doer := truncatedTree(t, 3)
	coverage := &filesInventoryCoverage{
		staticGitHubBlameCoverage: staticGitHubBlameCoverage{state: GitHubBlameProgressState{
			InFlightOutcomes: map[string]string{bigTreePath(0, 0): gitHubBlameOutcomeRows},
		}},
		result: freshInventory(doer),
	}
	claim := nativeTestClaim("github", "blame")
	if _, err := collectWithFilesInventory(t, doer, coverage, claim.GenerationKey()); err != nil {
		t.Fatal(err)
	}
	if coverage.calls != 0 || doer.counts["dir"] < 2 {
		t.Fatalf("calls=%d dir=%d, want the reader unused and the walk used", coverage.calls, doer.counts["dir"])
	}
}

// A path the inventory still lists but the tree no longer has is asked of
// GitHub once per run, answered with no ranges, recorded 'empty' and counted in
// the unit's result line; it does not fail the unit.
func TestGitHubBlameInventoryPathMissingFromTheTreeIsCountedAndDoesNotFailTheUnit(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := truncatedTree(t, 2)
	doer.emptyPaths = map[string]bool{"deleted/gone.go": true}
	coverage := &filesInventoryCoverage{result: freshInventory(doer, "deleted/gone.go")}
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
