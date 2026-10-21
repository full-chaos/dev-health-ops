package providersync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type walkEntry struct{ path, kind, sha string }

// walkTreeFake serves non-recursive tree reads for a map of sha -> entries.
type walkTreeFake struct {
	trees map[string][]walkEntry
	gets  int
}

func (fake *walkTreeFake) Do(request *http.Request) (*http.Response, error) {
	const prefix = "/repos/acme/api/git/trees/"
	sha := strings.TrimPrefix(request.URL.Path, prefix)
	entries, ok := fake.trees[sha]
	status, body := http.StatusOK, ""
	if !ok {
		status, body = http.StatusNotFound, `{"message":"Not Found"}`
	} else {
		fake.gets++
		var out bytes.Buffer
		out.WriteString(`{"tree":[`)
		for index, entry := range entries {
			if index > 0 {
				out.WriteByte(',')
			}
			if entry.kind == "tree" {
				fmt.Fprintf(&out, `{"path":%q,"type":"tree","sha":%q}`, entry.path, entry.sha)
			} else {
				fmt.Fprintf(&out, `{"path":%q,"type":"blob","size":10}`, entry.path)
			}
		}
		out.WriteString(`]}`)
		body = out.String()
	}
	return &http.Response{
		StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func (fake *walkTreeFake) client(t *testing.T) *providerfoundation.HTTPClient {
	return gitHubRepositoryClient(t, fakehttp.Client(fake), "https://api.github.com")
}

func (fake *walkTreeFake) walk(t *testing.T, limits gitHubTreeWalkLimits) ([]string, gitHubTreeWalkStats, error) {
	t.Helper()
	paths, _, stats, err := gitHubWalkTreeLimited(context.Background(), fake.client(t), "/repos/acme/api", "root", "", limits)
	return paths, stats, err
}

func walkLimitOf(t *testing.T, err error) *GitHubTreeWalkLimitError {
	t.Helper()
	var limit *GitHubTreeWalkLimitError
	if !errors.As(err, &limit) || !errors.Is(err, ErrGitHubTreeWalkLimit) {
		t.Fatalf("error=%v, want a tree walk limit", err)
	}
	return limit
}

// chain builds root -> d1 -> ... -> d<depth> with one blob in the deepest tree.
func walkChain(depth int) *walkTreeFake {
	fake := &walkTreeFake{trees: map[string][]walkEntry{}}
	name := func(level int) string {
		if level == 0 {
			return "root"
		}
		return fmt.Sprintf("d%d", level)
	}
	for level := 0; level < depth; level++ {
		fake.trees[name(level)] = []walkEntry{{path: "d", kind: "tree", sha: name(level + 1)}}
	}
	fake.trees[name(depth)] = []walkEntry{{path: "leaf.go", kind: "blob"}}
	return fake
}

// wide builds a root with `dirs-1` single-blob subtrees: dirs tree reads.
func walkWide(dirs int) *walkTreeFake {
	fake := &walkTreeFake{trees: map[string][]walkEntry{}}
	root := make([]walkEntry, 0, dirs-1)
	for index := 0; index < dirs-1; index++ {
		sha := fmt.Sprintf("c%d", index)
		root = append(root, walkEntry{path: fmt.Sprintf("p%d", index), kind: "tree", sha: sha})
		fake.trees[sha] = []walkEntry{{path: "f.go", kind: "blob"}}
	}
	fake.trees["root"] = root
	return fake
}

func TestGitHubTreeWalkStopsAtACycleButWalksTheSameDirectoryTwiceUnderTwoParents(t *testing.T) {
	cyclic := &walkTreeFake{trees: map[string][]walkEntry{
		"root": {{path: "loop", kind: "tree", sha: "a"}},
		"a":    {{path: "again", kind: "tree", sha: "root"}},
	}}
	if _, _, err := cyclic.walk(t, defaultGitHubTreeWalkLimits()); walkLimitOf(t, err).Limit != GitHubTreeWalkLimitCycle {
		t.Fatalf("err=%v", err)
	}
	if cyclic.gets > 3 {
		t.Fatalf("a cycle was walked %d times", cyclic.gets)
	}
	siblings := &walkTreeFake{trees: map[string][]walkEntry{
		"root": {{path: "x", kind: "tree", sha: "same"}, {path: "y", kind: "tree", sha: "same"}},
		"same": {{path: "f.go", kind: "blob"}},
	}}
	paths, stats, err := siblings.walk(t, defaultGitHubTreeWalkLimits())
	if err != nil || len(paths) != 2 || stats.dirs != 3 {
		t.Fatalf("identical directories under two parents are legal: paths=%v dirs=%d err=%v", paths, stats.dirs, err)
	}
}

func TestGitHubTreeWalkDepthCeilingIsInclusive(t *testing.T) {
	if paths, stats, err := walkChain(64).walk(t, defaultGitHubTreeWalkLimits()); err != nil || len(paths) != 1 || stats.depth != 64 {
		t.Fatalf("depth %d must pass: paths=%v stats=%+v err=%v", 64, paths, stats, err)
	}
	_, _, err := walkChain(64+1).walk(t, defaultGitHubTreeWalkLimits())
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitDepth || limit.Value != 64 {
		t.Fatalf("limit=%+v", limit)
	}
}

func TestGitHubTreeWalkDirectoryCeilingIsInclusive(t *testing.T) {
	fake := walkWide(5_000)
	if paths, stats, err := fake.walk(t, defaultGitHubTreeWalkLimits()); err != nil || stats.dirs != 5_000 || len(paths) != 5_000-1 {
		t.Fatalf("%d directories must pass: dirs=%d paths=%d err=%v", 5_000, stats.dirs, len(paths), err)
	}
	over := walkWide(5_000 + 1)
	_, stats, err := over.walk(t, defaultGitHubTreeWalkLimits())
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitDirs || stats.dirs != 5_000 || over.gets != 5_000 {
		t.Fatalf("limit=%+v dirs=%d gets=%d: the walk must stop BEFORE the request over the ceiling", limit, stats.dirs, over.gets)
	}
}

// entriesFake: a root of 100 directories; each lists `perDir` blobs, the last one `last`.
func walkEntries(last int) *walkTreeFake {
	fake := &walkTreeFake{trees: map[string][]walkEntry{}}
	root := make([]walkEntry, 0, 100)
	for dir := 0; dir < 100; dir++ {
		sha := fmt.Sprintf("e%d", dir)
		root = append(root, walkEntry{path: fmt.Sprintf("p%d", dir), kind: "tree", sha: sha})
		count := 1000
		if dir == 99 {
			count = last
		}
		blobs := make([]walkEntry, 0, count)
		for file := 0; file < count; file++ {
			blobs = append(blobs, walkEntry{path: fmt.Sprintf("f%d.go", file), kind: "blob"})
		}
		fake.trees[sha] = blobs
	}
	fake.trees["root"] = root
	return fake
}

func TestGitHubTreeWalkEntryCeilingIsInclusive(t *testing.T) {
	// 100 root entries + 99 x 1,000 + 900 = exactly 100,000.
	if _, stats, err := walkEntries(900).walk(t, defaultGitHubTreeWalkLimits()); err != nil || stats.entries != 100_000 {
		t.Fatalf("%d entries must pass: stats=%+v err=%v", 100_000, stats, err)
	}
	_, _, err := walkEntries(901).walk(t, defaultGitHubTreeWalkLimits())
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitEntries || limit.Value != 100_000 {
		t.Fatalf("limit=%+v", limit)
	}
}

func TestGitHubTreeWalkStopsBeforeTheNextRequestWhenItsTimeBudgetIsSpent(t *testing.T) {
	fake := walkWide(10)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limits := defaultGitHubTreeWalkLimits()
	limits.now = func() time.Time {
		current := now
		now = now.Add(5 * time.Minute)
		return current
	}
	_, _, err := fake.walk(t, limits)
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitTime {
		t.Fatalf("limit=%+v", limit)
	}
	if fake.gets > 4 {
		t.Fatalf("%d requests after the budget was spent", fake.gets)
	}
	// The job deadline too close to use the result stops it the same way.
	deadlined, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	_, _, _, err = gitHubWalkTreeLimited(deadlined, walkWide(3).client(t), "/repos/acme/api", "root", "", defaultGitHubTreeWalkLimits())
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitTime {
		t.Fatalf("deadline limit=%+v", limit)
	}
}

// The measured trees must pass every ceiling: the real prod blame walk (1,047
// directories, 11,205 paths) and the dev-health-ops tree (1,040 / 12,033).
func TestGitHubTreeWalkPassesTheMeasuredOrganisationTrees(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dirs, blobs int
	}{
		{"prod blame walk", 1047, 11205},
		{"dev-health-ops", 1040, 10993},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &walkTreeFake{trees: map[string][]walkEntry{}}
			root := make([]walkEntry, 0, tc.dirs-1)
			for dir := 0; dir < tc.dirs-1; dir++ {
				sha := fmt.Sprintf("m%d", dir)
				root = append(root, walkEntry{path: fmt.Sprintf("p%d", dir), kind: "tree", sha: sha})
				count := tc.blobs / (tc.dirs - 1)
				if dir < tc.blobs%(tc.dirs-1) {
					count++
				}
				blobs := make([]walkEntry, 0, count)
				for file := 0; file < count; file++ {
					blobs = append(blobs, walkEntry{path: fmt.Sprintf("f%d.go", file), kind: "blob"})
				}
				fake.trees[sha] = blobs
			}
			fake.trees["root"] = root
			paths, stats, err := fake.walk(t, defaultGitHubTreeWalkLimits())
			if err != nil || len(paths) != tc.blobs || stats.dirs != tc.dirs {
				t.Fatalf("paths=%d stats=%+v err=%v", len(paths), stats, err)
			}
			if gitHubTreeWalkMaxDirs < 2*stats.dirs || gitHubTreeWalkMaxEntries < 2*stats.entries {
				t.Fatalf("a ceiling is below twice the measured tree: %+v", stats)
			}
		})
	}
	if gitHubTreeWalkMaxDepth < 2*12 {
		t.Fatal("the depth ceiling is below twice the deepest org repository (12)")
	}
}

func captureWalkLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

// The blame inventory and the files inventory both walk with the ceilings, end
// with the typed limit, bank nothing, and log one WARN without a repository
// name or a path.
func TestGitHubBlameInventoryEndsOnACycleWithTheTypedLimitAndOneSafeWarn(t *testing.T) {
	logs := captureWalkLog(t)
	cyclic := &walkTreeFake{trees: map[string][]walkEntry{
		"root": {{path: "secret-dir", kind: "tree", sha: "a"}},
		"a":    {{path: "again", kind: "tree", sha: "root"}},
	}}
	// The recursive listing is above the cap, so the walk runs: model it with a
	// truncated recursive response in front of the cyclic tree.
	doer := fakehttp.Client(doerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.RawQuery == "recursive=true" {
			return &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(`{"tree":[],"truncated":true}`)), Request: request,
			}, nil
		}
		return cyclic.Do(request)
	}))
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	requests := 0
	_, err := gitHubBlameInventory(context.Background(), client, "/repos/acme/api", "root", &requests)
	if walkLimitOf(t, err).Limit != GitHubTreeWalkLimitCycle || !errors.Is(err, ErrGitHubBlameTraversalFailed) {
		t.Fatalf("err=%v", err)
	}
	warns := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "tree walk stopped at ceiling") {
			warns++
			for _, want := range []string{"level=WARN", "limit=cycle", "dirs=", "depth=", "entries="} {
				if !strings.Contains(line, want) {
					t.Fatalf("WARN lacks %q: %s", want, line)
				}
			}
			if strings.Contains(line, "secret-dir") || strings.Contains(line, "acme") {
				t.Fatalf("WARN carries a path or the repository: %s", line)
			}
		}
	}
	if warns != 1 {
		t.Fatalf("WARN lines=%d, want 1: %s", warns, logs.String())
	}
}

func TestGitHubFilesInventoryEndsOnACeilingWithTheTypedLimit(t *testing.T) {
	logs := captureWalkLog(t)
	deep := walkChain(gitHubTreeWalkMaxDepth + 1)
	doer := fakehttp.Client(doerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.RawQuery == "recursive=true" {
			return &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(`{"tree":[],"truncated":true}`)), Request: request,
			}, nil
		}
		return deep.Do(request)
	}))
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	_, _, _, err := gitHubFilesInventory(context.Background(), client, "/repos/acme/api", "root", "acme/api")
	if walkLimitOf(t, err).Limit != GitHubTreeWalkLimitDepth || !errors.Is(err, ErrGitHubFilesTraversalFailed) {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(logs.String(), "limit=depth") {
		t.Fatalf("no ceiling WARN: %s", logs.String())
	}
}

// A single file above the write bound is kept retryable (never blocks the unit)
// and now says so: one WARN per such file with counts, no path.
func TestGitHubBlameWarnsForASingleFileAboveTheWriteBound(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, linesPerFile: 7, bigDir: -1, truncatedDir: -1}
	limits := defaultGitHubBlameLimits()
	limits.maxRows = 5
	if _, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil); err != nil {
		t.Fatal(err)
	}
	warns := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "above the write bound kept retryable") {
			warns++
			for _, want := range []string{"level=WARN", "observed_lines=7", "max_rows=5"} {
				if !strings.Contains(line, want) {
					t.Fatalf("WARN lacks %q: %s", want, line)
				}
			}
			if strings.Contains(line, ".go") || strings.Contains(line, "acme") {
				t.Fatalf("WARN carries a path or the repository: %s", line)
			}
		}
	}
	if warns != 3 {
		t.Fatalf("WARN lines=%d, want one per file (3): %s", warns, logs.String())
	}
}
