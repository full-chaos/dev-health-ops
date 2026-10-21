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

// walkEntriesTotal builds a tree whose directories list exactly `total`
// entries: a root of 50 directories, 49 of 1,000 blobs and one with the rest.
func walkEntriesTotal(total int) *walkTreeFake {
	fake := &walkTreeFake{trees: map[string][]walkEntry{}}
	root := make([]walkEntry, 0, 50)
	remaining := total - 50
	for dir := 0; dir < 50; dir++ {
		sha := fmt.Sprintf("n%d", dir)
		root = append(root, walkEntry{path: fmt.Sprintf("p%d", dir), kind: "tree", sha: sha})
		count := 1000
		if dir == 49 {
			count = remaining
		}
		remaining -= count
		blobs := make([]walkEntry, 0, count)
		for file := 0; file < count; file++ {
			blobs = append(blobs, walkEntry{path: fmt.Sprintf("f%d.go", file), kind: "blob"})
		}
		fake.trees[sha] = blobs
	}
	fake.trees["root"] = root
	return fake
}

func nearCeilingWarns(logs *bytes.Buffer, limit string) int {
	count := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "passed half of a ceiling") && strings.Contains(line, " limit="+limit+" ") {
			count++
		}
	}
	return count
}

func nearCeilingCounter(t *testing.T, metrics *providerfoundation.Metrics, limit string) int {
	t.Helper()
	var out bytes.Buffer
	if err := metrics.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(out.String(), "\n") {
		prefix := fmt.Sprintf("dev_health_github_tree_walk_near_ceiling_total{limit=%q} ", limit)
		if strings.HasPrefix(line, prefix) {
			var count int
			fmt.Sscanf(strings.TrimPrefix(line, prefix), "%d", &count)
			return count
		}
	}
	return 0
}

// A walk that reaches half of a ceiling says so once, by the ceiling's closed
// name, long before it is stopped: one below half is silent, half and above
// fire. The numbers are the real ceilings (5,000 / 100,000 / 64 / 12 min).
func TestGitHubTreeWalkWarnsOncePerCeilingAtHalf(t *testing.T) {
	type tc struct {
		limit string
		name  string
		fake  func() *walkTreeFake
		clock func() func() time.Time
		want  int
	}
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	after := func(elapsed time.Duration) func() func() time.Time {
		return func() func() time.Time {
			calls := 0
			return func() time.Time {
				calls++
				if calls == 1 {
					return start
				}
				return start.Add(elapsed)
			}
		}
	}
	cases := []tc{
		{GitHubTreeWalkLimitDirs, "dirs 2,499", func() *walkTreeFake { return walkWide(2499) }, nil, 0},
		{GitHubTreeWalkLimitDirs, "dirs 2,500", func() *walkTreeFake { return walkWide(2500) }, nil, 1},
		{GitHubTreeWalkLimitDirs, "dirs 2,501", func() *walkTreeFake { return walkWide(2501) }, nil, 1},
		{GitHubTreeWalkLimitDirs, "dirs 2,600 still once", func() *walkTreeFake { return walkWide(2600) }, nil, 1},
		{GitHubTreeWalkLimitEntries, "entries 49,999", func() *walkTreeFake { return walkEntriesTotal(49_999) }, nil, 0},
		{GitHubTreeWalkLimitEntries, "entries 50,000", func() *walkTreeFake { return walkEntriesTotal(50_000) }, nil, 1},
		{GitHubTreeWalkLimitEntries, "entries 51,000", func() *walkTreeFake { return walkEntriesTotal(51_000) }, nil, 1},
		{GitHubTreeWalkLimitDepth, "depth 31", func() *walkTreeFake { return walkChain(31) }, nil, 0},
		{GitHubTreeWalkLimitDepth, "depth 32", func() *walkTreeFake { return walkChain(32) }, nil, 1},
		{GitHubTreeWalkLimitDepth, "depth 33", func() *walkTreeFake { return walkChain(33) }, nil, 1},
		{GitHubTreeWalkLimitTime, "time 5m59s", func() *walkTreeFake { return walkWide(5) }, after(5*time.Minute + 59*time.Second), 0},
		{GitHubTreeWalkLimitTime, "time 6m00s", func() *walkTreeFake { return walkWide(5) }, after(6 * time.Minute), 1},
		{GitHubTreeWalkLimitTime, "time 6m01s", func() *walkTreeFake { return walkWide(5) }, after(6*time.Minute + time.Second), 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logs := captureWalkLog(t)
			fake := c.fake()
			client := fake.client(t)
			metrics := providerfoundation.NewMetrics()
			client.Metrics = metrics
			limits := defaultGitHubTreeWalkLimits()
			if c.clock != nil {
				limits.now = c.clock()
			}
			if _, _, _, err := gitHubWalkTreeLimited(context.Background(), client, "/repos/acme/api", "root", "", limits); err != nil {
				t.Fatal(err)
			}
			if got := nearCeilingWarns(logs, c.limit); got != c.want {
				t.Fatalf("%s WARN lines=%d want %d: %s", c.limit, got, c.want, logs.String())
			}
			if got := nearCeilingCounter(t, metrics, c.limit); got != c.want {
				t.Fatalf("%s counter=%d want %d", c.limit, got, c.want)
			}
			for _, line := range strings.Split(logs.String(), "\n") {
				if strings.Contains(line, "passed half of a ceiling") &&
					(strings.Contains(line, "acme") || strings.Contains(line, "root") || strings.Contains(line, "p1")) {
					t.Fatalf("WARN carries the repository or a path: %s", line)
				}
			}
		})
	}
}

// A walk can first SEE its elapsed time at a directory entry that is already
// past the stop budget (a slow request between two entries). It must still say
// it passed half of the time ceiling, once, before it stops (r1 finding).
func TestGitHubTreeWalkWarnsAboutHalfTheTimeEvenWhenTheBudgetStopsItAtThatEntry(t *testing.T) {
	logs := captureWalkLog(t)
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	offsets := []time.Duration{0, 0, 12 * time.Minute} // walk start, root entry, first child entry
	calls := 0
	limits := defaultGitHubTreeWalkLimits()
	limits.now = func() time.Time {
		offset := offsets[min(calls, len(offsets)-1)]
		calls++
		return start.Add(offset)
	}
	client := walkWide(3).client(t)
	metrics := providerfoundation.NewMetrics()
	client.Metrics = metrics
	_, _, _, err := gitHubWalkTreeLimited(context.Background(), client, "/repos/acme/api", "root", "", limits)
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitTime {
		t.Fatalf("limit=%+v, want the budget stop", limit)
	}
	if got := nearCeilingWarns(logs, GitHubTreeWalkLimitTime); got != 1 {
		t.Fatalf("time half-way WARN lines=%d want 1: %s", got, logs.String())
	}
	if got := nearCeilingCounter(t, metrics, GitHubTreeWalkLimitTime); got != 1 {
		t.Fatalf("time counter=%d want 1", got)
	}
}

// The half-way comparison is overflow-safe and exact for odd ceilings: a custom
// limit as large as an int still fires at ceil(ceiling/2), never earlier, and a
// ceiling of zero or less never fires.
func TestGitHubTreeWalkHalfComparisonIsOverflowSafeAndExactForOddCeilings(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		ceiling, value int
		want           bool
	}{
		{maxInt, maxInt/2 + 1, true},
		{maxInt, maxInt / 2, false},
		{maxInt, maxInt, true},
		{maxInt - 1, (maxInt - 1) / 2, true},
		{maxInt - 1, (maxInt-1)/2 - 1, false},
		{5, 3, true},
		{5, 2, false},
		{4, 2, true},
		{4, 1, false},
		{1, 1, true},
		{1, 0, false},
		{0, 5, false},
		{-3, 5, false},
	} {
		logs := captureWalkLog(t)
		walker := &gitHubTreeWalker{nearWarn: map[string]bool{}}
		walker.noteNearCeiling(GitHubTreeWalkLimitDirs, tc.value, tc.ceiling)
		if got := nearCeilingWarns(logs, GitHubTreeWalkLimitDirs) == 1; got != tc.want {
			t.Fatalf("ceiling=%d value=%d fired=%v want %v", tc.ceiling, tc.value, got, tc.want)
		}
	}
}

// The job deadline is a way the time ends a walk: half of the time the walk
// may use (the deadline minus its margin, when that is less than the budget)
// is the half-way point, and a walk stopped by the deadline margin has said so.
func TestGitHubTreeWalkWarnsAboutHalfTheTimeBeforeADeadlineMarginStop(t *testing.T) {
	logs := captureWalkLog(t)
	start := time.Now()
	// deadline = start + 5 min; margin 2 min: the walk may use 3 min; half = 90 s.
	// The budget alone (12 min, half 6 min) would never have said anything.
	deadline := start.Add(5 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	offsets := []time.Duration{0, 0, 2 * time.Minute, 3 * time.Minute}
	calls := 0
	limits := defaultGitHubTreeWalkLimits()
	limits.now = func() time.Time {
		offset := offsets[min(calls, len(offsets)-1)]
		calls++
		return start.Add(offset)
	}
	client := walkWide(6).client(t)
	metrics := providerfoundation.NewMetrics()
	client.Metrics = metrics
	_, _, _, err := gitHubWalkTreeLimited(ctx, client, "/repos/acme/api", "root", "", limits)
	if limit := walkLimitOf(t, err); limit.Limit != GitHubTreeWalkLimitTime {
		t.Fatalf("limit=%+v, want the deadline margin stop", limit)
	}
	if got := nearCeilingWarns(logs, GitHubTreeWalkLimitTime); got != 1 {
		t.Fatalf("time half-way WARN lines=%d want 1 (2 min of the 3 usable passed at the third entry): %s", got, logs.String())
	}
	if got := nearCeilingCounter(t, metrics, GitHubTreeWalkLimitTime); got != 1 {
		t.Fatalf("time counter=%d want 1", got)
	}
}

// The time half-way compare is at full precision. A deadline-derived ceiling is
// fractional (usable = deadline - start - margin), and a whole-second compare
// missed the threshold, fired early, or never fired before a deadline stop
// (r3 repro). Also one nanosecond either side of half, for even and odd ceilings.
func TestGitHubTreeWalkTimeHalfCompareKeepsFullPrecision(t *testing.T) {
	margin := defaultGitHubTreeWalkLimits().deadlineMargin
	cases := []struct {
		name    string
		budget  time.Duration // custom budget (0 = default 12 min)
		usable  time.Duration // deadline - start - margin (0 = no deadline)
		elapsed time.Duration
		want    bool
	}{
		{"usable 11m59.8s elapsed 5m59.9s (exactly half)", 0, 11*time.Minute + 59*time.Second + 800*time.Millisecond, 5*time.Minute + 59*time.Second + 900*time.Millisecond, true},
		{"usable 11m58.8s elapsed 5m59.1s (below half 5m59.4s)", 0, 11*time.Minute + 58*time.Second + 800*time.Millisecond, 5*time.Minute + 59*time.Second + 100*time.Millisecond, false},
		{"usable 800ms elapsed 800ms (a stop before whole seconds)", 0, 800 * time.Millisecond, 800 * time.Millisecond, true},
		{"usable 800ms elapsed 399ms", 0, 800 * time.Millisecond, 399 * time.Millisecond, false},
		{"budget 12m, half minus 1ns", 0, 0, 6*time.Minute - 1, false},
		{"budget 12m, half", 0, 0, 6 * time.Minute, true},
		{"budget 12m, half plus 1ns", 0, 0, 6*time.Minute + 1, true},
		{"odd budget 12m+1ns, 6m", 12*time.Minute + 1, 0, 6 * time.Minute, false},
		{"odd budget 12m+1ns, 6m+1ns", 12*time.Minute + 1, 0, 6*time.Minute + 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logs := captureWalkLog(t)
			start := time.Now()
			ctx := context.Background()
			if c.usable > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, start.Add(c.usable+margin))
				defer cancel()
			}
			offsets := []time.Duration{0, 0, c.elapsed}
			calls := 0
			limits := defaultGitHubTreeWalkLimits()
			if c.budget > 0 {
				limits.budget = c.budget
			}
			limits.now = func() time.Time {
				offset := offsets[min(calls, len(offsets)-1)]
				calls++
				return start.Add(offset)
			}
			client := walkWide(2).client(t)
			metrics := providerfoundation.NewMetrics()
			client.Metrics = metrics
			_, _, _, _ = gitHubWalkTreeLimited(ctx, client, "/repos/acme/api", "root", "", limits)
			want := 0
			if c.want {
				want = 1
			}
			if got := nearCeilingWarns(logs, GitHubTreeWalkLimitTime); got != want {
				t.Fatalf("WARN lines=%d want %d: %s", got, want, logs.String())
			}
			if got := nearCeilingCounter(t, metrics, GitHubTreeWalkLimitTime); got != want {
				t.Fatalf("counter=%d want %d", got, want)
			}
		})
	}
}
