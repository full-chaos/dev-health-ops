package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// gitHubBigTreeDoer serves a repository whose tree is built from real
// directory listings: one root with `dirs` sub-directories of `filesPerDir`
// blobs. Its recursive listing is the concatenation of every directory, so it
// crosses the shared response cap exactly when a real large repository does.
type gitHubBigTreeDoer struct {
	t            *testing.T
	dirs         int
	filesPerDir  int
	linesPerFile int
	linesByPath  map[string]int
	// recursive: "full" (normal, not truncated), "truncated" (GitHub flag,
	// partial list), "" (derived: over the cap when large enough).
	recursive string
	// bigDir names a directory index whose listing is padded past the cap.
	bigDir int
	// truncatedDir names a directory index that GitHub marks truncated.
	truncatedDir int

	mu     sync.Mutex
	counts map[string]int
	blamed []string
}

func (doer *gitHubBigTreeDoer) count(kind string) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if doer.counts == nil {
		doer.counts = map[string]int{}
	}
	doer.counts[kind]++
}

func (doer *gitHubBigTreeDoer) total() int {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	sum := 0
	for _, value := range doer.counts {
		sum += value
	}
	return sum
}

func bigTreePath(dir, file int) string {
	return fmt.Sprintf("pkg/d%03d/file%04d.go", dir, file)
}

func (doer *gitHubBigTreeDoer) allEntries() []string {
	entries := make([]string, 0, doer.dirs*doer.filesPerDir)
	for dir := 0; dir < doer.dirs; dir++ {
		for file := 0; file < doer.filesPerDir; file++ {
			entries = append(entries, fmt.Sprintf(
				`{"path":%q,"type":"blob","sha":"%040x","size":120}`,
				bigTreePath(dir, file), dir*100000+file))
		}
	}
	return entries
}

func (doer *gitHubBigTreeDoer) allPaths() []string {
	paths := make([]string, 0, doer.dirs*doer.filesPerDir)
	for dir := 0; dir < doer.dirs; dir++ {
		for file := 0; file < doer.filesPerDir; file++ {
			paths = append(paths, bigTreePath(dir, file))
		}
	}
	return paths
}

func (doer *gitHubBigTreeDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	body := `{"full_name":"acme/api","default_branch":"main"}`
	recursive := request.URL.Query().Get("recursive") == "true"
	switch {
	case request.URL.Path == "/repos/acme/api":
		doer.count("repo")
	case request.URL.Path == "/repos/acme/api/commits":
		doer.count("commits")
		body = `[{"sha":"tree-sha"}]`
	case request.URL.Path == "/repos/acme/api/git/trees/tree-sha" && recursive:
		doer.count("recursive")
		switch doer.recursive {
		case "truncated":
			body = `{"truncated":true,"tree":[{"path":"README.md","type":"blob","size":5}]}`
		default:
			body = `{"truncated":false,"tree":[` + strings.Join(doer.allEntries(), ",") + `]}`
		}
	case request.URL.Path == "/repos/acme/api/git/trees/tree-sha":
		doer.count("dir")
		entries := []string{`{"path":"pkg","type":"tree","sha":"pkg-sha"}`}
		body = `{"truncated":false,"tree":[` + strings.Join(entries, ",") + `]}`
	case request.URL.Path == "/repos/acme/api/git/trees/pkg-sha":
		doer.count("dir")
		entries := make([]string, 0, doer.dirs)
		for dir := 0; dir < doer.dirs; dir++ {
			entries = append(entries, fmt.Sprintf(`{"path":"d%03d","type":"tree","sha":"dir-%03d"}`, dir, dir))
		}
		body = `{"truncated":false,"tree":[` + strings.Join(entries, ",") + `]}`
	case strings.HasPrefix(request.URL.Path, "/repos/acme/api/git/trees/dir-"):
		doer.count("dir")
		var dir int
		if _, err := fmt.Sscanf(request.URL.Path, "/repos/acme/api/git/trees/dir-%d", &dir); err != nil {
			doer.t.Fatal(err)
		}
		entries := make([]string, 0, doer.filesPerDir)
		for file := 0; file < doer.filesPerDir; file++ {
			entries = append(entries, fmt.Sprintf(
				`{"path":"file%04d.go","type":"blob","sha":"%040x","size":120}`, file, dir*100000+file))
		}
		switch {
		case dir == doer.bigDir && doer.bigDir >= 0 && doer.recursive == "bigdir":
			body = `{"truncated":false,"padding":"` + strings.Repeat("x", nativeMaxObjectBytes) + `","tree":[]}`
		case dir == doer.truncatedDir && doer.recursive == "truncateddir":
			body = `{"truncated":true,"tree":[` + strings.Join(entries[:1], ",") + `]}`
		default:
			body = `{"truncated":false,"tree":[` + strings.Join(entries, ",") + `]}`
		}
	case request.URL.Path == "/graphql":
		doer.count("blame")
		var requestBody struct {
			Variables map[string]string `json:"variables"`
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			doer.t.Fatal(err)
		}
		doer.mu.Lock()
		doer.blamed = append(doer.blamed, requestBody.Variables["path"])
		doer.mu.Unlock()
		lines := doer.linesPerFile
		if override, ok := doer.linesByPath[requestBody.Variables["path"]]; ok {
			lines = override
		}
		if lines == 0 {
			lines = 2
		}
		body = fmt.Sprintf(`{"data":{"repository":{"object":{"blame":{"ranges":[{"startingLine":1,"endingLine":%d,"commit":{"oid":"abc123","author":{"name":"Ada","email":"ada@example.com"}}}]}}}}}`, lines)
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func blameCaptureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func runBigTreeBlame(
	t *testing.T,
	doer *gitHubBigTreeDoer,
	handler GitHubBlameRouteHandler,
	done []string,
) (CompleteRouteBatch, error) {
	t.Helper()
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	handler.Coverage = staticGitHubBlameCoverage{state: GitHubBlameProgressState{BlamedPaths: done}}
	return handler.Collect(
		context.Background(), nativeTestClaim("github", "blame"),
		providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
}

func progressPaths(t *testing.T, batch CompleteRouteBatch) []string {
	t.Helper()
	paths := []string{}
	for _, raw := range batch.Effects[0].Rows {
		var row gitHubBlamePathProgressRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		if row.Outcome == gitHubBlameOutcomeRows {
			paths = append(paths, row.Path)
		}
	}
	return paths
}

// A file list above the response cap must still finish: the walk gets every
// path, each run blames the next 500, and over the runs no path is lost or done
// twice.
func TestGitHubBlameRouteWalksAFileListAboveTheResponseCapAndFinishesOverRuns(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 60, filesPerDir: 500, bigDir: -1, truncatedDir: -1}
	if size := len(strings.Join(doer.allEntries(), ",")); size <= nativeMaxObjectBytes {
		t.Fatalf("fixture recursive listing is %d bytes, not above the %d cap", size, nativeMaxObjectBytes)
	}
	want := doer.allPaths()
	done := []string{}
	for run := 0; run*gitHubBlameMaxFiles < len(want); run++ {
		batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, done)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		got := progressPaths(t, batch)
		if len(got) != gitHubBlameMaxFiles {
			t.Fatalf("run %d blamed %d paths, want %d", run, len(got), gitHubBlameMaxFiles)
		}
		for _, path := range got {
			if slices.Contains(done, path) {
				t.Fatalf("run %d blamed %s twice", run, path)
			}
		}
		if len(batch.Effects[1].Rows) != gitHubBlameMaxFiles*2 {
			t.Fatalf("run %d wrote %d blame rows", run, len(batch.Effects[1].Rows))
		}
		wantRemaining := len(want) - (run+1)*gitHubBlameMaxFiles
		if batch.Result["remaining_paths"] != wantRemaining {
			t.Fatalf("run %d remaining=%v want %d", run, batch.Result["remaining_paths"], wantRemaining)
		}
		if run == 0 {
			// 1 repo + 1 commits + 1 recursive + (1 root + 1 pkg + 60 dirs) walk + 500 blame.
			if want := 1 + 1 + 1 + 62 + 500; batch.Evidence.Requests != want {
				t.Fatalf("requests=%d want %d", batch.Evidence.Requests, want)
			}
		}
		done = append(done, got...)
	}
	slices.Sort(done)
	if !slices.Equal(done, want) {
		t.Fatalf("blamed %d distinct paths over the runs, want all %d", len(done), len(want))
	}
	if !strings.Contains(logs.String(), "class=recursive_over_cap") ||
		!strings.Contains(logs.String(), "walk_requests=62") {
		t.Fatalf("logs=%q, want one WARN with the class and the walk request count", logs.String())
	}
	if strings.Contains(logs.String(), "acme") || strings.Contains(logs.String(), "pkg/d") {
		t.Fatalf("logs=%q carry a repository name or a path", logs.String())
	}
}

func TestGitHubBlameRouteWalksATreeGitHubMarksTruncated(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 3, filesPerDir: 4, recursive: "truncated", bigDir: -1, truncatedDir: -1}
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := progressPaths(t, batch)
	slices.Sort(got)
	if !slices.Equal(got, doer.allPaths()) {
		t.Fatalf("blamed=%v want the complete walked list, not the truncated partial one", got)
	}
	if batch.Result["inventory_status"] != "complete" || batch.Result["remaining_paths"] != 0 {
		t.Fatalf("result=%v", batch.Result)
	}
	if !strings.Contains(logs.String(), "class=recursive_truncated") {
		t.Fatalf("logs=%q", logs.String())
	}
}

func TestGitHubBlameRouteEndsWithAClassWhenOneDirectoryIsAboveTheCap(t *testing.T) {
	for _, variant := range []struct{ mode, class string }{
		{"bigdir", "directory_over_cap"},
		{"truncateddir", "directory_truncated"},
	} {
		t.Run(variant.class, func(t *testing.T) {
			logs := blameCaptureSlog(t)
			doer := &gitHubBigTreeDoer{
				t: t, dirs: 60, filesPerDir: 500, recursive: variant.mode, bigDir: 7, truncatedDir: 7,
			}
			// Make the recursive listing fail the same way the real one does.
			doer.recursive = variant.mode
			batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, nil)
			if !errors.Is(err, ErrGitHubBlameFileListTooLarge) || !errors.Is(err, ErrGitHubBlameTraversalFailed) {
				t.Fatalf("err=%v, want a file-list-too-large class under the traversal class", err)
			}
			if len(batch.Effects) != 0 || doer.counts["blame"] != 0 {
				t.Fatalf("effects=%d blame requests=%d, want none", len(batch.Effects), doer.counts["blame"])
			}
			// One walk, then stop: root + pkg + the directories up to the bad one.
			if doer.counts["dir"] != 2+8 {
				t.Fatalf("directory requests=%d, want the walk to stop at the bad directory", doer.counts["dir"])
			}
			if strings.Count(logs.String(), "class="+variant.class) != 1 {
				t.Fatalf("logs=%q, want exactly one WARN with class=%s", logs.String(), variant.class)
			}
		})
	}
}

func TestGitHubBlameRouteTakesNoFallbackForANormalTree(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 2, filesPerDir: 3, bigDir: -1, truncatedDir: -1}
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if doer.counts["dir"] != 0 || doer.counts["recursive"] != 1 || doer.counts["blame"] != 6 ||
		batch.Evidence.Requests != 1+1+1+6 {
		t.Fatalf("counts=%v requests=%d, want repo+commits+recursive+6 blame and no walk", doer.counts, batch.Evidence.Requests)
	}
	if strings.Contains(logs.String(), "fell back") {
		t.Fatalf("logs=%q, want no fallback WARN", logs.String())
	}
}

// The write step refuses a unit above 100,000 rows. A 500-file unit of
// 300-line files is 150,000 rows: the unit must stop below the bound and the
// rest continues next run.
func TestGitHubBlameRouteStopsBelowTheWriteBoundAndContinues(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 2, filesPerDir: 500, linesPerFile: 300, bigDir: -1, truncatedDir: -1}
	first, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, nil)
	if err != nil {
		t.Fatalf("a unit above the bound must stop early, not fail: %v", err)
	}
	rows := len(first.Effects[1].Rows)
	if rows > maxEffectRows || rows != 333*300 {
		t.Fatalf("rows=%d, want 333 files x 300 lines = %d, at most %d", rows, 333*300, maxEffectRows)
	}
	if first.Effects[1].PayloadBytes > maxEffectPayloadBytes {
		t.Fatalf("payload=%d above %d", first.Effects[1].PayloadBytes, maxEffectPayloadBytes)
	}
	if first.Result["remaining_paths"] != 1000-333 || first.Result["inventory_status"] != "partial" {
		t.Fatalf("result=%v", first.Result)
	}
	if len(progressPaths(t, first)) != 333 {
		t.Fatalf("progress rows=%d, want one per blamed file only", len(progressPaths(t, first)))
	}
	if !strings.Contains(logs.String(), "class=rows") {
		t.Fatalf("logs=%q", logs.String())
	}
	second, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, progressPaths(t, first))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range progressPaths(t, second) {
		if slices.Contains(progressPaths(t, first), path) {
			t.Fatalf("%s blamed twice", path)
		}
	}
}

func TestGitHubBlameRouteKeepsAFileAboveTheWriteBoundRetryableAndMovesOn(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, linesPerFile: 7, bigDir: -1, truncatedDir: -1}
	limits := defaultGitHubBlameLimits()
	limits.maxRows = 5
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects[1].Rows) != 0 || batch.Result["retryable_path_failures"] != 3 ||
		batch.Result["remaining_paths"] != 3 {
		t.Fatalf("rows=%d result=%v, want every oversized file retryable and no row", len(batch.Effects[1].Rows), batch.Result)
	}
}

func TestGitHubBlameRouteStopsStartingFilesWhenTheTimeBudgetIsSpent(t *testing.T) {
	logs := blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 10, bigDir: -1, truncatedDir: -1}
	clock := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	limits := defaultGitHubBlameLimits()
	limits.softBudget = 3 * time.Minute
	limits.now = func() time.Time { clock = clock.Add(time.Minute); return clock }
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil)
	if err != nil {
		t.Fatal(err)
	}
	blamed := len(progressPaths(t, batch))
	if blamed < 1 || blamed >= 10 || batch.Result["remaining_paths"] != 10-blamed {
		t.Fatalf("blamed=%d result=%v, want a partial unit that kept the rest", blamed, batch.Result)
	}
	if !strings.Contains(logs.String(), "class=time") {
		t.Fatalf("logs=%q", logs.String())
	}
	// A budget already spent still blames one file: a run always makes progress.
	limits.softBudget = 0
	one, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil)
	if err != nil || len(progressPaths(t, one)) != 1 {
		t.Fatalf("err=%v blamed=%d, want exactly one file", err, len(progressPaths(t, one)))
	}
}

func TestGitHubBlameRouteStopsBeforeTheJobDeadline(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 10, bigDir: -1, truncatedDir: -1}
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	batch, err := (GitHubBlameRouteHandler{
		Coverage: staticGitHubBlameCoverage{},
	}).Collect(ctx, nativeTestClaim("github", "blame"), providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(progressPaths(t, batch)) != 1 || batch.Result["remaining_paths"] != 9 {
		t.Fatalf("result=%v, want one file then a stop inside the deadline margin", batch.Result)
	}
}

func TestGitHubBlameRouteStopsBelowThePayloadBound(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 6, linesPerFile: 9, bigDir: -1, truncatedDir: -1}
	probe, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	perFile := probe.Effects[1].PayloadBytes / 6
	limits := defaultGitHubBlameLimits()
	limits.maxPayloadBytes = perFile * 2
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(progressPaths(t, batch)); got != 2 || batch.Result["remaining_paths"] != 4 {
		t.Fatalf("blamed=%d result=%v, want the payload bound to stop the unit", got, batch.Result)
	}
	if batch.Effects[1].PayloadBytes > limits.maxPayloadBytes {
		t.Fatalf("payload=%d above bound %d", batch.Effects[1].PayloadBytes, limits.maxPayloadBytes)
	}
}

func TestGitHubBlameRouteBoundsAreInclusive(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, linesPerFile: 2, bigDir: -1, truncatedDir: -1}
	limits := defaultGitHubBlameLimits()
	limits.maxRows = 4
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil)
	if err != nil || len(batch.Effects[1].Rows) != 4 || len(progressPaths(t, batch)) != 2 {
		t.Fatalf("err=%v rows=%d, want exactly maxRows rows accepted", err, len(batch.Effects[1].Rows))
	}

	limits.maxRows = 5
	batch, err = runBigTreeBlame(t, doer, GitHubBlameRouteHandler{limits: &limits}, nil)
	if err != nil || len(batch.Effects[1].Rows) != 4 || len(progressPaths(t, batch)) != 2 {
		t.Fatalf("err=%v rows=%d, want 2 files: a third would make 6 rows above 5", err, len(batch.Effects[1].Rows))
	}

	fixed := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	limits = defaultGitHubBlameLimits()
	limits.now = func() time.Time { return fixed }
	limits.softBudget = 0
	if got := gitHubBlameBudgetStop(context.Background(), limits, fixed); got != "time" {
		t.Fatalf("elapsed == soft budget gave %q, want time", got)
	}
	limits.softBudget = time.Hour
	ctx, cancel := context.WithDeadline(context.Background(), fixed.Add(limits.deadlineMargin))
	defer cancel()
	if got := gitHubBlameBudgetStop(ctx, limits, fixed); got != "deadline" {
		t.Fatalf("margin == time left gave %q, want deadline", got)
	}
	ctx2, cancel2 := context.WithDeadline(context.Background(), fixed.Add(limits.deadlineMargin+time.Nanosecond))
	defer cancel2()
	if got := gitHubBlameBudgetStop(ctx2, limits, fixed); got != "" {
		t.Fatalf("time left above margin gave %q, want continue", got)
	}
}

// A recovery run replays exactly the paths a crashed run attempted; it must
// not cut that set by the new per-run bounds.
func TestGitHubBlameRecoveryReplaysEveryInFlightPathWithoutTheRunBounds(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{t: t, dirs: 1, filesPerDir: 3, linesPerFile: 2, bigDir: -1, truncatedDir: -1}
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	claim := nativeTestClaim("github", "blame")
	limits := defaultGitHubBlameLimits()
	limits.maxRows = 1
	limits.softBudget = 0
	inflight := map[string]string{}
	for _, path := range doer.allPaths() {
		inflight[path] = gitHubBlameOutcomeRows
	}
	batch, err := collectGitHubBlame(
		context.Background(), claim, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
		staticGitHubBlameCoverage{state: GitHubBlameProgressState{InFlightOutcomes: inflight}},
		gitHubBlameMaxFiles, false, claim.GenerationKey(), limits,
	)
	if err != nil || len(progressPaths(t, batch)) != 3 || len(batch.Effects[1].Rows) != 6 {
		t.Fatalf("err=%v progress=%d rows=%d, want all 3 in-flight paths replayed", err, len(progressPaths(t, batch)), len(batch.Effects[1].Rows))
	}
}

// A file whose single range is far above the write bound is refused from the
// range bounds, without a row per line ever being allocated.
func TestGitHubBlameRouteRefusesAHugeRangeWithoutMaterialisingIt(t *testing.T) {
	blameCaptureSlog(t)
	doer := &gitHubBigTreeDoer{
		t: t, dirs: 1, filesPerDir: 3, linesPerFile: 2, bigDir: -1, truncatedDir: -1,
		linesByPath: map[string]int{bigTreePath(0, 1): 2_000_000},
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	batch, err := runBigTreeBlame(t, doer, GitHubBlameRouteHandler{}, nil)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 100<<20 {
		t.Fatalf("allocated %d bytes for a refused file, want no per-line rows", allocated)
	}
	got := progressPaths(t, batch)
	if !slices.Equal(got, []string{bigTreePath(0, 0)}) || batch.Result["remaining_paths"] != 2 ||
		len(batch.Effects[1].Rows) != 2 {
		t.Fatalf("blamed=%v result=%v, want the unit to stop before the huge file", got, batch.Result)
	}
}
