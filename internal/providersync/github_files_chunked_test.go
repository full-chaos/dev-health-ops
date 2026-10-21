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
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// filesChunkFake serves a GitHub repository whose tree is `trees[head]`. The
// text of a blob is `sizes[path]` bytes of "x" unless `texts` overrides it.
type filesChunkFake struct {
	t     *testing.T
	mu    sync.Mutex
	head  string
	trees map[string][]string
	sizes map[string]int

	texts  map[string]string
	binary map[string]bool
	// treeStatus answers a tree read of that sha with a bare status instead.
	treeStatus map[string]int

	graphql  []filesChunkRequest
	treeGets []string
}

type filesChunkRequest struct {
	ref      string
	paths    []string
	declared int
}

var filesChunkExpression = regexp.MustCompile(`f(\d+): object\(expression: ("(?:[^"\\]|\\.)*")`)

func (fake *filesChunkFake) Do(request *http.Request) (*http.Response, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	body := `{"full_name":"acme/api","default_branch":"main"}`
	switch {
	case request.URL.Path == "/repos/acme/api":
	case request.URL.Path == "/repos/acme/api/commits":
		body = `[{"sha":"` + fake.head + `"}]`
	case strings.HasPrefix(request.URL.Path, "/repos/acme/api/git/trees/"):
		sha := strings.TrimPrefix(request.URL.Path, "/repos/acme/api/git/trees/")
		if status, forced := fake.treeStatus[sha]; forced {
			return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"message":"forced"}`)), Request: request}, nil
		}
		paths, ok := fake.trees[sha]
		if !ok {
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"message":"Not Found"}`)), Request: request}, nil
		}
		fake.treeGets = append(fake.treeGets, sha)
		var entries bytes.Buffer
		for index, path := range paths {
			if index > 0 {
				entries.WriteByte(',')
			}
			fmt.Fprintf(&entries, `{"path":%q,"type":"blob","size":%d}`, path, fake.sizes[path])
		}
		body = `{"tree":[` + entries.String() + `]}`
	case request.URL.Path == "/graphql":
		raw, err := io.ReadAll(request.Body)
		if err != nil {
			fake.t.Fatal(err)
		}
		var envelope struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			fake.t.Fatal(err)
		}
		call := filesChunkRequest{}
		var fields strings.Builder
		for _, match := range filesChunkExpression.FindAllStringSubmatch(envelope.Query, -1) {
			var expression string
			if err := json.Unmarshal([]byte(match[2]), &expression); err != nil {
				fake.t.Fatal(err)
			}
			ref, path, _ := strings.Cut(expression, ":")
			call.ref = ref
			call.paths = append(call.paths, path)
			call.declared += fake.sizes[path]
			text, ok := fake.texts[path]
			if !ok {
				text = strings.Repeat("x", fake.sizes[path])
			}
			encoded, _ := json.Marshal(text)
			if fields.Len() > 0 {
				fields.WriteByte(',')
			}
			if fake.binary[path] {
				fmt.Fprintf(&fields, `"f%s":{"text":null,"isBinary":true,"isTruncated":false}`, match[1])
				continue
			}
			fmt.Fprintf(&fields, `"f%s":{"text":%s,"isBinary":false,"isTruncated":false}`, match[1], encoded)
		}
		fake.graphql = append(fake.graphql, call)
		body = `{"data":{"repository":{` + fields.String() + `}}}`
	default:
		fake.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func newFilesChunkFake(t *testing.T, goFiles, goSize, otherFiles int) *filesChunkFake {
	fake := &filesChunkFake{t: t, head: "tree-1", sizes: map[string]int{}, texts: map[string]string{}, binary: map[string]bool{}, treeStatus: map[string]int{}}
	paths := make([]string, 0, goFiles+otherFiles)
	for index := 0; index < goFiles; index++ {
		path := fmt.Sprintf("src/pkg%03d/f%05d.go", index%97, index)
		fake.sizes[path] = goSize
		paths = append(paths, path)
	}
	for index := 0; index < otherFiles; index++ {
		path := fmt.Sprintf("docs/d%05d.md", index)
		fake.sizes[path] = 100
		paths = append(paths, path)
	}
	fake.trees = map[string][]string{"tree-1": paths}
	return fake
}

func (fake *filesChunkFake) client(t *testing.T) *providerfoundation.HTTPClient {
	return gitHubRepositoryClient(t, fakehttp.Client(fake), "https://api.github.com")
}

type filesChunkRun struct {
	emissions []ChunkRouteEmission
	rows      []gitFileRow
}

func runFilesChunks(
	t *testing.T, fake *filesChunkFake, resume string, stopAfterData int,
) (filesChunkRun, error) {
	t.Helper()
	var run filesChunkRun
	data := 0
	err := (GitHubFilesRouteHandler{}).CollectChunks(
		context.Background(), nativeTestClaim("github", "files"), providerfoundation.Credential{}, fake.client(t),
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC), resume,
		func(emission ChunkRouteEmission) error {
			run.emissions = append(run.emissions, emission)
			for _, effect := range emission.Batch.Effects {
				for _, raw := range effect.Rows {
					var row gitFileRow
					if err := json.Unmarshal(raw, &row); err != nil {
						t.Fatal(err)
					}
					run.rows = append(run.rows, row)
				}
			}
			if !emission.Final {
				data++
				if stopAfterData > 0 && data >= stopAfterData {
					return ChunkContinuationError{Reason: ChunkStopChunkBound, Chunks: data}
				}
			}
			return nil
		},
	)
	return run, err
}

func TestGitHubFilesChunksResumeFromTheStoredCursorUnionEqualsTheFullTreeNoRepeats(t *testing.T) {
	// 40 blobs of 600,000 bytes: two per 1.5 MiB chunk, 20 data chunks.
	fake := newFilesChunkFake(t, 40, 600_000, 5)
	first, err := runFilesChunks(t, fake, "", 7)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || len(first.emissions) != 7 {
		t.Fatalf("run 1 err=%v emissions=%d, want a continuation after 7 chunks", err, len(first.emissions))
	}
	resume := first.emissions[len(first.emissions)-1].CursorAfter
	second, err := runFilesChunks(t, fake, resume, 0)
	if err != nil {
		t.Fatal(err)
	}
	if second.emissions[0].CursorBefore != resume || first.emissions[0].CursorBefore != "" {
		t.Fatalf("cursor chain broken: first before=%q second before=%q resume=%q", first.emissions[0].CursorBefore, second.emissions[0].CursorBefore, resume)
	}
	seen := map[string]int{}
	for _, row := range append(append([]gitFileRow(nil), first.rows...), second.rows...) {
		seen[row.Path]++
		if strings.HasSuffix(row.Path, ".go") && (row.Contents == nil || len(*row.Contents) != 600_000) {
			t.Fatalf("%s has no contents", row.Path)
		}
	}
	if len(seen) != 45 {
		t.Fatalf("union=%d paths, want 45", len(seen))
	}
	for path, count := range seen {
		if count != 1 {
			t.Fatalf("%s emitted %d times", path, count)
		}
	}
	for _, emission := range append(append([]ChunkRouteEmission(nil), first.emissions...), second.emissions...) {
		declared := 0
		for _, effect := range emission.Batch.Effects {
			for _, raw := range effect.Rows {
				var row gitFileRow
				_ = json.Unmarshal(raw, &row)
				if row.Contents != nil {
					declared += len(*row.Contents)
				}
			}
		}
		if declared > gitHubFileChunkMaxBytes {
			t.Fatalf("one emission holds %d bytes of text, bound %d", declared, gitHubFileChunkMaxBytes)
		}
	}
	final := second.emissions[len(second.emissions)-1]
	if !final.Final || final.Batch.Result["files_synced"] != 45 || final.Batch.Result["files_with_contents"] != 40 ||
		final.Batch.Result["inventory_status"] != "complete" {
		t.Fatalf("final=%+v", final.Batch.Result)
	}
	for _, call := range fake.graphql {
		if call.ref != "tree-1" || call.declared > gitHubFileChunkMaxBytes {
			t.Fatalf("content request ref=%s declared=%d", call.ref, call.declared)
		}
	}
}

func TestGitHubFilesChunkHoldsExactlyTheBoundAndSplitsOneByteOver(t *testing.T) {
	for _, tc := range []struct {
		name       string
		second     int
		wantChunks int
	}{
		{"exactly at the bound stays in one chunk", gitHubFileChunkMaxBytes / 2, 1},
		{"one byte over splits", gitHubFileChunkMaxBytes/2 + 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFilesChunkFake(t, 2, gitHubFileChunkMaxBytes/2, 0)
			paths := fake.trees["tree-1"]
			fake.sizes[paths[1]] = tc.second
			run, err := runFilesChunks(t, fake, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			data := 0
			for _, emission := range run.emissions {
				if !emission.Final {
					data++
				}
			}
			if data != tc.wantChunks || len(run.rows) != 2 {
				t.Fatalf("data chunks=%d rows=%d want %d chunks, 2 rows", data, len(run.rows), tc.wantChunks)
			}
		})
	}
}

func TestGitHubFilesChunkKeepsThePinnedTreeWhenTheBranchMovesBetweenRuns(t *testing.T) {
	fake := newFilesChunkFake(t, 10, 600_000, 0)
	fake.trees["tree-2"] = []string{"src/new.go"}
	fake.sizes["src/new.go"] = 10
	first, err := runFilesChunks(t, fake, "", 2)
	if !errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("err=%v", err)
	}
	fake.head = "tree-2"
	second, err := runFilesChunks(t, fake, first.emissions[len(first.emissions)-1].CursorAfter, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.rows)+len(second.rows) != 10 {
		t.Fatalf("rows=%d, want the 10 paths of the pinned tree", len(first.rows)+len(second.rows))
	}
	for _, row := range second.rows {
		if row.Path == "src/new.go" {
			t.Fatal("the branch move leaked into a resumed unit")
		}
	}
	for _, call := range fake.graphql {
		if call.ref != "tree-1" {
			t.Fatalf("content read at %s, want the pinned tree-1", call.ref)
		}
	}
}

func TestGitHubFilesChunksGiveEveryEligibleBlobItsContentsPastTheOldTwoThousandCap(t *testing.T) {
	fake := newFilesChunkFake(t, 2100, 100, 0)
	run, err := runFilesChunks(t, fake, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	withContents := 0
	for _, row := range run.rows {
		if row.Contents != nil {
			withContents++
		}
	}
	if len(run.rows) != 2100 || withContents != 2100 {
		t.Fatalf("rows=%d with contents=%d, want 2100 both", len(run.rows), withContents)
	}
}

func TestGitHubFileContentRequestsAreSizedByBytesNotOnlyByBlobCount(t *testing.T) {
	fake := newFilesChunkFake(t, 50, 1_000_000, 0)
	paths := fake.trees["tree-1"]
	sizes := map[string]*int{}
	for _, path := range paths {
		size := fake.sizes[path]
		sizes[path] = &size
	}
	contents, err := fetchGitHubFileContents(context.Background(), fake.client(t), "acme", "api", "tree-1", paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if len(contents) != 50 {
		t.Fatalf("contents=%d", len(contents))
	}
	for _, call := range fake.graphql {
		if call.declared > gitHubFileChunkMaxBytes {
			t.Fatalf("a request declared %d bytes of text, bound %d (50 x 1 MB in one request is 50 MB, above the 32 MiB response cap)", call.declared, gitHubFileChunkMaxBytes)
		}
	}
	if len(fake.graphql) != 50 {
		t.Fatalf("requests=%d, want 50 (one 1 MB blob per request: two exceed the bound)", len(fake.graphql))
	}
}

func TestGitHubFilesChunkKeepsAnOverEncodedRowWithoutContentsAndSaysSo(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	fake := newFilesChunkFake(t, 2, 300_000, 0)
	paths := fake.trees["tree-1"]
	// "<" encodes as the six bytes <: 300,000 of them are 1.8 MB.
	fake.texts[paths[0]] = strings.Repeat("<", 300_000)
	run, err := runFilesChunks(t, fake, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	var withContents int
	for _, row := range run.rows {
		if row.Contents != nil {
			withContents++
		}
	}
	final := run.emissions[len(run.emissions)-1].Batch.Result
	if len(run.rows) != 2 || withContents != 1 || final["contents_oversize"] != 1 {
		t.Fatalf("rows=%d with contents=%d result=%v", len(run.rows), withContents, final)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "skipped=1") {
		t.Fatalf("no WARN with the count: %s", logs.String())
	}
}

func TestGitHubFilesRouteIsChunkedWithItsOwnPolicyAndNotPreparedSnapshot(t *testing.T) {
	descriptor, ok := Descriptor("github", "files")
	if !ok || !descriptor.Chunked || descriptor.PreparedManifestRecovery || descriptor.ChunkPolicy != GitHubFilesChunkPolicy() {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if err := GitHubFilesChunkPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
	gitlab, _ := Descriptor("gitlab", "files")
	if gitlab.Chunked {
		t.Fatal("gitlab/files must stay one batch")
	}
	var _ ChunkedCompleteRouteHandler = GitHubFilesRouteHandler{}
}

// filesReadbackSink records every write and every readback inspection; a
// recorded row reads back exact, an unrecorded one absent.
type filesReadbackSink struct {
	mu        sync.Mutex
	rows      map[string]int
	versions  map[string][]time.Time
	inspected int
	failFrom  int
	writes    int
	// crashAfterWrite records the write before failing: the chunk is in the
	// store but the ledger never saw it, so recovery must read it back.
	crashAfterWrite bool
}

func (sink *filesReadbackSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.writes++
	crash := sink.failFrom > 0 && sink.writes >= sink.failFrom
	if crash && !sink.crashAfterWrite {
		return errors.New("simulated crash")
	}
	if sink.rows == nil {
		sink.rows = map[string]int{}
	}
	for _, raw := range batch.Rows {
		var row gitFileRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		sink.rows[row.Path]++
		if sink.versions == nil {
			sink.versions = map[string][]time.Time{}
		}
		sink.versions[row.Path] = append(sink.versions[row.Path], row.LastSynced)
	}
	if crash {
		return errors.New("simulated crash after the write")
	}
	return nil
}

func (sink *filesReadbackSink) InspectEffect(_ context.Context, _ Claim, effect EffectBatch) (EffectInspection, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.inspected++
	if len(effect.Rows) == 0 {
		return EffectAbsent, nil
	}
	found := 0
	for _, raw := range effect.Rows {
		var row gitFileRow
		_ = json.Unmarshal(raw, &row)
		if sink.rows[row.Path] > 0 {
			found++
		}
	}
	switch found {
	case 0:
		return EffectAbsent, nil
	case len(effect.Rows):
		return EffectExact, nil
	default:
		return EffectConflict, nil
	}
}

func filesChunkExecutor(t *testing.T, fake *filesChunkFake, sink *filesReadbackSink) (
	CompleteRouteExecutor, *LeaseSession, CompleteRouteDescriptor, *chunkMemoryStore,
) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	_, session := completeRouteSessionFor(t, now, false, "github", "files")
	descriptor, ok := Descriptor("github", "files")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	store := newChunkMemoryStore()
	executor := completeRouteExecutor(now, GitHubFilesRouteHandler{}, store, sink)
	executor.Committer.Readback = sink
	executor.Comparator = ProductionContractComparator{}
	executor.Doer = fakehttp.Client(fake)
	executor.BudgetLimits = map[CostClass]int{CostHeavy: 1}
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	return executor, session, descriptor, store
}

// An ops-sized repository (12,033 entries, 6,494 eligible blobs, 70.4 MB of
// eligible text: the measured dev-health-ops tree) runs through the real
// chunked executor, attempt after attempt, until the unit completes.
func TestGitHubFilesOpsSizedRepositoryConvergesAcrossAttemptsThroughTheChunkedExecutor(t *testing.T) {
	if testing.Short() {
		t.Skip("70 MB fixture")
	}
	fake := newFilesChunkFake(t, 6494, 10_835, 5539)
	sink := &filesReadbackSink{}
	executor, session, descriptor, store := filesChunkExecutor(t, fake, sink)
	attempts := 0
	var result CompleteRouteExecutionResult
	for {
		attempts++
		if attempts > 20 {
			t.Fatal("the unit does not complete in 20 attempts")
		}
		var err error
		result, err = executor.Execute(context.Background(), session, descriptor)
		if err == nil {
			break
		}
		var stopped ChunkContinuationError
		if !errors.As(err, &stopped) {
			t.Fatalf("attempt %d: %v", attempts, err)
		}
		if stopped.Chunks > descriptor.ChunkPolicy.MaxChunksPerAttempt {
			t.Fatalf("attempt %d committed %d chunks over the bound", attempts, stopped.Chunks)
		}
	}
	if len(sink.rows) != 12_033 {
		t.Fatalf("distinct rows written=%d, want 12033", len(sink.rows))
	}
	for path, count := range sink.rows {
		if count != 1 {
			t.Fatalf("%s written %d times", path, count)
		}
	}
	if fmt.Sprint(result.Result["files_synced"]) != "12033" || fmt.Sprint(result.Result["files_with_contents"]) != "6494" ||
		result.Comparison.NativeRecords != 12_033 || result.CommittedRows != 12_033 {
		t.Fatalf("result=%v comparison=%+v committed=%d", result.Result, result.Comparison, result.CommittedRows)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), session.Claim, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(fake.graphql, func(i, j int) bool { return fake.graphql[i].declared > fake.graphql[j].declared })
	if fake.graphql[0].declared > gitHubFileChunkMaxBytes {
		t.Fatalf("largest request declares %d bytes", fake.graphql[0].declared)
	}
	t.Logf("OPS-SIZED: %d entries, %d eligible blobs, %d MB eligible text -> %d prepared chunks, %d GraphQL requests, %d attempts, %d tree reads, %d readback inspections",
		12_033, 6_494, 6_494*10_835/1_000_000, checkpoint.TotalChunks, len(fake.graphql), attempts, len(fake.treeGets), sink.inspected)
}

// A crash after chunk k keeps the rows of chunks <= k (nothing deletes them: the
// route has no delete path) and the next attempt writes only the rest.
func TestGitHubFilesPartialRunKeepsItsRowsAndTheNextRunWritesTheRest(t *testing.T) {
	fake := newFilesChunkFake(t, 60, 600_000, 0)
	sink := &filesReadbackSink{failFrom: 6}
	executor, session, descriptor, _ := filesChunkExecutor(t, fake, sink)
	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil {
		t.Fatal("the crash must fail the attempt")
	}
	kept := len(sink.rows)
	if kept == 0 || kept >= 60 {
		t.Fatalf("rows after the crash=%d, want some but not all", kept)
	}
	sink.failFrom = 0
	for attempt := 0; ; attempt++ {
		if attempt > 20 {
			t.Fatal("no completion")
		}
		if _, err := executor.Execute(context.Background(), session, descriptor); err == nil {
			break
		} else if !errors.As(err, new(ChunkContinuationError)) {
			t.Fatal(err)
		}
	}
	if len(sink.rows) != 60 {
		t.Fatalf("rows=%d, want 60", len(sink.rows))
	}
	for path, count := range sink.rows {
		if count != 1 {
			t.Fatalf("%s written %d times", path, count)
		}
	}
}

// The crash lands after a chunk reached the store but before the ledger marked
// it: the resumed attempt reads that chunk back (EffectExact) and does not write
// it again.
func TestGitHubFilesCrashAfterAChunkWriteIsReadBackNotRewritten(t *testing.T) {
	fake := newFilesChunkFake(t, 60, 600_000, 0)
	sink := &filesReadbackSink{failFrom: 6, crashAfterWrite: true}
	executor, session, descriptor, _ := filesChunkExecutor(t, fake, sink)
	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil {
		t.Fatal("the crash must fail the attempt")
	}
	sink.failFrom = 0
	for attempt := 0; ; attempt++ {
		if attempt > 20 {
			t.Fatal("no completion")
		}
		if _, err := executor.Execute(context.Background(), session, descriptor); err == nil {
			break
		} else if !errors.As(err, new(ChunkContinuationError)) {
			t.Fatal(err)
		}
	}
	if len(sink.rows) != 60 || sink.inspected == 0 {
		t.Fatalf("rows=%d inspected=%d, want 60 rows and a readback of the crashed chunk", len(sink.rows), sink.inspected)
	}
	for path, count := range sink.rows {
		if count != 1 {
			t.Fatalf("%s written %d times", path, count)
		}
	}
}

// An eligible empty file is a successful read of "": its row keeps contents ""
// (present), and only a file the provider returned no text for stays without
// contents, counted as unavailable. A missing blob never reads as complete.
func TestGitHubFilesChunkKeepsAnEmptyFileAsEmptyContentsAndCountsUnavailableOnes(t *testing.T) {
	fake := newFilesChunkFake(t, 3, 100, 1)
	paths := fake.trees["tree-1"]
	fake.sizes[paths[0]] = 0
	fake.texts[paths[0]] = ""
	fake.binary[paths[1]] = true
	run, err := runFilesChunks(t, fake, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]gitFileRow{}
	for _, row := range run.rows {
		byPath[row.Path] = row
	}
	if row := byPath[paths[0]]; row.Contents == nil || *row.Contents != "" {
		t.Fatalf("empty eligible file contents=%v, want present and empty", row.Contents)
	}
	if byPath[paths[1]].Contents != nil {
		t.Fatalf("binary file contents=%v, want missing", byPath[paths[1]].Contents)
	}
	if row := byPath[paths[2]]; row.Contents == nil || len(*row.Contents) != 100 {
		t.Fatalf("regular file contents=%v", row.Contents)
	}
	if byPath["docs/d00000.md"].Contents != nil {
		t.Fatal("an ineligible file must have no contents")
	}
	final := run.emissions[len(run.emissions)-1].Batch.Result
	if final["files_with_contents"] != 2 || final["contents_unavailable"] != 1 {
		t.Fatalf("result=%v, want 2 with contents (empty + regular) and 1 unavailable (binary)", final)
	}
}

func withChunkBound(descriptor CompleteRouteDescriptor, chunks int) CompleteRouteDescriptor {
	descriptor.ChunkPolicy.MaxChunksPerAttempt = chunks
	return descriptor
}

// The pinned tree is gone when a unit resumes (its commit was garbage-collected
// or force-pushed away): the cursor could never advance. The unit restarts from
// the current head under a later write instant, counts it, and completes.
func TestGitHubFilesUnitRestartsFromTheHeadWhenThePinnedTreeIsGone(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusUnprocessableEntity} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previous) })
			fake := newFilesChunkFake(t, 60, 600_000, 0)
			sink := &filesReadbackSink{}
			executor, session, descriptor, store := filesChunkExecutor(t, fake, sink)
			first := fake.trees["tree-1"]
			_, err := executor.Execute(context.Background(), session, withChunkBound(descriptor, 2))
			if !errors.As(err, new(ChunkContinuationError)) || len(sink.rows) == 0 {
				t.Fatalf("attempt 1 err=%v rows=%d, want a continuation after committed chunks", err, len(sink.rows))
			}
			fake.mu.Lock()
			fake.trees["tree-2"] = append(append([]string(nil), first[:55]...), "src/new.go")
			fake.sizes["src/new.go"] = 100
			fake.treeStatus["tree-1"] = status
			fake.head = "tree-2"
			fake.mu.Unlock()
			var result CompleteRouteExecutionResult
			for attempt := 0; ; attempt++ {
				if attempt > 40 {
					t.Fatal("the unit does not complete")
				}
				result, err = executor.Execute(context.Background(), session, descriptor)
				if err == nil {
					break
				}
				if !errors.As(err, new(ChunkContinuationError)) {
					t.Fatalf("attempt %d: %v", attempt+2, err)
				}
			}
			for _, path := range fake.trees["tree-2"] {
				if sink.rows[path] == 0 {
					t.Fatalf("%s of the new head was never written", path)
				}
			}
			rewritten := 0
			for path, versions := range sink.versions {
				if len(versions) == 2 {
					rewritten++
					if !versions[1].After(versions[0]) {
						t.Fatalf("%s rewritten at %s, not after %s", path, versions[1], versions[0])
					}
				}
			}
			if rewritten == 0 {
				t.Fatal("no path was rewritten under the later instant")
			}
			if fmt.Sprint(result.Result["pin_lost"]) != "1" || fmt.Sprint(result.Result["paths_total"]) != "56" ||
				fmt.Sprint(result.Result["eligible_total"]) != "56" || fmt.Sprint(result.Result["files_synced"]) != "56" ||
				fmt.Sprint(result.Result["files_with_contents"]) != "56" {
				t.Fatalf("result=%v", result.Result)
			}
			text := logs.String()
			for _, want := range []string{"pinned tree is gone", "pin_lost=1", "paths_total=56", "eligible_total=56"} {
				if !strings.Contains(text, want) {
					t.Fatalf("log lacks %q", want)
				}
			}
			if checkpoint, err := store.LoadChunkCheckpoint(context.Background(), session.Claim, time.Now()); err != nil || !checkpoint.InventoryComplete {
				t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
			}
		})
	}
}

// Negative control: a server error or a rate limit on the pinned tree is not a
// vanished pin. The cursor and the pin stay, and the next attempt finishes the
// SAME tree.
func TestGitHubFilesUnitKeepsItsPinOnAServerErrorOfThePinnedTree(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusTooManyRequests} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fake := newFilesChunkFake(t, 60, 600_000, 0)
			sink := &filesReadbackSink{}
			executor, session, descriptor, store := filesChunkExecutor(t, fake, sink)
			if _, err := executor.Execute(context.Background(), session, withChunkBound(descriptor, 2)); !errors.As(err, new(ChunkContinuationError)) {
				t.Fatalf("attempt 1: %v", err)
			}
			before, err := store.LoadChunkCheckpoint(context.Background(), session.Claim, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			fake.treeStatus["tree-1"] = status
			fake.head = "tree-2"
			fake.trees["tree-2"] = []string{"src/other.go"}
			fake.mu.Unlock()
			if _, err := executor.Execute(context.Background(), session, descriptor); err == nil || errors.As(err, new(ChunkContinuationError)) {
				t.Fatalf("a %d on the pinned tree must fail the attempt, got %v", status, err)
			}
			after, err := store.LoadChunkCheckpoint(context.Background(), session.Claim, time.Now())
			if err != nil || after.NextCursor != before.NextCursor || after.PreparedChunks != before.PreparedChunks {
				t.Fatalf("cursor moved: before=%q after=%q err=%v", before.NextCursor, after.NextCursor, err)
			}
			fake.mu.Lock()
			delete(fake.treeStatus, "tree-1")
			fake.mu.Unlock()
			var result CompleteRouteExecutionResult
			for attempt := 0; ; attempt++ {
				if attempt > 40 {
					t.Fatal("no completion")
				}
				result, err = executor.Execute(context.Background(), session, descriptor)
				if err == nil {
					break
				}
				if !errors.As(err, new(ChunkContinuationError)) {
					t.Fatal(err)
				}
			}
			if fmt.Sprint(result.Result["pin_lost"]) != "0" || len(sink.rows) != 60 {
				t.Fatalf("pin_lost=%v rows=%d, want the pinned tree finished untouched", result.Result["pin_lost"], len(sink.rows))
			}
			for _, call := range fake.graphql {
				if call.ref != "tree-1" {
					t.Fatalf("content read at %s", call.ref)
				}
			}
		})
	}
}
