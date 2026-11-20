package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

const runsGroupFirstID = 9_000_001

// coalesceClock is the executor clock of the wall cells: GitHub reads and sink writes advance it by their measured cost.
type coalesceClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *coalesceClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *coalesceClock) advance(by time.Duration) {
	if clock == nil {
		return
	}
	clock.mu.Lock()
	clock.now = clock.now.Add(by)
	clock.mu.Unlock()
}

// runsGroupDoer lists `runs` completed runs newest first, perPage per listing page (Link to the next page), and answers
// each run's jobs read with jobsPerRun small jobs (bigJobs for bigRun, 100 per page). The artifacts listing is empty, so
// a walk is the runs phase and the final emission. failJobsOnce fails one run's first jobs read.
type runsGroupDoer struct {
	t             *testing.T
	runs          int
	perPage       int
	jobsPerRun    int
	bigRun        string
	bigJobs       int
	failJobsOnce  string
	clock         *coalesceClock
	readCost      time.Duration
	wallAt        time.Time
	readsPastWall int
	mu            sync.Mutex
	jobGets       map[string]int
}

func newRunsGroupDoer(t *testing.T, runs, jobsPerRun int) *runsGroupDoer {
	return &runsGroupDoer{t: t, runs: runs, perPage: 100, jobsPerRun: jobsPerRun, jobGets: map[string]int{}}
}

func runsGroupRunID(index int) string { return strconv.Itoa(runsGroupFirstID + index) }

func (doer *runsGroupDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	page := 1
	if raw := request.URL.Query().Get("page"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			doer.t.Errorf("page query %q", raw)
		}
		page = parsed
	}
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		if githubTestsIsArtifactRunListing(request.URL.Query()) {
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[]}`), nil
		}
		first := (page - 1) * doer.perPage
		last := min(first+doer.perPage, doer.runs)
		if last < doer.runs {
			next := *request.URL
			query := next.Query()
			query.Set("page", strconv.Itoa(page+1))
			next.RawQuery = query.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
		}
		var body strings.Builder
		body.WriteString(`{"workflow_runs":[`)
		base := time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC)
		for index := first; index < last; index++ {
			if index > first {
				body.WriteByte(',')
			}
			id := runsGroupRunID(index)
			created := base.Add(-time.Duration(index) * 10 * time.Second)
			body.WriteString(`{"id":` + id + `,"name":"CI","status":"completed","conclusion":"success","created_at":"` +
				created.Format(time.RFC3339) + `","run_started_at":"` + created.Add(time.Minute).Format(time.RFC3339) +
				`","updated_at":"` + created.Add(5*time.Minute).Format(time.RFC3339) +
				`","run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/` +
				id + `","pull_requests":[]}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		runID := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/jobs")
		doer.mu.Lock()
		if doer.clock != nil && !doer.wallAt.IsZero() && !doer.clock.Now().Before(doer.wallAt) {
			doer.readsPastWall++
		}
		doer.mu.Unlock()
		doer.clock.advance(doer.readCost)
		doer.mu.Lock()
		doer.jobGets[runID]++
		fail := runID == doer.failJobsOnce
		if fail {
			doer.failJobsOnce = ""
		}
		doer.mu.Unlock()
		if fail {
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: header,
				Body: http.NoBody, Request: request}, nil
		}
		total := doer.jobsPerRun
		if runID == doer.bigRun {
			total = doer.bigJobs
		}
		first := (page - 1) * githubTestsPerRunPerPage
		last := min(first+githubTestsPerRunPerPage, total)
		if last < total {
			next := *request.URL
			query := next.Query()
			query.Set("page", strconv.Itoa(page+1))
			next.RawQuery = query.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
		}
		var body strings.Builder
		body.WriteString(`{"total_count":` + strconv.Itoa(total) + `,"jobs":[`)
		for job := first; job < last; job++ {
			if job > first {
				body.WriteByte(',')
			}
			body.WriteString(`{"id":` + runID + fmt.Sprintf("%04d", job) + `,"name":"job-` + strconv.Itoa(job) +
				`","status":"completed","conclusion":"success","started_at":"2026-07-22T13:01:00Z","completed_at":"2026-07-22T13:04:00Z","labels":["ubuntu-latest"]}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	default:
		doer.t.Errorf("unexpected request %s", request.URL.String())
		return &http.Response{StatusCode: http.StatusNotFound, Header: header, Body: http.NoBody, Request: request}, nil
	}
}

// runsGroupSink records every written row (a ReplacingMergeTree reader sees each physical row), counts the writes that
// carry pipeline rows (one per committed runs-phase chunk) and reads back exactly what it holds. failJobWrite fails that
// one ci_job_runs write (1-based) once.
type runsGroupSink struct {
	mu             sync.Mutex
	clock          *coalesceClock
	writeCost      time.Duration
	failJobWrite   int
	jobWrites      int
	pipelineWrites int
	pipelineRows   map[string]int
	jobRows        map[string]int
	rows           map[string]map[string]int
}

func newRunsGroupSink() *runsGroupSink {
	return &runsGroupSink{pipelineRows: map[string]int{}, jobRows: map[string]int{}, rows: map[string]map[string]int{}}
}

func (sink *runsGroupSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	if len(batch.Rows) > 0 {
		sink.clock.advance(sink.writeCost)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if batch.Destination == "ci_job_runs" && len(batch.Rows) > 0 {
		sink.jobWrites++
		if sink.jobWrites == sink.failJobWrite {
			return errors.New("sink unavailable")
		}
	}
	if sink.rows[batch.Destination] == nil {
		sink.rows[batch.Destination] = map[string]int{}
	}
	for _, raw := range batch.Rows {
		sink.rows[batch.Destination][string(raw)]++
		var row struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		switch batch.Destination {
		case "ci_pipeline_runs":
			sink.pipelineRows[row.RunID]++
		case "ci_job_runs":
			sink.jobRows[row.RunID]++
		}
	}
	if batch.Destination == "ci_pipeline_runs" && len(batch.Rows) > 0 {
		sink.pipelineWrites++
	}
	return nil
}

func (sink *runsGroupSink) InspectEffect(_ context.Context, _ Claim, batch EffectBatch) (EffectInspection, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := 0
	for _, raw := range batch.Rows {
		if sink.rows[batch.Destination][string(raw)] > 0 {
			found++
		}
	}
	switch found {
	case len(batch.Rows):
		return EffectExact, nil
	case 0:
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

// duplicates counts rows physically written more than once.
func (sink *runsGroupSink) duplicates() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	total := 0
	for _, rows := range sink.rows {
		for _, count := range rows {
			if count > 1 {
				total++
			}
		}
	}
	return total
}

// runsGroupSession is a github/cicd claim over 2026-07-22 12:00..20:00 (no UTC midnight, so no deep re-run range).
func runsGroupSession(t *testing.T, now time.Time) (Claim, *LeaseSession) {
	t.Helper()
	unit := nativeTestClaim("github", "cicd").Unit
	since, before := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC), time.Date(2026, 7, 22, 20, 0, 0, 0, time.UTC)
	unit.SinceAt, unit.BeforeAt = &since, &before
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim, &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: 15 * time.Minute,
		Deadline: now.Add(48 * time.Hour), Now: func() time.Time { return now },
	}
}

func runsGroupExecutor(
	now time.Time, handler CompleteRouteHandler, store EffectLedger, sink *runsGroupSink, doer *runsGroupDoer, clock *coalesceClock,
) CompleteRouteExecutor {
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Committer.Readback = sink
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.Doer = fakehttp.Client(doer)
	executor.BudgetLimits = map[CostClass]int{CostMedium: 100_000, CostHeavy: 100_000, CostLight: 100_000}
	if clock != nil {
		executor.Now = clock.Now
	}
	return executor
}

// noCoalesceHandler is the tests route with every emission's Coalescable cleared: today's one group per emission.
type noCoalesceHandler struct{ GitHubTestsRouteHandler }

func (handler noCoalesceHandler) CollectChunks(
	ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient,
	normalizedAt time.Time, resumeCursor string, emit func(ChunkRouteEmission) error,
) error {
	return handler.GitHubTestsRouteHandler.CollectChunks(ctx, claim, credential, client, normalizedAt, resumeCursor,
		func(emission ChunkRouteEmission) error {
			emission.Coalescable = false
			return emit(emission)
		})
}

// runUntilDone runs attempts until the unit completes and returns each attempt's continuation (nil for the last).
func runUntilDone(t *testing.T, executor CompleteRouteExecutor, session *LeaseSession, descriptor CompleteRouteDescriptor) []*ChunkContinuationError {
	t.Helper()
	var stops []*ChunkContinuationError
	for attempt := 1; ; attempt++ {
		if attempt > 200 {
			t.Fatal("the unit does not complete in 200 attempts")
		}
		_, err := executor.Execute(context.Background(), session, descriptor)
		if err == nil {
			return append(stops, nil)
		}
		var stopped ChunkContinuationError
		if !errors.As(err, &stopped) {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		stops = append(stops, &stopped)
	}
}

func cicdDescriptor(t *testing.T) CompleteRouteDescriptor {
	t.Helper()
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	return descriptor
}

func assertEachRunOnce(t *testing.T, sink *runsGroupSink, doer *runsGroupDoer, runs int) {
	t.Helper()
	if len(sink.pipelineRows) != runs {
		t.Fatalf("pipeline rows for %d runs, want %d", len(sink.pipelineRows), runs)
	}
	for index := 0; index < runs; index++ {
		id := runsGroupRunID(index)
		if sink.pipelineRows[id] != 1 {
			t.Fatalf("run %s pipeline row written %d times, want 1", id, sink.pipelineRows[id])
		}
		want := doer.jobsPerRun
		if id == doer.bigRun {
			want = doer.bigJobs
		}
		if sink.jobRows[id] != want {
			t.Fatalf("run %s job rows=%d, want %d", id, sink.jobRows[id], want)
		}
	}
	if duplicated := sink.duplicates(); duplicated != 0 {
		t.Fatalf("%d rows written more than once", duplicated)
	}
}

// storedRunGroups returns the run ids of each stored chunk that carries pipeline rows, in ordinal order, and checks
// that no destination of any stored chunk passes the policy's row cap.
func storedRunGroups(t *testing.T, store *chunkMemoryStore, policy ChunkPolicy) [][]string {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	groups := [][]string{}
	for ordinal := 0; ordinal < len(store.chunks); ordinal++ {
		chunk, ok := store.chunks[ordinal]
		if !ok {
			t.Fatalf("stored chunks have a gap at %d", ordinal)
		}
		var runs []string
		for _, effect := range chunk.Effects {
			if len(effect.Rows) > min(policy.MaxEffectRows, policy.MaxSourceItems) {
				t.Fatalf("chunk %d %s holds %d rows over the cap", ordinal, effect.Destination, len(effect.Rows))
			}
			if effect.Destination != "ci_pipeline_runs" {
				continue
			}
			for _, raw := range effect.Rows {
				var row struct {
					RunID string `json:"run_id"`
				}
				_ = json.Unmarshal(raw, &row)
				runs = append(runs, row.RunID)
			}
		}
		if len(runs) > 0 {
			groups = append(groups, runs)
		}
	}
	return groups
}

// 100 small runs (3 jobs each) fill chunk groups up to the 100-row cap of ci_job_runs: 33 + 33 + 33 + 1, each group one
// sub-chunk, each run written once. The base committed 100 groups.
func TestChunkedRunsPhaseFillsGroupsUpToTheRowCap(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	doer, sink, store := newRunsGroupDoer(t, 100, 3), newRunsGroupSink(), newChunkMemoryStore()
	runUntilDone(t, runsGroupExecutor(now, GitHubTestsRouteHandler{}, store, sink, doer, nil), session, descriptor)
	assertEachRunOnce(t, sink, doer, 100)
	groups := storedRunGroups(t, store, descriptor.ChunkPolicy)
	sizes := make([]int, 0, len(groups))
	for _, group := range groups {
		sizes = append(sizes, len(group))
	}
	if fmt.Sprint(sizes) != "[33 33 33 1]" || sink.pipelineWrites != 4 {
		t.Fatalf("run groups %v (%d pipeline writes), want [33 33 33 1]", sizes, sink.pipelineWrites)
	}
	if len(store.chunks) != 5 {
		t.Fatalf("stored chunks=%d, want 4 run groups + the final chunk (a coalesced group is ONE sub-chunk)", len(store.chunks))
	}
	for index, gets := range doer.jobGets {
		if gets != 1 {
			t.Fatalf("run %s jobs read %d times", index, gets)
		}
	}
}

// A group never spans a listing page: the page visit moves the cursor's page counters, so the first run of the next page
// does not start where the last run of the page ended. 250 runs on pages of 30, one job each, give 9 groups.
func TestChunkedRunsPhaseGroupNeverSpansAListingPage(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	doer, sink, store := newRunsGroupDoer(t, 250, 1), newRunsGroupSink(), newChunkMemoryStore()
	doer.perPage = 30
	runUntilDone(t, runsGroupExecutor(now, GitHubTestsRouteHandler{}, store, sink, doer, nil), session, descriptor)
	assertEachRunOnce(t, sink, doer, 250)
	groups := storedRunGroups(t, store, descriptor.ChunkPolicy)
	if len(groups) != 9 {
		t.Fatalf("groups=%d, want 9 (one per listing page)", len(groups))
	}
	for _, group := range groups {
		first, _ := strconv.Atoi(group[0])
		for _, id := range group {
			index, _ := strconv.Atoi(id)
			if (index-runsGroupFirstID)/30 != (first-runsGroupFirstID)/30 {
				t.Fatalf("group %v spans listing pages", group)
			}
		}
	}
}

// A stream error drops only the group that was not durable yet: the five runs held before the failing jobs read are
// read again by the next attempt and written once; nothing was prepared for them.
func TestChunkedRunsPhaseStreamErrorDropsOnlyTheHeldGroup(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	claim, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	doer, sink, store := newRunsGroupDoer(t, 40, 2), newRunsGroupSink(), newChunkMemoryStore()
	doer.failJobsOnce = runsGroupRunID(5)
	executor := runsGroupExecutor(now, GitHubTestsRouteHandler{}, store, sink, doer, nil)
	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil || errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("attempt 1 err=%v, want the jobs read failure", err)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil && !errors.Is(err, ErrChunkCheckpointNotFound) {
		t.Fatal(err)
	}
	if checkpoint.PreparedChunks != 0 || len(sink.pipelineRows) != 0 {
		t.Fatalf("after the failed attempt: prepared=%d written runs=%d, want 0 0 (the held group is not durable)",
			checkpoint.PreparedChunks, len(sink.pipelineRows))
	}
	runUntilDone(t, executor, session, descriptor)
	assertEachRunOnce(t, sink, doer, 40)
	for index := 0; index < 40; index++ {
		want := 1
		if index <= 5 {
			want = 2
		}
		if got := doer.jobGets[runsGroupRunID(index)]; got != want {
			t.Fatalf("run %d jobs read %d times, want %d", index, got, want)
		}
	}
}

// A run whose jobs split into several sub-chunks is never coalesced: it keeps its own prepared group and the yield
// inside it, and the attempt chunk bound holds; every run is written once.
func TestChunkedRunsPhaseBigRunKeepsItsOwnGroupAndTheBounds(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	descriptor.ChunkPolicy.MaxChunksPerAttempt = 2
	doer, sink, store := newRunsGroupDoer(t, 100, 3), newRunsGroupSink(), newChunkMemoryStore()
	doer.bigRun, doer.bigJobs = runsGroupRunID(50), 250
	stops := runUntilDone(t, runsGroupExecutor(now, GitHubTestsRouteHandler{}, store, sink, doer, nil), session, descriptor)
	assertEachRunOnce(t, sink, doer, 100)
	for index, stop := range stops {
		if stop != nil && stop.Chunks > 2 {
			t.Fatalf("attempt %d committed %d chunks over the bound of 2", index+1, stop.Chunks)
		}
	}
	bigChunks := 0
	store.mu.Lock()
	for _, chunk := range store.chunks {
		for _, effect := range chunk.Effects {
			if effect.Destination == "ci_job_runs" && len(effect.Rows) > 0 &&
				strings.Contains(string(effect.Rows[0]), `"run_id":"`+doer.bigRun+`"`) {
				bigChunks++
			}
		}
	}
	store.mu.Unlock()
	groups := storedRunGroups(t, store, descriptor.ChunkPolicy)
	sizes := make([]int, 0, len(groups))
	for _, group := range groups {
		sizes = append(sizes, len(group))
	}
	if bigChunks != 3 || fmt.Sprint(sizes) != "[33 17 1 33 16]" {
		t.Fatalf("big run in %d sub-chunks, run groups %v; want 3 and [33 17 1 33 16]", bigChunks, sizes)
	}
}

// The wall bound still ends an attempt at once: no GitHub read starts once the clock passed MaxWallTime (an operation
// count on the injected clock). Filling groups raises the runs one 45 s attempt commits. Costs per MEASURE-14408-1: 1.04 s per jobs read, 0.136 s per sink write; 9 jobs per run (11 runs per group).
func TestChunkedRunsPhaseThroughputPerAttemptBaseVersusHead(t *testing.T) {
	wall := cicdDescriptor(t).ChunkPolicy.MaxWallTime
	attempt := func(handler CompleteRouteHandler) (int, *ChunkContinuationError, *runsGroupSink, *runsGroupDoer, CompleteRouteExecutor, *LeaseSession) {
		now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
		_, session := runsGroupSession(t, now)
		clock := &coalesceClock{now: now}
		doer, sink, store := newRunsGroupDoer(t, 300, 9), newRunsGroupSink(), newChunkMemoryStore()
		doer.clock, doer.readCost, doer.wallAt = clock, 1040*time.Millisecond, now.Add(wall)
		sink.clock, sink.writeCost = clock, 136*time.Millisecond
		executor := runsGroupExecutor(now, handler, store, sink, doer, clock)
		_, err := executor.Execute(context.Background(), session, cicdDescriptor(t))
		var stopped ChunkContinuationError
		if !errors.As(err, &stopped) {
			t.Fatalf("first attempt err=%v, want a wall continuation", err)
		}
		if stopped.Reason != ChunkStopWallTime || doer.readsPastWall != 0 {
			t.Fatalf("first attempt stopped on %s with %d GitHub reads started past the wall, want the wall and 0",
				stopped.Reason, doer.readsPastWall)
		}
		doer.wallAt = time.Time{}
		return len(sink.pipelineRows), &stopped, sink, doer, executor, session
	}
	base, baseStop, _, _, _, _ := attempt(noCoalesceHandler{})
	head, headStop, sink, doer, executor, session := attempt(GitHubTestsRouteHandler{})
	t.Logf("THROUGHPUT: runs per 45 s attempt base=%d (%d chunks) head=%d (%d chunks)", base, baseStop.Chunks, head, headStop.Chunks)
	if head*4 < base*5 {
		t.Fatalf("head commits %d runs per attempt, base %d: want at least 1.25x", head, base)
	}
	runUntilDone(t, executor, session, cicdDescriptor(t))
	assertEachRunOnce(t, sink, doer, 300)
}

// coalesceSyntheticHandler emits its listed emissions, then the final metadata emission.
type coalesceSyntheticHandler struct{ emissions []ChunkRouteEmission }

func (handler *coalesceSyntheticHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *coalesceSyntheticHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ time.Time, resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	last := resumeCursor
	if resumeCursor == "" {
		for _, emission := range handler.emissions {
			if err := emit(emission); err != nil {
				return err
			}
			last = emission.CursorAfter
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: last, CursorAfter: last, Final: true,
	})
}

// syntheticRunEmission is one coalescable emission of the seven cicd destinations with `rows` ci_pipeline_runs rows.
func syntheticRunEmission(t *testing.T, before, after string, rows int, pipelineRecovery EffectRecoveryPolicy) ChunkRouteEmission {
	t.Helper()
	effects := make([]EffectBatch, 0, 7)
	for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
		recovery := EffectReadbackRequired
		var values []json.RawMessage
		if destination == "ci_pipeline_runs" {
			recovery = pipelineRecovery
			for index := 0; index < rows; index++ {
				values = append(values, json.RawMessage(`{"org_id":"org-acme","run_id":"`+after+`-`+strconv.Itoa(index)+`"}`))
			}
		}
		effect, err := BuildEffectBatch(destination, recovery, values)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: effects}, CursorBefore: before, CursorAfter: after, Coalescable: true}
}

func runSynthetic(t *testing.T, emissions []ChunkRouteEmission) *chunkMemoryStore {
	t.Helper()
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	store := newChunkMemoryStore()
	executor := runsGroupExecutor(now, &coalesceSyntheticHandler{emissions: emissions}, store, newRunsGroupSink(),
		newRunsGroupDoer(t, 0, 0), nil)
	runUntilDone(t, executor, session, cicdDescriptor(t))
	return store
}

func storedCursors(store *chunkMemoryStore) []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	out := make([]string, 0, len(store.chunks))
	for ordinal := 0; ordinal < len(store.chunks); ordinal++ {
		chunk := store.chunks[ordinal]
		out = append(out, chunk.CursorBefore+">"+chunk.CursorAfter)
	}
	return out
}

// Only contiguous emissions join a group: e2 does not start where e1 ended, so e1 is committed alone and e2 + e3 form
// the next group with e2's own before cursor.
func TestChunkedCoalescingJoinsOnlyContiguousEmissions(t *testing.T) {
	store := runSynthetic(t, []ChunkRouteEmission{
		syntheticRunEmission(t, "a", "b", 1, EffectReadbackRequired),
		syntheticRunEmission(t, "c", "d", 1, EffectReadbackRequired),
		syntheticRunEmission(t, "d", "e", 1, EffectReadbackRequired),
	})
	if got := fmt.Sprint(storedCursors(store)); got != "[a>b c>e e>e]" {
		t.Fatalf("stored chunk cursors %s, want [a>b c>e e>e]", got)
	}
}

// A group stops at the row cap: 60 + 40 rows fill one sub-chunk of 100, the next 1 row starts a new group.
func TestChunkedCoalescingStopsAtTheRowCap(t *testing.T) {
	store := runSynthetic(t, []ChunkRouteEmission{
		syntheticRunEmission(t, "a", "b", 60, EffectReadbackRequired),
		syntheticRunEmission(t, "b", "c", 40, EffectReadbackRequired),
		syntheticRunEmission(t, "c", "d", 1, EffectReadbackRequired),
	})
	if got := fmt.Sprint(storedCursors(store)); got != "[a>c c>d d>d]" {
		t.Fatalf("stored chunk cursors %s, want [a>c c>d d>d]", got)
	}
}

// Two emissions whose shared destination has different recovery policies never share a group.
func TestChunkedCoalescingKeepsRecoveryPoliciesApart(t *testing.T) {
	store := runSynthetic(t, []ChunkRouteEmission{
		syntheticRunEmission(t, "a", "b", 1, EffectReadbackRequired),
		syntheticRunEmission(t, "b", "c", 1, EffectReplaySafe),
	})
	if got := fmt.Sprint(storedCursors(store)); got != "[a>b b>c c>c]" {
		t.Fatalf("stored chunk cursors %s, want [a>b b>c c>c]", got)
	}
}

// Only the runs phase marks its emissions coalescable: the open phase carries a skip event in its own emission's cursor
// (ObserveCommittedChunk reads it from the committed chunk), and the artifacts and final emissions keep one group each.
func TestGitHubTestsRouteMarksOnlyRunsPhaseEmissionsCoalescable(t *testing.T) {
	repoID := listingRepoID(t)
	fake := newListingFake(t, makeListingRuns(1, 3, listingSince, time.Minute))
	reader := openSkipStore(repoID, fake, 7200, 3)
	doer := newOpenSkipDoer(fake)
	doer.runs[7201] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
	client := githubTestsClient(t, fakehttp.Client(doer))
	phases := map[string]int{}
	err := GitHubTestsRouteHandler{OpenRuns: reader}.CollectChunks(context.Background(), openWindowClaim(),
		providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			before, err := decodeGitHubTestsChunkCursor(emission.CursorBefore)
			if err != nil {
				return err
			}
			want := before.Phase == githubTestsRunsPhase && !emission.Final
			if emission.Coalescable != want {
				t.Errorf("emission in phase %q final=%t coalescable=%t, want %t", before.Phase, emission.Final, emission.Coalescable, want)
			}
			phases[before.Phase]++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if phases[githubTestsRunsPhase] == 0 || phases[githubTestsArtifactsPhase] == 0 || phases[githubTestsOpenPhase] == 0 {
		t.Fatalf("emissions by phase %v, want runs, artifacts and open emissions", phases)
	}
	t.Logf("emissions by phase %v", phases)
}
