package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// 100 small runs (12 jobs each) fill chunk groups up to the 500-row cap of ci_job_runs: 41 + 41 + 18, each group one
// sub-chunk, each run written once. The base committed 100 groups.
func TestChunkedRunsPhaseFillsGroupsUpToTheRowCap(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	doer, sink, store := newRunsGroupDoer(t, 100, 12), newRunsGroupSink(), newChunkMemoryStore()
	runUntilDone(t, runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, store, sink, doer, nil), session, descriptor)
	assertEachRunOnce(t, sink, doer, 100)
	groups := storedRunGroups(t, store, descriptor.ChunkPolicy)
	sizes := make([]int, 0, len(groups))
	for _, group := range groups {
		sizes = append(sizes, len(group))
	}
	if fmt.Sprint(sizes) != "[41 41 18]" || sink.pipelineWrites != 3 {
		t.Fatalf("run groups %v (%d pipeline writes), want [41 41 18]", sizes, sink.pipelineWrites)
	}
	if len(store.chunks) != 4 {
		t.Fatalf("stored chunks=%d, want 3 run groups + the final chunk (a coalesced group is ONE sub-chunk)", len(store.chunks))
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
	runUntilDone(t, runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, store, sink, doer, nil), session, descriptor)
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

// A source error on a run's jobs read commits the runs held before it under the live lease (a group per run would
// already have committed them): the five held runs are written once and not read again; the failing run is read again.
func TestChunkedRunsPhaseStreamErrorCommitsTheHeldGroup(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	claim, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	doer, sink, store := newRunsGroupDoer(t, 40, 2), newRunsGroupSink(), newChunkMemoryStore()
	doer.failJobsOnce = runsGroupRunID(5)
	executor := runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, store, sink, doer, nil)
	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil || errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("attempt 1 err=%v, want the jobs read failure", err)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.PreparedChunks != 1 || checkpoint.NextOrdinal != 1 || len(sink.pipelineRows) != 5 {
		t.Fatalf("after the failed attempt: prepared=%d next=%d written runs=%d, want 1 1 5 (the held group committed)",
			checkpoint.PreparedChunks, checkpoint.NextOrdinal, len(sink.pipelineRows))
	}
	runUntilDone(t, executor, session, descriptor)
	assertEachRunOnce(t, sink, doer, 40)
	for index := 0; index < 40; index++ {
		want := 1
		if index == 5 {
			want = 2
		}
		if got := doer.jobGets[runsGroupRunID(index)]; got != want {
			t.Fatalf("run %d jobs read %d times, want %d", index, got, want)
		}
	}
}

// A run whose jobs split into several sub-chunks is never coalesced: it keeps its own prepared group and the yield
// inside it, and the attempt chunk bound holds; every run is written once. A run that arrives when a flush reaches the
// bound is staged as its own group (the lone 1 after the second 33).
func TestChunkedRunsPhaseBigRunKeepsItsOwnGroupAndTheBounds(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	descriptor.ChunkPolicy.MaxChunksPerAttempt = 2
	// A run is capped at 500 jobs, so under the 500-row bound no run can split: the 100-row bound is pinned here to keep a split run.
	descriptor.ChunkPolicy.MaxSourceItems = 100
	doer, sink, store := newRunsGroupDoer(t, 100, 3), newRunsGroupSink(), newChunkMemoryStore()
	doer.bigRun, doer.bigJobs = runsGroupRunID(50), 250
	stops := runUntilDone(t, runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, store, sink, doer, nil), session, descriptor)
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
	if bigChunks != 3 || fmt.Sprint(sizes) != "[33 17 1 33 1 15]" {
		t.Fatalf("big run in %d sub-chunks, run groups %v; want 3 and [33 17 1 33 1 15]", bigChunks, sizes)
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
	base, baseStop, _, _, _, _ := attempt(GitHubTestsRouteHandler{})
	head, headStop, sink, doer, executor, session := attempt(GitHubTestsRouteHandler{CoalesceRuns: true})
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

// A group stops at the row cap: 300 + 200 rows fill one sub-chunk of 500, the next 1 row starts a new group.
func TestChunkedCoalescingStopsAtTheRowCap(t *testing.T) {
	store := runSynthetic(t, []ChunkRouteEmission{
		syntheticRunEmission(t, "a", "b", 300, EffectReadbackRequired),
		syntheticRunEmission(t, "b", "c", 200, EffectReadbackRequired),
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

// Only the flag-on runs phase marks its emissions coalescable; the open phase (its skip event rides in its own
// emission's cursor), the artifacts phase and the final emission stay unmarked. Flag off marks nothing and emits no
// barrier. (Here the artifacts emissions already end the held group, so no barrier is needed before the open phase.)
func TestGitHubTestsRouteMarksOnlyRunsPhaseEmissionsCoalescable(t *testing.T) {
	for _, on := range []bool{true, false} {
		repoID := listingRepoID(t)
		fake := newListingFake(t, makeListingRuns(1, 3, listingSince, time.Minute))
		reader := openSkipStore(repoID, fake, 7200, 3)
		doer := newOpenSkipDoer(fake)
		doer.runs[7201] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
		client := githubTestsClient(t, fakehttp.Client(doer))
		phases, barriers, unfenced, lastCoalescable := map[string]int{}, 0, 0, false
		err := GitHubTestsRouteHandler{OpenRuns: reader, CoalesceRuns: on}.CollectChunks(context.Background(), openWindowClaim(),
			providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
			func(emission ChunkRouteEmission) error {
				if emission.Barrier {
					barriers++
					lastCoalescable = false
					return nil
				}
				before, err := decodeGitHubTestsChunkCursor(emission.CursorBefore)
				if err != nil {
					return err
				}
				want := on && before.Phase == githubTestsRunsPhase && !emission.Final
				if emission.Coalescable != want {
					t.Errorf("flag %t: emission in phase %q final=%t coalescable=%t, want %t", on, before.Phase, emission.Final, emission.Coalescable, want)
				}
				if before.Phase == githubTestsOpenPhase && lastCoalescable {
					unfenced++
				}
				lastCoalescable = emission.Coalescable
				phases[before.Phase]++
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if phases[githubTestsRunsPhase] == 0 || phases[githubTestsArtifactsPhase] == 0 || phases[githubTestsOpenPhase] == 0 {
			t.Fatalf("flag %t: emissions by phase %v, want runs, artifacts and open emissions", on, phases)
		}
		if unfenced != 0 || (!on && barriers != 0) {
			t.Fatalf("flag %t: %d barriers, %d open emissions right after a coalescable one; want none unfenced and no barrier when off",
				on, barriers, unfenced)
		}
	}
}

// The re-seek after a moved page re-writes one surviving run with the flag on, exactly as the base does
// (TestGitHubTestsMovedPageReseekRewriteBaseControl): coalescing does not add a rewrite.
func TestGitHubTestsMovedPageReseekRewriteMatchesTheBase(t *testing.T) {
	off := reseekRewrittenRuns(t, GitHubTestsRouteHandler{})
	on := reseekRewrittenRuns(t, GitHubTestsRouteHandler{CoalesceRuns: true})
	if on != off || off != 1 {
		t.Fatalf("runs written twice after the re-seek: flag on %d, off %d, want 1 and 1", on, off)
	}
}

// A run read at or after the wall is committed by its attempt with the flag on, as on the base: 2 runs either way.
func TestGitHubTestsRunReadAtTheWallMatchesTheBase(t *testing.T) {
	for _, late := range []time.Duration{0, time.Millisecond} {
		offRuns, offPipelines := wallDurableRuns(t, GitHubTestsRouteHandler{}, late)
		onRuns, onPipelines := wallDurableRuns(t, GitHubTestsRouteHandler{CoalesceRuns: true}, late)
		if onRuns != offRuns || onPipelines != offPipelines || offRuns != 2 {
			t.Fatalf("late %s: first attempt runs on %d/%d off %d/%d, want 2/2 both", late, onRuns, onPipelines, offRuns, offPipelines)
		}
	}
}

// The run whose arrival ends a full group is kept when that group's flush stops the attempt: on the chunk bound
// (continuation) and on a sink failure (error), the arriving run is prepared as its own group and the next attempt
// drains it without reading GitHub again.
func TestChunkedRunsPhaseFlushStopStagesTheArrivingRun(t *testing.T) {
	for _, path := range []string{"continuation", "error"} {
		t.Run(path, func(t *testing.T) {
			now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
			claim, session := runsGroupSession(t, now)
			descriptor := cicdDescriptor(t)
			doer, sink, store := newRunsGroupDoer(t, 100, 3), newRunsGroupSink(), newChunkMemoryStore()
			wantNext := 0
			if path == "continuation" {
				descriptor.ChunkPolicy.MaxChunksPerAttempt = 1
				wantNext = 1
			} else {
				sink.failJobWrite = 1
			}
			executor := runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, store, sink, doer, nil)
			_, err := executor.Execute(context.Background(), session, descriptor)
			isStop := errors.As(err, new(ChunkContinuationError))
			if err == nil || isStop != (path == "continuation") {
				t.Fatalf("attempt 1 err=%v, want the %s", err, path)
			}
			checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
			if err != nil {
				t.Fatal(err)
			}
			if checkpoint.PreparedChunks != 2 || checkpoint.NextOrdinal != wantNext {
				t.Fatalf("after attempt 1 prepared=%d next=%d, want 2 %d (the full group and the arriving run 33 prepared)",
					checkpoint.PreparedChunks, checkpoint.NextOrdinal, wantNext)
			}
			runUntilDone(t, executor, session, descriptor)
			assertEachRunOnce(t, sink, doer, 100)
			for index := 0; index < 100; index++ {
				if gets := doer.jobGets[runsGroupRunID(index)]; gets != 1 {
					t.Fatalf("run %d jobs read %d times, want once", index, gets)
				}
			}
		})
	}
}

// barrierCountingHandler counts the barrier emissions the route sends to the executor.
type barrierCountingHandler struct {
	GitHubTestsRouteHandler
	barriers *int
}

func (handler barrierCountingHandler) CollectChunks(
	ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient,
	normalizedAt time.Time, resumeCursor string, emit func(ChunkRouteEmission) error,
) error {
	return handler.GitHubTestsRouteHandler.CollectChunks(ctx, claim, credential, client, normalizedAt, resumeCursor,
		func(emission ChunkRouteEmission) error {
			if emission.Barrier {
				*handler.barriers++
			}
			return emit(emission)
		})
}

// openBarrierSink writes like the ClickHouse sink and refreshes the stored open rows the way the open reader sees them.
type openBarrierSink struct {
	*runsGroupSink
	reader *fakeOpenRuns
}

func (sink openBarrierSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.runsGroupSink.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	if effect.Destination == "ci_pipeline_runs" {
		rows, err := decodeEffectRows[githubTestsPipelineRow](effect)
		if err != nil {
			return err
		}
		sink.reader.finalize(rows)
	}
	return nil
}

// The open phase reads stored pipeline rows, so the runs-phase group the executor holds is made durable first (the
// barrier): a run stored as running, read completed by the runs phase and gone from GitHub before the artifacts listing,
// is written once as success and never re-read by id or written as deleted.
func TestGitHubTestsOpenPhaseReadsAfterTheHeldRunsAreDurable(t *testing.T) {
	for _, on := range []bool{false, true} {
		for _, oldStatus := range []string{"running", "queued"} {
			since, before := listingSince.Add(-time.Hour), listingSince.Add(time.Hour)
			now := before.Add(time.Hour)
			claim, session := runsGroupWindowSession(t, now, since, before)
			created := since.Add(-48 * time.Hour)
			runs := makeListingRuns(42, 1, created, time.Minute)
			runs[0].updated = before
			fake := newListingFake(t, runs)
			reader := &fakeOpenRuns{rows: []githubTestsPipelineRow{openRow(listingRepoID(t), "42", created, oldStatus)}}
			sink := newRunsGroupSink()
			barriers := 0
			executor := runsGroupExecutor(now, barrierCountingHandler{GitHubTestsRouteHandler{OpenRuns: reader, CoalesceRuns: on}, &barriers},
				NewInProcessChunkLedger(claim), sink, coalesceDoerFunc(func(request *http.Request) (*http.Response, error) {
					response, err := fake.Do(request)
					if strings.HasSuffix(request.URL.Path, "/42/jobs") {
						fake.runs = nil
					}
					return response, err
				}), nil)
			executor.Committer.Sink = openBarrierSink{sink, reader}
			runUntilDone(t, executor, session, cicdDescriptor(t))
			if wantBarriers := map[bool]int{true: 1, false: 0}[on]; barriers != wantBarriers {
				t.Fatalf("flag %t stored %s: %d barriers, want %d", on, oldStatus, barriers, wantBarriers)
			}
			if status := stringPtrValue(reader.rows[0].Status); sink.pipelineRows["42"] != 1 || len(fake.runGets) != 0 || status != "success" {
				t.Fatalf("flag %t stored %s: run 42 written %d times, by-id reads %v, last status %q; want 1, none, success",
					on, oldStatus, sink.pipelineRows["42"], fake.runGets, status)
			}
		}
	}
}

// A barrier with nothing held is a no-op; one between two contiguous emissions ends the held group there.
func TestChunkedBarrierFlushesOnlyAHeldGroup(t *testing.T) {
	store := runSynthetic(t, []ChunkRouteEmission{
		{Barrier: true},
		syntheticRunEmission(t, "a", "b", 1, EffectReadbackRequired),
		syntheticRunEmission(t, "b", "c", 1, EffectReadbackRequired),
	})
	if got := fmt.Sprint(storedCursors(store)); got != "[a>c c>c]" {
		t.Fatalf("leading barrier: stored chunk cursors %s, want [a>c c>c]", got)
	}
	store = runSynthetic(t, []ChunkRouteEmission{
		syntheticRunEmission(t, "a", "b", 1, EffectReadbackRequired),
		{Barrier: true},
		syntheticRunEmission(t, "b", "c", 1, EffectReadbackRequired),
	})
	if got := fmt.Sprint(storedCursors(store)); got != "[a>b b>c c>c]" {
		t.Fatalf("barrier between: stored chunk cursors %s, want [a>b b>c c>c]", got)
	}
}

// An emission kind the executor does not know fails the attempt loudly: a barrier that carries anything else (any
// cursor, flag or batch field: the batch must be the zero CompleteRouteBatch), and a final emission marked coalescable.
func TestChunkedExecutorRefusesAnUnknownEmissionKind(t *testing.T) {
	barrierWithBatch := syntheticRunEmission(t, "a", "b", 1, EffectReadbackRequired)
	barrierWithBatch.Barrier, barrierWithBatch.Coalescable = true, false
	finalCoalescable := syntheticRunEmission(t, "a", "b", 1, EffectReadbackRequired)
	finalCoalescable.Final = true
	for name, emission := range map[string]ChunkRouteEmission{
		"barrier and final":          {Barrier: true, Final: true},
		"barrier and coalescable":    {Barrier: true, Coalescable: true},
		"barrier with a cursor":      {Barrier: true, CursorAfter: "x"},
		"barrier with a batch":       barrierWithBatch,
		"final and coalescable":      finalCoalescable,
		"barrier with evidence":      {Barrier: true, Batch: CompleteRouteBatch{Evidence: FetchEvidence{Provider: "github"}}},
		"barrier with worklogs":      {Barrier: true, Batch: CompleteRouteBatch{WorklogObservations: []JiraWorklogFetchObservation{{}}}},
		"barrier with empty effects": {Barrier: true, Batch: CompleteRouteBatch{Effects: []EffectBatch{}}},
	} {
		now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
		_, session := runsGroupSession(t, now)
		store := newChunkMemoryStore()
		executor := runsGroupExecutor(now, &coalesceSyntheticHandler{emissions: []ChunkRouteEmission{emission}}, store,
			newRunsGroupSink(), newRunsGroupDoer(t, 0, 0), nil)
		if _, err := executor.Execute(context.Background(), session, cicdDescriptor(t)); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s: err=%v, want ErrInvalidConfiguration", name, err)
		}
		if len(store.chunks) != 0 {
			t.Fatalf("%s: %d chunks stored, want none", name, len(store.chunks))
		}
	}
}

// A terminal source error after held runs commits them under the live lease, as the base had already committed them;
// the error keeps its class. A lost lease writes nothing; a failed flush is joined to the source error.
func TestChunkedRunsPhaseTerminalErrorKeepsTheHeldRuns(t *testing.T) {
	sinkDown := errors.New("sink down")
	for _, cell := range []string{"flag off", "flag on", "lease lost", "flush fails"} {
		t.Run(cell, func(t *testing.T) {
			since, before := listingSince, listingSince.Add(2*time.Hour)
			now := before.Add(time.Hour)
			claim, session := runsGroupWindowSession(t, now, since, before)
			fake := newListingFake(t, makeListingRuns(42, 3, since, time.Second))
			sink := newRunsGroupSink()
			failWrites := false
			store := NewInProcessChunkLedger(claim)
			executor := runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: cell != "flag off"}, store, sink,
				coalesceDoerFunc(func(request *http.Request) (*http.Response, error) {
					if strings.HasSuffix(request.URL.Path, "/42/jobs") {
						switch cell {
						case "lease lost":
							leases := session.Repository.(*memoryLeaseRepository)
							leases.mu.Lock()
							leases.terminal = true
							leases.mu.Unlock()
						case "flush fails":
							failWrites = true
						}
						return fake.respond(request, http.Header{}, `{"message":"Not Found"}`, http.StatusNotFound), nil
					}
					return fake.Do(request)
				}), nil)
			executor.Committer.Sink = failingAfterSink{sink, &failWrites, sinkDown}
			_, err := executor.Execute(context.Background(), session, cicdDescriptor(t))
			var providerErr *providerfoundation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorNotFound {
				t.Fatalf("err=%v, want the provider not-found class", err)
			}
			want := map[string]int{"flag off": 2, "flag on": 2, "lease lost": 0, "flush fails": 0}[cell]
			if len(sink.pipelineRows) != want {
				t.Fatalf("runs written %v, want %d", sink.pipelineRows, want)
			}
			if cell == "flush fails" && !errors.Is(err, sinkDown) {
				t.Fatalf("err=%v, want the flush failure joined", err)
			}
		})
	}
}

// failingAfterSink fails every write once *fail is set.
type failingAfterSink struct {
	*runsGroupSink
	fail *bool
	err  error
}

func (sink failingAfterSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if *sink.fail && len(effect.Rows) > 0 {
		return sink.err
	}
	return sink.runsGroupSink.WriteEffect(ctx, claim, effect)
}

// prepareFailStore fails its failAt-th PrepareChunkGroup call once, before anything is stored.
type prepareFailStore struct {
	*chunkMemoryStore
	failAt, calls int
}

func (store *prepareFailStore) PrepareChunkGroup(
	ctx context.Context, claim Claim, chunks []PreparedProviderChunk, now time.Time,
) ([]PreparedProviderChunk, error) {
	store.calls++
	if store.calls == store.failAt {
		return nil, errors.New("store unavailable")
	}
	return store.chunkMemoryStore.PrepareChunkGroup(ctx, claim, chunks, now)
}

// A held group whose prepare fails is not durable, so nothing is staged after it: the attempt fails with the prepare
// error, the checkpoint stays where it was, and the retry writes every run once (a staged successor would take the held
// group's ordinal and move the cursor past runs never written).
func TestChunkedRunsPhaseNothingIsStagedAfterAFailedPrepare(t *testing.T) {
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	claim, session := runsGroupSession(t, now)
	descriptor := cicdDescriptor(t)
	doer, sink := newRunsGroupDoer(t, 100, 3), newRunsGroupSink()
	store := &prepareFailStore{chunkMemoryStore: newChunkMemoryStore(), failAt: 1}
	executor := runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, store, sink, doer, nil)
	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil || errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("attempt 1 err=%v, want the prepare failure", err)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil && !errors.Is(err, ErrChunkCheckpointNotFound) {
		t.Fatal(err)
	}
	if checkpoint.PreparedChunks != 0 || checkpoint.NextCursor != "" {
		t.Fatalf("after the failed prepare: prepared=%d cursor set=%t, want 0 and unset", checkpoint.PreparedChunks, checkpoint.NextCursor != "")
	}
	runUntilDone(t, executor, session, descriptor)
	assertEachRunOnce(t, sink, doer, 100)
}

// The source-error flush that commits the held group and reaches an attempt bound does not turn the source error into
// a continuation: the worker sees the provider not-found and does not snooze into the same 404.
func TestChunkedRunsPhaseSourceErrorFlushAtTheBoundKeepsTheSourceError(t *testing.T) {
	since, before := listingSince, listingSince.Add(2*time.Hour)
	now := before.Add(time.Hour)
	claim, session := runsGroupWindowSession(t, now, since, before)
	descriptor := cicdDescriptor(t)
	descriptor.ChunkPolicy.MaxChunksPerAttempt = 1
	fake := newListingFake(t, makeListingRuns(42, 3, since, time.Second))
	sink := newRunsGroupSink()
	executor := runsGroupExecutor(now, GitHubTestsRouteHandler{CoalesceRuns: true}, NewInProcessChunkLedger(claim), sink,
		coalesceDoerFunc(func(request *http.Request) (*http.Response, error) {
			if strings.HasSuffix(request.URL.Path, "/42/jobs") {
				return fake.respond(request, http.Header{}, `{"message":"Not Found"}`, http.StatusNotFound), nil
			}
			return fake.Do(request)
		}), nil)
	_, err := executor.Execute(context.Background(), session, descriptor)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorNotFound || errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("err=%v, want the provider not-found alone (no continuation)", err)
	}
	if len(sink.pipelineRows) != 2 {
		t.Fatalf("runs written %v, want the 2 held runs", sink.pipelineRows)
	}
}

// Coalescing ships dark: the production wiring leaves CoalesceRuns unset, and an unset flag is the base path: every
// run is its own chunk group, no emission is coalescable and no barrier is sent (0 held groups).
func TestGitHubTestsCoalescingShipsDark(t *testing.T) {
	for _, dataset := range []string{"cicd", "tests"} {
		route, ok := SelectGitFamilyRoute("github", dataset, GitFamilyDeps{Conn: selectorConn{}})
		handler, isTests := route.Handler.(GitHubTestsRouteHandler)
		if !ok || !isTests || handler.CoalesceRuns {
			t.Fatalf("production github/%s route: ok=%t tests handler=%t CoalesceRuns=%t, want true true false", dataset, ok, isTests, handler.CoalesceRuns)
		}
	}
	now := time.Date(2026, 7, 22, 21, 0, 0, 0, time.UTC)
	_, session := runsGroupSession(t, now)
	doer, sink, store := newRunsGroupDoer(t, 100, 3), newRunsGroupSink(), newChunkMemoryStore()
	counted := &emissionCountingHandler{}
	runUntilDone(t, runsGroupExecutor(now, counted, store, sink, doer, nil), session, cicdDescriptor(t))
	assertEachRunOnce(t, sink, doer, 100)
	if sink.pipelineWrites != 100 || counted.coalescable != 0 || counted.barriers != 0 {
		t.Fatalf("flag unset: %d run groups, %d coalescable emissions, %d barriers; want 100, 0, 0",
			sink.pipelineWrites, counted.coalescable, counted.barriers)
	}
}

// emissionCountingHandler is the tests route with its flag unset, counting coalescable and barrier emissions.
type emissionCountingHandler struct {
	GitHubTestsRouteHandler
	coalescable, barriers int
}

func (handler *emissionCountingHandler) CollectChunks(
	ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient,
	normalizedAt time.Time, resumeCursor string, emit func(ChunkRouteEmission) error,
) error {
	return handler.GitHubTestsRouteHandler.CollectChunks(ctx, claim, credential, client, normalizedAt, resumeCursor,
		func(emission ChunkRouteEmission) error {
			if emission.Coalescable {
				handler.coalescable++
			}
			if emission.Barrier {
				handler.barriers++
			}
			return emit(emission)
		})
}
