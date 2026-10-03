package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-7692: an attempt of a large chunked unit used to end after 8 chunks
// (~5 s of work on prod), so a heavy unit needed ~100-400 continuations and held
// its cap slot through every 1 s snooze hand-off. The default policy now lets the
// wall bound (45 s) end a normal attempt; the count bound sits just under it.

// pagedChunkHandler streams `pages` provider pages, one distinct row per
// destination per page, resuming from the cursor like a paginating route.
type pagedChunkHandler struct {
	pages int
}

func (handler *pagedChunkHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func pagedChunkCursor(page int) string { return `{"page":` + strconv.Itoa(page) + `}` }

func (handler *pagedChunkHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ time.Time, resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	start := 0
	if resumeCursor != "" {
		var cursor struct {
			Page int `json:"page"`
		}
		if err := json.Unmarshal([]byte(resumeCursor), &cursor); err != nil {
			return err
		}
		start = cursor.Page
	}
	destinations := []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots"}
	for page := start; page < handler.pages; page++ {
		effects := make([]EffectBatch, 0, len(destinations))
		for _, destination := range destinations {
			row := json.RawMessage(`{"org_id":"` + claim.OrgID + `","page":` + strconv.Itoa(page) +
				`,"destination":"` + destination + `"}`)
			effect, err := BuildEffectBatch(destination, EffectReplaySafe, []json.RawMessage{row})
			if err != nil {
				return err
			}
			effects = append(effects, effect)
		}
		if err := emit(ChunkRouteEmission{
			Batch:        CompleteRouteBatch{Effects: effects},
			CursorBefore: pagedChunkCursor(page), CursorAfter: pagedChunkCursor(page + 1),
		}); err != nil {
			return err
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: pagedChunkCursor(handler.pages), CursorAfter: pagedChunkCursor(handler.pages), Final: true,
	})
}

func pagedChunkExecutor(t *testing.T, pages int, sink EffectSink, store *chunkMemoryStore) (
	CompleteRouteExecutor, *LeaseSession, CompleteRouteDescriptor, Claim,
) {
	t.Helper()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	descriptor.ChunkPolicy = DefaultChunkPolicy()
	executor := completeRouteExecutor(now, &pagedChunkHandler{pages: pages}, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	return executor, session, descriptor, claim
}

func TestDefaultChunkPolicyLetsTheWallBoundEndANormalAttempt(t *testing.T) {
	t.Parallel()
	policy := DefaultChunkPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("default policy invalid: %v", err)
	}
	// At the ~0.66 s a chunk measured on prod, 64 chunks are ~42 s: the count bound
	// is chosen to sit just under the 45 s wall (the value is pinned: a larger one
	// would let the count bound drift above the wall for a normal attempt).
	if policy.MaxChunksPerAttempt != 64 || policy.MaxWallTime != 45*time.Second {
		t.Fatalf("default policy chunks=%d wall=%s, want exactly 64 chunks and a 45 s wall",
			policy.MaxChunksPerAttempt, policy.MaxWallTime)
	}
}

// With a clock that advances while chunks commit, a slow-chunk attempt must stop
// on the WALL bound after more than the old 8 chunks, and say so.
func TestChunkedAttemptStopsOnTheWallBoundNotTheOldEightChunks(t *testing.T) {
	t.Parallel()
	store := newChunkMemoryStore()
	executor, session, descriptor, claim := pagedChunkExecutor(t, 200, &recoveryRowSink{}, store)
	clock := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	executor.Now = func() time.Time {
		clock = clock.Add(250 * time.Millisecond)
		return clock
	}

	_, err := executor.Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) {
		t.Fatalf("first attempt error=%v, want a chunk continuation", err)
	}
	if stopped.Reason != ChunkStopWallTime {
		t.Fatalf("stop reason=%q after %d chunks, want %q", stopped.Reason, stopped.Chunks, ChunkStopWallTime)
	}
	if stopped.Chunks <= 8 || stopped.Chunks >= DefaultChunkPolicy().MaxChunksPerAttempt {
		t.Fatalf("committed %d chunks before the wall bound, want more than 8 and fewer than %d",
			stopped.Chunks, DefaultChunkPolicy().MaxChunksPerAttempt)
	}
	if stopped.Elapsed < DefaultChunkPolicy().MaxWallTime {
		t.Fatalf("elapsed=%s, want at least the %s wall bound", stopped.Elapsed, DefaultChunkPolicy().MaxWallTime)
	}
	checkpoint, loadErr := store.LoadChunkCheckpoint(context.Background(), claim, time.Now())
	if loadErr != nil || checkpoint.NextOrdinal != stopped.Chunks {
		t.Fatalf("checkpoint NextOrdinal=%d err=%v, want the %d committed chunks", checkpoint.NextOrdinal, loadErr, stopped.Chunks)
	}
}

// A crash now loses up to a whole 45 s attempt of chunks, not 8: the fence has
// to keep recovery idempotent at that size. The sink fails in chunk 13 of the
// first attempt (more than the old 8-chunk attempt could ever reach); recovery
// must finish without writing any row twice.
func TestChunkedCrashBeyondTheOldEightChunksStaysIdempotent(t *testing.T) {
	t.Parallel()
	store := newChunkMemoryStore()
	const pages, crashChunk = 40, 13
	sink := &recoveryRowSink{failFrom: 6*(crashChunk-1) + 1}
	executor, session, descriptor, claim := pagedChunkExecutor(t, pages, sink, store)

	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil ||
		errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("attempt 1 error=%v, want a sink failure in chunk %d (not a continuation)", err, crashChunk)
	}
	crash, err := store.LoadChunkCheckpoint(context.Background(), claim, time.Now())
	if err != nil || crash.NextOrdinal < 8 {
		t.Fatalf("crash checkpoint NextOrdinal=%d err=%v, want more than the old 8 chunks committed", crash.NextOrdinal, err)
	}

	sink.failFrom = 0
	recovery, recoverySession, recoveryDescriptor, _ := pagedChunkExecutor(t, pages, sink, store)
	if _, err := recovery.Execute(context.Background(), recoverySession, recoveryDescriptor); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if duplicated := sink.duplicates(); duplicated != 0 {
		t.Fatalf("%d row(s) physically written more than once across the crash boundary", duplicated)
	}
	if len(sink.rows) != pages*6 {
		t.Fatalf("distinct rows written=%d, want %d", len(sink.rows), pages*6)
	}
}

// bigPageChunkHandler emits ONE provider page of `rows` rows in a single
// destination, which the policy splits into one prepared chunk per row.
type bigPageChunkHandler struct{ rows int }

func (handler *bigPageChunkHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *bigPageChunkHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ time.Time, resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	if resumeCursor == "" {
		rows := make([]json.RawMessage, 0, handler.rows)
		for index := 0; index < handler.rows; index++ {
			rows = append(rows, json.RawMessage(`{"org_id":"`+claim.OrgID+`","row":`+strconv.Itoa(index)+`}`))
		}
		effects := make([]EffectBatch, 0, 6)
		for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots"} {
			destinationRows := []json.RawMessage(nil)
			if destination == "ci_pipeline_runs" {
				destinationRows = rows
			}
			effect, err := BuildEffectBatch(destination, EffectReplaySafe, destinationRows)
			if err != nil {
				return err
			}
			effects = append(effects, effect)
		}
		if err := emit(ChunkRouteEmission{
			Batch: CompleteRouteBatch{Effects: effects}, CursorAfter: `{"page":1}`,
		}); err != nil {
			return err
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: `{"page":1}`, CursorAfter: `{"page":1}`, Final: true,
	})
}

// Draining chunks that a crash left prepared-but-uncommitted obeys the same
// wall bound as collecting new ones: 97 leftover chunks must not be committed in
// one attempt just because the count bound is large.
func TestChunkedDrainOfPreparedChunksStopsOnTheWallBound(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	policy := DefaultChunkPolicy()
	policy.MaxEffectRows = 1
	descriptor.ChunkPolicy = policy
	store := newChunkMemoryStore()
	sink := &recoveryRowSink{failFrom: 3}
	build := func(sink EffectSink) CompleteRouteExecutor {
		executor := completeRouteExecutor(now, &bigPageChunkHandler{rows: 100}, store, sink)
		executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
		executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
		return executor
	}
	_, attemptErr := build(sink).Execute(context.Background(), session, descriptor)
	if attemptErr == nil || errors.Is(attemptErr, ErrChunkContinuation) {
		t.Fatalf("attempt 1 error=%v, want a sink failure that leaves chunks prepared", attemptErr)
	}
	t.Logf("attempt 1 error: %v", attemptErr)
	crash, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil || crash.PreparedChunks-crash.NextOrdinal < 90 {
		t.Fatalf("premise: prepared=%d next=%d err=%v, want ~97 chunks left to drain", crash.PreparedChunks, crash.NextOrdinal, err)
	}

	sink.failFrom = 0
	recovery := build(sink)
	clock := now
	recovery.Now = func() time.Time {
		clock = clock.Add(250 * time.Millisecond)
		return clock
	}
	_, err = recovery.Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || stopped.Reason != ChunkStopWallTime {
		t.Fatalf("drain attempt error=%v stop=%+v, want a continuation on %q", err, stopped, ChunkStopWallTime)
	}
	if stopped.Chunks < 1 || stopped.Chunks >= policy.MaxChunksPerAttempt {
		t.Fatalf("drained %d chunks, want fewer than the %d-chunk count bound", stopped.Chunks, policy.MaxChunksPerAttempt)
	}
}

// Production builds the executor WITHOUT an injected clock (workerservice/provider_sync.go): the attempt is then measured on the
// process clock, and the wall bound must still end an attempt there. A wall bound of a few nanoseconds makes the real clock end
// it after the first committed chunk; a dead process-clock branch (always zero) never ends it.
func TestChunkedAttemptWallBoundUsesTheProcessClockWhenNoClockIsInjected(t *testing.T) {
	t.Parallel()
	store := newChunkMemoryStore()
	executor, session, descriptor, _ := pagedChunkExecutor(t, 200, &recoveryRowSink{}, store)
	executor.Now = nil
	descriptor.ChunkPolicy.MaxWallTime = time.Nanosecond

	_, err := executor.Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || stopped.Reason != ChunkStopWallTime {
		t.Fatalf("error=%v stop=%+v, want a continuation on %q from the process clock", err, stopped, ChunkStopWallTime)
	}
	if stopped.Chunks < 1 || stopped.Chunks >= DefaultChunkPolicy().MaxChunksPerAttempt {
		t.Fatalf("committed %d chunks, want the wall bound to end the attempt well before the %d-chunk bound",
			stopped.Chunks, DefaultChunkPolicy().MaxChunksPerAttempt)
	}
	if stopped.Elapsed < time.Nanosecond {
		t.Fatalf("elapsed=%s, want a measured process-clock duration", stopped.Elapsed)
	}
}

// batchOnlyChunkHandler serves one collected batch through Collect and implements no CollectChunks, so the NON-streaming chunked
// executor runs it (no route registered today needs that executor, but its bounds are changed by this work and must hold).
type batchOnlyChunkHandler struct{ rows int }

func (handler *batchOnlyChunkHandler) Collect(
	_ context.Context, claim Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient, _ time.Time,
) (CompleteRouteBatch, error) {
	rows := make([]json.RawMessage, 0, handler.rows)
	for index := 0; index < handler.rows; index++ {
		rows = append(rows, json.RawMessage(`{"org_id":"`+claim.OrgID+`","row":`+strconv.Itoa(index)+`}`))
	}
	effects := make([]EffectBatch, 0, 6)
	for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots"} {
		destinationRows := []json.RawMessage(nil)
		if destination == "ci_pipeline_runs" {
			destinationRows = rows
		}
		effect, err := BuildEffectBatch(destination, EffectReplaySafe, destinationRows)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		effects = append(effects, effect)
	}
	return CompleteRouteBatch{Effects: effects, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt}, nil
}

func batchOnlyExecutor(t *testing.T, rows int, policy ChunkPolicy) (CompleteRouteExecutor, *LeaseSession, CompleteRouteDescriptor) {
	t.Helper()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	_, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	descriptor.ChunkPolicy = policy
	executor := completeRouteExecutor(now, &batchOnlyChunkHandler{rows: rows}, newChunkMemoryStore(), &recoveryRowSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	return executor, session, descriptor
}

// The non-streaming executor ends an attempt on the wall bound too, and reports the elapsed time it measured.
func TestNonStreamingChunkedExecutorStopsOnTheWallBoundAndReportsElapsed(t *testing.T) {
	t.Parallel()
	policy := DefaultChunkPolicy()
	policy.MaxEffectRows = 1
	executor, session, descriptor := batchOnlyExecutor(t, 100, policy)
	clock := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	executor.Now = func() time.Time {
		clock = clock.Add(250 * time.Millisecond)
		return clock
	}
	_, err := executor.Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || stopped.Reason != ChunkStopWallTime {
		t.Fatalf("error=%v stop=%+v, want a continuation on %q", err, stopped, ChunkStopWallTime)
	}
	if stopped.Chunks <= 8 || stopped.Chunks >= policy.MaxChunksPerAttempt {
		t.Fatalf("committed %d chunks, want more than 8 and fewer than %d", stopped.Chunks, policy.MaxChunksPerAttempt)
	}
	if stopped.Elapsed < policy.MaxWallTime {
		t.Fatalf("elapsed=%s, want at least the %s wall bound (the continuation reports the attempt's measured time)", stopped.Elapsed, policy.MaxWallTime)
	}
}

// The count bound of the prepared-chunk drain: with a clock that never advances only the count can end the attempt.
func TestChunkedDrainOfPreparedChunksStopsOnTheCountBound(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	policy := DefaultChunkPolicy()
	policy.MaxEffectRows = 1
	policy.MaxChunksPerAttempt = 3
	descriptor.ChunkPolicy = policy
	store := newChunkMemoryStore()
	sink := &recoveryRowSink{failFrom: 3}
	build := func(sink EffectSink) CompleteRouteExecutor {
		executor := completeRouteExecutor(now, &bigPageChunkHandler{rows: 100}, store, sink)
		executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
		executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
		return executor
	}
	if _, err := build(sink).Execute(context.Background(), session, descriptor); err == nil || errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("attempt 1 error=%v, want a sink failure that leaves chunks prepared", err)
	}
	crash, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil || crash.PreparedChunks-crash.NextOrdinal < 90 {
		t.Fatalf("premise: prepared=%d next=%d err=%v, want ~97 chunks left to drain", crash.PreparedChunks, crash.NextOrdinal, err)
	}
	sink.failFrom = 0
	_, err = build(sink).Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || stopped.Reason != ChunkStopChunkBound || stopped.Chunks != policy.MaxChunksPerAttempt {
		t.Fatalf("drain error=%v stop=%+v, want a continuation on %q after exactly %d chunks",
			err, stopped, ChunkStopChunkBound, policy.MaxChunksPerAttempt)
	}
}
