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

// CHAOS-8328: the per-attempt bounds (chunk count and wall time) are checked after EVERY committed sub-chunk, also inside one provider
// emission. Before, they were checked only after the whole emission was committed, so one emission of N sub-chunks committed all N in one
// attempt whatever the bounds (measured on bigboy: 221 chunks in one attempt against a bound of 64; the r1 repro: 100 against 64).
// Invariant: an attempt commits at most MaxChunksPerAttempt sub-chunks, or stops at the first sub-chunk after the wall bound; the rest of the
// emission is already durable (prepared as one group before any commit: CHAOS-3821) and the next attempt drains it before it asks the provider
// for anything, so nothing is lost and nothing is written twice.

// emissionBoundHarness is the one-emission scenario: 100 rows in one destination, one prepared sub-chunk per row (MaxEffectRows = 1).
func emissionBoundHarness(t *testing.T, policy ChunkPolicy) (
	run func() (CompleteRouteExecutionResult, error), store *chunkMemoryStore, sink *recoveryRowSink, claim Claim,
) {
	t.Helper()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	policy.MaxEffectRows = 1
	descriptor.ChunkPolicy = policy
	store = newChunkMemoryStore()
	sink = &recoveryRowSink{}
	executor := completeRouteExecutor(now, &bigPageChunkHandler{rows: 100}, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	run = func() (CompleteRouteExecutionResult, error) {
		return executor.Execute(context.Background(), session, descriptor)
	}
	return run, store, sink, claim
}

func continuationOf(t *testing.T, err error) ChunkContinuationError {
	t.Helper()
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) {
		t.Fatalf("error=%v, want a chunk continuation", err)
	}
	return stopped
}

// The count bound ends the attempt after exactly MaxChunksPerAttempt sub-chunks of the one emission (the clock never advances: only the count can stop it).
func TestAnEmissionOfManySubchunksStopsAtTheCountBoundMidEmission(t *testing.T) {
	t.Parallel()
	policy := DefaultChunkPolicy()
	policy.MaxChunksPerAttempt = 5
	run, store, _, claim := emissionBoundHarness(t, policy)
	_, err := run()
	stopped := continuationOf(t, err)
	if stopped.Reason != ChunkStopChunkBound || stopped.Chunks != 5 {
		t.Fatalf("stop reason=%q chunks=%d, want %q after exactly 5 chunks (the bound), not the whole emission", stopped.Reason, stopped.Chunks, ChunkStopChunkBound)
	}
	checkpoint, loadErr := store.LoadChunkCheckpoint(context.Background(), claim, time.Now())
	if loadErr != nil || checkpoint.NextOrdinal != 5 || checkpoint.PreparedChunks <= checkpoint.NextOrdinal {
		t.Fatalf("checkpoint next=%d prepared=%d err=%v, want 5 committed and the rest of the emission durable but uncommitted",
			checkpoint.NextOrdinal, checkpoint.PreparedChunks, loadErr)
	}
}

// The wall bound ends the attempt at the first sub-chunk after it, not after the whole emission (a clock that advances 2 s per read, a 1 s bound).
func TestAnEmissionOfManySubchunksStopsAtTheWallBoundMidEmission(t *testing.T) {
	t.Parallel()
	policy := DefaultChunkPolicy()
	policy.MaxWallTime = time.Second
	policy.MaxChunksPerAttempt = 64
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, _ := Descriptor("github", "cicd")
	policy.MaxEffectRows = 1
	descriptor.ChunkPolicy = policy
	store := newChunkMemoryStore()
	executor := completeRouteExecutor(now, &bigPageChunkHandler{rows: 100}, store, &recoveryRowSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	clock := now
	executor.Now = func() time.Time { clock = clock.Add(2 * time.Second); return clock }
	_, err := executor.Execute(context.Background(), session, descriptor)
	stopped := continuationOf(t, err)
	if stopped.Reason != ChunkStopWallTime || stopped.Chunks != 1 {
		t.Fatalf("stop reason=%q chunks=%d, want %q after the first sub-chunk past the wall bound, not the whole emission", stopped.Reason, stopped.Chunks, ChunkStopWallTime)
	}
	checkpoint, loadErr := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if loadErr != nil || checkpoint.NextOrdinal != 1 || checkpoint.PreparedChunks <= 1 {
		t.Fatalf("checkpoint next=%d prepared=%d err=%v, want 1 committed and the rest durable", checkpoint.NextOrdinal, checkpoint.PreparedChunks, loadErr)
	}
}

// The uncommitted rest of the emission is not lost and not written twice: attempt after attempt (each at most the bound) the unit completes, with every
// row of the emission written exactly once.
func TestTheRestOfAnEmissionIsDrainedByTheNextAttemptsWithoutLossOrDoubleWrite(t *testing.T) {
	t.Parallel()
	policy := DefaultChunkPolicy()
	policy.MaxChunksPerAttempt = 5
	run, _, sink, _ := emissionBoundHarness(t, policy)
	attempts := 0
	for {
		attempts++
		if attempts > 200 {
			t.Fatal("the unit does not complete after 200 attempts")
		}
		_, err := run()
		if err == nil {
			break
		}
		stopped := continuationOf(t, err)
		if stopped.Chunks > policy.MaxChunksPerAttempt {
			t.Fatalf("attempt %d committed %d chunks, over the bound of %d", attempts, stopped.Chunks, policy.MaxChunksPerAttempt)
		}
	}
	if attempts < 20 {
		t.Fatalf("completed in %d attempts: 100 sub-chunks at 5 per attempt need at least 20", attempts)
	}
	if duplicated := sink.duplicates(); duplicated != 0 {
		t.Fatalf("%d row(s) written more than once across the attempts", duplicated)
	}
	if len(sink.rows) != 100 {
		t.Fatalf("distinct rows written=%d, want all 100 of the emission", len(sink.rows))
	}
}

// bigFinalChunkHandler emits ONE FINAL emission of `rows` rows: the case where the bounds fall inside the final emission (no later emission
// exists; the unit must still finish from durable state).
type bigFinalChunkHandler struct{ bigPageChunkHandler }

func (handler *bigFinalChunkHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ time.Time, _ string,
	emit func(ChunkRouteEmission) error,
) error {
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
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: effects, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: "", CursorAfter: `{"page":1}`, Final: true,
	})
}

// The bounds also fall inside the FINAL emission: the attempt stops mid-emission, the next attempts drain the rest, and the unit finishes (the final sub-chunk is
// committed by a later attempt and the inventory is completed from durable state), with every row written once and no attempt over the bound.
func TestTheBoundsAlsoFallInsideTheFinalEmissionAndTheUnitStillFinishes(t *testing.T) {
	t.Parallel()
	policy := DefaultChunkPolicy()
	policy.MaxChunksPerAttempt = 5
	policy.MaxEffectRows = 1
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	_, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, _ := Descriptor("github", "cicd")
	descriptor.ChunkPolicy = policy
	store := newChunkMemoryStore()
	sink := &recoveryRowSink{}
	executor := completeRouteExecutor(now, &bigFinalChunkHandler{bigPageChunkHandler{rows: 100}}, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	attempts := 0
	for {
		attempts++
		if attempts > 200 {
			t.Fatal("the unit does not finish after 200 attempts")
		}
		result, err := executor.Execute(context.Background(), session, descriptor)
		if err == nil {
			if result.Result == nil {
				t.Fatalf("the unit finished without its final result: %+v", result)
			}
			break
		}
		if stopped := continuationOf(t, err); stopped.Chunks > policy.MaxChunksPerAttempt {
			t.Fatalf("attempt %d committed %d chunks, over the bound of %d", attempts, stopped.Chunks, policy.MaxChunksPerAttempt)
		}
	}
	if attempts < 20 {
		t.Fatalf("finished in %d attempts: 100 sub-chunks at 5 per attempt need at least 20", attempts)
	}
	if sink.duplicates() != 0 || len(sink.rows) != 100 {
		t.Fatalf("rows written: distinct=%d duplicated=%d, want 100 and 0", len(sink.rows), sink.duplicates())
	}
}
