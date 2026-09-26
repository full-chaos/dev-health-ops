package providersync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRunToCompletionRepeatsTheAttemptUntilThereIsNoContinuation(t *testing.T) {
	calls := 0
	result, err := runToCompletion(context.Background(), func() (CompleteRouteExecutionResult, error) {
		calls++
		if calls < 4 {
			return CompleteRouteExecutionResult{}, ChunkContinuationError{Next: time.Now().Add(time.Hour)}
		}
		return CompleteRouteExecutionResult{CommittedRows: 7}, nil
	})
	if err != nil || calls != 4 || result.CommittedRows != 7 {
		t.Fatalf("calls=%d result=%+v err=%v: three continuations, then the run's own result", calls, result, err)
	}
	// A failure that is not a continuation ends the run at once, unchanged.
	failure := errors.New("provider said no")
	calls = 0
	if _, err := runToCompletion(context.Background(), func() (CompleteRouteExecutionResult, error) {
		calls++
		return CompleteRouteExecutionResult{}, failure
	}); !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRunToCompletionEndsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := runToCompletion(ctx, func() (CompleteRouteExecutionResult, error) {
		calls++
		cancel()
		return CompleteRouteExecutionResult{}, ChunkContinuationError{Next: time.Now()}
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v: a cancelled run must stop asking for attempts", calls, err)
	}
}

// The in-process store serves one claim: another owner, unit or generation is a
// lost lease, and to a load it is simply not there (the Postgres query joins the
// owner). The full conformance against PostgresRepository is
// TestInProcessChunkLedgerMatchesPostgres.
func TestInProcessChunkLedgerServesOnlyItsClaim(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	store := NewInProcessChunkLedger(claim)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"1"}`)
	chunk := PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 0, CursorAfter: "c1", Effects: []EffectBatch{effect},
	}
	stranger := claim
	stranger.Owner = "77777777-7777-4777-8777-777777777777"
	if _, err := store.PrepareChunk(context.Background(), stranger, chunk, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("another owner prepared a chunk: %v", err)
	}
	if _, err := store.PrepareChunk(context.Background(), claim, chunk, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadChunkCheckpoint(context.Background(), stranger, now); !errors.Is(err, ErrChunkCheckpointNotFound) {
		t.Fatalf("another owner saw the checkpoint: %v", err)
	}
	if _, err := store.LoadPreparedChunk(context.Background(), stranger, 0, now); !errors.Is(err, ErrPreparedChunkNotFound) {
		t.Fatalf("another owner saw the chunk: %v", err)
	}
	other := claim
	other.ID = "88888888-8888-4888-8888-888888888888"
	if err := store.BeginChunkEffect(context.Background(), other, 0, 0, effect.ContentDigest, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("another unit began an effect: %v", err)
	}
}

// One chunk's whole life, through the executor's own calls: prepared pending, its
// effect written, marked committed with its rows counted once, and the inventory
// finalized only when every chunk is committed.
func TestInProcessChunkLedgerRunsAChunkThroughItsLife(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("github", "tests")
	store := NewInProcessChunkLedger(claim)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"1"}`, `{"org_id":"org-acme","run_id":"2"}`)
	prepared, err := store.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 0, TotalChunks: 1, InventoryComplete: true, CursorAfter: "end",
		Effects: []EffectBatch{effect}, Result: map[string]any{"records": float64(2)},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkChunkCommitted(ctx, claim, 0, prepared.ManifestDigest, now); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a chunk whose effect is still pending was marked committed: %v", err)
	}
	if err := store.BeginChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now); !errors.Is(err, ErrEffectRecoveryAmbiguous) {
		t.Fatalf("a second begin of a writing effect: %v", err)
	}
	if err := store.CommitChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ { // the second is an idempotent replay
		if err := store.MarkChunkCommitted(ctx, claim, 0, prepared.ManifestDigest, now); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint, err := store.LoadChunkCheckpoint(ctx, claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextOrdinal != 1 || checkpoint.CommittedRows != 2 || checkpoint.NextCursor != "end" ||
		fmt.Sprint(checkpoint.AggregateResult["records"]) != "2" || checkpoint.FinalOrdinal != 0 {
		t.Fatalf("checkpoint = %+v: rows counted once (2), cursor and aggregate kept", checkpoint)
	}
	if err := store.MarkInventoryComplete(ctx, claim, now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkInventoryComplete(ctx, claim, now); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("the inventory was completed twice: %v", err)
	}
	if checkpoint, _ = store.LoadChunkCheckpoint(ctx, claim, now); !checkpoint.InventoryComplete || checkpoint.TotalChunks != 1 {
		t.Fatalf("checkpoint after completion = %+v", checkpoint)
	}
}

// A cancelled context fails the call and leaves the state alone, as it does against
// PostgreSQL (a transaction cannot begin, a query fails): the r1 defect of #3252.
func TestInProcessChunkLedgerRefusesACancelledContextWithoutChangingState(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	store := NewInProcessChunkLedger(claim)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"1"}`)
	chunk := PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 0, CursorAfter: "c1", Effects: []EffectBatch{effect},
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := store.PrepareChunk(cancelled, claim, chunk, now); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("a cancelled prepare: %v", err)
	}
	if _, err := store.LoadChunkCheckpoint(context.Background(), claim, now); !errors.Is(err, ErrChunkCheckpointNotFound) {
		t.Fatalf("a cancelled prepare left a checkpoint: %v", err)
	}
	if _, err := store.PrepareChunk(context.Background(), claim, chunk, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadChunkCheckpoint(cancelled, claim, now); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a cancelled load: %v", err)
	}
	if err := store.BeginChunkEffect(cancelled, claim, 0, 0, effect.ContentDigest, now); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("a cancelled begin: %v", err)
	}
	loaded, err := store.LoadPreparedChunk(context.Background(), claim, 0, now)
	if err != nil || loaded.Ledger.Effects[0].Status != GenerationBlockPending {
		t.Fatalf("a cancelled begin changed the ledger: %+v %v", loaded.Ledger.Effects, err)
	}
}
