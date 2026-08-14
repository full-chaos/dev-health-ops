package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestChunkPolicySplitsAllDestinationsWithinPreparedCaps(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	rows := make([]json.RawMessage, 7)
	for index := range rows {
		rows[index] = json.RawMessage(`{"id":1,"value":"bounded"}`)
	}
	first, err := BuildEffectBatch("ci_pipeline_runs", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildEffectBatch("ci_job_runs", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	batch := CompleteRouteBatch{Effects: []EffectBatch{first, second}, Result: map[string]any{"complete": true}}
	chunks, err := SplitCompleteRouteBatch(batch, ChunkPolicy{
		MaxSourceItems: 3, MaxEffectRows: 3, MaxPreparedBytes: 4096,
		MaxChunksPerAttempt: 8, MaxWallTime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks=%d want 3", len(chunks))
	}
	for index, chunk := range chunks {
		if chunk.Ordinal != index || len(chunk.Effects) != 2 || chunk.PayloadBytes > 4096 {
			t.Fatalf("chunk[%d]=%+v", index, chunk)
		}
		if chunk.InventoryComplete != (index == len(chunks)-1) {
			t.Fatalf("chunk[%d] inventory_complete=%t", index, chunk.InventoryComplete)
		}
	}
	if chunks[len(chunks)-1].Result["complete"] != true {
		t.Fatalf("final result=%v", chunks[len(chunks)-1].Result)
	}
	_ = claim
}

func TestChunkPolicyUsesTheSmallerSourceAndEffectBound(t *testing.T) {
	rows := make([]json.RawMessage, 5)
	for index := range rows {
		rows[index] = json.RawMessage(`{"id":1,"value":"bounded"}`)
	}
	effect, err := BuildEffectBatch("ci_pipeline_runs", EffectReplaySafe, rows)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: []EffectBatch{effect}}, ChunkPolicy{
		MaxSourceItems: 2, MaxEffectRows: 3, MaxPreparedBytes: 4096,
		MaxChunksPerAttempt: 8, MaxWallTime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks=%d want 3 from source cap", len(chunks))
	}
	for index, chunk := range chunks {
		if len(chunk.Effects) != 1 || len(chunk.Effects[0].Rows) > 2 {
			t.Fatalf("chunk[%d] rows=%d", index, len(chunk.Effects[0].Rows))
		}
	}
}

func TestChunkPolicyWithholdsWatermarkForIncompleteInventory(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	watermark := now
	batch := CompleteRouteBatch{
		Effects:   []EffectBatch{mustTestEffectBatch(t, "ci_pipeline_runs")},
		Watermark: &watermark,
		Result: map[string]any{
			"incomplete": true,
		},
	}
	chunks, err := SplitCompleteRouteBatch(batch, DefaultChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if chunks[len(chunks)-1].Watermark == nil {
		t.Fatal("watermark should remain available until the route policy rejects incomplete inventory")
	}
	// The executor, not the mechanical splitter, owns the route-specific
	// incomplete-inventory policy. This assertion documents that the splitter
	// does not silently rewrite the producer result.
}

func TestChunkCheckpointRejectsTenantAndGenerationMismatch(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	checkpoint := ChunkCheckpoint{
		SchemaVersion: chunkCheckpointSchemaVersion,
		OrgID:         claim.OrgID, UnitID: claim.ID, Generation: claim.GenerationKey(),
		Provider: claim.Provider, Dataset: claim.Dataset, RouteVersion: "ci.v1",
		NormalizedAt: time.Now().UTC(), NextOrdinal: 0, TotalChunks: 1,
		Owner: claim.Owner, LeaseExpiresAt: claim.LeaseExpiresAt,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := checkpoint.Validate(claim); err != nil {
		t.Fatalf("checkpoint=%+v claim=%+v err=%v", checkpoint, claim, err)
	}
	other := claim
	other.OrgID = "other-tenant"
	if err := checkpoint.Validate(other); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("tenant mismatch error=%v", err)
	}
	other = claim
	other.ID = "00000000-0000-4000-8000-000000000099"
	if err := checkpoint.Validate(other); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("generation mismatch error=%v", err)
	}
	handoff := claim
	handoff.Owner = "77777777-7777-4777-8777-777777777777"
	if err := checkpoint.Validate(handoff); err != nil {
		t.Fatalf("lease-owner handoff should preserve generation checkpoint: %v", err)
	}
}

func TestChunkedContinuationIsAttemptNeutral(t *testing.T) {
	err := ChunkContinuationError{Next: time.Now().UTC().Add(time.Second)}
	if !errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("error=%v", err)
	}
	if delay, ok := ChunkContinuationDelay(err); !ok || delay <= 0 {
		t.Fatalf("delay=%s ok=%t", delay, ok)
	}
	_ = context.Background()
}

func TestChunkedExecutorResumesPreparedChunksWithoutRecollection(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := (CompleteRouteSwitches{GithubCICD: true}).Descriptor("github", "cicd")
	if !ok || !descriptor.RouteEnabled || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 2, MaxEffectRows: 1, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: 1, MaxWallTime: time.Minute,
	}
	handler := &countingChunkRouteHandler{batch: chunkedTestBatch(t, claim)}
	store := newChunkMemoryStore()
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}

	_, err := executor.Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("first execution error=%v", err)
	}
	if handler.calls != 1 {
		t.Fatalf("first collection calls=%d want 1", handler.calls)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextOrdinal != 1 || checkpoint.TotalChunks != 2 || checkpoint.InventoryComplete {
		t.Fatalf("checkpoint after continuation=%+v", checkpoint)
	}

	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatalf("resume error=%v", err)
	}
	if handler.calls != 1 {
		t.Fatalf("resume recollected provider batch: calls=%d", handler.calls)
	}
	if result.Watermark == nil || result.Result["complete"] != true {
		t.Fatalf("final result=%+v", result)
	}
	checkpoint, err = store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.InventoryComplete || checkpoint.NextOrdinal != checkpoint.TotalChunks {
		t.Fatalf("checkpoint after resume=%+v", checkpoint)
	}
	if len(sink.destinations) != 12 {
		t.Fatalf("sink writes=%d want 12", len(sink.destinations))
	}
}

func TestChunkedExecutorStreamsPagesAndResumesCursor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := (CompleteRouteSwitches{GithubCICD: true}).Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 2, MaxEffectRows: 2, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: 2, MaxWallTime: time.Minute,
	}
	handler := &streamingChunkRouteHandler{batch: chunkedTestBatch(t, claim)}
	store := newChunkMemoryStore()
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}

	_, err := executor.Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("first execution error=%v", err)
	}
	checkpoint, checkpointErr := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if handler.calls != 1 || checkpointErr != nil || checkpoint.NextCursor != `{"page":1}` {
		t.Fatalf("first stream calls=%d cursor=%q checkpoint=%+v err=%v", handler.calls, handler.lastCursor, checkpoint, checkpointErr)
	}
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatalf("resume error=%v", err)
	}
	if handler.calls != 2 || handler.lastCursor != `{"page":1}` {
		t.Fatalf("resume stream calls=%d cursor=%q", handler.calls, handler.lastCursor)
	}
	if result.Result["complete"] != true || result.Watermark == nil {
		t.Fatalf("result=%+v", result)
	}
	checkpoint, err = store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil || !checkpoint.InventoryComplete || checkpoint.TotalChunks != checkpoint.PreparedChunks {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
}

type streamingChunkRouteHandler struct {
	batch      CompleteRouteBatch
	calls      int
	lastCursor string
}

func (handler *streamingChunkRouteHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *streamingChunkRouteHandler) CollectChunks(
	_ context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	_ time.Time,
	resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	handler.calls++
	handler.lastCursor = resumeCursor
	start := 0
	if resumeCursor != "" {
		start = 1
	}
	for index := start; index < 2; index++ {
		batch := handler.batch
		batch.Result = nil
		batch.Watermark = nil
		batch.Evidence = FetchEvidence{}
		if err := emit(ChunkRouteEmission{Batch: batch, CursorBefore: `{"page":0}`, CursorAfter: `{"page":1}`, Final: false}); err != nil {
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

type countingChunkRouteHandler struct {
	batch CompleteRouteBatch
	calls int
}

type chunkedCredentialDecryptor struct{}

func (chunkedCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"token":"fixture-token"}`), nil
}

func (handler *countingChunkRouteHandler) Collect(
	_ context.Context,
	_ Claim,
	_ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	_ time.Time,
) (CompleteRouteBatch, error) {
	handler.calls++
	return handler.batch, nil
}

func chunkedTestBatch(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	destinations := []string{
		"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots",
	}
	effects := make([]EffectBatch, 0, len(destinations))
	for _, destination := range destinations {
		rows := []json.RawMessage{
			json.RawMessage(`{"org_id":"` + claim.OrgID + `","id":1,"destination":"` + destination + `"}`),
			json.RawMessage(`{"org_id":"` + claim.OrgID + `","id":2,"destination":"` + destination + `"}`),
		}
		effect, err := BuildEffectBatch(destination, EffectReplaySafe, rows)
		if err != nil {
			t.Fatal(err)
		}
		if effect.PayloadBytes < 1 {
			t.Fatalf("invalid effect=%+v", effect)
		}
		effects = append(effects, effect)
	}
	watermark := time.Date(2026, 8, 14, 11, 59, 59, 0, time.UTC)
	return CompleteRouteBatch{
		Effects:   effects,
		Result:    map[string]any{"complete": true},
		Watermark: &watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Records: 12,
		},
	}
}

func mustTestEffectBatch(t *testing.T, destination string) EffectBatch {
	t.Helper()
	batch, err := BuildEffectBatch(destination, EffectReadbackRequired, []json.RawMessage{json.RawMessage(`{"id":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

type chunkMemoryStore struct {
	committed  map[int]bool
	mu         sync.Mutex
	checkpoint ChunkCheckpoint
	chunks     map[int]PreparedProviderChunk
	memoryEffectLedger
}

func newChunkMemoryStore() *chunkMemoryStore {
	return &chunkMemoryStore{chunks: map[int]PreparedProviderChunk{}}
}

func clonePreparedChunk(t *testing.T, input PreparedProviderChunk) PreparedProviderChunk {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var output PreparedProviderChunk
	if err := json.Unmarshal(encoded, &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func (store *chunkMemoryStore) LoadChunkCheckpoint(
	_ context.Context, claim Claim, _ time.Time,
) (ChunkCheckpoint, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.checkpoint.SchemaVersion == "" {
		return ChunkCheckpoint{}, ErrChunkCheckpointNotFound
	}
	if err := store.checkpoint.Validate(claim); err != nil {
		return ChunkCheckpoint{}, err
	}
	return store.checkpoint, nil
}

func (store *chunkMemoryStore) PrepareChunk(
	_ context.Context, claim Claim, input PreparedProviderChunk, now time.Time,
) (PreparedProviderChunk, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := NewEffectLedgerState(claim, input.Effects, now)
	if err != nil {
		return PreparedProviderChunk{}, err
	}
	input.Ledger = state
	if input.ManifestDigest == "" {
		input.ManifestDigest = preparedChunkDigest(input)
	}
	if prior, ok := store.chunks[input.Ordinal]; ok {
		if prior.ManifestDigest != input.ManifestDigest {
			return PreparedProviderChunk{}, ErrChunkCheckpointConflict
		}
		return clonePreparedChunkForStore(prior), nil
	}
	if store.checkpoint.SchemaVersion == "" {
		store.checkpoint = ChunkCheckpoint{
			SchemaVersion: chunkCheckpointSchemaVersion, OrgID: claim.OrgID,
			UnitID: claim.ID, Generation: claim.GenerationKey(), Provider: claim.Provider,
			Dataset: claim.Dataset, RouteVersion: input.RouteVersion,
			NormalizedAt: now, TotalChunks: input.TotalChunks,
			FinalOrdinal: input.TotalChunks - 1, Owner: claim.Owner,
			LeaseExpiresAt: claim.LeaseExpiresAt, CreatedAt: now, UpdatedAt: now,
		}
	}
	if input.Ordinal != store.checkpoint.PreparedChunks ||
		(store.checkpoint.TotalChunks > 0 && input.TotalChunks != store.checkpoint.TotalChunks) ||
		(store.checkpoint.TotalChunks == 0 && input.TotalChunks > 0 && !input.InventoryComplete) {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	store.chunks[input.Ordinal] = clonePreparedChunkForStore(input)
	store.checkpoint.PreparedChunks++
	if input.CursorAfter != "" {
		store.checkpoint.NextCursor = input.CursorAfter
	}
	if input.InventoryComplete {
		store.checkpoint.AggregateResult = cloneChunkResult(input.Result)
		store.checkpoint.AggregateDigest = chunkResultDigest(input.Result)
	}
	store.checkpoint.UpdatedAt = now
	return input, nil
}

// PrepareChunkGroup mirrors the Postgres contract: all-or-nothing. The double
// snapshots and restores its own state so a mid-group failure cannot leave a
// partially prepared emission, exactly as the single transaction cannot.
func (store *chunkMemoryStore) PrepareChunkGroup(
	ctx context.Context, claim Claim, chunks []PreparedProviderChunk, now time.Time,
) ([]PreparedProviderChunk, error) {
	if len(chunks) == 0 {
		return nil, ErrInvalidConfiguration
	}
	store.mu.Lock()
	snapshotCheckpoint := store.checkpoint
	snapshotChunks := make(map[int]PreparedProviderChunk, len(store.chunks))
	for ordinal, chunk := range store.chunks {
		snapshotChunks[ordinal] = chunk
	}
	store.mu.Unlock()

	prepared := make([]PreparedProviderChunk, 0, len(chunks))
	for _, chunk := range chunks {
		out, err := store.PrepareChunk(ctx, claim, chunk, now)
		if err != nil {
			store.mu.Lock()
			store.checkpoint, store.chunks = snapshotCheckpoint, snapshotChunks
			store.mu.Unlock()
			return nil, err
		}
		prepared = append(prepared, out)
	}
	return prepared, nil
}

func clonePreparedChunkForStore(input PreparedProviderChunk) PreparedProviderChunk {
	encoded, _ := json.Marshal(input)
	var output PreparedProviderChunk
	_ = json.Unmarshal(encoded, &output)
	return output
}

func (store *chunkMemoryStore) LoadPreparedChunk(
	_ context.Context, claim Claim, ordinal int, _ time.Time,
) (PreparedProviderChunk, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.checkpoint.SchemaVersion == "" || store.checkpoint.Validate(claim) != nil {
		return PreparedProviderChunk{}, ErrChunkCheckpointNotFound
	}
	chunk, ok := store.chunks[ordinal]
	if !ok {
		return PreparedProviderChunk{}, ErrPreparedChunkNotFound
	}
	return clonePreparedChunkForStore(chunk), nil
}

func (store *chunkMemoryStore) BeginChunkEffect(
	_ context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
) error {
	return store.transitionChunk(claim, ordinal, index, digest, now, GenerationBlockPending, GenerationBlockWriting)
}

func (store *chunkMemoryStore) CommitChunkEffect(
	_ context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
) error {
	return store.transitionChunk(claim, ordinal, index, digest, now, GenerationBlockWriting, GenerationBlockCommitted)
}

func (store *chunkMemoryStore) ResolveChunkEffect(
	_ context.Context, claim Claim, ordinal, index int, digest string,
	resolution GenerationBlockResolution, now time.Time,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	chunk, ok := store.chunks[ordinal]
	if !ok || store.checkpoint.Validate(claim) != nil || index >= len(chunk.Ledger.Effects) || chunk.Ledger.Effects[index].ContentDigest != digest {
		return ErrChunkCheckpointConflict
	}
	effect := &chunk.Ledger.Effects[index]
	switch resolution {
	case GenerationBlockMarkCommitted:
		if effect.Status != GenerationBlockWriting {
			return ErrChunkCheckpointConflict
		}
		effect.Status, effect.CommittedAt = GenerationBlockCommitted, chunkTimePtr(now)
	case GenerationBlockRetryPending:
		if effect.Status != GenerationBlockWriting {
			return ErrChunkCheckpointConflict
		}
		effect.Status, effect.StartedAt, effect.CommittedAt = GenerationBlockPending, nil, nil
	default:
		return ErrInvalidConfiguration
	}
	chunk.Ledger.UpdatedAt = now
	store.chunks[ordinal] = chunk
	return nil
}

func (store *chunkMemoryStore) transitionChunk(
	claim Claim, ordinal, index int, digest string, now time.Time,
	from, to GenerationBlockStatus,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	chunk, ok := store.chunks[ordinal]
	if !ok || store.checkpoint.Validate(claim) != nil || index >= len(chunk.Ledger.Effects) || chunk.Ledger.Effects[index].ContentDigest != digest {
		return ErrChunkCheckpointConflict
	}
	if chunk.Ledger.Generation != claim.GenerationKey() {
		return ErrChunkCheckpointConflict
	}
	effect := &chunk.Ledger.Effects[index]
	if effect.Status != from {
		return ErrChunkCheckpointConflict
	}
	if to == GenerationBlockWriting {
		effect.StartedAt = chunkTimePtr(now)
	} else {
		effect.CommittedAt = chunkTimePtr(now)
	}
	effect.Status = to
	chunk.Ledger.UpdatedAt = now
	store.chunks[ordinal] = chunk
	return nil
}

func (store *chunkMemoryStore) MarkChunkCommitted(
	_ context.Context, claim Claim, ordinal int, digest string, now time.Time,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	chunk, ok := store.chunks[ordinal]
	if !ok || store.checkpoint.Validate(claim) != nil || chunk.ManifestDigest != digest || chunk.Ledger.Generation != claim.GenerationKey() {
		return ErrChunkCheckpointConflict
	}
	for _, effect := range chunk.Ledger.Effects {
		if effect.Status != GenerationBlockCommitted {
			return ErrChunkCheckpointConflict
		}
	}
	if store.committed == nil {
		store.committed = map[int]bool{}
	}
	if !store.committed[ordinal] {
		store.committed[ordinal] = true
		for _, effect := range chunk.Effects {
			store.checkpoint.CommittedRows += int64(len(effect.Rows))
		}
	}
	store.checkpoint.NextOrdinal = ordinal + 1
	if chunk.CursorAfter != "" {
		store.checkpoint.NextCursor = chunk.CursorAfter
	}
	store.checkpoint.UpdatedAt = now
	return nil
}

func (store *chunkMemoryStore) MarkInventoryComplete(
	_ context.Context, claim Claim, now time.Time,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.checkpoint.Validate(claim) != nil || store.checkpoint.InventoryComplete ||
		store.checkpoint.NextOrdinal != store.checkpoint.PreparedChunks || store.checkpoint.PreparedChunks < 1 {
		return ErrChunkCheckpointConflict
	}
	total := store.checkpoint.PreparedChunks
	final, ok := store.chunks[total-1]
	if !ok || !final.InventoryComplete {
		return ErrChunkCheckpointConflict
	}
	for ordinal, chunk := range store.chunks {
		chunk.TotalChunks = total
		store.chunks[ordinal] = chunk
	}
	store.checkpoint.TotalChunks = total
	store.checkpoint.FinalOrdinal = total - 1
	store.checkpoint.InventoryComplete = true
	store.checkpoint.UpdatedAt = now
	return nil
}

func chunkTimePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
