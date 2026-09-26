package providersync

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// InProcessChunkLedger is the chunk store of a single in-process attempt: the
// ChunkedEffectStore the chunked routes (cicd, tests) need, with the checkpoint
// and the prepared sidecars in memory instead of the Postgres tables. It
// enforces the same guards and transitions as PostgresRepository (every method
// of chunked_repository_postgres.go has its counterpart here, and both go
// through the same buildPreparedChunkMaterial, so a chunk is validated, digested
// and size-checked by one function), so the streaming executor cannot tell it
// from the durable one; it forgets when the process exits, and there is no
// lease or run row to fence on, so the fence is the claim's own identity (org,
// unit, owner, generation). One store belongs to one claim.
//
// Like the sidecar columns, the payload and the ledger are held as JSON and
// decoded on every load, so a value that does not survive the round trip (a
// time zone, a nil versus empty slice) fails here exactly where it would fail
// against Postgres.
type InProcessChunkLedger struct {
	*InProcessEffectLedger

	mu         sync.Mutex
	claim      Claim
	checkpoint *ChunkCheckpoint
	chunks     map[int]*inProcessChunkRow
}

// inProcessChunkRow is one sync_run_unit_effect_chunks row.
type inProcessChunkRow struct {
	schemaVersion     string
	routeVersion      string
	ordinal           int
	totalChunks       int
	cursorBefore      string
	cursorAfter       string
	inventoryComplete bool
	payloadRaw        []byte
	ledgerRaw         []byte
	payloadBytes      int
	manifestDigest    string
	status            string
}

var (
	_ ChunkedEffectStore = (*InProcessChunkLedger)(nil)
	_ EffectLedger       = (*InProcessChunkLedger)(nil)
)

// NewInProcessChunkLedger returns an empty store for the claim (and the plain
// effect ledger the non-chunked paths of the executor use).
func NewInProcessChunkLedger(claim Claim) *InProcessChunkLedger {
	return &InProcessChunkLedger{
		claim:                 claim,
		InProcessEffectLedger: &InProcessEffectLedger{},
		chunks:                map[int]*inProcessChunkRow{},
	}
}

// fence is assertChunkClaimTx for a store that owns one claim: any other org, unit,
// owner or generation is a lost lease, as a different owner is in Postgres. (The
// lease's expiry is not checked: an in-process run has no lease row, and its
// heartbeat has nothing to renew.)
func (store *InProcessChunkLedger) fence(claim Claim) error {
	if store.claim.OrgID != claim.OrgID || store.claim.ID != claim.ID || store.claim.Owner != claim.Owner ||
		store.claim.GenerationKey() != claim.GenerationKey() {
		return ErrLeaseLost
	}
	return nil
}

func copyCheckpoint(source *ChunkCheckpoint) (ChunkCheckpoint, error) {
	checkpoint := *source
	checkpoint.AggregateResult = nil
	if source.AggregateResult != nil {
		encoded, err := json.Marshal(source.AggregateResult)
		if err != nil {
			return ChunkCheckpoint{}, ErrChunkCheckpointConflict
		}
		if decodeResultObjectExact(encoded, &checkpoint.AggregateResult) != nil {
			return ChunkCheckpoint{}, ErrChunkCheckpointConflict
		}
	}
	return checkpoint, nil
}

func (store *InProcessChunkLedger) LoadChunkCheckpoint(
	_ context.Context, claim Claim, now time.Time,
) (ChunkCheckpoint, error) {
	if claim.Validate() != nil || now.IsZero() {
		return ChunkCheckpoint{}, ErrInvalidConfiguration
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.loadCheckpointLocked(claim)
}

func (store *InProcessChunkLedger) loadCheckpointLocked(claim Claim) (ChunkCheckpoint, error) {
	if store.fence(claim) != nil || store.checkpoint == nil {
		// A row of another owner or generation is invisible to the query.
		return ChunkCheckpoint{}, ErrChunkCheckpointNotFound
	}
	checkpoint, err := copyCheckpoint(store.checkpoint)
	if err != nil {
		return ChunkCheckpoint{}, err
	}
	if err := checkpoint.Validate(claim); err != nil {
		return ChunkCheckpoint{}, err
	}
	return checkpoint, nil
}

// PrepareChunk is PrepareChunkGroup of one, so both paths share every guard.
func (store *InProcessChunkLedger) PrepareChunk(
	ctx context.Context, claim Claim, chunk PreparedProviderChunk, now time.Time,
) (PreparedProviderChunk, error) {
	group, err := store.PrepareChunkGroup(ctx, claim, []PreparedProviderChunk{chunk}, now)
	if err != nil {
		return PreparedProviderChunk{}, err
	}
	if len(group) != 1 {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	return group[0], nil
}

// PrepareChunkGroup persists every sub-chunk of one provider emission or none of
// it (CHAOS-3821): the work is done on a copy of the state and swapped in on
// success, the way one transaction commits or rolls back.
func (store *InProcessChunkLedger) PrepareChunkGroup(
	_ context.Context, claim Claim, chunks []PreparedProviderChunk, now time.Time,
) ([]PreparedProviderChunk, error) {
	if claim.Validate() != nil || now.IsZero() || len(chunks) == 0 {
		return nil, ErrInvalidConfiguration
	}
	materials := make([]preparedChunkMaterial, 0, len(chunks))
	for _, chunk := range chunks {
		material, err := buildPreparedChunkMaterial(claim, chunk, now)
		if err != nil {
			return nil, err
		}
		materials = append(materials, material)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.fence(claim); err != nil {
		return nil, err
	}
	working := store.snapshotLocked()
	prepared := make([]PreparedProviderChunk, 0, len(materials))
	for _, material := range materials {
		out, err := working.prepareChunk(claim, material, now)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, out)
	}
	store.checkpoint, store.chunks = working.checkpoint, working.chunks
	return prepared, nil
}

// inProcessChunkState is a working copy of the store's tables.
type inProcessChunkState struct {
	checkpoint *ChunkCheckpoint
	chunks     map[int]*inProcessChunkRow
}

func (store *InProcessChunkLedger) snapshotLocked() *inProcessChunkState {
	state := &inProcessChunkState{chunks: make(map[int]*inProcessChunkRow, len(store.chunks))}
	if store.checkpoint != nil {
		copied, _ := copyCheckpoint(store.checkpoint)
		state.checkpoint = &copied
	}
	for ordinal, row := range store.chunks {
		copied := *row
		state.chunks[ordinal] = &copied
	}
	return state
}

// storedResult is the aggregate column: the JSON the chunk's Result encodes to, or
// nil (SQL NULL) for a chunk with none.
func storedResult(raw []byte) map[string]any {
	if raw == nil {
		return nil
	}
	var result map[string]any
	if decodeResultObjectExact(raw, &result) != nil {
		return nil
	}
	return result
}

// prepareChunk is prepareChunkInTx.
func (state *inProcessChunkState) prepareChunk(
	claim Claim, material preparedChunkMaterial, now time.Time,
) (PreparedProviderChunk, error) {
	chunk, payloadRaw, ledgerRaw := material.chunk, material.payloadRaw, material.ledgerRaw
	if state.checkpoint == nil {
		finalOrdinal := -1
		if chunk.TotalChunks > 0 {
			finalOrdinal = chunk.TotalChunks - 1
		}
		state.checkpoint = &ChunkCheckpoint{
			SchemaVersion: chunkCheckpointSchemaVersion, OrgID: claim.OrgID,
			UnitID: claim.ID, Generation: claim.GenerationKey(),
			Provider: claim.Provider, Dataset: claim.Dataset,
			RouteVersion: chunk.RouteVersion, NormalizedAt: chunk.Ledger.CreatedAt,
			NextOrdinal: 0, PreparedChunks: 0, TotalChunks: chunk.TotalChunks,
			FinalOrdinal: finalOrdinal, AggregateResult: storedResult(material.aggregate),
			AggregateDigest: chunkResultDigest(chunk.Result), Owner: claim.Owner,
			LeaseExpiresAt: claim.LeaseExpiresAt, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		}
	}
	checkpoint := state.checkpoint
	if checkpoint.RouteVersion != chunk.RouteVersion ||
		(checkpoint.TotalChunks > 0 && checkpoint.TotalChunks != chunk.TotalChunks) ||
		(checkpoint.TotalChunks == 0 && chunk.TotalChunks > 0 && !chunk.InventoryComplete) ||
		chunk.Ordinal > checkpoint.PreparedChunks {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	existing, present := state.chunks[chunk.Ordinal]
	switch {
	case !present:
		if chunk.Ordinal != checkpoint.PreparedChunks {
			return PreparedProviderChunk{}, ErrChunkCheckpointConflict
		}
		state.chunks[chunk.Ordinal] = &inProcessChunkRow{
			schemaVersion: chunk.SchemaVersion, routeVersion: chunk.RouteVersion,
			ordinal: chunk.Ordinal, totalChunks: chunk.TotalChunks,
			cursorBefore: chunk.CursorBefore, cursorAfter: chunk.CursorAfter,
			inventoryComplete: chunk.InventoryComplete, payloadRaw: payloadRaw, ledgerRaw: ledgerRaw,
			payloadBytes: chunk.PayloadBytes, manifestDigest: chunk.ManifestDigest, status: "pending",
		}
	case existing.manifestDigest != chunk.ManifestDigest:
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	case chunk.Ordinal < checkpoint.PreparedChunks:
		// Replayable only when the digest matches: return the durable ledger state
		// instead of rebuilding a pending one.
		return state.loadRow(claim, chunk.Ordinal)
	}
	// updateChunkPreparedSQL.
	if chunk.Ordinal+1 > checkpoint.PreparedChunks {
		checkpoint.PreparedChunks = chunk.Ordinal + 1
	}
	if chunk.InventoryComplete {
		checkpoint.TotalChunks = chunk.TotalChunks
		checkpoint.FinalOrdinal = chunk.Ordinal
	}
	if chunk.CursorAfter != "" {
		checkpoint.NextCursor = chunk.CursorAfter
	}
	if chunk.InventoryComplete && checkpoint.AggregateResult == nil {
		checkpoint.AggregateResult = storedResult(material.aggregate)
		checkpoint.AggregateDigest = chunkResultDigest(chunk.Result)
	}
	checkpoint.UpdatedAt = now.UTC()
	return chunk, nil
}

// loadRow is loadPreparedChunkRow.
func (state *inProcessChunkState) loadRow(claim Claim, ordinal int) (PreparedProviderChunk, error) {
	row, present := state.chunks[ordinal]
	if !present {
		return PreparedProviderChunk{}, ErrPreparedChunkNotFound
	}
	var chunk PreparedProviderChunk
	if json.Unmarshal(row.payloadRaw, &chunk) != nil || json.Unmarshal(row.ledgerRaw, &chunk.Ledger) != nil ||
		(row.status != "pending" && row.status != "writing" && row.status != "committed") ||
		chunk.SchemaVersion != row.schemaVersion || chunk.RouteVersion != row.routeVersion ||
		chunk.Ordinal != row.ordinal || chunk.CursorBefore != row.cursorBefore ||
		chunk.CursorAfter != row.cursorAfter || chunk.InventoryComplete != row.inventoryComplete ||
		chunk.PayloadBytes != row.payloadBytes || chunk.ManifestDigest != row.manifestDigest {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	// The relational column is the canonical count (see loadPreparedChunkRow).
	chunk.TotalChunks = row.totalChunks
	if chunk.TotalChunks < 0 || (chunk.TotalChunks > 0 && chunk.Ordinal >= chunk.TotalChunks) ||
		chunk.ManifestDigest == "" || chunk.Ledger.validate() != nil {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	_ = claim
	return chunk, nil
}

func (store *InProcessChunkLedger) LoadPreparedChunk(
	_ context.Context, claim Claim, ordinal int, now time.Time,
) (PreparedProviderChunk, error) {
	if claim.Validate() != nil || ordinal < 0 || now.IsZero() {
		return PreparedProviderChunk{}, ErrInvalidConfiguration
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fence(claim) != nil {
		return PreparedProviderChunk{}, ErrPreparedChunkNotFound
	}
	return (&inProcessChunkState{checkpoint: store.checkpoint, chunks: store.chunks}).loadRow(claim, ordinal)
}

func (store *InProcessChunkLedger) BeginChunkEffect(
	_ context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
) error {
	return store.mutateEffect(claim, ordinal, index, digest, now, GenerationBlockPending, GenerationBlockWriting)
}

func (store *InProcessChunkLedger) CommitChunkEffect(
	_ context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
) error {
	return store.mutateEffect(claim, ordinal, index, digest, now, GenerationBlockWriting, GenerationBlockCommitted)
}

func (store *InProcessChunkLedger) ResolveChunkEffect(
	_ context.Context, claim Claim, ordinal, index int, digest string,
	resolution GenerationBlockResolution, now time.Time,
) error {
	if resolution != GenerationBlockMarkCommitted && resolution != GenerationBlockRetryPending {
		return ErrInvalidConfiguration
	}
	if claim.Validate() != nil || ordinal < 0 || index < 0 || now.IsZero() {
		return ErrInvalidConfiguration
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.fence(claim); err != nil {
		return err
	}
	state := &inProcessChunkState{checkpoint: store.checkpoint, chunks: store.chunks}
	chunk, err := state.loadRow(claim, ordinal)
	if err != nil {
		return err
	}
	if index >= len(chunk.Ledger.Effects) || chunk.Ledger.Effects[index].ContentDigest != digest {
		return ErrChunkCheckpointConflict
	}
	effect := &chunk.Ledger.Effects[index]
	current := now.UTC()
	switch resolution {
	case GenerationBlockMarkCommitted:
		if effect.Status == GenerationBlockCommitted {
			return nil
		}
		if effect.Status != GenerationBlockWriting {
			return ErrChunkCheckpointConflict
		}
		effect.Status, effect.CommittedAt = GenerationBlockCommitted, &current
	case GenerationBlockRetryPending:
		if effect.Status == GenerationBlockPending {
			return nil
		}
		if effect.Status != GenerationBlockWriting {
			return ErrChunkCheckpointConflict
		}
		effect.Status, effect.StartedAt, effect.CommittedAt = GenerationBlockPending, nil, nil
	}
	chunk.Ledger.UpdatedAt = current
	encoded := encodeEffectLedgerState(chunk.Ledger)
	if len(encoded) == 0 {
		return ErrChunkCheckpointConflict
	}
	store.chunks[ordinal].ledgerRaw = encoded
	return nil
}

// mutateEffect is mutateChunkEffect.
func (store *InProcessChunkLedger) mutateEffect(
	claim Claim, ordinal, index int, digest string, now time.Time, from, to GenerationBlockStatus,
) error {
	if claim.Validate() != nil || ordinal < 0 || index < 0 || now.IsZero() {
		return ErrInvalidConfiguration
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.fence(claim); err != nil {
		return err
	}
	state := &inProcessChunkState{checkpoint: store.checkpoint, chunks: store.chunks}
	chunk, err := state.loadRow(claim, ordinal)
	if err != nil {
		return err
	}
	if index >= len(chunk.Ledger.Effects) || chunk.Ledger.Effects[index].ContentDigest != digest {
		return ErrChunkCheckpointConflict
	}
	effect := &chunk.Ledger.Effects[index]
	if effect.Status == GenerationBlockWriting && from == GenerationBlockPending {
		return ErrEffectRecoveryAmbiguous
	}
	if effect.Status == GenerationBlockCommitted {
		if to == GenerationBlockCommitted {
			return nil
		}
		return ErrChunkCheckpointConflict
	}
	if effect.Status != from {
		return ErrChunkCheckpointConflict
	}
	current := now.UTC()
	if to == GenerationBlockWriting {
		effect.StartedAt = &current
	} else if to == GenerationBlockCommitted {
		effect.CommittedAt = &current
	} else {
		return ErrInvalidConfiguration
	}
	effect.Status = to
	chunk.Ledger.UpdatedAt = current
	encoded := encodeEffectLedgerState(chunk.Ledger)
	if len(encoded) == 0 {
		return ErrChunkCheckpointConflict
	}
	store.chunks[ordinal].ledgerRaw = encoded
	return nil
}

func (store *InProcessChunkLedger) MarkChunkCommitted(
	_ context.Context, claim Claim, ordinal int, digest string, now time.Time,
) error {
	if claim.Validate() != nil || ordinal < 0 || now.IsZero() {
		return ErrInvalidConfiguration
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.fence(claim); err != nil {
		return err
	}
	state := &inProcessChunkState{checkpoint: store.checkpoint, chunks: store.chunks}
	chunk, err := state.loadRow(claim, ordinal)
	if err != nil {
		return err
	}
	if chunk.ManifestDigest != digest {
		return ErrChunkCheckpointConflict
	}
	for _, effect := range chunk.Ledger.Effects {
		if effect.Status != GenerationBlockCommitted {
			return ErrChunkCheckpointConflict
		}
	}
	// markChunkCommittedSQL matches only a pending sidecar: a real transition adds
	// the rows once, a replay of a committed one adds nothing.
	row := store.chunks[ordinal]
	rows := int64(0)
	if row.status == "pending" {
		row.status = "committed"
		for _, effect := range chunk.Effects {
			rows += int64(len(effect.Rows))
		}
	} else if row.status != "committed" {
		return ErrChunkCheckpointConflict
	}
	// advanceChunkCheckpointSQL.
	checkpoint := store.checkpoint
	if checkpoint == nil {
		return ErrChunkCheckpointConflict
	}
	if ordinal+1 > checkpoint.NextOrdinal {
		checkpoint.NextOrdinal = ordinal + 1
	}
	if chunk.CursorAfter != "" {
		checkpoint.NextCursor = chunk.CursorAfter
	}
	checkpoint.CommittedRows += rows
	checkpoint.UpdatedAt = now.UTC()
	return nil
}

func (store *InProcessChunkLedger) MarkInventoryComplete(
	_ context.Context, claim Claim, now time.Time,
) error {
	if claim.Validate() != nil || now.IsZero() {
		return ErrInvalidConfiguration
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.fence(claim); err != nil {
		return err
	}
	checkpoint, err := store.loadCheckpointLocked(claim)
	if err != nil {
		return err
	}
	if checkpoint.InventoryComplete || checkpoint.PreparedChunks < 1 || checkpoint.NextOrdinal != checkpoint.PreparedChunks {
		return ErrChunkCheckpointConflict
	}
	total := checkpoint.PreparedChunks
	// finalizePreparedChunksSQL, then finalizeChunkCheckpointSQL.
	for ordinal, row := range store.chunks {
		if ordinal < total {
			row.totalChunks = total
		}
	}
	held := store.checkpoint
	if held.NextOrdinal != total || held.PreparedChunks != total || (held.TotalChunks != 0 && held.TotalChunks != total) {
		return ErrChunkCheckpointConflict
	}
	held.TotalChunks, held.FinalOrdinal, held.InventoryComplete = total, total-1, true
	held.UpdatedAt = now.UTC()
	return nil
}
