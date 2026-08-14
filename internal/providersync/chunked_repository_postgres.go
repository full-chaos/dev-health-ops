package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// The chunk tables are an additive persistence surface. The unit lease remains
// the authority: every query joins the owning unit and run and checks tenant,
// owner, live lease, and non-terminal run state.
const loadChunkCheckpointSQL = `
SELECT checkpoint.schema_version, checkpoint.org_id, checkpoint.sync_run_unit_id,
       checkpoint.generation, checkpoint.provider, checkpoint.dataset_key,
       checkpoint.route_version, checkpoint.normalized_at, checkpoint.next_cursor,
       checkpoint.inventory_complete, checkpoint.next_ordinal,
       checkpoint.prepared_chunks, checkpoint.total_chunks, checkpoint.final_ordinal,
       COALESCE(checkpoint.aggregate_result::text, ''), checkpoint.aggregate_digest,
       checkpoint.owner, checkpoint.lease_expires_at,
       checkpoint.created_at, checkpoint.updated_at
FROM public.sync_run_unit_chunk_checkpoints AS checkpoint
JOIN public.sync_run_units AS unit
  ON unit.org_id = checkpoint.org_id AND unit.id = checkpoint.sync_run_unit_id
JOIN public.sync_runs AS run
  ON run.org_id = unit.org_id AND run.id = unit.sync_run_id
WHERE checkpoint.org_id = $1
  AND checkpoint.sync_run_unit_id = $2::uuid
  AND checkpoint.generation = $3
  AND unit.status = 'running'
  AND unit.lease_owner = $4
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $5
  AND run.status NOT IN ('success', 'partial_failed', 'failed')`

const loadPreparedChunkSQL = `
SELECT chunk.schema_version, chunk.route_version, chunk.ordinal,
       chunk.total_chunks, chunk.cursor_before, chunk.cursor_after,
       chunk.inventory_complete, chunk.payload::text, chunk.ledger::text,
       chunk.payload_bytes, chunk.manifest_digest, chunk.status
FROM public.sync_run_unit_effect_chunks AS chunk
JOIN public.sync_run_units AS unit
  ON unit.org_id = chunk.org_id AND unit.id = chunk.sync_run_unit_id
JOIN public.sync_runs AS run
  ON run.org_id = unit.org_id AND run.id = unit.sync_run_id
WHERE chunk.org_id = $1
  AND chunk.sync_run_unit_id = $2::uuid
  AND chunk.generation = $3
  AND chunk.ordinal = $4
  AND unit.status = 'running'
  AND unit.lease_owner = $5
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $6
  AND run.status NOT IN ('success', 'partial_failed', 'failed')`

func (repository *PostgresRepository) LoadChunkCheckpoint(
	ctx context.Context, claim Claim, now time.Time,
) (ChunkCheckpoint, error) {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() {
		return ChunkCheckpoint{}, ErrInvalidConfiguration
	}
	var checkpoint ChunkCheckpoint
	var aggregateRaw []byte
	err := repository.Pool.QueryRow(ctx, loadChunkCheckpointSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Owner, now.UTC(),
	).Scan(
		&checkpoint.SchemaVersion, &checkpoint.OrgID, &checkpoint.UnitID,
		&checkpoint.Generation, &checkpoint.Provider, &checkpoint.Dataset,
		&checkpoint.RouteVersion, &checkpoint.NormalizedAt, &checkpoint.NextCursor,
		&checkpoint.InventoryComplete, &checkpoint.NextOrdinal,
		&checkpoint.PreparedChunks, &checkpoint.TotalChunks, &checkpoint.FinalOrdinal,
		&aggregateRaw, &checkpoint.AggregateDigest, &checkpoint.Owner,
		&checkpoint.LeaseExpiresAt, &checkpoint.CreatedAt, &checkpoint.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChunkCheckpoint{}, ErrChunkCheckpointNotFound
	}
	if err != nil {
		return ChunkCheckpoint{}, ErrChunkCheckpointConflict
	}
	if len(aggregateRaw) > 0 {
		if json.Unmarshal(aggregateRaw, &checkpoint.AggregateResult) != nil {
			return ChunkCheckpoint{}, ErrChunkCheckpointConflict
		}
	}
	if err := checkpoint.Validate(claim); err != nil {
		return ChunkCheckpoint{}, err
	}
	return checkpoint, nil
}

func (repository *PostgresRepository) PrepareChunk(
	ctx context.Context, claim Claim, chunk PreparedProviderChunk, now time.Time,
) (PreparedProviderChunk, error) {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() || chunk.Ordinal < 0 ||
		chunk.TotalChunks < 0 || chunk.RouteVersion == "" {
		return PreparedProviderChunk{}, ErrInvalidConfiguration
	}
	state, err := NewEffectLedgerState(claim, chunk.Effects, now.UTC())
	if err != nil {
		return PreparedProviderChunk{}, err
	}
	chunk.Ledger = state
	if chunk.ManifestDigest == "" {
		chunk.ManifestDigest = preparedChunkDigest(chunk)
	}
	payload := chunk
	payload.Ledger = EffectLedgerState{}
	payloadRaw, err := encodedPreparedChunkPayload(payload)
	if err != nil || len(payloadRaw) > maxPreparedRouteSnapshotBytes {
		return PreparedProviderChunk{}, ErrChunkPolicyExceeded
	}
	chunk.PayloadBytes = len(payloadRaw)
	ledgerRaw := encodeEffectLedgerState(state)
	if len(ledgerRaw) == 0 {
		return PreparedProviderChunk{}, ErrEffectRecoveryUnsafe
	}
	if chunk.Validate(claim, DefaultChunkPolicy()) != nil ||
		len(payloadRaw)+len(ledgerRaw) > maxPreparedRouteSnapshotBytes {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	aggregateRaw, _ := json.Marshal(chunk.Result)
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return PreparedProviderChunk{}, ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := assertChunkClaimTx(ctx, tx, claim, now.UTC()); err != nil {
		return PreparedProviderChunk{}, err
	}
	checkpoint, err := loadChunkCheckpointTx(ctx, tx, claim, now.UTC(), true)
	if errors.Is(err, ErrChunkCheckpointNotFound) {
		finalOrdinal := -1
		if chunk.TotalChunks > 0 {
			finalOrdinal = chunk.TotalChunks - 1
		}
		checkpoint = ChunkCheckpoint{
			SchemaVersion: chunkCheckpointSchemaVersion, OrgID: claim.OrgID,
			UnitID: claim.ID, Generation: claim.GenerationKey(),
			Provider: claim.Provider, Dataset: claim.Dataset,
			RouteVersion: chunk.RouteVersion, NormalizedAt: state.CreatedAt,
			NextOrdinal: 0, PreparedChunks: 0, TotalChunks: chunk.TotalChunks,
			FinalOrdinal: finalOrdinal, AggregateResult: chunk.Result,
			AggregateDigest: chunkResultDigest(chunk.Result), Owner: claim.Owner,
			LeaseExpiresAt: claim.LeaseExpiresAt, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		}
		if _, err := tx.Exec(ctx, insertChunkCheckpointSQL, checkpoint.OrgID,
			checkpoint.UnitID, checkpoint.SchemaVersion, checkpoint.Generation, checkpoint.Provider,
			checkpoint.Dataset, checkpoint.RouteVersion, checkpoint.NormalizedAt,
			checkpoint.NextCursor, checkpoint.InventoryComplete, checkpoint.NextOrdinal,
			checkpoint.PreparedChunks, checkpoint.TotalChunks, checkpoint.FinalOrdinal,
			aggregateRaw, checkpoint.AggregateDigest, checkpoint.Owner,
			checkpoint.LeaseExpiresAt, checkpoint.CreatedAt, checkpoint.UpdatedAt,
		); err != nil {
			return PreparedProviderChunk{}, ErrChunkCheckpointConflict
		}
	} else if err != nil {
		return PreparedProviderChunk{}, err
	}
	if checkpoint.RouteVersion != chunk.RouteVersion ||
		(checkpoint.TotalChunks > 0 && checkpoint.TotalChunks != chunk.TotalChunks) ||
		(checkpoint.TotalChunks == 0 && chunk.TotalChunks > 0 && !chunk.InventoryComplete) ||
		chunk.Ordinal > checkpoint.PreparedChunks {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	var existingDigest string
	err = tx.QueryRow(ctx, `SELECT manifest_digest FROM public.sync_run_unit_effect_chunks WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3 AND ordinal=$4 FOR UPDATE`, claim.OrgID, claim.ID, claim.GenerationKey(), chunk.Ordinal).Scan(&existingDigest)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if chunk.Ordinal != checkpoint.PreparedChunks {
			return PreparedProviderChunk{}, ErrChunkCheckpointConflict
		}
		if _, err := tx.Exec(ctx, insertPreparedChunkSQL,
			claim.OrgID, claim.ID, chunk.SchemaVersion, claim.GenerationKey(), chunk.RouteVersion,
			chunk.Ordinal, chunk.TotalChunks, chunk.CursorBefore, chunk.CursorAfter,
			chunk.InventoryComplete, payloadRaw, ledgerRaw, chunk.PayloadBytes,
			chunk.ManifestDigest, now.UTC(), now.UTC(),
		); err != nil {
			return PreparedProviderChunk{}, ErrChunkCheckpointConflict
		}
	case err != nil:
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	case existingDigest != chunk.ManifestDigest:
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	case chunk.Ordinal < checkpoint.PreparedChunks:
		// The insert and checkpoint advance are one transaction, so an already
		// prepared ordinal is safe to replay only when its digest matches. Return
		// the durable ledger state instead of rebuilding a pending in-memory one.
		persisted, loadErr := loadPreparedChunkRow(ctx, tx, claim, chunk.Ordinal, now.UTC())
		if loadErr != nil {
			return PreparedProviderChunk{}, loadErr
		}
		if err := tx.Commit(ctx); err != nil {
			return PreparedProviderChunk{}, ErrInvalidConfiguration
		}
		return persisted, nil
	}
	if _, err := tx.Exec(ctx, updateChunkPreparedSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), chunk.Ordinal+1,
		chunk.TotalChunks, chunk.InventoryComplete, chunk.CursorAfter,
		chunk.Result, chunkResultDigest(chunk.Result), now.UTC(),
	); err != nil {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return PreparedProviderChunk{}, ErrInvalidConfiguration
	}
	return chunk, nil
}

func (repository *PostgresRepository) LoadPreparedChunk(
	ctx context.Context, claim Claim, ordinal int, now time.Time,
) (PreparedProviderChunk, error) {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil ||
		ordinal < 0 || now.IsZero() {
		return PreparedProviderChunk{}, ErrInvalidConfiguration
	}
	return loadPreparedChunkRow(ctx, repository.Pool, claim, ordinal, now.UTC())
}

func loadPreparedChunkRow(
	ctx context.Context, queryer interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}, claim Claim, ordinal int, now time.Time,
) (PreparedProviderChunk, error) {
	var chunk PreparedProviderChunk
	var payloadRaw, ledgerRaw []byte
	var storedSchemaVersion, storedRouteVersion string
	var storedOrdinal, storedTotal int
	var storedCursorBefore, storedCursorAfter string
	var storedInventoryComplete bool
	var storedPayloadBytes int
	var storedManifestDigest string
	var status string
	err := queryer.QueryRow(ctx, loadPreparedChunkSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), ordinal, claim.Owner, now,
	).Scan(
		&storedSchemaVersion, &storedRouteVersion, &storedOrdinal,
		&storedTotal, &storedCursorBefore, &storedCursorAfter,
		&storedInventoryComplete, &payloadRaw, &ledgerRaw,
		&storedPayloadBytes, &storedManifestDigest, &status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return PreparedProviderChunk{}, ErrPreparedChunkNotFound
	}
	if err != nil {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	if json.Unmarshal(payloadRaw, &chunk) != nil || json.Unmarshal(ledgerRaw, &chunk.Ledger) != nil ||
		(status != "pending" && status != "writing" && status != "committed") ||
		chunk.SchemaVersion != storedSchemaVersion || chunk.RouteVersion != storedRouteVersion ||
		chunk.Ordinal != storedOrdinal || chunk.CursorBefore != storedCursorBefore ||
		chunk.CursorAfter != storedCursorAfter || chunk.InventoryComplete != storedInventoryComplete ||
		chunk.PayloadBytes != storedPayloadBytes || chunk.ManifestDigest != storedManifestDigest {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	// The sidecar payload is prepared before the final inventory count is
	// known. The relational column is finalized transactionally and is the
	// canonical count used by recovery; do not let the provisional JSON value
	// overwrite it during decode.
	chunk.TotalChunks = storedTotal
	if chunk.TotalChunks < 0 || (chunk.TotalChunks > 0 && chunk.Ordinal >= chunk.TotalChunks) {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	if chunk.ManifestDigest == "" || chunk.TotalChunks < 0 ||
		(chunk.TotalChunks > 0 && chunk.Ordinal >= chunk.TotalChunks) ||
		chunk.Ledger.validate() != nil {
		return PreparedProviderChunk{}, ErrChunkCheckpointConflict
	}
	return chunk, nil
}

func (repository *PostgresRepository) BeginChunkEffect(
	ctx context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
) error {
	return repository.mutateChunkEffect(ctx, claim, ordinal, index, digest, now, GenerationBlockPending, GenerationBlockWriting)
}

func (repository *PostgresRepository) CommitChunkEffect(
	ctx context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
) error {
	return repository.mutateChunkEffect(ctx, claim, ordinal, index, digest, now, GenerationBlockWriting, GenerationBlockCommitted)
}

func (repository *PostgresRepository) ResolveChunkEffect(
	ctx context.Context, claim Claim, ordinal, index int, digest string,
	resolution GenerationBlockResolution, now time.Time,
) error {
	if resolution != GenerationBlockMarkCommitted && resolution != GenerationBlockRetryPending {
		return ErrInvalidConfiguration
	}
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil || ordinal < 0 || index < 0 || now.IsZero() {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := assertChunkClaimTx(ctx, tx, claim, now.UTC()); err != nil {
		return err
	}
	chunk, err := loadPreparedChunkRow(ctx, tx, claim, ordinal, now.UTC())
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
			return tx.Commit(ctx)
		}
		if effect.Status != GenerationBlockWriting {
			return ErrChunkCheckpointConflict
		}
		effect.Status, effect.CommittedAt = GenerationBlockCommitted, &current
	case GenerationBlockRetryPending:
		if effect.Status == GenerationBlockPending {
			return tx.Commit(ctx)
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
	if _, err := tx.Exec(ctx, updateChunkLedgerSQL, claim.OrgID, claim.ID, claim.GenerationKey(), ordinal, encoded, current); err != nil {
		return ErrChunkCheckpointConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func (repository *PostgresRepository) mutateChunkEffect(
	ctx context.Context, claim Claim, ordinal, index int, digest string, now time.Time,
	from, to GenerationBlockStatus,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil || ordinal < 0 || index < 0 || now.IsZero() {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := assertChunkClaimTx(ctx, tx, claim, now.UTC()); err != nil {
		return err
	}
	chunk, err := loadPreparedChunkRow(ctx, tx, claim, ordinal, now.UTC())
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
			return tx.Commit(ctx)
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
	if _, err := tx.Exec(ctx, updateChunkLedgerSQL, claim.OrgID, claim.ID, claim.GenerationKey(), ordinal, encoded, current); err != nil {
		return ErrChunkCheckpointConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func (repository *PostgresRepository) MarkChunkCommitted(
	ctx context.Context, claim Claim, ordinal int, digest string, now time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil || ordinal < 0 || now.IsZero() {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := assertChunkClaimTx(ctx, tx, claim, now.UTC()); err != nil {
		return err
	}
	chunk, err := loadPreparedChunkRow(ctx, tx, claim, ordinal, now.UTC())
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
	if _, err := tx.Exec(ctx, markChunkCommittedSQL, claim.OrgID, claim.ID, claim.GenerationKey(), ordinal, now.UTC()); err != nil {
		return ErrChunkCheckpointConflict
	}
	if _, err := tx.Exec(ctx, advanceChunkCheckpointSQL, claim.OrgID, claim.ID, claim.GenerationKey(), ordinal+1, chunk.CursorAfter, now.UTC()); err != nil {
		return ErrChunkCheckpointConflict
	}
	return tx.Commit(ctx)
}

func (repository *PostgresRepository) MarkInventoryComplete(
	ctx context.Context, claim Claim, now time.Time,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil || claim.Validate() != nil || now.IsZero() {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return ErrInvalidConfiguration
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := assertChunkClaimTx(ctx, tx, claim, now.UTC()); err != nil {
		return err
	}
	checkpoint, err := loadChunkCheckpointTx(ctx, tx, claim, now.UTC(), true)
	if err != nil || checkpoint.InventoryComplete || checkpoint.PreparedChunks < 1 ||
		checkpoint.NextOrdinal != checkpoint.PreparedChunks {
		if err != nil {
			return err
		}
		return ErrChunkCheckpointConflict
	}
	total := checkpoint.PreparedChunks
	if _, err := tx.Exec(ctx, finalizePreparedChunksSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), total, now.UTC()); err != nil {
		return ErrChunkCheckpointConflict
	}
	command, err := tx.Exec(ctx, finalizeChunkCheckpointSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), total, now.UTC())
	if err != nil || command.RowsAffected() != 1 {
		return ErrChunkCheckpointConflict
	}
	return tx.Commit(ctx)
}

func assertChunkClaimTx(ctx context.Context, tx pgx.Tx, claim Claim, now time.Time) error {
	var present bool
	err := tx.QueryRow(ctx, `
SELECT TRUE
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.org_id=unit.org_id AND run.id=unit.sync_run_id
WHERE unit.org_id=$1 AND unit.id=$2::uuid AND unit.status='running'
  AND unit.lease_owner=$3 AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at>$4 AND run.status NOT IN ('success','partial_failed','failed')
FOR UPDATE OF unit`, claim.OrgID, claim.ID, claim.Owner, now).Scan(&present)
	if errors.Is(err, pgx.ErrNoRows) || !present {
		return ErrLeaseLost
	}
	if err != nil {
		return ErrChunkCheckpointConflict
	}
	return nil
}

func loadChunkCheckpointTx(ctx context.Context, tx pgx.Tx, claim Claim, now time.Time, lock bool) (ChunkCheckpoint, error) {
	query := loadChunkCheckpointSQL
	if lock {
		query += " FOR UPDATE OF checkpoint"
	}
	var checkpoint ChunkCheckpoint
	var aggregateRaw []byte
	err := tx.QueryRow(ctx, query,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Owner, now,
	).Scan(
		&checkpoint.SchemaVersion, &checkpoint.OrgID, &checkpoint.UnitID,
		&checkpoint.Generation, &checkpoint.Provider, &checkpoint.Dataset,
		&checkpoint.RouteVersion, &checkpoint.NormalizedAt, &checkpoint.NextCursor,
		&checkpoint.InventoryComplete, &checkpoint.NextOrdinal,
		&checkpoint.PreparedChunks, &checkpoint.TotalChunks, &checkpoint.FinalOrdinal,
		&aggregateRaw, &checkpoint.AggregateDigest, &checkpoint.Owner,
		&checkpoint.LeaseExpiresAt, &checkpoint.CreatedAt, &checkpoint.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChunkCheckpoint{}, ErrChunkCheckpointNotFound
	}
	if err != nil {
		return ChunkCheckpoint{}, ErrChunkCheckpointConflict
	}
	if len(aggregateRaw) > 0 && json.Unmarshal(aggregateRaw, &checkpoint.AggregateResult) != nil {
		return ChunkCheckpoint{}, ErrChunkCheckpointConflict
	}
	if err := checkpoint.Validate(claim); err != nil {
		return ChunkCheckpoint{}, err
	}
	return checkpoint, nil
}

const insertChunkCheckpointSQL = `
INSERT INTO public.sync_run_unit_chunk_checkpoints
(org_id,sync_run_unit_id,schema_version,generation,provider,dataset_key,route_version,normalized_at,
 next_cursor,inventory_complete,next_ordinal,prepared_chunks,total_chunks,final_ordinal,
 aggregate_result,aggregate_digest,owner,lease_expires_at,created_at,updated_at)
VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17,$18,$19,$20)`

const insertPreparedChunkSQL = `
INSERT INTO public.sync_run_unit_effect_chunks
(org_id,sync_run_unit_id,schema_version,generation,route_version,ordinal,total_chunks,cursor_before,
 cursor_after,inventory_complete,payload,ledger,payload_bytes,manifest_digest,status,created_at,updated_at)
VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13,$14,'pending',$15,$16)`

const updateChunkPreparedSQL = `
UPDATE public.sync_run_unit_chunk_checkpoints
SET prepared_chunks=GREATEST(prepared_chunks,$4),
    total_chunks=CASE WHEN $6 THEN $5 ELSE total_chunks END,
    final_ordinal=CASE WHEN $6 THEN $4-1 ELSE final_ordinal END,
    next_cursor=$7,
    aggregate_result=CASE WHEN $6 THEN $8::jsonb ELSE aggregate_result END,
    aggregate_digest=CASE WHEN $6 THEN $9 ELSE aggregate_digest END,
    updated_at=$10
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3`

const updateChunkLedgerSQL = `
UPDATE public.sync_run_unit_effect_chunks
SET ledger=$5::jsonb, updated_at=$6
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3 AND ordinal=$4`

const markChunkCommittedSQL = `
UPDATE public.sync_run_unit_effect_chunks
SET status='committed', updated_at=$5
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3 AND ordinal=$4
  AND status IN ('pending','committed')`

const advanceChunkCheckpointSQL = `
UPDATE public.sync_run_unit_chunk_checkpoints
SET next_ordinal=GREATEST(next_ordinal,$4), next_cursor=$5, updated_at=$6
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3`

const finalizePreparedChunksSQL = `
UPDATE public.sync_run_unit_effect_chunks
SET total_chunks=$4, updated_at=$5
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3
  AND ordinal < $4`

const finalizeChunkCheckpointSQL = `
UPDATE public.sync_run_unit_chunk_checkpoints
SET total_chunks=$4, final_ordinal=$4-1, inventory_complete=TRUE, updated_at=$5
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3
  AND next_ordinal=$4 AND prepared_chunks=$4
  AND (total_chunks=0 OR total_chunks=$4)`

const deletePreparedEffectChunksSQL = `
DELETE FROM public.sync_run_unit_effect_chunks
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3`

const deletePreparedCheckpointSQL = `
DELETE FROM public.sync_run_unit_chunk_checkpoints
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3`

func deletePreparedChunkStateTx(ctx context.Context, tx pgx.Tx, claim Claim) error {
	var present bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass('public.sync_run_unit_effect_chunks') IS NOT NULL`,
	).Scan(&present); err != nil {
		return ErrInvalidConfiguration
	}
	if !present {
		return nil
	}
	if _, err := tx.Exec(ctx, deletePreparedEffectChunksSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	if _, err := tx.Exec(ctx, deletePreparedCheckpointSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

var _ ChunkedEffectStore = (*PostgresRepository)(nil)
