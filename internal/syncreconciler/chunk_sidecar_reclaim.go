package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ChunkSidecarReclaim deletes the stored chunk rows (sync_run_unit_effect_chunks
// and their sync_run_unit_chunk_checkpoints row) of units that are already
// terminal. Only a RUNNING unit with a live lease can read them
// (internal/providersync/chunked_repository_postgres.go), success and failed are
// absorbing, and Complete/failTx are the only writers that delete them; every
// other terminal writer (lease repair, the unreclaimable sweep, the dispatch
// denials, ...) leaves them behind (CHAOS-14243).
//
// It takes no lease and never locks the unit row. Each statement runs in its
// own transaction, holds only the candidate's checkpoint row lock, and deletes
// at most RowsPerStatement rows and BytesPerStatement payload bytes (the first
// row always goes). The checkpoint row goes last, once no chunk row is left, so
// its ON DELETE CASCADE never deletes chunk rows. BytesPerMinute bounds the
// payload this process deletes in any 60-second window.
type ChunkSidecarReclaim struct {
	begin  chunkSidecarReclaimBeginFunc
	config ChunkSidecarReclaimConfig

	mu     sync.Mutex
	spent  []chunkReclaimSpend
	active bool
}

type chunkSidecarReclaimBeginFunc func(context.Context) (pgx.Tx, error)

type chunkReclaimSpend struct {
	at    time.Time
	bytes int64
}

// ChunkSidecarReclaimStepper is the pipeline seam for ChunkSidecarReclaim.
type ChunkSidecarReclaimStepper interface {
	Step(context.Context, time.Time, int) (ChunkSidecarReclaimResult, error)
}

type ChunkSidecarReclaimConfig struct {
	RowsPerStatement  int
	BytesPerStatement int64
	BytesPerMinute    int64
	StatementsPerTick int
}

// ChunkSidecarReclaimResult is one Step's work. RemainingUnits is measured only
// when the Step did work or was throttled.
type ChunkSidecarReclaimResult struct {
	Statements     int
	Rows           int64
	Bytes          int64
	Units          int
	Throttled      bool
	RemainingUnits int64
}

// maxChunkPayloadBytes is ck_sync_chunk_payload_bytes' upper bound: a statement
// starts only when the minute window can take one whole row.
const maxChunkPayloadBytes = 2 << 20

const chunkReclaimWindow = time.Minute

func DefaultChunkSidecarReclaimConfig() ChunkSidecarReclaimConfig {
	return ChunkSidecarReclaimConfig{
		RowsPerStatement:  256,
		BytesPerStatement: 32 << 20,
		BytesPerMinute:    256 << 20,
		StatementsPerTick: 4,
	}
}

func (config ChunkSidecarReclaimConfig) valid() bool {
	return config.RowsPerStatement >= 1 && config.RowsPerStatement <= 1000 &&
		config.BytesPerStatement >= 1 &&
		config.BytesPerMinute >= maxChunkPayloadBytes &&
		config.StatementsPerTick >= 1 && config.StatementsPerTick <= 64
}

func NewChunkSidecarReclaim(pool *pgxpool.Pool) (*ChunkSidecarReclaim, error) {
	return NewChunkSidecarReclaimWithConfig(pool, DefaultChunkSidecarReclaimConfig())
}

func NewChunkSidecarReclaimWithConfig(pool *pgxpool.Pool, config ChunkSidecarReclaimConfig) (*ChunkSidecarReclaim, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newChunkSidecarReclaim(pool.Begin, config)
}

func newChunkSidecarReclaim(begin chunkSidecarReclaimBeginFunc, config ChunkSidecarReclaimConfig) (*ChunkSidecarReclaim, error) {
	if begin == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &ChunkSidecarReclaim{begin: begin, config: config}, nil
}

// FOR UPDATE OF cp only: the unit row is read, never locked. A second reclaimer
// skips the checkpoint this one holds.
const selectChunkReclaimCandidateSQL = `
SELECT cp.org_id, cp.sync_run_unit_id::text, cp.generation
FROM public.sync_run_unit_chunk_checkpoints AS cp
JOIN public.sync_run_units AS unit
  ON unit.org_id = cp.org_id AND unit.id = cp.sync_run_unit_id
WHERE unit.status IN ('success', 'failed')
ORDER BY cp.updated_at, cp.sync_run_unit_id
LIMIT 1
FOR UPDATE OF cp SKIP LOCKED`

// The running sum keeps a statement under $5 payload bytes; running =
// payload_bytes holds only for the first row (payload_bytes >= 1), which always
// goes so a row larger than the cap still drains.
const deleteChunkReclaimBatchSQL = `
WITH batch AS (
  SELECT chunk.ordinal, chunk.payload_bytes::bigint AS payload_bytes,
         sum(chunk.payload_bytes::bigint) OVER (
           ORDER BY chunk.ordinal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS running
  FROM public.sync_run_unit_effect_chunks AS chunk
  WHERE chunk.org_id = $1 AND chunk.sync_run_unit_id = $2::uuid AND chunk.generation = $3
  ORDER BY chunk.ordinal
  LIMIT $4
), deleted AS (
  DELETE FROM public.sync_run_unit_effect_chunks AS chunk
  USING batch
  WHERE chunk.org_id = $1 AND chunk.sync_run_unit_id = $2::uuid AND chunk.generation = $3
    AND chunk.ordinal = batch.ordinal
    AND (batch.running <= $5::bigint OR batch.running = batch.payload_bytes)
    AND EXISTS (
      SELECT 1 FROM public.sync_run_units AS unit
      WHERE unit.org_id = $1 AND unit.id = $2::uuid AND unit.status IN ('success', 'failed')
    )
  RETURNING chunk.payload_bytes
)
SELECT count(*), COALESCE(sum(payload_bytes), 0)::bigint FROM deleted`

const deleteChunkReclaimCheckpointSQL = `
DELETE FROM public.sync_run_unit_chunk_checkpoints AS cp
WHERE cp.org_id = $1 AND cp.sync_run_unit_id = $2::uuid AND cp.generation = $3
  AND NOT EXISTS (
    SELECT 1 FROM public.sync_run_unit_effect_chunks AS chunk
    WHERE chunk.org_id = cp.org_id AND chunk.sync_run_unit_id = cp.sync_run_unit_id
      AND chunk.generation = cp.generation
  )
  AND EXISTS (
    SELECT 1 FROM public.sync_run_units AS unit
    WHERE unit.org_id = cp.org_id AND unit.id = cp.sync_run_unit_id
      AND unit.status IN ('success', 'failed')
  )`

const countChunkReclaimRemainingSQL = `
SELECT count(*)
FROM public.sync_run_unit_chunk_checkpoints AS cp
JOIN public.sync_run_units AS unit
  ON unit.org_id = cp.org_id AND unit.id = cp.sync_run_unit_id
WHERE unit.status IN ('success', 'failed')`

// Step runs at most min(StatementsPerTick, limit) delete statements. After the
// first it starts another only while the stage deadline leaves twice the last
// statement's duration.
func (reclaim *ChunkSidecarReclaim) Step(ctx context.Context, now time.Time, limit int) (ChunkSidecarReclaimResult, error) {
	if reclaim == nil || reclaim.begin == nil || ctx == nil || now.IsZero() || limit < 1 {
		return ChunkSidecarReclaimResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return ChunkSidecarReclaimResult{}, err
	}
	statements := reclaim.config.StatementsPerTick
	if limit < statements {
		statements = limit
	}
	var result ChunkSidecarReclaimResult
	drained := false
	for result.Statements < statements {
		allowance := reclaim.allowance(now)
		if allowance < maxChunkPayloadBytes {
			result.Throttled = true
			break
		}
		budget := reclaim.config.BytesPerStatement
		if allowance < budget {
			budget = allowance
		}
		started := time.Now()
		rows, bytes, units, found, err := reclaim.statement(ctx, budget)
		if err != nil {
			return result, err
		}
		if !found {
			drained = true
			break
		}
		reclaim.record(now, bytes)
		result.Statements++
		result.Rows += rows
		result.Bytes += bytes
		result.Units += units
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 2*time.Since(started) {
			break
		}
	}
	if result.Statements > 0 || result.Throttled {
		remaining, err := reclaim.remaining(ctx)
		if err != nil {
			return result, err
		}
		result.RemainingUnits = remaining
		done := drained && remaining == 0
		reclaim.setActive(!done)
		slog.Info("syncreconciler.chunk_sidecar_reclaim",
			"bytes", result.Bytes, "rows", result.Rows, "units", result.Units,
			"statements", result.Statements, "throttled", result.Throttled,
			"remaining_units", remaining, "done", done,
		)
		return result, nil
	}
	if drained && reclaim.setActive(false) {
		slog.Info("syncreconciler.chunk_sidecar_reclaim",
			"bytes", 0, "rows", 0, "units", 0, "statements", 0, "throttled", false,
			"remaining_units", 0, "done", true,
		)
	}
	return result, nil
}

func (reclaim *ChunkSidecarReclaim) statement(ctx context.Context, budget int64) (rows, bytes int64, units int, found bool, err error) {
	tx, err := reclaim.begin(ctx)
	if err != nil {
		return 0, 0, 0, false, reclaimError(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var orgID, unitID, generation string
	err = tx.QueryRow(ctx, selectChunkReclaimCandidateSQL).Scan(&orgID, &unitID, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, 0, false, reclaimError(ctx, err)
	}
	if err := tx.QueryRow(ctx, deleteChunkReclaimBatchSQL,
		orgID, unitID, generation, reclaim.config.RowsPerStatement, budget,
	).Scan(&rows, &bytes); err != nil {
		return 0, 0, 0, false, reclaimError(ctx, err)
	}
	if rows < int64(reclaim.config.RowsPerStatement) {
		command, err := tx.Exec(ctx, deleteChunkReclaimCheckpointSQL, orgID, unitID, generation)
		if err != nil {
			return 0, 0, 0, false, reclaimError(ctx, err)
		}
		units = int(command.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, 0, false, reclaimError(ctx, err)
	}
	return rows, bytes, units, true, nil
}

func (reclaim *ChunkSidecarReclaim) remaining(ctx context.Context) (int64, error) {
	tx, err := reclaim.begin(ctx)
	if err != nil {
		return 0, reclaimError(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count int64
	if err := tx.QueryRow(ctx, countChunkReclaimRemainingSQL).Scan(&count); err != nil {
		return 0, reclaimError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, reclaimError(ctx, err)
	}
	return count, nil
}

func reclaimError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%w: chunk sidecar reclaim: %w", ErrUnavailable, err)
}

func (reclaim *ChunkSidecarReclaim) allowance(now time.Time) int64 {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	cutoff := now.Add(-chunkReclaimWindow)
	kept := reclaim.spent[:0]
	var used int64
	for _, spend := range reclaim.spent {
		if spend.at.After(cutoff) {
			kept = append(kept, spend)
			used += spend.bytes
		}
	}
	reclaim.spent = kept
	return reclaim.config.BytesPerMinute - used
}

func (reclaim *ChunkSidecarReclaim) record(now time.Time, bytes int64) {
	if bytes <= 0 {
		return
	}
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	reclaim.spent = append(reclaim.spent, chunkReclaimSpend{at: now, bytes: bytes})
}

// setActive reports whether the state changed.
func (reclaim *ChunkSidecarReclaim) setActive(active bool) bool {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	changed := reclaim.active != active
	reclaim.active = active
	return changed
}
