package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
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
// its ON DELETE CASCADE never deletes chunk rows.
//
// Every tick first takes a cluster-wide advisory lock
// (chunkSidecarReclaimLockKey) for its whole length; a tick that does not get
// it is skipped and counted, so only one reconciler deletes at a time. The
// in-process limiter bounds the payload that holder deletes in any 60-second
// window. When the lock moves to another process, that process's limiter
// starts empty (the handover case: each process that wins the lock within one
// window can delete up to BytesPerMinute).
type ChunkSidecarReclaim struct {
	begin    chunkSidecarReclaimBeginFunc
	tickLock chunkSidecarReclaimTickLockFunc
	config   ChunkSidecarReclaimConfig
	limiter  *chunkReclaimLimiter
	clock    func() time.Time

	mu             sync.Mutex
	active         bool
	statementBytes int64
	lockSkipped    atomic.Uint64
}

type chunkSidecarReclaimBeginFunc func(context.Context) (pgx.Tx, error)

// chunkSidecarReclaimTickLockFunc takes the tick lock without waiting. When
// acquired, release must be called once the tick is over.
type chunkSidecarReclaimTickLockFunc func(context.Context) (release func(), acquired bool, err error)

// chunkSidecarReclaimLockKey is the single-bigint advisory lock key of the
// reclaim tick (CHAOS-14243); no other code in this repository takes it.
const chunkSidecarReclaimLockKey int64 = 1424300000000001

// poolChunkSidecarReclaimTickLock takes a TRANSACTION-level advisory lock in
// one explicit transaction on one pooled connection and keeps that transaction
// open for the tick. The domain DSN goes through PgBouncer in transaction mode
// (chart go-pgbouncer.yaml POSTGRES_URI -> go-pgbouncer-transaction), which
// pins a server connection only for a transaction, so a session lock could be
// taken on one server connection and "released" on another. The transaction
// lock ends with the transaction on the same server connection. A connection
// whose rollback is not confirmed is closed (fresh context), which ends the
// transaction and its lock on the server side.
func poolChunkSidecarReclaimTickLock(pool *pgxpool.Pool) chunkSidecarReclaimTickLockFunc {
	return func(ctx context.Context) (func(), bool, error) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return nil, false, err
		}
		discard := func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = conn.Conn().Close(closeCtx)
			conn.Release()
		}
		end := func(tx pgx.Tx) {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := tx.Rollback(rollbackCtx)
			cancel()
			if err != nil {
				discard()
				return
			}
			conn.Release()
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			discard()
			return nil, false, err
		}
		var acquired bool
		if err := tx.QueryRow(ctx, `SELECT pg_catalog.pg_try_advisory_xact_lock($1)`, chunkSidecarReclaimLockKey).Scan(&acquired); err != nil {
			discard()
			return nil, false, err
		}
		if !acquired {
			end(tx)
			return nil, false, nil
		}
		return func() { end(tx) }, true, nil
	}
}

// ChunkSidecarReclaimStepper is the pipeline seam for ChunkSidecarReclaim.
type ChunkSidecarReclaimStepper interface {
	Step(context.Context, time.Time, int) (ChunkSidecarReclaimResult, error)
}

// chunkSidecarReclaimFloorStepper is the optional floor-size run a stepper
// offers for the tick after a capped yield streak; without it the pipeline
// calls Step.
type chunkSidecarReclaimFloorStepper interface {
	StepAtFloor(context.Context, time.Time, int) (ChunkSidecarReclaimResult, error)
}

// ChunkSidecarReclaimConfig values may only be lowered from the defaults:
// DefaultChunkSidecarReclaimConfig is also the upper bound valid() enforces.
type ChunkSidecarReclaimConfig struct {
	RowsPerStatement  int
	BytesPerStatement int64
	BytesPerMinute    int64
	StatementsPerTick int
}

// ChunkSidecarReclaimResult is one Step's committed work. RemainingUnits is -1
// when the Step did not measure the backlog.
type ChunkSidecarReclaimResult struct {
	Statements     int
	Rows           int64
	Bytes          int64
	Units          int
	Throttled      bool
	RemainingUnits int64
	Done           bool
	LockSkipped    bool
}

// maxChunkPayloadBytes is ck_sync_chunk_payload_bytes' upper bound, the largest
// row the stage can delete: a statement is admitted only when the window can
// take one whole row, and BytesPerStatement may not go below it.
const maxChunkPayloadBytes = 2 << 20

const chunkReclaimWindow = time.Minute

// chunkReclaimDeadlineCutsPerWindow bounds the statements the stage deadline
// may cut in one limiter window: a cut statement committed nothing, so it is
// charged BytesPerMinute/8 (not refunded), and a slow server gets at most 8
// budget-long statements a minute before the cap throttles the reclaim.
const chunkReclaimDeadlineCutsPerWindow = 8

// DefaultChunkSidecarReclaimConfig sizes one statement to fit the 1500 ms stage
// budget on a slow server (CHAOS-14380): in prod a 256-row statement was about
// 13.6 MiB (55.8 KB per row) and often ran past the budget; 64 rows is about
// 3.4 MiB, under the 4 MiB byte cap, and the byte cap may not go below one
// 2 MiB row (maxChunkPayloadBytes).
func DefaultChunkSidecarReclaimConfig() ChunkSidecarReclaimConfig {
	return ChunkSidecarReclaimConfig{
		RowsPerStatement:  64,
		BytesPerStatement: 4 << 20,
		BytesPerMinute:    256 << 20,
		StatementsPerTick: 4,
	}
}

func (config ChunkSidecarReclaimConfig) valid() bool {
	ceiling := DefaultChunkSidecarReclaimConfig()
	return config.RowsPerStatement >= 1 && config.RowsPerStatement <= ceiling.RowsPerStatement &&
		config.BytesPerStatement >= maxChunkPayloadBytes && config.BytesPerStatement <= ceiling.BytesPerStatement &&
		config.BytesPerMinute >= maxChunkPayloadBytes && config.BytesPerMinute <= ceiling.BytesPerMinute &&
		config.StatementsPerTick >= 1 && config.StatementsPerTick <= ceiling.StatementsPerTick
}

// processChunkReclaimLimiter is shared by every reclaimer the production
// constructor builds, so the per-minute bound holds per process.
var processChunkReclaimLimiter = newChunkReclaimLimiter(DefaultChunkSidecarReclaimConfig().BytesPerMinute)

func NewChunkSidecarReclaim(pool *pgxpool.Pool) (*ChunkSidecarReclaim, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	reclaim, err := newChunkSidecarReclaim(pool.Begin, poolChunkSidecarReclaimTickLock(pool), DefaultChunkSidecarReclaimConfig())
	if err != nil {
		return nil, err
	}
	reclaim.limiter = processChunkReclaimLimiter
	return reclaim, nil
}

// NewChunkSidecarReclaimWithConfig builds a reclaimer with its own limiter.
func NewChunkSidecarReclaimWithConfig(pool *pgxpool.Pool, config ChunkSidecarReclaimConfig) (*ChunkSidecarReclaim, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newChunkSidecarReclaim(pool.Begin, poolChunkSidecarReclaimTickLock(pool), config)
}

func newChunkSidecarReclaim(begin chunkSidecarReclaimBeginFunc, tickLock chunkSidecarReclaimTickLockFunc, config ChunkSidecarReclaimConfig) (*ChunkSidecarReclaim, error) {
	if begin == nil || tickLock == nil || !config.valid() {
		return nil, ErrInvalidConfiguration
	}
	return &ChunkSidecarReclaim{
		begin:          begin,
		tickLock:       tickLock,
		config:         config,
		limiter:        newChunkReclaimLimiter(config.BytesPerMinute),
		clock:          time.Now,
		statementBytes: config.BytesPerStatement,
	}, nil
}

// LockSkippedTicks counts the ticks skipped because another session held the
// tick lock.
func (reclaim *ChunkSidecarReclaim) LockSkippedTicks() uint64 {
	return reclaim.lockSkipped.Load()
}

// chunkSidecarReclaimMissing reports a nil dependency, including a typed nil
// held in the interface.
func chunkSidecarReclaimMissing(stepper ChunkSidecarReclaimStepper) bool {
	if stepper == nil {
		return true
	}
	value := reflect.ValueOf(stepper)
	switch value.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.Interface:
		return value.IsNil()
	}
	return false
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
// statement's duration. remaining_units and done come from the backlog count,
// never from candidate selection; a tick that committed work logs it even when
// a later statement or the count fails.
func (reclaim *ChunkSidecarReclaim) Step(ctx context.Context, now time.Time, limit int) (ChunkSidecarReclaimResult, error) {
	return reclaim.step(ctx, now, limit, false)
}

// StepAtFloor runs one statement of at most one largest row
// (maxChunkPayloadBytes): the pipeline's run after a capped yield streak
// (CHAOS-14380), so a stage that is slow every tick cannot starve the cleanup.
func (reclaim *ChunkSidecarReclaim) StepAtFloor(ctx context.Context, now time.Time, limit int) (ChunkSidecarReclaimResult, error) {
	return reclaim.step(ctx, now, limit, true)
}

func (reclaim *ChunkSidecarReclaim) step(ctx context.Context, now time.Time, limit int, floor bool) (ChunkSidecarReclaimResult, error) {
	if reclaim == nil || reclaim.begin == nil || reclaim.tickLock == nil || reclaim.limiter == nil ||
		reclaim.clock == nil || ctx == nil || now.IsZero() || limit < 1 {
		return ChunkSidecarReclaimResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return ChunkSidecarReclaimResult{}, err
	}
	release, acquired, err := reclaim.tickLock(ctx)
	if err != nil {
		return reclaim.report(ChunkSidecarReclaimResult{RemainingUnits: -1}, reclaimError(ctx, err))
	}
	if !acquired {
		reclaim.lockSkipped.Add(1)
		return ChunkSidecarReclaimResult{RemainingUnits: -1, LockSkipped: true}, nil
	}
	defer release()
	statements := reclaim.config.StatementsPerTick
	if limit < statements {
		statements = limit
	}
	if floor {
		statements = 1
	}
	result := ChunkSidecarReclaimResult{RemainingUnits: -1}
	for result.Statements < statements {
		statementBytes := reclaim.nextStatementBytes()
		if floor {
			statementBytes = maxChunkPayloadBytes
		}
		reservation := reclaim.limiter.reserve(reclaim.clock(), statementBytes)
		if reservation == nil {
			result.Throttled = true
			break
		}
		started := time.Now()
		rows, bytes, units, found, err := reclaim.statement(ctx, reservation)
		if err != nil {
			return reclaim.report(result, err)
		}
		if !found {
			break
		}
		result.Statements++
		result.Rows += rows
		result.Bytes += bytes
		result.Units += units
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 2*time.Since(started) {
			break
		}
	}
	if result.Statements == 0 && !result.Throttled && !reclaim.isActive() {
		return result, nil
	}
	remaining, err := reclaim.remaining(ctx)
	if err != nil {
		return reclaim.report(result, err)
	}
	result.RemainingUnits = remaining
	result.Done = remaining == 0
	if result.Done {
		result.Throttled = false
	}
	if result.Statements == 0 && result.Done && !reclaim.isActive() {
		return result, nil
	}
	return reclaim.report(result, nil)
}

// report writes the one per-tick line and passes err through. A failed tick
// keeps its committed totals and reports remaining_units=-1 (not measured); it
// stays silent only when the reclaimer was idle and nothing was throttled.
func (reclaim *ChunkSidecarReclaim) report(result ChunkSidecarReclaimResult, err error) (ChunkSidecarReclaimResult, error) {
	if err != nil {
		result.RemainingUnits = -1
		result.Done = false
		if result.Statements == 0 && !result.Throttled && !reclaim.isActive() {
			return result, err
		}
	}
	reclaim.setActive(!result.Done)
	attributes := []any{
		"bytes", result.Bytes, "rows", result.Rows, "units", result.Units,
		"statements", result.Statements, "throttled", result.Throttled,
		"remaining_units", result.RemainingUnits, "done", result.Done,
	}
	if err != nil {
		attributes = append(attributes, "error", err.Error())
	}
	slog.Info("syncreconciler.chunk_sidecar_reclaim", attributes...)
	return result, err
}

// statement settles reservation with the committed bytes, releases it when the
// transaction is known not to have committed, and keeps the whole reservation
// when COMMIT itself fails (outcome unknown). A statement the stage deadline
// cut after BEGIN is charged instead of released, and the next statement is
// half as large: a refund let every tick re-run the same doomed statement for
// its whole budget while the byte cap never engaged (CHAOS-14380).
func (reclaim *ChunkSidecarReclaim) statement(ctx context.Context, reservation *chunkReclaimSpend) (rows, bytes int64, units int, found bool, err error) {
	committed := false
	defer func() {
		if !committed && err == nil {
			reclaim.limiter.settle(reservation, 0, reclaim.clock())
		}
	}()
	tx, err := reclaim.begin(ctx)
	if err != nil {
		reclaim.limiter.settle(reservation, 0, reclaim.clock())
		return 0, 0, 0, false, reclaimError(ctx, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	failed := func(cause error, unknownOutcome bool) (int64, int64, int, bool, error) {
		charge := int64(0)
		if unknownOutcome {
			charge = reservation.bytes
		}
		if ctx.Err() != nil {
			charge = max(charge, reservation.bytes, reclaim.config.BytesPerMinute/chunkReclaimDeadlineCutsPerWindow)
			reclaim.shrinkStatement()
		}
		committed = true
		reclaim.limiter.settle(reservation, charge, reclaim.clock())
		return 0, 0, 0, false, reclaimError(ctx, cause)
	}
	var orgID, unitID, generation string
	err = tx.QueryRow(ctx, selectChunkReclaimCandidateSQL).Scan(&orgID, &unitID, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, 0, false, nil
	}
	if err != nil {
		return failed(err, false)
	}
	if err := tx.QueryRow(ctx, deleteChunkReclaimBatchSQL,
		orgID, unitID, generation, reclaim.config.RowsPerStatement, reservation.bytes,
	).Scan(&rows, &bytes); err != nil {
		return failed(err, false)
	}
	if rows < int64(reclaim.config.RowsPerStatement) {
		command, err := tx.Exec(ctx, deleteChunkReclaimCheckpointSQL, orgID, unitID, generation)
		if err != nil {
			return failed(err, false)
		}
		units = int(command.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return failed(err, true)
	}
	committed = true
	reclaim.limiter.settle(reservation, bytes, reclaim.clock())
	reclaim.growStatement()
	return rows, bytes, units, true, nil
}

func (reclaim *ChunkSidecarReclaim) nextStatementBytes() int64 {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	return reclaim.statementBytes
}

func (reclaim *ChunkSidecarReclaim) shrinkStatement() {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	reclaim.statementBytes = max(reclaim.statementBytes/2, maxChunkPayloadBytes)
}

func (reclaim *ChunkSidecarReclaim) growStatement() {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	reclaim.statementBytes = min(reclaim.statementBytes*2, reclaim.config.BytesPerStatement)
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

func (reclaim *ChunkSidecarReclaim) isActive() bool {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	return reclaim.active
}

func (reclaim *ChunkSidecarReclaim) setActive(active bool) {
	reclaim.mu.Lock()
	defer reclaim.mu.Unlock()
	reclaim.active = active
}

type chunkReclaimSpend struct {
	at      time.Time
	bytes   int64
	settled bool
}

// chunkReclaimLimiter admits a statement by reserving its byte budget under one
// lock, so overlapping reclaimers sharing it can never spend the same bytes. A
// reservation counts from admission until 60 s after it settles; an unsettled
// one always counts.
type chunkReclaimLimiter struct {
	mu     sync.Mutex
	limit  int64
	spends []*chunkReclaimSpend
}

func newChunkReclaimLimiter(limit int64) *chunkReclaimLimiter {
	return &chunkReclaimLimiter{limit: limit}
}

// reserve returns nil when the window cannot take one whole row.
func (limiter *chunkReclaimLimiter) reserve(now time.Time, maxBytes int64) *chunkReclaimSpend {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	available := limiter.limit - limiter.usedLocked(now)
	if available < maxChunkPayloadBytes {
		return nil
	}
	granted := maxBytes
	if available < granted {
		granted = available
	}
	spend := &chunkReclaimSpend{at: now, bytes: granted}
	limiter.spends = append(limiter.spends, spend)
	return spend
}

func (limiter *chunkReclaimLimiter) settle(spend *chunkReclaimSpend, bytes int64, at time.Time) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	spend.bytes = bytes
	spend.at = at
	spend.settled = true
}

// available is the budget a reservation at now could get.
func (limiter *chunkReclaimLimiter) available(now time.Time) int64 {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return limiter.limit - limiter.usedLocked(now)
}

func (limiter *chunkReclaimLimiter) usedLocked(now time.Time) int64 {
	cutoff := now.Add(-chunkReclaimWindow)
	kept := limiter.spends[:0]
	var used int64
	for _, spend := range limiter.spends {
		if !spend.settled || (spend.bytes > 0 && spend.at.After(cutoff)) {
			kept = append(kept, spend)
			used += spend.bytes
		}
	}
	for index := len(kept); index < len(limiter.spends); index++ {
		limiter.spends[index] = nil
	}
	limiter.spends = kept
	return used
}
