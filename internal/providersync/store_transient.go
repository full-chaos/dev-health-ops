package providersync

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/dialclass"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrStoreTransient is the class of a unit write that the database refused for
// a reason a later attempt can outlast: a deadlock (40P01), a serialization
// failure (40001) or a lost connection. It is not a configuration error.
var ErrStoreTransient = errors.New("provider sync store write is transient")

// Store write labels (StoreTransientError.Write).
const (
	storeWriteComplete             = "complete"
	storeWriteCompleteLinearFamily = "complete_linear_family"
	storeWriteFail                 = "fail"
	storeWriteRelease              = "release"
)

// Store step labels (StoreTransientError.Step).
const (
	storeStepBegin          = "begin"
	storeStepUnitUpdate     = "unit_update"
	storeStepBump           = "bump"
	storeStepProof          = "proof"
	storeStepSnapshotDelete = "snapshot_delete"
	storeStepChunkDelete    = "chunk_delete"
	storeStepDatasetClear   = "dataset_clear"
	storeStepDatasetMark    = "dataset_mark"
	storeStepWatermark      = "watermark"
	storeStepFinalize       = "finalize"
	storeStepCommit         = "commit"
	storeStepNeedsAttention = "needs_attention"
	storeStepReadBack       = "read_back"
)

// storeWriteAttempts bounds the in-place retry of one unit write.
const storeWriteAttempts = 3

// StoreTransientError names the write and step a transient error hit. Its text
// holds fixed labels and the SQLSTATE only: a server message can name
// processes, relations and values.
type StoreTransientError struct {
	Write    string
	Step     string
	SQLState string
	// outcomeUnknown: the connection was lost at COMMIT, so the transaction may
	// have committed.
	outcomeUnknown bool
}

func (e *StoreTransientError) Error() string {
	return fmt.Sprintf("%s: write=%s step=%s sqlstate=%s", ErrStoreTransient.Error(), e.Write, e.Step, e.SQLState)
}

func (e *StoreTransientError) Is(target error) bool { return target == ErrStoreTransient }

// IsTransientStoreError classifies err by type and code only, never by text.
func IsTransientStoreError(err error) bool {
	if IsDeadlockDetected(err) || isSerializationFailure(err) || isCrashShutdown(err) {
		return true
	}
	return dialclass.IsTransient(err)
}

// isCrashShutdown is SQLSTATE 57P02 (crash_shutdown): the server lost its
// connections because another backend crashed, a lost connection like the 57P01
// and 57P03 that dialclass already accepts.
func isCrashShutdown(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "57P02"
}

func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func sqlStateOf(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "none"
}

// storeErr maps a database error of one step: a transient error becomes a
// *StoreTransientError, any other error becomes fallback, the answer the step
// gave before the transient class existed.
func storeErr(write, step string, err, fallback error) error {
	if !IsTransientStoreError(err) {
		return fallback
	}
	transient := &StoreTransientError{Write: write, Step: step, SQLState: sqlStateOf(err)}
	transient.outcomeUnknown = step == storeStepCommit && !IsDeadlockDetected(err) && !isSerializationFailure(err)
	return transient
}

// commit ends a unit write transaction. commitTx is the test seam that stands
// in for a connection lost at COMMIT; nil in production.
func (repository *PostgresRepository) commit(ctx context.Context, tx pgx.Tx) error {
	if repository.commitTx != nil {
		return repository.commitTx(ctx, tx)
	}
	return tx.Commit(ctx)
}

// storeRetryWait waits before in-place attempt `next` (2 or 3). storeRetrySleep
// replaces it in tests, so no cell measures wall time.
func (repository *PostgresRepository) storeRetryWait(ctx context.Context, next int) error {
	if repository.storeRetrySleep != nil {
		return repository.storeRetrySleep(ctx, next)
	}
	low, high := storeRetryWindow(next)
	timer := time.NewTimer(storeRetryJitter(rand.Reader, low, high))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// storeRetryWindow is the jitter window before in-place attempt `next`.
func storeRetryWindow(next int) (low, high time.Duration) {
	if next > 2 {
		return 100 * time.Millisecond, 400 * time.Millisecond
	}
	return 25 * time.Millisecond, 100 * time.Millisecond
}

// storeRetryJitter draws a uniform wait in [low, high) from entropy (crypto/rand
// in production: the code scanner refuses math/rand). When entropy fails it
// waits low.
func storeRetryJitter(entropy io.Reader, low, high time.Duration) time.Duration {
	if high <= low {
		return low
	}
	offset, err := rand.Int(entropy, big.NewInt(int64(high-low)))
	if err != nil {
		return low
	}
	return low + time.Duration(offset.Int64())
}

// storeOutcome is the state of a unit after a write whose COMMIT outcome is
// unknown.
type storeOutcome int

const (
	storeOutcomeCommitted storeOutcome = iota
	storeOutcomeNotCommitted
	storeOutcomeLeaseLost
)

// readBackLockTimeout bounds how long one read-back waits on the unit row lock.
const readBackLockTimeout = 5 * time.Second

// ReadBackUnitWriteSQL tells a committed unit write from one that did not
// commit. It is exported only for the worker-role grant cell that runs it under
// the NOSUPERUSER domain role (FOR SHARE needs UPDATE privilege on the table);
// its one caller is readBackUnitWrite. completeUnitSQL, failUnitSQL and
// releaseForRetrySQL each set status, clear lease_owner and stamp updated_at
// and last_heartbeat_at with the write's own instant; a unit still running
// under the same owner did not commit.
// FOR SHARE waits on the row lock an in-flight COMMIT of the first transaction
// still holds, so the row read is the committed or the aborted truth, never the
// version from before a COMMIT that is about to land.
const ReadBackUnitWriteSQL = `
SELECT unit.status = $3 AND unit.lease_owner IS NULL
       AND unit.updated_at = $4::timestamptz AND unit.last_heartbeat_at = $4::timestamptz,
       unit.status = 'running' AND unit.lease_owner IS NOT DISTINCT FROM $5
FROM public.sync_run_units AS unit
WHERE unit.id = $1::uuid AND unit.org_id = $2
FOR SHARE OF unit`

// readBackLockTimeoutValue is lock_timeout for one read-back: readBackLockTimeout,
// or less when the context ends sooner.
func readBackLockTimeoutValue(ctx context.Context) string {
	limit := readBackLockTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < limit {
			limit = remaining
		}
	}
	if limit < time.Millisecond {
		limit = time.Millisecond
	}
	return fmt.Sprintf("%dms", limit.Milliseconds())
}

func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// readBackUnitWrite reads the unit once, in a short transaction of its own.
func (repository *PostgresRepository) readBackUnitWrite(
	ctx context.Context, claim Claim, wantStatus string, writtenAt time.Time,
) (storeOutcome, error) {
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return storeOutcomeNotCommitted, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true)`, readBackLockTimeoutValue(ctx)); err != nil {
		return storeOutcomeNotCommitted, err
	}
	var committed, ours bool
	err = tx.QueryRow(ctx, ReadBackUnitWriteSQL,
		claim.ID, claim.OrgID, wantStatus, writtenAt.UTC(), claim.Owner,
	).Scan(&committed, &ours)
	if errors.Is(err, pgx.ErrNoRows) {
		return storeOutcomeLeaseLost, nil
	}
	if err != nil {
		return storeOutcomeNotCommitted, err
	}
	// The SELECT read committed state under the row lock; ending the lock-only
	// transaction cannot change what it read, so its error is ignored.
	_ = tx.Rollback(ctx)
	switch {
	case committed:
		return storeOutcomeCommitted, nil
	case ours:
		return storeOutcomeNotCommitted, nil
	default:
		return storeOutcomeLeaseLost, nil
	}
}

// resolveUnitWrite settles a write whose COMMIT outcome is unknown. A
// transient error or a lock timeout on the read leaves the outcome unknown, so
// the read is tried again, up to storeWriteAttempts times; resolved is false
// when no read settled it.
func (repository *PostgresRepository) resolveUnitWrite(
	ctx context.Context, claim Claim, write, wantStatus string, writtenAt time.Time,
) (outcome storeOutcome, resolved bool) {
	for attempt := 1; ; attempt++ {
		outcome, err := repository.readBackUnitWrite(ctx, claim, wantStatus, writtenAt)
		if err == nil {
			return outcome, true
		}
		if !IsTransientStoreError(err) && !isLockTimeout(err) {
			return storeOutcomeNotCommitted, false
		}
		logStoreRetry(ctx, claim, write, storeStepReadBack, sqlStateOf(err), attempt)
		if attempt >= storeWriteAttempts {
			return storeOutcomeNotCommitted, false
		}
		if repository.storeRetryWait(ctx, attempt+1) != nil {
			return storeOutcomeNotCommitted, false
		}
	}
}

// unitWriteAttempt is one attempt of a unit write transaction. When the attempt
// reached COMMIT it returns afterCommit, the steps that run once per committed
// outcome; rollupKnown is false when the commit was only confirmed by a read-back.
type unitWriteAttempt func() (afterCommit func(rollupKnown bool), err error)

// retryUnitWrite runs one unit write (a whole transaction) up to
// storeWriteAttempts times while it fails transiently. Postgres rolled the
// transaction back on 40P01, 40001 and on a connection lost before COMMIT; a
// connection lost AT COMMIT is resolved by a read-back first.
func (repository *PostgresRepository) retryUnitWrite(
	ctx context.Context, claim Claim, wantStatus string, writtenAt time.Time, write unitWriteAttempt,
) error {
	for attempt := 1; ; attempt++ {
		afterCommit, err := write()
		if err == nil {
			afterCommit(true)
			return nil
		}
		var transient *StoreTransientError
		if !errors.As(err, &transient) {
			return err
		}
		if transient.outcomeUnknown {
			outcome, resolved := repository.resolveUnitWrite(ctx, claim, transient.Write, wantStatus, writtenAt)
			if !resolved {
				return err
			}
			switch outcome {
			case storeOutcomeCommitted:
				afterCommit(false)
				return nil
			case storeOutcomeLeaseLost:
				return ErrLeaseLost
			}
		}
		logStoreRetry(ctx, claim, transient.Write, transient.Step, transient.SQLState, attempt)
		if attempt >= storeWriteAttempts {
			return err
		}
		if repository.storeRetryWait(ctx, attempt+1) != nil {
			return err
		}
	}
}

func logStoreRetry(ctx context.Context, claim Claim, write, step, sqlState string, attempt int) {
	outcome := "retrying"
	if attempt >= storeWriteAttempts {
		outcome = "exhausted"
	}
	slog.WarnContext(ctx, "provider sync store write transient",
		slog.String("provider", claim.Provider),
		slog.String("dataset", claim.Dataset),
		slog.String("write", write),
		slog.String("step", step),
		slog.String("sqlstate", sqlState),
		slog.Int("attempt", attempt),
		slog.Int("max_attempts", storeWriteAttempts),
		slog.String("outcome", outcome),
	)
}
