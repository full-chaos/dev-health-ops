package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	if IsDeadlockDetected(err) || isSerializationFailure(err) {
		return true
	}
	return dialclass.IsTransient(err)
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
	low, high := 25*time.Millisecond, 100*time.Millisecond
	if next > 2 {
		low, high = 100*time.Millisecond, 400*time.Millisecond
	}
	timer := time.NewTimer(low + rand.N(high-low))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// storeOutcome is the state of a unit after a write whose COMMIT outcome is
// unknown.
type storeOutcome int

const (
	storeOutcomeCommitted storeOutcome = iota
	storeOutcomeNotCommitted
	storeOutcomeLeaseLost
)

// readBackUnitWriteSQL tells a committed terminal write from one that did not
// commit. completeUnitSQL and failUnitSQL stamp last_heartbeat_at with the
// write's completedAt and clear lease_owner; a unit still running under the
// same owner did not commit. Postgres rounds both timestamps the same way.
const readBackUnitWriteSQL = `
SELECT unit.status = $3 AND unit.last_heartbeat_at = $4::timestamptz,
       unit.status = 'running' AND unit.lease_owner IS NOT DISTINCT FROM $5
FROM public.sync_run_units AS unit
WHERE unit.id = $1::uuid AND unit.org_id = $2`

func (repository *PostgresRepository) readBackUnitWrite(
	ctx context.Context, claim Claim, terminalStatus string, completedAt time.Time,
) (storeOutcome, error) {
	var committed, ours bool
	err := repository.Pool.QueryRow(ctx, readBackUnitWriteSQL,
		claim.ID, claim.OrgID, terminalStatus, completedAt.UTC(), claim.Owner,
	).Scan(&committed, &ours)
	if errors.Is(err, pgx.ErrNoRows) {
		return storeOutcomeLeaseLost, nil
	}
	if err != nil {
		return storeOutcomeNotCommitted, err
	}
	switch {
	case committed:
		return storeOutcomeCommitted, nil
	case ours:
		return storeOutcomeNotCommitted, nil
	default:
		return storeOutcomeLeaseLost, nil
	}
}

// retryUnitWrite runs one terminal unit write (a whole transaction) up to
// storeWriteAttempts times while it fails transiently. Postgres rolled the
// transaction back on 40P01, 40001 and on a connection lost before COMMIT; a
// connection lost AT COMMIT is resolved by a read-back first. committed runs
// when the read-back finds the write committed (its post-commit steps did not
// run).
func (repository *PostgresRepository) retryUnitWrite(
	ctx context.Context, claim Claim, terminalStatus string, completedAt time.Time,
	write func() error, committed func(),
) error {
	var err error
	for attempt := 1; ; attempt++ {
		err = write()
		var transient *StoreTransientError
		if !errors.As(err, &transient) {
			return err
		}
		if transient.outcomeUnknown {
			outcome, readErr := repository.readBackUnitWrite(ctx, claim, terminalStatus, completedAt)
			if readErr != nil {
				return err
			}
			switch outcome {
			case storeOutcomeCommitted:
				committed()
				return nil
			case storeOutcomeLeaseLost:
				return ErrLeaseLost
			}
		}
		logStoreRetry(ctx, claim, transient, attempt)
		if attempt >= storeWriteAttempts {
			return err
		}
		if waitErr := repository.storeRetryWait(ctx, attempt+1); waitErr != nil {
			return err
		}
	}
}

func logStoreRetry(ctx context.Context, claim Claim, transient *StoreTransientError, attempt int) {
	outcome := "retrying"
	if attempt >= storeWriteAttempts {
		outcome = "exhausted"
	}
	slog.WarnContext(ctx, "provider sync store write transient",
		slog.String("provider", claim.Provider),
		slog.String("dataset", claim.Dataset),
		slog.String("write", transient.Write),
		slog.String("step", transient.Step),
		slog.String("sqlstate", transient.SQLState),
		slog.Int("attempt", attempt),
		slog.Int("max_attempts", storeWriteAttempts),
		slog.String("outcome", outcome),
	)
}
