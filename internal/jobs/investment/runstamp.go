package investment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// RunStampLockNamespace is the first key of the run stamp's advisory lock,
// pg_advisory_xact_lock(hashtext(RunStampLockNamespace), hashtext(org_id)).
// The two-key form is its own lock space (pg_locks objsubid 2), apart from
// every one-key bigint lock; the other two-key namespaces in the repo are
// hashtext of their own names and the integers 8846 / 8847.
const RunStampLockNamespace = "investment_run_stamp"

// runStampMaxWaits bounds the wait for the PostgreSQL clock to leave the
// stamped millisecond (one pg_sleep(0.001) each).
const runStampMaxWaits = 50

const runStampClockLeftSQL = `SELECT date_trunc('milliseconds', clock_timestamp()) > $1 FROM pg_sleep(0.001)`

// ErrRunStampClockHeld reports a PostgreSQL clock that stayed in the stamped
// millisecond for every wait: another run may hold that millisecond, so the
// run fails rather than stamp it.
var ErrRunStampClockHeld = errors.New("investment: the PostgreSQL clock did not leave the stamped millisecond")

// RunStamp issues the computed_at of one investment run.
type RunStamp interface {
	Stamp(ctx context.Context, orgID string) (time.Time, error)
}

// RunStamper issues run stamps that are strictly increasing per org: under
// the org's transaction advisory lock it reads the PostgreSQL clock at
// millisecond precision and commits only after that clock has left the
// millisecond. computed_at is the ReplacingMergeTree version of
// work_unit_investments, so two runs of one org never store one unit at one
// version, and the later-started run is the newest version in every reader
// and in a merge. One clock for every worker, so worker clock skew does not
// matter; a PostgreSQL clock step back can still repeat a millisecond. The
// lock lives only for this short transaction, never across the run.
type RunStamper struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
	// clockLeftSQL answers whether the clock left the stamp ($1); a test
	// replaces it to hold the clock.
	clockLeftSQL string
}

// ErrRunStampUnavailable reports a stamper built without a pool.
var ErrRunStampUnavailable = errors.New("investment: run stamp pool unavailable")

// NewRunStamper stamps on pool, the worker's domain pool.
func NewRunStamper(pool *pgxpool.Pool, logger *slog.Logger) (*RunStamper, error) {
	if pool == nil || logger == nil {
		return nil, ErrRunStampUnavailable
	}
	return &RunStamper{pool: pool, logger: logger, clockLeftSQL: runStampClockLeftSQL}, nil
}

// Stamp returns the run's stamp. A failure is retryable: no row is written
// yet.
func (stamper *RunStamper) Stamp(ctx context.Context, orgID string) (time.Time, error) {
	stamp, err := stamper.stamp(ctx, orgID)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: stamp the investment run: %w", workgraph.ErrUnavailable, err)
	}
	return stamp, nil
}

func (stamper *RunStamper) stamp(ctx context.Context, orgID string) (time.Time, error) {
	tx, err := stamper.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, RunStampLockNamespace, orgID); err != nil {
		return time.Time{}, err
	}
	var stamp time.Time
	if err := tx.QueryRow(ctx, `SELECT date_trunc('milliseconds', clock_timestamp())`).Scan(&stamp); err != nil {
		return time.Time{}, err
	}
	left := false
	for range runStampMaxWaits {
		if err := tx.QueryRow(ctx, stamper.clockLeftSQL, stamp).Scan(&left); err != nil {
			return time.Time{}, err
		}
		if left {
			break
		}
	}
	if !left {
		stamper.logger.ErrorContext(ctx, "investment run stamp refused: the PostgreSQL clock did not leave the stamped millisecond",
			slog.String("org_id", orgID), slog.String("stamp", stamp.UTC().Format(time.RFC3339Nano)), slog.Int("waits", runStampMaxWaits))
		return time.Time{}, fmt.Errorf("%w (stamp %s)", ErrRunStampClockHeld, stamp.UTC().Format(time.RFC3339Nano))
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, err
	}
	return stamp.UTC(), nil
}
