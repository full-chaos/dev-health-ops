package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/workersignals"
	"github.com/jackc/pgx/v5"
)

// dispatchLockWaitBudget bounds how long ONE acquisition (a pass's bucket locks,
// or its budget locks) may wait for advisory locks another pass holds
// (CHAOS-6889). A blocking pg_advisory_xact_lock parks the pass's transaction
// connection in the lock wait for as long as the holder runs; with the small
// domain pool (4 by default) several passes on one bucket held every
// connection between them, so the holder could not get one for its own reads
// and the whole pool, a lease heartbeat included, sat behind that wait. The
// wait is now a bounded try-lock poll: a pass that cannot get its locks inside
// the budget gives its connection back and is retried later.
//
// A variable so the tests can shorten it; production never changes it.
var dispatchLockWaitBudget = 5 * time.Second

// advisoryLockPollStart and advisoryLockPollMax bound the try-lock polling
// backoff.
const (
	advisoryLockPollStart = 5 * time.Millisecond
	advisoryLockPollMax   = 100 * time.Millisecond
)

// ErrDispatchLockBusy is a pass that could not take the advisory locks it needs
// inside dispatchLockWaitBudget because another pass holds them. It is
// retryable (it wraps ErrDiscoveryTransientFailure, the class every other
// transient database outcome in this package already has): the transaction rolls
// back, the connection returns to the pool, and the job is retried later.
var ErrDispatchLockBusy = fmt.Errorf("%w: advisory lock is held by another dispatch pass", ErrDiscoveryTransientFailure)

// acquireAdvisoryLocksBounded takes each key's transaction-scoped advisory lock
// in the order given (callers sort the keys: a consistent global order is the
// deadlock defence, unchanged) by polling pg_try_advisory_xact_lock until the
// shared budget is spent. On a budget miss it returns ErrDispatchLockBusy
// naming how many keys were taken and how long it waited; what it took is
// released with the transaction's rollback.
func acquireAdvisoryLocksBounded(ctx context.Context, tx pgx.Tx, keys []int64, what string, budget time.Duration) error {
	err := acquireAdvisoryLocksWithinBudget(ctx, tx, keys, what, budget)
	switch {
	case err == nil:
		workersignals.RecordDispatchLockWait(workersignals.LockOutcomeAcquired)
	case isDispatchLockBusy(err):
		workersignals.RecordDispatchLockWait(workersignals.LockOutcomeBusy)
	}
	return err
}

// acquireAdvisoryLocksWithinBudget is the polling loop; acquireAdvisoryLocksBounded counts its
// outcome (CHAOS-6920), so a quiet prod window reads as zeros, not as an absent series.
func acquireAdvisoryLocksWithinBudget(ctx context.Context, tx pgx.Tx, keys []int64, what string, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for index, key := range keys {
		wait := advisoryLockPollStart
		for {
			var acquired bool
			if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&acquired); err != nil {
				return fmt.Errorf("%w: acquire %s advisory lock: %w", ErrDiscoveryTransientFailure, what, err)
			}
			if acquired {
				break
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("%w: %s lock %d of %d not free after %s", ErrDispatchLockBusy, what, index+1, len(keys), budget)
			}
			if wait > remaining {
				wait = remaining
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("%w: acquire %s advisory lock: %w", ErrDiscoveryTransientFailure, what, ctx.Err())
			case <-timer.C:
			}
			if wait *= 2; wait > advisoryLockPollMax {
				wait = advisoryLockPollMax
			}
		}
	}
	return nil
}

// isDispatchLockBusy reports whether err is a bounded-wait miss.
func isDispatchLockBusy(err error) bool { return errors.Is(err, ErrDispatchLockBusy) }
