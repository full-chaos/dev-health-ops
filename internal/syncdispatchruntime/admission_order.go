package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
)

// runOrder is the admission position of a run (CHAOS-8880): runs of one bucket are admitted oldest first on the
// total order (createdAt, id). backfill marks a run that never holds back a non-backfill run.
type runOrder struct {
	id        string
	createdAt time.Time
	backfill  bool
	// mode and triggeredBy are the stored columns backfill is derived from; readRunOrder fills them for the log.
	mode        string
	triggeredBy string
}

// admissionLivenessBound is how long a bucket sweep's reservation holds its slot (CHAOS-14250). A reserved run whose
// pass never runs (a paused route) stops holding the slot after it.
func admissionLivenessBound() time.Duration {
	return syncbucket.ReservationTTL()
}

// readRunOrder reads the deciding run's admission position in the guard transaction.
func readRunOrder(ctx context.Context, tx pgx.Tx, runID string) (runOrder, error) {
	order := runOrder{id: runID}
	err := tx.QueryRow(ctx, `
SELECT created_at, (triggered_by = 'backfill' OR mode = 'backfill'), mode, triggered_by
FROM public.sync_runs
WHERE id = $1::uuid`, runID).Scan(&order.createdAt, &order.backfill, &order.mode, &order.triggeredBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return runOrder{}, fmt.Errorf("%w: read run order: sync run %s not found", ErrDiscoveryTransientFailure, runID)
	}
	if err != nil {
		return runOrder{}, fmt.Errorf("%w: read run order: %w", ErrDiscoveryTransientFailure, err)
	}
	return order, nil
}

// countBucketReservations counts the bucket's live reservations of other runs (syncbucket.CountReservations).
func countBucketReservations(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, exceptRunID string, now time.Time) (int, int, error) {
	reserved, reservedBackfill, err := syncbucket.CountReservations(ctx, tx, bucket.syncBucket(), exceptRunID, now)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	return reserved, reservedBackfill, nil
}

func retireRunReservations(ctx context.Context, tx pgx.Tx, runID string) error {
	if err := syncbucket.RetireRunReservations(ctx, tx, runID); err != nil {
		return fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	return nil
}

// wakeReservedRuns arms one dispatch wakeup per distinct run a pass reserved a slot for, after the pass committed.
func wakeReservedRuns(ctx context.Context, db syncrunrollup.Execer, runIDs []string, now time.Time) {
	syncrunrollup.WakeReserved(ctx, db, runIDs, now, syncrunrollup.DispatchWakePathHoldBack)
}
