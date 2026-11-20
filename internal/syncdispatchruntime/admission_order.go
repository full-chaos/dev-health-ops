package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	return 2 * redispatchCountdownMax(redispatchCountdown())
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

// bucketReservationsSQL counts the live reservations of a bucket (CHAOS-14250), all and those of backfill runs,
// leaving out the run $4 (empty: none). A reservation is a slot a bucket sweep handed to a run whose pass has not yet
// run; the sweep writes at most the bucket's free slots, so the read is a few rows of the primary key's range.
const bucketReservationsSQL = `
SELECT count(*), count(*) FILTER (WHERE reservation.backfill)
FROM public.sync_dispatch_bucket_reservations AS reservation
WHERE reservation.org_id = $1 AND reservation.provider = $2 AND reservation.cost_class = $3
    AND reservation.sync_run_id::text <> $4
    AND reservation.reserved_at > $5::timestamptz - make_interval(secs => $6::double precision)`

// countBucketReservations runs bucketReservationsSQL under the bucket lock.
func countBucketReservations(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, exceptRunID string, now time.Time) (int, int, error) {
	var reserved, reservedBackfill int
	if err := tx.QueryRow(ctx, bucketReservationsSQL, bucket.orgID, bucket.provider, bucket.costClass, exceptRunID, now,
		admissionLivenessBound().Seconds()).Scan(&reserved, &reservedBackfill); err != nil {
		return 0, 0, fmt.Errorf("%w: count bucket reservations: %w", ErrDiscoveryTransientFailure, err)
	}
	return reserved, reservedBackfill, nil
}

// retireRunReservationsSQL drops every reservation of the run: its pass is the one the slot was kept for, so the slot
// is now the pass's to claim (or, if it claims none, free again).
const retireRunReservationsSQL = `DELETE FROM public.sync_dispatch_bucket_reservations WHERE sync_run_id = $1::uuid`

func retireRunReservations(ctx context.Context, tx pgx.Tx, runID string) error {
	if _, err := tx.Exec(ctx, retireRunReservationsSQL, runID); err != nil {
		return fmt.Errorf("%w: retire run reservations: %w", ErrDiscoveryTransientFailure, err)
	}
	return nil
}
