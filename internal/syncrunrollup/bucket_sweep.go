package syncrunrollup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5"
)

// SlotFree names one unit that left its concurrency slot: its run and its (org, provider, cost class) bucket. RetryAt
// is the unit's new available_at when the writer moved it to a retry (zero otherwise).
type SlotFree struct {
	RunID     string
	OrgID     string
	Provider  string
	CostClass string
	RetryAt   time.Time
}

// QueryExecer is what the bucket signal statements need: *pgxpool.Pool and pgx.Tx both satisfy it.
type QueryExecer interface {
	Execer
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

// BumpBucketGenerationSQL signals a bucket (CHAOS-14250): every wake source moves the bucket's generation before it
// enqueues the sweep, so a sweep that is already running (River drops the enqueue as its duplicate) sees the move when
// it acknowledges, and runs again.
const BumpBucketGenerationSQL = `
INSERT INTO public.sync_dispatch_bucket_signals (org_id, provider, cost_class, generation, acked_generation, updated_at)
VALUES ($1, $2, $3, 1, 0, $4)
ON CONFLICT (org_id, provider, cost_class) DO UPDATE
SET generation = public.sync_dispatch_bucket_signals.generation + 1, updated_at = EXCLUDED.updated_at`

// BumpBucketGeneration runs BumpBucketGenerationSQL as its own statement, after the caller's commit.
func BumpBucketGeneration(ctx context.Context, db Execer, orgID, provider, costClass string, now time.Time) error {
	if _, err := db.Exec(ctx, BumpBucketGenerationSQL, orgID, provider, costClass, now.UTC()); err != nil {
		return fmt.Errorf("bump bucket generation: %w", err)
	}
	return nil
}

const readBucketGenerationSQL = `
SELECT generation FROM public.sync_dispatch_bucket_signals WHERE org_id = $1 AND provider = $2 AND cost_class = $3`

// ReadBucketGeneration is the bucket's generation (0 for a bucket never signalled).
func ReadBucketGeneration(ctx context.Context, db QueryExecer, orgID, provider, costClass string) (int64, error) {
	var generation int64
	err := db.QueryRow(ctx, readBucketGenerationSQL, orgID, provider, costClass).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read bucket generation: %w", err)
	}
	return generation, nil
}

const ackBucketGenerationSQL = `
UPDATE public.sync_dispatch_bucket_signals SET acked_generation = $4
WHERE org_id = $1 AND provider = $2 AND cost_class = $3 AND generation = $4`

// AckBucketGeneration acknowledges the generation a sweep read at its start, and reports signaled when the bucket's
// generation has moved since (that generation stays unacknowledged; the caller sweeps again).
func AckBucketGeneration(ctx context.Context, db QueryExecer, orgID, provider, costClass string, generation int64) (bool, error) {
	tag, err := db.Exec(ctx, ackBucketGenerationSQL, orgID, provider, costClass, generation)
	if err != nil {
		return false, fmt.Errorf("ack bucket generation: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return false, nil
	}
	current, err := ReadBucketGeneration(ctx, db, orgID, provider, costClass)
	if err != nil {
		return false, err
	}
	return current != generation, nil
}

// BucketSweepEnqueuer enqueues the sync.bucket_sweep job of one bucket (CHAOS-14250). The job is unique per bucket,
// so concurrent calls collapse into one pending sweep. EnqueueBucketSweep is a release: a slot is free now, so a sweep
// that waits on its safety re-arm is made due now. EnsureBucketSweep only makes sure the bucket has a sweep (a pass
// the cap held back): a pending one keeps its due time. Both run after the caller's commit, outside any transaction,
// and their error never fails the caller.
type BucketSweepEnqueuer interface {
	EnqueueBucketSweep(ctx context.Context, orgID, provider, costClass string) error
	EnsureBucketSweep(ctx context.Context, orgID, provider, costClass string) error
}

// UnacknowledgedBucketSignalsSQL lists the buckets whose last signal is at least the recovery age old ($1 is now less
// that age) and still unacknowledged, oldest signal first, a page of $2 after the keyset cursor ($3 updated_at, $4
// org, $5 provider, $6 cost class): the reconciler's materializer enqueues their sweeps.
const UnacknowledgedBucketSignalsSQL = `
SELECT signal.org_id, signal.provider, signal.cost_class, signal.updated_at
FROM public.sync_dispatch_bucket_signals AS signal
WHERE signal.generation > signal.acked_generation
	AND signal.updated_at <= $1
	AND (signal.updated_at, signal.org_id, signal.provider, signal.cost_class) > ($3::timestamptz, $4::text, $5::text, $6::text)
ORDER BY signal.updated_at, signal.org_id, signal.provider, signal.cost_class
LIMIT $2`

// ReleaseSlot is the post-commit wake every writer that moves a unit out of its slot runs. With a sweep enqueuer it
// signals the bucket and the bucket's sweep hands the slot to the oldest eligible waiter of the bucket, which may be
// the freeing run itself; the freeing run is not woken on its own, so it cannot overtake an older waiter. A unit moved
// to a retry in the future also arms its run's own wake at that instant (the sweep passes over the run until then).
// Without an enqueuer (a writer built without the River client), or when the signal or the enqueue fails, it falls
// back to the run's own wake (CHAOS-10013) and the sweep's safety re-arm bounds the rest.
func ReleaseSlot(ctx context.Context, db Execer, sweeps BucketSweepEnqueuer, slot SlotFree, now time.Time, path string) {
	if sweeps != nil {
		if slot.RetryAt.After(now) {
			WakeDispatchAt(ctx, db, slot.RunID, slot.RetryAt, now, path)
		}
		err := BumpBucketGeneration(ctx, db, slot.OrgID, slot.Provider, slot.CostClass, now)
		if err == nil {
			err = sweeps.EnqueueBucketSweep(ctx, slot.OrgID, slot.Provider, slot.CostClass)
		}
		if err == nil {
			return
		}
		synclog.Default().Warn(ctx, synclog.MsgDispatchSyncRunUnitCompletionWakeFailed,
			synclog.Run(synclog.ParseID(slot.RunID)),
			synclog.Text(synclog.KeyPath, synclog.ParseLabel(DispatchWakePathBucketSweep)),
			synclog.Failure(err),
		)
	}
	WakeDispatch(ctx, db, slot.RunID, now, path)
}
