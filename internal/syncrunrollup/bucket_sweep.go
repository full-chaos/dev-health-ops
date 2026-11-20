package syncrunrollup

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// SlotFree names one unit that left its concurrency slot: its run and its (org, provider, cost class) bucket.
type SlotFree struct {
	RunID     string
	OrgID     string
	Provider  string
	CostClass string
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

// ReleaseSlot is the post-commit wake every writer that moves a unit out of its slot runs. With a sweep enqueuer the
// bucket's sweep hands the slot to the oldest eligible waiter of the bucket, which may be the freeing run itself; the
// freeing run is not woken on its own, so it cannot overtake an older waiter. Without one (a writer built without the
// River client), or when the enqueue fails, it falls back to the run's own wake (CHAOS-10013) and the sweep's safety
// re-arm bounds the rest.
func ReleaseSlot(ctx context.Context, db Execer, sweeps BucketSweepEnqueuer, slot SlotFree, now time.Time, path string) {
	if sweeps != nil {
		err := sweeps.EnqueueBucketSweep(ctx, slot.OrgID, slot.Provider, slot.CostClass)
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
