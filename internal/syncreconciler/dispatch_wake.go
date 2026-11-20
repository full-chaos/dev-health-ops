package syncreconciler

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// appendWakeRun records a run and bucket whose unit this pass moved out of its
// concurrency slot. Every reconciler writer walks its candidates in ascending
// sync_run_id order, so a slot repeats only adjacently and the last element is
// the whole dedupe.
func appendWakeRun(runs []syncrunrollup.SlotFree, slot syncrunrollup.SlotFree) []syncrunrollup.SlotFree {
	if len(runs) > 0 && runs[len(runs)-1] == slot {
		return runs
	}
	return append(runs, slot)
}

// wakeDispatchAfterCommit releases each slot (CHAOS-10013, CHAOS-14250): the bucket's sweep when sweeps is set, else
// the run's own wake. It
// is called only after the writer's transaction committed, one bounded
// statement per run, and it never fails the writer: see
// syncrunrollup.WakeDispatch. db is nil only on the begin-func constructors the
// unit tests use, which have no pool to wake through.
func wakeDispatchAfterCommit(ctx context.Context, db syncrunrollup.Execer, sweeps syncrunrollup.BucketSweepEnqueuer, runs []syncrunrollup.SlotFree, now time.Time, path string) {
	if db == nil {
		return
	}
	for _, slot := range runs {
		syncrunrollup.ReleaseSlot(ctx, db, sweeps, slot, now, path)
	}
}
