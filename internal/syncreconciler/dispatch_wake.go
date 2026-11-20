package syncreconciler

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
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
func wakeDispatchAfterCommit(ctx context.Context, db syncrunrollup.Execer, sweeps syncrunrollup.BucketSweepEnqueuer, runs []syncrunrollup.SlotFree, reserved []string, now time.Time, path string) {
	if db == nil {
		return
	}
	syncrunrollup.WakeReserved(ctx, db, reserved, now, path)
	for _, slot := range runs {
		syncrunrollup.ReleaseSlot(ctx, db, sweeps, slot, now, path)
	}
}

// reserveReleasedSlots gives the slots these writes freed to their buckets' oldest eligible waiters, in the writer's
// transaction, under the bucket locks it took before any row (syncbucket.ReserveHeads, CHAOS-14250 N1). It returns the
// runs to wake after the commit.
func reserveReleasedSlots(ctx context.Context, tx pgx.Tx, runs []syncrunrollup.SlotFree, now time.Time) ([]string, error) {
	buckets := make([]syncbucket.Bucket, 0, len(runs))
	for _, slot := range runs {
		buckets = append(buckets, syncbucket.Bucket{OrgID: slot.OrgID, Provider: slot.Provider, CostClass: slot.CostClass})
	}
	var reserved []string
	for _, bucket := range syncbucket.Sorted(buckets) {
		heads, err := syncbucket.ReserveHeads(ctx, tx, bucket, now, "")
		if err != nil {
			return nil, err
		}
		reserved = append(reserved, heads.Reserved...)
	}
	return reserved, nil
}
