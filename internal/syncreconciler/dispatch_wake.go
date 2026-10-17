package syncreconciler

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// appendWakeRun records a run whose unit this pass moved out of its
// concurrency slot. Every reconciler writer walks its candidates in ascending
// sync_run_id order, so a run repeats only adjacently and the last element is
// the whole dedupe.
func appendWakeRun(runs []string, syncRunID string) []string {
	if len(runs) > 0 && runs[len(runs)-1] == syncRunID {
		return runs
	}
	return append(runs, syncRunID)
}

// wakeDispatchAfterCommit arms each run's next dispatch pass (CHAOS-10013). It
// is called only after the writer's transaction committed, one bounded
// statement per run, and it never fails the writer: see
// syncrunrollup.WakeDispatch. db is nil only on the begin-func constructors the
// unit tests use, which have no pool to wake through.
func wakeDispatchAfterCommit(ctx context.Context, db syncrunrollup.Execer, runs []string, now time.Time, path string) {
	if db == nil {
		return
	}
	for _, syncRunID := range runs {
		syncrunrollup.WakeDispatch(ctx, db, syncRunID, now, path)
	}
}
