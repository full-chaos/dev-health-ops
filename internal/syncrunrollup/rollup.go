// Package syncrunrollup is the ONE seam every terminal-status writer of
// public.sync_run_units in this codebase must call to keep the parent
// sync_runs row's completed_units/failed_units counters live.
//
// CHAOS-4559 first added this recompute for providersync's normal per-unit
// success/failure commit (Complete/CompleteLinearWorkItemFamily/failTx).
// CHAOS-4586 found the SAME staleness gap in five syncdispatchruntime
// denial/exhaustion paths and, on review (chris: "Not again"), generalized
// this into its own shared package rather than letting a third package
// (internal/syncreconciler's unreclaimable-dispatch sweep, also fixed under
// CHAOS-4586) grow a THIRD private copy of the same twelve lines. This is
// the CHAOS-3990 idiom applied to the rollup write instead of the unit
// write: no other code path may recompute this row.
package syncrunrollup

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// SQL keeps sync_runs.completed_units/failed_units live via a fresh
// COUNT(*) per call. Idempotent no matter how many times it runs for the
// same unit: COUNT(*) WHERE status=X cannot double-count a unit that was
// previously counted under a different terminal status, unlike a blind
// increment.
const SQL = `
UPDATE public.sync_runs
SET completed_units = (
      SELECT count(*) FROM public.sync_run_units
      WHERE sync_run_id = $1 AND status = 'success'
    ),
    failed_units = (
      SELECT count(*) FROM public.sync_run_units
      WHERE sync_run_id = $1 AND status = 'failed'
    )
WHERE id = $1
RETURNING completed_units, failed_units, total_units`

// Bump runs SQL and returns the resulting counters (completed, failed,
// total) so the caller can log/record telemetry.
//
// Lock-first, ALWAYS: the sync_runs row is locked (SELECT ... FOR UPDATE)
// in its own statement BEFORE the recompute runs. SQL's two COUNT(*)
// subqueries reference only the run id, so Postgres plans them as
// InitPlans -- evaluated ONCE per statement, using the snapshot at
// statement start, not a fresh one per row. Without locking the row first,
// a second concurrent Bump on the same run that blocked on the first's row
// lock would resume and overwrite with its own STALE pre-wait count,
// silently dropping the first caller's contribution (CHAOS-4559 codex
// round 1, P1). Locking first serializes every concurrent Bump on one run
// behind the SAME row lock, so whichever call resumes second always
// recomputes from a snapshot that already reflects the first call's write.
//
// This ordering is also what makes two CONCURRENT callers safe to
// interleave AS LONG AS each one only ever touches ONE run per
// transaction: providersync's per-unit commit and syncdispatchruntime's
// denial/exhaustion paths both do, in the sequence unit-row-lock (their own
// UPDATE public.sync_run_units, before calling Bump) then run-row-lock
// (Bump's own SELECT ... FOR UPDATE above).
//
// A caller whose single transaction terminalizes candidates spanning
// MULTIPLE different sync_run_ids -- syncreconciler's LeaseRepair.Step and
// UnreclaimableSweep.Step both do exactly that, one candidate (and one Bump
// call) per run -- must additionally agree with every OTHER such caller on
// which order it walks through ITS OWN candidates' runs: two such callers
// running concurrently, each following the SAME per-candidate
// unit-then-Bump(run) sequence as every single-run caller, but visiting
// their own runs in DIFFERENT orders (codex round 2, P1: LeaseRepair orders
// by lease_expires_at, UnreclaimableSweep by created_at), can still lock
// two overlapping runs in opposite order relative to each other and
// deadlock in Postgres.
//
// codex round 2 first tried fixing this with a LockRuns helper that
// pre-locked every run a pass would touch, all at once, before any
// candidate write -- but that inverts the order to run-before-unit for
// those two callers while every single-run caller keeps unit-before-run,
// which trades the reconciler-vs-reconciler deadlock for a reconciler-vs-
// single-run-writer one instead (codex round 3, P1). LockRuns was removed;
// the actual fix lives in LeaseRepair.Step and UnreclaimableSweep.Step,
// which now sort their own candidates by ascending sync_run_id before
// their per-candidate loop, so both walk any overlapping run set in the
// same order as each other while still calling Bump AFTER each candidate's
// own unit write, same as every other caller of this function.
func Bump(
	ctx context.Context, tx pgx.Tx, syncRunID string,
) (completedUnits, failedUnits, totalUnits int, err error) {
	if err := LockRun(ctx, tx, syncRunID); err != nil {
		return 0, 0, 0, err
	}
	if err := tx.QueryRow(ctx, SQL, syncRunID).Scan(
		&completedUnits, &failedUnits, &totalUnits,
	); err != nil {
		return 0, 0, 0, err
	}
	return completedUnits, failedUnits, totalUnits, nil
}

// LockRun takes the sync_runs row lock Bump's own lock-first step above
// uses, extracted so a caller that computes sync_runs.completed_units/
// failed_units its OWN way -- not through Bump's SQL -- can still get the
// SAME protection Bump gives every other caller (codex round 10, P1):
// syncdispatchruntime's terminalizeFeatureDisabledRun counts unit statuses
// via a separate `SELECT status FROM sync_run_units` read into Go
// variables, rather than Bump's atomic same-statement COUNT(*) subqueries,
// specifically because it also needs a `running` count Bump's SQL does not
// return and writes several OTHER sync_runs columns (error, result,
// status, completed_at) in the SAME statement as the rollup counters.
// Locking the row here, immediately before that counting read, closes the
// identical staleness gap Bump's own doc comment above describes: without
// it, a concurrent Bump call on the same run (providersync's Complete,
// this package's own denial/exhaustion paths, ...) that commits BETWEEN
// this counting read and this caller's later sync_runs write can have its
// fresh, correct counts silently overwritten by this caller's now-stale
// ones, since neither side is a compare-and-swap -- whichever commits last
// wins outright.
func LockRun(ctx context.Context, tx pgx.Tx, syncRunID string) error {
	var locked int
	return tx.QueryRow(ctx, `SELECT 1 FROM public.sync_runs WHERE id = $1 FOR UPDATE`, syncRunID).Scan(&locked)
}
