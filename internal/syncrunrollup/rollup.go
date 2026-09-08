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
	"time"

	"github.com/google/uuid"
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

// ArmFinalizeSQL re-arms a run's finalize_sync_run dispatch-outbox row so the
// finalizer re-evaluates the run after a unit reached a terminal status.
//
// # Why this belongs next to Bump and not in each caller
//
// Bump keeps completed_units/failed_units live and NOTHING ELSE -- it never
// writes sync_runs.status or completed_at, deliberately, because it is the
// counter seam. What actually closes a run is finalize_sync_run, and the run
// only reaches it if something re-arms its outbox row: the finalizer declines
// while any unit is non-terminal, and its row is left 'dispatched' with nothing
// scheduled to reconsider it.
//
// providersync's per-unit commit paths always did both, one line apart. The two
// RECOVERY writers -- syncreconciler's UnreclaimableSweep and LeaseRepair -- did
// only the Bump. Measured consequence on run
// 115e6246-6e8c-5f53-a2c4-f6b109daba68: the sweep terminalized its last 17
// non-terminal units at 09:31:48-09:32:50Z on 2026-09-07, the counters went to
// 44 success / 19 failed / 63 total, and the run stayed status='dispatching'
// with completed_at NULL for over an hour with nothing left that could ever
// re-evaluate it -- its finalize row had stood 'dispatched' since 03:23:26Z,
// from the pass that correctly declined while those units were still open.
//
// So this is the exact CHAOS-4586 shape one layer out: a second and third copy
// of the same statement in the packages that terminalize units. It lives here,
// beside Bump, and every terminal-status writer calls both.
//
// # The feature_disabled exception, preserved verbatim
//
// A run terminated by the entitlement gate parks its finalize row at
// 'dispatched' with last_error='feature_disabled'. That is a decision, not a
// pending step, and re-arming it would put the run back in the finalizer's
// queue forever. Every CASE below therefore leaves such a row completely
// untouched -- status, availability, dispatch stamp, error and claim alike.
// Copied unchanged from providersync's original; do not "simplify" the repeated
// predicate into one CASE, because each column's else-branch differs.
//
// # Claim handling
//
// A LIVE claim (claim_expires_at in the future) is preserved: another
// dispatcher is mid-flight on this row and stealing its token would let two
// finalizers run. An EXPIRED claim is cleared, which is what makes the row
// claimable again on the next pass.
const ArmFinalizeSQL = `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    created_at, updated_at
) VALUES ($1, $2, $3::uuid, 'finalize_sync_run', 'pending', $4, 0, $4, $4)
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET status = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.status
        ELSE 'pending'
    END,
    available_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.available_at
        ELSE LEAST(public.sync_dispatch_outbox.available_at, EXCLUDED.available_at)
    END,
    dispatched_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_at
        ELSE NULL
    END,
    last_error = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.last_error
        ELSE NULL
    END,
    claim_token = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_token
        ELSE NULL
    END,
    claim_expires_at = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_expires_at
        ELSE NULL
    END,
    updated_at = EXCLUDED.updated_at`

// ArmFinalize runs ArmFinalizeSQL in the caller's transaction.
//
// Call it in the SAME transaction as the unit's terminal write and the Bump,
// never afterwards: a crash between the two would leave a run whose counters
// say every unit is terminal and whose finalizer is not scheduled -- the
// permanently-open state this function exists to prevent, reached by a
// narrower window.
//
// The row id is minted here rather than taken from the caller because the
// conflict target is (sync_run_id, kind): the id is used only when the row does
// not exist yet, so nothing observable depends on which value it takes.
func ArmFinalize(
	ctx context.Context, tx pgx.Tx, syncRunID, orgID string, now time.Time,
) error {
	_, err := tx.Exec(ctx, ArmFinalizeSQL, uuid.New(), orgID, syncRunID, now.UTC())
	return err
}
