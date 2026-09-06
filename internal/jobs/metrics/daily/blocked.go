package daily

import (
	"context"
	"time"
)

// A run is BLOCKED when it holds at least one 'failed_permanent' partition and
// every one of its other partitions has already succeeded -- nothing left can
// advance, so CompletePartition's "zero non-succeeded partitions" gate can
// never open, finalize is never published, and the run's completion fence is
// never written. Everything handed off behind that fence (workgraph.build,
// then investment.materialize) waits forever while reading as 'pending',
// which is indistinguishable from legitimately in-flight work. That is the
// silent shape CHAOS-4970 hit in prod on 2026-09-01.
//
// "Something can still happen" is decided by the LEASE, not by the status
// alone. 'pending' and 'failed' are re-published by the fan-out, and a
// 'running' partition under a LIVE lease is being worked on right now. But a
// 'running' partition whose lease has EXPIRED is stranded: ClaimPartition
// would reclaim that exact row, yet only a metrics.daily_partition job ever
// calls ClaimPartition, and DispatchablePartitions returns nothing but
// 'pending'/'failed' (postgres.go), so once the final River attempt dies after
// claiming, nothing automatic re-publishes a job for it and nothing reclaims
// it.
//
// This was wrong until codex review round 2 on #2224, which is worth recording
// because the error was in the reasoning, not the SQL: the earlier comment
// argued "ClaimPartition reclaims an expired lease, so the run can still make
// progress". The premise is true and the conclusion does not follow -- a
// mechanism that nothing invokes cannot make progress. RedriveStrandedPartitions
// (redrive.go, step 2) already treated this shape as redrivable, so the marker
// and the redrive held two different definitions of "stuck" in one package.
// They now share one.
const (
	// BlockedReasonAllPartitionsPermanent marks a run whose partitions are
	// ALL failed_permanent -- nothing succeeded at all.
	BlockedReasonAllPartitionsPermanent = "all_partitions_failed_permanent"
	// BlockedReasonPartialPartitionsPermanent marks a run that produced real
	// output for some partitions and permanently lost others. Wedged
	// identically -- the fence is equally unreachable -- but an operator
	// deciding whether a redrive is safe needs to know partial output exists,
	// because a needless recompute inflates the families whose readers SUM raw
	// rows instead of deduplicating by computed_at.
	BlockedReasonPartialPartitionsPermanent = "partial_partitions_failed_permanent"
	// BlockedReasonFinalizeFailed marks a run whose PARTITIONS all succeeded
	// but whose finalization terminally failed (CHAOS-4290, #2241 r3 Finding 2).
	//
	// Before this the marker only ever fired on a failed_permanent PARTITION,
	// so a finalize that exhausted its River attempts left the run at
	// status='running', finalization_status='failed', every partition
	// succeeded -- and blocked_at NULL. The run was invisible to the very sweep
	// built to surface wedged runs, and the finalize policy's own claim that
	// "max attempts -> the blocked marker records it" was false.
	//
	// It is a THIRD reason rather than reuse of either above, because the
	// operator action differs: the partition reasons mean recompute some
	// partitions, this one means the partitions are fine and only the
	// cross-partition finalize needs re-running.
	BlockedReasonFinalizeFailed = "finalize_failed"
)

// dailyMetricsRunBlockedReasons is the closed vocabulary the marker accepts,
// mirroring dailyMetricsPartitionFailureReasons' shape. Both values fit the
// column's 64-character bound.
var dailyMetricsRunBlockedReasons = map[string]struct{}{
	BlockedReasonAllPartitionsPermanent:     {},
	BlockedReasonPartialPartitionsPermanent: {},
	BlockedReasonFinalizeFailed:             {},
}

// BlockedReconcileOutcome summarizes one reconciliation pass.
type BlockedReconcileOutcome struct {
	// Marked counts runs this pass newly marked blocked.
	Marked int
	// Cleared counts runs whose marker this pass removed because the
	// predicate stopped holding -- the reversibility path. It is DERIVED from
	// the same predicate as Marked rather than being a second code path, so a
	// redrive that resets failed_permanent partitions unblocks the run on the
	// next pass without anything having to remember to undo the marker.
	Cleared int
	// Blocked is the total number of this organization's runs left carrying
	// the marker after the pass -- the value the observer gauge reports.
	Blocked int
}

// reconcileBlockedRunsSQL applies the predicate in BOTH directions in one
// statement. Writing set and clear as two statements would be two places for
// the definition of "blocked" to live, and they would drift; here the CASE
// arms are the only difference between marking and clearing, and both read
// the same `blocked` column computed once in `decided`.
//
// The WHERE clause at the end restricts the UPDATE to rows whose stored state
// DISAGREES with the predicate, which makes the pass idempotent: a steady
// state writes zero rows, so this can run on every tick without churning
// updated_at or generating write load proportional to the number of runs.
const reconcileBlockedRunsSQL = `
WITH decided AS (
    SELECT run.id,
           EXISTS (
               SELECT 1 FROM public.daily_metrics_partitions AS partition
               WHERE partition.run_id = run.id
                 AND partition.status = 'failed_permanent'
           )
           AND NOT EXISTS (
               SELECT 1 FROM public.daily_metrics_partitions AS partition
               WHERE partition.run_id = run.id
                 AND partition.status NOT IN ('succeeded', 'failed_permanent')
                 -- An expired lease does NOT keep the run alive: nothing
                 -- automatic will ever reclaim it.
                 --
                 -- <= not <, per codex review round 3 (P3). classifyLease
                 -- (postgres.go) returns leaseHeld only when
                 -- leaseExpiresAt.After(now), so a lease landing exactly ON now
                 -- is RECLAIMABLE there. With < this predicate called that same
                 -- instant "live" and left the run unmarked while ClaimPartition
                 -- would have reclaimed it -- the shared boundary this comment
                 -- claimed was false at exactly one point. RedriveStrandedPartitions
                 -- carries the identical <= for the same reason, so all three
                 -- now agree on the closed interval.
                 AND NOT (
                     partition.status = 'running'
                     AND partition.lease_expires_at <= $2::timestamptz
                 )
           ) AS blocked,
           -- CHAOS-4290 #2241 r3 F2, CORRECTED by the confirmation pass.
           --
           -- The first version keyed on finalization_status='failed' alone and
           -- fired EXACTLY BACKWARDS. That state is what ReleaseFinalize writes
           -- after ANY failed attempt, and ClaimFinalize treats it as claimable
           -- (postgres.go), so it is the ordinary retryable state -- while a
           -- terminally failed run also has status='failed' and was excluded by
           -- this query's own status='running' scope. The marker therefore fired
           -- on healthy retrying runs and never on stranded ones.
           --
           -- It now keys on the TERMINAL shape, which FailFinalizePermanently
           -- writes only on the final River attempt: BOTH columns 'failed'.
           -- Because the state exists only after retries are exhausted, a
           -- transient failure followed by a successful retry never acquires a
           -- marker, so CompleteFinalize needs no clearing path.
           (
               run.status = 'failed'
               AND run.finalization_status = 'failed'
               AND NOT EXISTS (
                   SELECT 1 FROM public.daily_metrics_partitions AS partition
                   WHERE partition.run_id = run.id
                     AND partition.status <> 'succeeded'
               )
               AND EXISTS (
                   SELECT 1 FROM public.daily_metrics_partitions AS partition
                   WHERE partition.run_id = run.id
               )
           ) AS finalize_blocked,
           NOT EXISTS (
               SELECT 1 FROM public.daily_metrics_partitions AS partition
               WHERE partition.run_id = run.id
                 AND partition.status = 'succeeded'
           ) AS none_succeeded
    FROM public.daily_metrics_runs AS run
    -- Widened from status='running' (CHAOS-4290 confirmation pass): a
    -- terminally failed finalize has status='failed', so the old scope excluded
    -- the exact rows the finalize arm needs to see.
    --
    -- WHAT KEEPS THIS SAFE is more than "the partition arms need a
    -- failed_permanent partition" (peer read, lane-gate-rounds). Widening
    -- exposes EVERY status='failed' row to both arms, including
    -- MaterializeScheduledFanout's overCap rows. Three things hold the outcome:
    --
    --   1. finalize_blocked requires EXISTS(partition) -- overCap only fires
    --      when the run has ZERO partitions, so those rows cannot match it.
    --   2. finalize_blocked requires NOT EXISTS(partition <> 'succeeded'), so a
    --      run with any unfinished or permanently-failed partition is excluded.
    --   3. blocked_reason's CASE evaluates the PARTITION reasons first, so a row
    --      that somehow satisfied both arms still reports the partition cause --
    --      which is the one an operator must act on.
    --
    -- The guards are what make this correct, not the absence of overlap.
    WHERE run.org_id = $1::uuid AND run.status IN ('running', 'failed')
)
UPDATE public.daily_metrics_runs AS run
SET blocked_at = CASE WHEN decided.blocked OR decided.finalize_blocked THEN $2::timestamptz ELSE NULL END,
    blocked_reason = CASE
        WHEN decided.blocked AND decided.none_succeeded THEN $3
        WHEN decided.blocked THEN $4
        -- Checked AFTER the partition reasons: a run with a failed_permanent
        -- partition AND a failed finalize is a partition problem first, and
        -- reporting the finalize would send an operator to recompute the wrong
        -- thing.
        WHEN decided.finalize_blocked THEN $5
        ELSE NULL
    END,
    updated_at = $2
FROM decided
WHERE run.id = decided.id
  AND (decided.blocked OR decided.finalize_blocked) <> (run.blocked_at IS NOT NULL)
RETURNING (decided.blocked OR decided.finalize_blocked)`

// ReconcileBlockedRuns marks and unmarks this organization's wedged runs from
// live partition state. It is the ONLY writer of the marker, in either
// direction, so "blocked" has exactly one definition.
//
// Deliberately scoped to one organization: it is meant to run inside work the
// per-organization daily fan-out already does, not as an unbounded table scan.
func (store *PostgresStore) ReconcileBlockedRuns(
	ctx context.Context, orgID string,
) (BlockedReconcileOutcome, error) {
	var outcome BlockedReconcileOutcome
	if !store.valid() || !validUUID(orgID) {
		return outcome, ErrUnavailable
	}
	now := store.now().UTC()
	rows, err := store.pool.Query(ctx, reconcileBlockedRunsSQL, orgID, now,
		BlockedReasonAllPartitionsPermanent, BlockedReasonPartialPartitionsPermanent,
		BlockedReasonFinalizeFailed)
	if err != nil {
		return outcome, ErrUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var blocked bool
		if err := rows.Scan(&blocked); err != nil {
			return BlockedReconcileOutcome{}, ErrUnavailable
		}
		if blocked {
			outcome.Marked++
		} else {
			outcome.Cleared++
		}
	}
	if rows.Err() != nil {
		return BlockedReconcileOutcome{}, ErrUnavailable
	}
	// Counted separately rather than derived as (previous + Marked - Cleared):
	// the pass only touches DISAGREEING rows, so a run already correctly
	// marked before this pass contributes to neither counter. Deriving the
	// total from the deltas would report 0 blocked on a steady state that
	// still has wedged runs -- exactly the silent-zero the gauge exists to
	// prevent.
	//
	// status IN ('running', 'failed'), not status = 'running' (r2 finding #2,
	// CHAOS-4290): reconcileBlockedRunsSQL above writes blocked_at on a
	// finalize_blocked row whose status is 'failed', not 'running' -- the
	// WRITE was widened for exactly this shape, but this readback still
	// scoped to 'running' alone would silently undercount the gauge by every
	// terminally-failed finalize the write side had just correctly marked.
	if err := store.pool.QueryRow(ctx, `
SELECT count(*) FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND status IN ('running', 'failed') AND blocked_at IS NOT NULL`,
		orgID).Scan(&outcome.Blocked); err != nil {
		return BlockedReconcileOutcome{}, ErrUnavailable
	}
	return outcome, nil
}

// BlockedRun is one row of the operator readback.
type BlockedRun struct {
	RunID     string
	TargetDay time.Time
	BlockedAt time.Time
	Reason    string
	// FailureReasons lists the DISTINCT bounded failure_reason values on this
	// run's failed_permanent partitions. Without it the readback says a run is
	// stuck but not what refused, which is the same "visible but unexplained"
	// gap the marker exists to close.
	FailureReasons []string
	// PermanentPartitions and SucceededPartitions let an operator judge how
	// much real output a redrive would recompute.
	PermanentPartitions int
	SucceededPartitions int
}

// BlockedRuns lists an organization's currently-marked runs for the workerctl
// readback, newest target day first.
func (store *PostgresStore) BlockedRuns(
	ctx context.Context, orgID string, limit int,
) ([]BlockedRun, error) {
	if !store.valid() || !validUUID(orgID) {
		return nil, ErrUnavailable
	}
	if limit <= 0 {
		limit = defaultFinalizeSweepLimit
	}
	rows, err := store.pool.Query(ctx, `
SELECT run.id::text, run.target_day, run.blocked_at, run.blocked_reason,
       COALESCE(ARRAY(
           SELECT DISTINCT partition.failure_reason
           FROM public.daily_metrics_partitions AS partition
           WHERE partition.run_id = run.id
             AND partition.status = 'failed_permanent'
             AND partition.failure_reason IS NOT NULL
           ORDER BY partition.failure_reason
       ), ARRAY[]::text[]),
       (SELECT count(*) FROM public.daily_metrics_partitions AS partition
        WHERE partition.run_id = run.id AND partition.status = 'failed_permanent'),
       (SELECT count(*) FROM public.daily_metrics_partitions AS partition
        WHERE partition.run_id = run.id AND partition.status = 'succeeded')
FROM public.daily_metrics_runs AS run
-- status IN ('running', 'failed'), not status = 'running' (r2 finding #2,
-- CHAOS-4290): a finalize_blocked run's status is 'failed', matching
-- ReconcileBlockedRuns' own gauge query above -- without this, the operator
-- readback (workerctl's own entry point for this marker) can never show the
-- exact run class BlockedReasonFinalizeFailed exists to surface.
WHERE run.org_id = $1::uuid AND run.status IN ('running', 'failed') AND run.blocked_at IS NOT NULL
ORDER BY run.target_day DESC, run.id
LIMIT $2::int`, orgID, limit)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var blocked []BlockedRun
	for rows.Next() {
		var row BlockedRun
		if err := rows.Scan(&row.RunID, &row.TargetDay, &row.BlockedAt, &row.Reason,
			&row.FailureReasons, &row.PermanentPartitions, &row.SucceededPartitions); err != nil {
			return nil, ErrUnavailable
		}
		blocked = append(blocked, row)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return blocked, nil
}
