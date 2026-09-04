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
// A 'running' partition with an expired lease is deliberately NOT treated as
// blocking-free on the grounds that it is stuck: ClaimPartition reclaims that
// exact shape, so the run can still make progress and must not be marked. The
// predicate below therefore asks whether ANY partition is in a state other
// than 'succeeded' or 'failed_permanent' -- pending, failed and running all
// mean something can still happen.
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
)

// dailyMetricsRunBlockedReasons is the closed vocabulary the marker accepts,
// mirroring dailyMetricsPartitionFailureReasons' shape. Both values fit the
// column's 64-character bound.
var dailyMetricsRunBlockedReasons = map[string]struct{}{
	BlockedReasonAllPartitionsPermanent:     {},
	BlockedReasonPartialPartitionsPermanent: {},
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
           ) AS blocked,
           NOT EXISTS (
               SELECT 1 FROM public.daily_metrics_partitions AS partition
               WHERE partition.run_id = run.id
                 AND partition.status = 'succeeded'
           ) AS none_succeeded
    FROM public.daily_metrics_runs AS run
    WHERE run.org_id = $1::uuid AND run.status = 'running'
)
UPDATE public.daily_metrics_runs AS run
SET blocked_at = CASE WHEN decided.blocked THEN $2::timestamptz ELSE NULL END,
    blocked_reason = CASE
        WHEN decided.blocked AND decided.none_succeeded THEN $3
        WHEN decided.blocked THEN $4
        ELSE NULL
    END,
    updated_at = $2
FROM decided
WHERE run.id = decided.id
  AND decided.blocked <> (run.blocked_at IS NOT NULL)
RETURNING decided.blocked`

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
		BlockedReasonAllPartitionsPermanent, BlockedReasonPartialPartitionsPermanent)
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
	if err := store.pool.QueryRow(ctx, `
SELECT count(*) FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND status = 'running' AND blocked_at IS NOT NULL`,
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
WHERE run.org_id = $1::uuid AND run.status = 'running' AND run.blocked_at IS NOT NULL
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
