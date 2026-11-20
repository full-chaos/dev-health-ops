//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-3 seam: the sweep (sweepBucketOnce, its wakes, then the generation acknowledgement) with the hooks the
// cells need between its steps.

type p3Hooks struct {
	afterPick     func() // after the bucket transaction commits its picks and reservations, before the wakes
	betweenRounds func() // after the first sweep's wakes, before its acknowledgement; a signal here is swept at once
	beforeFinish  func() // after the wakes, before the acknowledgement (the sweep's last read)
}

const p3HasGeneration = true

// p3SweepHooked runs the bucket's sweep as the worker does. again is River's JobSnooze(0): a signal reached the
// sweep while it ran. A signal raised by betweenRounds is swept at once by the second run, folded into this call.
func p3SweepHooked(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass string, now time.Time, hooks p3Hooks) (waiting bool, woken []string, again bool, err error) {
	bucket := dispatchBucket{orgID: orgID, provider: provider, costClass: costClass}
	for round := 0; ; round++ {
		picked, stillWaiting, generation, err := p3PickAndCommit(ctx, pool, bucket, now)
		if err != nil {
			return false, nil, false, err
		}
		if round == 0 && hooks.afterPick != nil {
			hooks.afterPick()
		}
		woken = append(woken, picked...)
		waiting = stillWaiting
		for _, runID := range picked {
			syncrunrollup.WakeDispatch(ctx, pool, runID, now, syncrunrollup.DispatchWakePathBucketSweep)
		}
		if round == 0 && hooks.betweenRounds != nil {
			hooks.betweenRounds()
		}
		if round == 0 && hooks.beforeFinish != nil {
			hooks.beforeFinish()
		}
		signaled, err := syncrunrollup.AckBucketGeneration(ctx, pool, orgID, provider, costClass, generation)
		if err != nil {
			return false, nil, false, err
		}
		if !signaled {
			return waiting, woken, false, nil
		}
		if round > 0 || hooks.betweenRounds == nil {
			return waiting, woken, true, nil
		}
	}
}

// p3PickAndCommit is sweepBucketOnce with the commit in view: the cells' afterPick hook runs right after this commit,
// the instant the bucket lock is released.
func p3PickAndCommit(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now time.Time) ([]string, bool, int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, false, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	picked, waiting, generation, err := pickBucketSlots(ctx, tx, bucket, now)
	if err != nil {
		return nil, false, 0, err
	}
	return picked, waiting, generation, tx.Commit(ctx)
}

// p3Release is a writer's release. The lease-repair writer passes the unit's new available_at as SlotFree.RetryAt;
// the cells' unit end writes the unit itself, so the seam reads that instant back the way the writer has it.
func p3Release(ctx context.Context, pool *pgxpool.Pool, sweeps syncrunrollup.BucketSweepEnqueuer, slot syncrunrollup.SlotFree, now time.Time, path string) {
	if path == syncrunrollup.DispatchWakePathLeaseRepair && slot.RetryAt.IsZero() {
		var retryAt *time.Time
		_ = pool.QueryRow(ctx, `SELECT max(available_at) FROM sync_run_units
WHERE sync_run_id = $1::uuid AND provider = $2 AND cost_class = $3 AND status = 'retrying' AND updated_at = $4`,
			slot.RunID, slot.Provider, slot.CostClass, now).Scan(&retryAt)
		if retryAt != nil {
			slot.RetryAt = *retryAt
		}
	}
	syncrunrollup.ReleaseSlot(ctx, pool, sweeps, slot, now, path)
}

func p3Attach(service *NativeDispatchSyncRunService, sweeps syncrunrollup.BucketSweepEnqueuer) {
	service.WithBucketSweeps(sweeps)
}

type planNode struct {
	RelationName        string     `json:"Relation Name"`
	ActualRows          float64    `json:"Actual Rows"`
	ActualLoops         float64    `json:"Actual Loops"`
	RowsRemovedByFilter float64    `json:"Rows Removed by Filter"`
	Plans               []planNode `json:"Plans"`
}

// rowVisits sums, over every node that reads table, the rows it returned plus the rows its filter removed, times its
// loops.
func rowVisits(node planNode, table string) float64 {
	total := 0.0
	if node.RelationName == table {
		total += (node.ActualRows + node.RowsRemovedByFilter) * node.ActualLoops
	}
	for _, child := range node.Plans {
		total += rowVisits(child, table)
	}
	return total
}

func p3Explain(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (planNode, error) {
	var raw string
	if err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+sql, args...).Scan(&raw); err != nil {
		return planNode{}, err
	}
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &plans); err != nil || len(plans) != 1 {
		return planNode{}, err
	}
	return plans[0].Plan, nil
}

// p3HeadsRowsRead is the head read of one class of a bucket, limit 1.
func p3HeadsRowsRead(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass string, backfill bool, now time.Time) (float64, error) {
	class := 0
	if backfill {
		class = 1
	}
	plan, err := p3Explain(ctx, pool, bucketHeadsSQL, orgID, provider, costClass, class, now, 1)
	if err != nil {
		return 0, err
	}
	return rowVisits(plan, "sync_run_units") + rowVisits(plan, "sync_runs"), nil
}

// p3HoldBackRowsRead is the guard's hold-back read of a bucket for the run self: its reservation count.
func p3HoldBackRowsRead(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass, selfID string, now time.Time) (float64, error) {
	plan, err := p3Explain(ctx, pool, bucketReservationsSQL, orgID, provider, costClass, selfID, now, admissionLivenessBound().Seconds())
	if err != nil {
		return 0, err
	}
	return rowVisits(plan, "sync_run_units") + rowVisits(plan, "sync_runs") + rowVisits(plan, "sync_dispatch_bucket_reservations"), nil
}
