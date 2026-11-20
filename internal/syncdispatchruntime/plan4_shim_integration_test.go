//go:build integration

package syncdispatchruntime

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-4 seam: a slot writer's transaction as every writer now runs it (the bucket lock first, the write, the
// reservation of the freed slot for the bucket's heads, the commit, then the wakes and the release), and the head
// read's row cost.

// p4WriterRelease runs write in a writer's transaction the way providersync and the reconciler do.
func p4WriterRelease(ctx context.Context, pool *pgxpool.Pool, sweeps syncrunrollup.BucketSweepEnqueuer, slot syncrunrollup.SlotFree,
	now time.Time, path string, write func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	bucket := syncbucket.Bucket{OrgID: slot.OrgID, Provider: slot.Provider, CostClass: slot.CostClass}
	if err := syncbucket.LockCommitPath(ctx, tx, []syncbucket.Bucket{bucket}, syncbucket.LockWaitPathProviderUnit); err != nil {
		return err
	}
	if err := write(tx); err != nil {
		return err
	}
	heads, err := syncbucket.ReserveHeads(ctx, tx, bucket, now, "")
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	syncrunrollup.WakeReserved(ctx, pool, heads.Reserved, now, path)
	p3Release(ctx, pool, sweeps, slot, now, path)
	return nil
}

// p4HeadReadRows is the rows the head read of one class of a bucket (limit 1) visits, over every relation it reads.
func p4HeadReadRows(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass string, backfill bool, now time.Time) (float64, error) {
	class := 0
	if backfill {
		class = 1
	}
	plan, err := p3Explain(ctx, pool, syncbucket.HeadsSQL, orgID, provider, costClass, class, now, 1, now.Add(-staleDispatchSeconds()))
	if err != nil {
		return 0, err
	}
	return planRowVisits(plan), nil
}

func planRowVisits(node planNode) float64 {
	total := 0.0
	if node.RelationName != "" {
		total += (node.ActualRows + node.RowsRemovedByFilter) * node.ActualLoops
	}
	for _, child := range node.Plans {
		total += planRowVisits(child)
	}
	return total
}
