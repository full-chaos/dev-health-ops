//go:build integration

package syncdispatchruntime

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The seam between the CHAOS-14250 cells and the code under test: the release a unit end runs, the sweep enqueuer a
// pass gets, and one sweep of a bucket. The cells drive only these, so the same cells run on a tree without the
// bucket sweep (where they are red) and with it.

const rqSweepMarkKey = bucketSweepWokenKey

func rqRelease(ctx context.Context, pool *pgxpool.Pool, sweeps *rqSweeps, slot syncrunrollup.SlotFree, now time.Time) {
	syncrunrollup.ReleaseSlot(ctx, pool, sweeps, slot, now, syncrunrollup.DispatchWakePathProviderUnit)
}

func rqAttachSweeps(service *NativeDispatchSyncRunService, sweeps *rqSweeps) {
	service.WithBucketSweeps(sweeps)
}

func rqSweep(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass string, now time.Time) (bool, []string, error) {
	result, err := runBucketSweep(ctx, pool, dispatchBucket{orgID: orgID, provider: provider, costClass: costClass},
		func() time.Time { return now })
	return result.waiting, result.woken, err
}

func rqSweepRearm() time.Duration { return cappedRearmMax() }
