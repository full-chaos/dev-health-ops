//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The seam between the CHAOS-14250 cells and the code under test: the release a unit end runs, the sweep enqueuer a
// pass gets, and one sweep of a bucket. The cells drive only these, so the same cells run on a tree without the
// bucket sweep (where they are red) and with it.

func rqRelease(ctx context.Context, pool *pgxpool.Pool, sweeps *rqSweeps, slot syncrunrollup.SlotFree, now time.Time) {
	syncrunrollup.ReleaseSlot(ctx, pool, sweeps, slot, now, syncrunrollup.DispatchWakePathProviderUnit)
}

func rqAttachSweeps(service *NativeDispatchSyncRunService, sweeps *rqSweeps) {
	service.WithBucketSweeps(sweeps)
}

func rqSweep(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass string, now time.Time) (bool, []string, error) {
	bucket := dispatchBucket{orgID: orgID, provider: provider, costClass: costClass}
	var woken []string
	for {
		result, err := runBucketSweep(ctx, pool, bucket, func() time.Time { return now })
		woken = append(woken, result.woken...)
		if err != nil || !result.signaled {
			return result.waiting, woken, err
		}
	}
}

// rqReservation is the reserved_at of the run's sweep reservation in the bucket, if it holds one.
func rqReservation(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass, runID string) (time.Time, bool, error) {
	var at time.Time
	err := pool.QueryRow(ctx, `SELECT reserved_at FROM sync_dispatch_bucket_reservations
WHERE org_id = $1 AND provider = $2 AND cost_class = $3 AND sync_run_id = $4::uuid`, orgID, provider, costClass, runID).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	return at, err == nil, err
}

// rqSetReservation writes the run's sweep reservation in the bucket at the instant at.
func rqSetReservation(ctx context.Context, pool *pgxpool.Pool, orgID, provider, costClass, runID string, backfill bool, at time.Time) error {
	_, err := pool.Exec(ctx, `INSERT INTO sync_dispatch_bucket_reservations (org_id, provider, cost_class, sync_run_id, backfill, reserved_at)
VALUES ($1, $2, $3, $4::uuid, $5, $6)
ON CONFLICT (org_id, provider, cost_class, sync_run_id) DO UPDATE SET reserved_at = EXCLUDED.reserved_at`,
		orgID, provider, costClass, runID, backfill, at)
	return err
}

func rqSweepRearm() time.Duration { return cappedRearmMax() }
