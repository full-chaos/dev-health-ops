//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14250 r1 P1-1: a dispatch pass the bucket cap holds back parks its own dispatch row
// (status dispatched, last_error 'bucket_parked'); only the bucket sweep wakes it. The materializer
// must not re-arm parked rows, at any queue depth, while it still re-arms a plain stale row.
func TestMaterializerSkipsBucketParkedDispatchRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.November, 20, 6, 0, 0, 0, time.UTC)
	cutoff := now.Add(-15 * time.Minute)
	for _, depth := range []int{70, 140, 350} {
		resetMaterializerIntegrationTables(t, ctx, pool)
		for i := 0; i < depth; i++ {
			runID := fmt.Sprintf("00000000-0000-4000-9142-%012x", i)
			seedRun(t, ctx, pool, runID, "running", now.Add(-2*time.Hour))
			seedUnit(t, ctx, pool, fmt.Sprintf("00000000-0000-4000-9143-%012x", i), runID, "planned", nil, now.Add(-2*time.Hour))
			seedMaterializerDispatchedOutbox(t, ctx, pool, runID, fmt.Sprintf("river-parked-%d", i), now.Add(-time.Hour))
		}
		if _, err := pool.Exec(ctx, `UPDATE public.sync_dispatch_outbox SET last_error = 'bucket_parked' WHERE kind = 'dispatch_sync_run'`); err != nil {
			t.Fatal(err)
		}
		control := "00000000-0000-4000-9144-000000000001"
		seedRun(t, ctx, pool, control, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-9145-000000000001", control, "planned", nil, now.Add(-2*time.Hour))
		seedMaterializerDispatchedOutbox(t, ctx, pool, control, "river-control", now.Add(-time.Hour))
		for step := 0; step < depth/100+3; step++ {
			if _, err := materializer.Step(ctx, now, cutoff, 100); err != nil {
				t.Fatal(err)
			}
		}
		var parkedRearmed, controlRearmed int
		if err := pool.QueryRow(ctx, `SELECT
	count(*) FILTER (WHERE sync_run_id <> $1::uuid AND status = 'pending'),
	count(*) FILTER (WHERE sync_run_id = $1::uuid AND status = 'pending')
FROM public.sync_dispatch_outbox WHERE kind = 'dispatch_sync_run'`, control).Scan(&parkedRearmed, &controlRearmed); err != nil {
			t.Fatal(err)
		}
		// One stale window (900 s): the re-arms per hour are 4x this number.
		t.Logf("depth %d: parked rows re-armed per stale window = %d (per hour %d); control re-armed = %d", depth, parkedRearmed, 4*parkedRearmed, controlRearmed)
		if parkedRearmed != 0 || controlRearmed != 1 {
			t.Errorf("depth %d: the materializer re-armed %d parked rows (want 0) and %d control rows (want 1)", depth, parkedRearmed, controlRearmed)
		}
	}
}
