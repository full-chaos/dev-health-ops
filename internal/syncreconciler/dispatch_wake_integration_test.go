//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedDeliveredDispatchWakeup leaves the run's dispatch_sync_run row in the
// state a pass that already ran leaves it: 'dispatched', armed long ago.
func seedDeliveredDispatchWakeup(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, at time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			created_at, updated_at
		) VALUES (gen_random_uuid(), $1, $2, 'dispatch_sync_run', 'dispatched', $3, 3, $3, 'river', 1, $3, $3)`,
		sweepOrg, runID, at); err != nil {
		t.Fatal(err)
	}
}

func dispatchWakeupStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM public.sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
		runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// TestUnreclaimableSweepWakesDispatchForAWaitingSibling: the sweep frees the
// stranded unit's slot, so a planned sibling of the same run is due a pass now,
// not after the redispatch countdown (CHAOS-10013, Codex round 401 P1).
//
// RED CONTROL: drop the wakeDispatchAfterCommit call in UnreclaimableSweep.Step
// and the row stays 'dispatched'.
func TestUnreclaimableSweepWakesDispatchForAWaitingSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	stranded := sweepUnitID(90)
	seedSweepUnit(t, ctx, pool, strandedSpec(stranded, "repo-metadata", "light", now))
	seedSweepDelivery(t, ctx, pool, stranded, "cancelled", "JobCancelError: dev-health job failed [validation]")
	seedSweepUnit(t, ctx, pool, sweepUnitSpec{
		id: sweepUnitID(91), dataset: "repo-metadata", costClass: "light", status: "planned",
		createdAt: now, updatedAt: now,
	})
	seedDeliveredDispatchWakeup(t, ctx, pool, sweepRun, now.Add(-time.Hour))
	armedBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathUnreclaimableSweep, syncrunrollup.DispatchWakeOutcomeArmed)

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 1 {
		t.Fatalf("Terminalized = %d, want 1 (the stranded unit only; the planned sibling is not a candidate)", result.Terminalized)
	}
	if status := dispatchWakeupStatus(t, ctx, pool, sweepRun); status != "pending" {
		t.Fatalf("dispatch_sync_run status = %q, want pending so the planned sibling can run", status)
	}
	if moved := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathUnreclaimableSweep, syncrunrollup.DispatchWakeOutcomeArmed) - armedBefore; moved != 1 {
		t.Fatalf("unreclaimable_sweep armed counter moved %d, want 1", moved)
	}
}

// TestRouteUnavailableTerminationWakesDispatchForARetryingSibling: the
// route-unavailable pass terminalizes the parked units; a due 'retrying'
// sibling (not a route-unavailable candidate) must get its pass now
// (CHAOS-10013, Codex round 401 P1).
//
// RED CONTROL: drop the wakeDispatchAfterCommit call in
// terminalizeRouteUnavailable and the row stays 'dispatched'.
func TestRouteUnavailableTerminationWakesDispatchForARetryingSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	setProviderUnitRoute(t, ctx, pool, "river", true)
	seedRouteFaultRun(t, ctx, pool, routeFaultRun, now.Add(-RouteUnavailableWindow-time.Minute), 2)
	seedDeliveredDispatchWakeup(t, ctx, pool, routeFaultRun, now.Add(-time.Hour))
	parked, retrying := routeFaultUnitID(1), routeFaultUnitID(2)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRun, parked, "commits", "planned", now)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRun, retrying, "commits", "retrying", now)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET available_at = $2 WHERE id = $1`, retrying, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	armedBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathRouteUnavailable, syncrunrollup.DispatchWakeOutcomeArmed)

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.RouteUnavailableTerminalized != 1 {
		t.Fatalf("RouteUnavailableTerminalized = %d, want 1 (the parked unit only)", result.RouteUnavailableTerminalized)
	}
	if status := dispatchWakeupStatus(t, ctx, pool, routeFaultRun); status != "pending" {
		t.Fatalf("route-unavailable dispatch_sync_run status = %q, want pending so the retrying sibling can run", status)
	}
	if moved := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathRouteUnavailable, syncrunrollup.DispatchWakeOutcomeArmed) - armedBefore; moved != 1 {
		t.Fatalf("route_unavailable armed counter moved %d, want 1", moved)
	}
}
