//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A run parked by a sync.provider_unit route no River runtime can serve.
//
// The dispatcher never reads worker_job_routes: it claims and publishes
// regardless, and the relay resolves the route at drain. So a route the
// relay cannot resolve leaves a run's units parked -- 'planned' units never
// claimed, 'dispatching' units published to worker_job_outbox and never
// delivered -- while finalize waits for a terminal status that nothing
// produces. These tests drive the sweep past the route-unavailable bound and
// assert the EFFECT: the units reach 'failed' carrying the route-unavailable
// reason, the run's rollup counts every unit terminal, and the finalize
// wakeup is armed so the run itself can close.
const (
	routeFaultRun    = "00000000-0000-4000-8000-000000004900"
	routeFaultRunAlt = "00000000-0000-4000-8000-000000004901"
)

func routeFaultUnitID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", 4950+index)
}

// totalUnits is stamped at plan time in production and is not recomputed by
// syncrunrollup.Bump, so the fixture has to carry the same value the planner
// would have written or the rollup assertion proves nothing.
func seedRouteFaultRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	id string, plannedAt time.Time, totalUnits int,
) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.sync_runs (id, org_id, status, total_units, created_at)
		 VALUES ($1, $2, 'dispatching', $3, $4)`,
		id, sweepOrg, totalUnits, plannedAt); err != nil {
		t.Fatal(err)
	}
}

func seedRouteFaultUnit(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	runID, unitID, dataset, status string, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, provider, dataset_key, cost_class, mode,
			status, attempts, created_at, updated_at
		) VALUES ($1, $2, $3, 'github', $4, 'heavy', 'incremental', $5, 0, $6, $6)`,
		unitID, sweepOrg, runID, dataset, status, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
}

// publishRouteFaultUnit is the production shape of a 'dispatching' unit under
// a faulted route: the dispatcher staged a worker_job_outbox row and the relay
// never drained it, so the row is still 'pending' with no River delivery.
func publishRouteFaultUnit(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (
			dedupe_key, job_kind, contract_version, args, payload_hash, queue,
			priority, max_attempts, scheduled_at, status, next_attempt_at, attempt_count
		) VALUES ($1, 'sync.provider_unit', 1, '{}'::jsonb, 'sha256:0', 'default',
			1, 5, $2, 'pending', $2, 0)`,
		"sync.provider_unit:"+unitID, now); err != nil {
		t.Fatal(err)
	}
}

func seedDispatchWakeup(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, attempts int, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			created_at, updated_at
		) VALUES (gen_random_uuid(), $1, $2, 'dispatch_sync_run', 'pending', $3, $4, $3, $3)`,
		sweepOrg, runID, now, attempts); err != nil {
		t.Fatal(err)
	}
}

func setProviderUnitRoute(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, transport string, paused bool,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		UPDATE public.worker_job_routes
		SET transport = $2, paused = $3, generation = generation + 1
		WHERE job_kind = $1`, unreclaimableProviderUnitID, transport, paused); err != nil {
		t.Fatal(err)
	}
}

func deleteProviderUnitRoute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`DELETE FROM public.worker_job_routes WHERE job_kind = $1`,
		unreclaimableProviderUnitID); err != nil {
		t.Fatal(err)
	}
}

func routeFaultRunRollup(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string,
) (completed, failed, total int) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		`SELECT completed_units, failed_units, total_units
		 FROM public.sync_runs WHERE id = $1`, runID,
	).Scan(&completed, &failed, &total); err != nil {
		t.Fatal(err)
	}
	return completed, failed, total
}

func finalizeWakeupStatus(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string,
) string {
	t.Helper()
	var status string
	err := pool.QueryRow(ctx,
		`SELECT status FROM public.sync_dispatch_outbox
		 WHERE sync_run_id = $1 AND kind = 'finalize_sync_run'`, runID,
	).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

// assertRouteUnavailableTermination is the EFFECT the ticket asks for, stated
// once: every parked unit terminal with the reason recorded, the run's rollup
// agreeing that nothing is outstanding, and the finalize wakeup armed so the
// run itself reaches a terminal status.
func assertRouteUnavailableTermination(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, unitIDs []string,
) {
	t.Helper()
	for _, unitID := range unitIDs {
		status, errText, reason, _ := sweepUnitState(t, ctx, pool, unitID)
		if status != "failed" {
			t.Fatalf("unit %s status = %q, want failed -- a run parked past the "+
				"route-unavailable bound must terminalize, not wait forever", unitID, status)
		}
		if errText == nil || *errText != "route_unavailable" {
			t.Fatalf("unit %s error = %v, want %q", unitID, errText, "route_unavailable")
		}
		if reason == nil || !strings.Contains(*reason, "sync.provider_unit") {
			t.Fatalf("unit %s last_retry_reason = %v, want a reason naming the "+
				"sync.provider_unit route fault", unitID, reason)
		}
	}
	completed, failed, total := routeFaultRunRollup(t, ctx, pool, runID)
	if failed != len(unitIDs) || total != len(unitIDs) || completed+failed != total {
		t.Fatalf("run rollup = (completed %d, failed %d, total %d), want every "+
			"unit terminal so finalize_sync_run can complete the run",
			completed, failed, total)
	}
	if status := finalizeWakeupStatus(t, ctx, pool, runID); status != "pending" {
		t.Fatalf("finalize_sync_run wakeup status = %q, want pending -- without "+
			"the re-arm nothing reconsiders the run and it never closes", status)
	}
}

// TestSweepTerminalizesUnitsParkedPastTheRouteUnavailableWindow drives each
// durable state that leaves the relay unable to route sync.provider_unit.
func TestSweepTerminalizesUnitsParkedPastTheRouteUnavailableWindow(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		apply func(*testing.T, context.Context, *pgxpool.Pool)
	}{
		{"rolled back to celery", func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			setProviderUnitRoute(t, ctx, pool, "celery", false)
		}},
		{"paused", func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			setProviderUnitRoute(t, ctx, pool, "river", true)
		}},
		{"transport drift", func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			setProviderUnitRoute(t, ctx, pool, "shadow", false)
		}},
		{"route row absent", func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			deleteProviderUnitRoute(t, ctx, pool)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			pool := startSweepPostgres(t, ctx)
			now := time.Now().UTC()
			testCase.apply(t, ctx, pool)

			// Planned one minute past the window, with both parked unit
			// shapes: one never claimed, one claimed and published into an
			// outbox row the relay can never drain.
			seedRouteFaultRun(t, ctx, pool, routeFaultRun, now.Add(-RouteUnavailableWindow-time.Minute), 2)
			seedDispatchWakeup(t, ctx, pool, routeFaultRun, 1, now)
			planned, dispatching := routeFaultUnitID(1), routeFaultUnitID(2)
			seedRouteFaultUnit(t, ctx, pool, routeFaultRun, planned, "commits", "planned", now)
			seedRouteFaultUnit(t, ctx, pool, routeFaultRun, dispatching, "tests", "dispatching", now)
			publishRouteFaultUnit(t, ctx, pool, dispatching, now)

			if _, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100); err != nil {
				t.Fatalf("sweep: %v", err)
			}
			assertRouteUnavailableTermination(t, ctx, pool, routeFaultRun,
				[]string{planned, dispatching})
		})
	}
}

// TestSweepTerminalizesUnitsParkedPastTheRouteUnavailableAttemptBound proves
// the second, faster bound: a run planned well inside the window that has
// already re-driven dispatch against the fault is released on attempts alone.
func TestSweepTerminalizesUnitsParkedPastTheRouteUnavailableAttemptBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	setProviderUnitRoute(t, ctx, pool, "celery", false)

	seedRouteFaultRun(t, ctx, pool, routeFaultRun, now.Add(-2*time.Minute), 1)
	seedDispatchWakeup(t, ctx, pool, routeFaultRun, RouteUnavailableAttempts, now)
	unit := routeFaultUnitID(3)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRun, unit, "commits", "dispatching", now)
	publishRouteFaultUnit(t, ctx, pool, unit, now)

	if _, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	assertRouteUnavailableTermination(t, ctx, pool, routeFaultRun, []string{unit})
}

// TestSweepHoldsUnitsInsideTheRouteUnavailableBound is the other half: the
// bound has to be a bound. A route fault an operator clears inside the window
// must not have cost anybody a run.
func TestSweepHoldsUnitsInsideTheRouteUnavailableBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	setProviderUnitRoute(t, ctx, pool, "celery", false)

	seedRouteFaultRun(t, ctx, pool, routeFaultRun, now.Add(-time.Minute), 1)
	seedDispatchWakeup(t, ctx, pool, routeFaultRun, RouteUnavailableAttempts-1, now)
	unit := routeFaultUnitID(4)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRun, unit, "commits", "dispatching", now)
	publishRouteFaultUnit(t, ctx, pool, unit, now)

	if _, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" {
		t.Fatalf("unit status = %q, want dispatching -- a fault inside both "+
			"bounds is a wait, not a failure", status)
	}
}

// TestSweepShadowModeReportsRouteUnavailableWithoutWriting keeps the mode
// switch meaning the same thing on this branch as on every other.
func TestSweepShadowModeReportsRouteUnavailableWithoutWriting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	setProviderUnitRoute(t, ctx, pool, "celery", false)

	seedRouteFaultRun(t, ctx, pool, routeFaultRunAlt, now.Add(-RouteUnavailableWindow-time.Minute), 1)
	seedDispatchWakeup(t, ctx, pool, routeFaultRunAlt, 1, now)
	unit := routeFaultUnitID(5)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRunAlt, unit, "commits", "dispatching", now)

	if _, err := newSweepForTest(t, pool, SweepModeShadow).Step(ctx, now, 100); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" {
		t.Fatalf("unit status = %q, want dispatching -- shadow mode reports, it "+
			"never writes", status)
	}
}

// TestSweepLeavesParkedUnitsAloneWhileTheProviderUnitRouteIsUsable is the
// branch guard. A usable route means the ordinary strand sweep owns the pass,
// and a young unit is not its business either, so nothing may be written.
func TestSweepLeavesParkedUnitsAloneWhileTheProviderUnitRouteIsUsable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()

	// The fixture seeds river_canary, which River owns. The run is well past
	// both bounds, so only the route stands between it and termination.
	seedRouteFaultRun(t, ctx, pool, routeFaultRun, now.Add(-RouteUnavailableWindow-time.Hour), 1)
	seedDispatchWakeup(t, ctx, pool, routeFaultRun, RouteUnavailableAttempts+3, now)
	unit := routeFaultUnitID(6)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRun, unit, "commits", "dispatching", now)
	publishRouteFaultUnit(t, ctx, pool, unit, now)

	if _, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" {
		t.Fatalf("unit status = %q, want dispatching -- a usable route is not a "+
			"route fault, whatever the run's age", status)
	}
}

// TestSweepRouteUnavailableTerminationIsIdempotent proves the pass stops
// re-driving: once the units are terminal a second pass selects nothing, so a
// reconciler ticking every second against a route fault that lasts for hours
// writes once and then goes quiet.
func TestSweepRouteUnavailableTerminationIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	setProviderUnitRoute(t, ctx, pool, "celery", false)

	seedRouteFaultRun(t, ctx, pool, routeFaultRun, now.Add(-RouteUnavailableWindow-time.Minute), 1)
	seedDispatchWakeup(t, ctx, pool, routeFaultRun, 1, now)
	unit := routeFaultUnitID(7)
	seedRouteFaultUnit(t, ctx, pool, routeFaultRun, unit, "commits", "dispatching", now)
	publishRouteFaultUnit(t, ctx, pool, unit, now)

	sweep := newSweepForTest(t, pool, SweepModeActive)
	first, err := sweep.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	if first.RouteUnavailableTerminalized != 1 {
		t.Fatalf("first pass terminalized %d, want 1", first.RouteUnavailableTerminalized)
	}
	assertRouteUnavailableTermination(t, ctx, pool, routeFaultRun, []string{unit})

	second, err := sweep.Step(ctx, now.Add(time.Second), 100)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if second.RouteUnavailableCandidates != 0 || second.RouteUnavailableTerminalized != 0 {
		t.Fatalf("second pass = (candidates %d, terminalized %d), want zeroes -- a "+
			"terminal unit must not be reselected on every tick",
			second.RouteUnavailableCandidates, second.RouteUnavailableTerminalized)
	}
}
