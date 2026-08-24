//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dispatchServiceTestOutbox = "00000000-0000-4000-8000-0000000000e9"
)

func withDispatchServicePool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createReferenceDiscoveryTables(t, ctx, pool)
	// DispatchGuard.authorize_run's total-cap resolution
	// (scheduledsync.ResolveMaxSyncUnitsCap -> loadPlanLimits) queries
	// organizations/org_licenses -- absent in production only for a
	// malformed org id, never for a real one, so these tables must exist
	// here too: a missing-relation ERROR poisons the whole enclosing
	// Postgres transaction (25P02), which Go-level fallback-to-default
	// error handling cannot undo without a savepoint. Production always has
	// these tables; this is a test-fixture-completeness requirement, not a
	// production behavior this port needs to defend against.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text NULL);
CREATE TABLE public.org_licenses (org_id uuid PRIMARY KEY, tier text NULL, limits_override jsonb NULL);
CREATE TABLE public.tier_limits (tier text NOT NULL, limit_key text NOT NULL, limit_value text NULL, PRIMARY KEY (tier, limit_key));
INSERT INTO public.organizations (id, tier) VALUES ('`+discoveryTestOrg+`', 'community');`); err != nil {
		t.Fatal(err)
	}
	fn(ctx, pool)
}

// seedDispatchRoute mirrors seedDiscoveryRoute but for kind=dispatch_sync_run
// -- the currentTransportReference guard Dispatch() checks first, before any
// business logic.
func seedDispatchRoute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
		 VALUES ('dispatch_sync_run','river',1,false,'celery')`,
		`INSERT INTO sync_dispatch_outbox
		    (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
		 VALUES ('` + dispatchServiceTestOutbox + `','` + discoveryTestRun + `','` + discoveryTestOrg + `',
		         'dispatch_sync_run','dispatched',now(),'river',1,now(),now())`,
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + discoveryTestRun + `','` +
			discoveryTestOrg + `','` + discoveryTestIntegration + `')`,
		`INSERT INTO integrations (id,org_id,provider) VALUES ('` + discoveryTestIntegration + `','` + discoveryTestOrg + `','github')`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func dispatchTestArgs() DispatchSyncRunArgs {
	return DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
		DispatchOutbox: dispatchServiceTestOutbox, RouteGeneration: 1,
	}}
}

func markReferenceDiscoverySucceeded(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at)
VALUES ('00000000-0000-4000-8000-0000000000ea',$1,$2,$3,1,now())`,
		discoveryTestRun, discoveryTestOrg, discoveryStatusSuccess); err != nil {
		t.Fatal(err)
	}
}

func newTestDispatchService(t *testing.T, pool *pgxpool.Pool) *NativeDispatchSyncRunService {
	t.Helper()
	service, err := NewNativeDispatchSyncRunService(pool, nil, &fakeBudgetEstimator{})
	if err != nil {
		t.Fatalf("NewNativeDispatchSyncRunService: %v", err)
	}
	return service
}

// TestDispatchReturnsNilWhenTheTransportReferenceIsStale pins the
// outbox-relay fence: a job whose route generation no longer matches the
// current durable route must be a silent no-op, not business logic
// running against a superseded route.
func TestDispatchReturnsNilWhenTheTransportReferenceIsStale(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// Deliberately do NOT seed the route -- currentTransportReference's
		// EXISTS query then has nothing to match.
		if _, err := pool.Exec(ctx, `INSERT INTO sync_runs (id,org_id,integration_id) VALUES ($1,$2,$3)`,
			discoveryTestRun, discoveryTestOrg, discoveryTestIntegration); err != nil {
			t.Fatal(err)
		}
		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (stale reference is a silent no-op)", err)
		}
	})
}

// TestDispatchCommitsAndReturnsNilForAMissingRun pins the missing-run
// branch: Python returns {"status": "missing", ...} without error; the Go
// port commits a no-op and returns nil.
func TestDispatchCommitsAndReturnsNilForAMissingRun(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// Seed the route/outbox but NOT the sync_runs row itself.
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
VALUES ('dispatch_sync_run','river',1,false,'celery')`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
VALUES ($1,$2,$3,'dispatch_sync_run','dispatched',now(),'river',1,now(),now())`,
			dispatchServiceTestOutbox, discoveryTestRun, discoveryTestOrg); err != nil {
			t.Fatal(err)
		}
		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}
	})
}

// TestDispatchBlocksOnReferenceDiscoveryAndArmsAWakeup pins the
// reference-discovery gate: with no success ledger row, Dispatch() must
// not proceed to DispatchGuard at all, and must arm a reference-discovery
// wakeup.
func TestDispatchBlocksOnReferenceDiscoveryAndArmsAWakeup(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`,
			discoveryTestRun).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("got %d reference_discovery outbox rows, want 1 (a wakeup was armed)", count)
		}
	})
}

// TestDispatchDeniesWithActiveUnitsFailsOnlyStrandedOnes pins the total-cap
// hard-deny branch's FIRST shape: with an active (RUNNING) unit present,
// only the never-going-to-dispatch-again units are failed, the run itself
// stays non-terminal, and a finalize check is armed instead of running the
// whole run to FAILED immediately.
func TestDispatchDeniesWithActiveUnitsFailsOnlyStrandedOnes(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := time.Now().UTC()
		strandedUnit := "00000000-0000-4000-8000-0000000000eb"
		runningUnit := "00000000-0000-4000-8000-0000000000ec"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5),
       ($4,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','running',$5)`,
			strandedUnit, discoveryTestOrg, discoveryTestRun, runningUnit, now); err != nil {
			t.Fatal(err)
		}
		// Force DispatchGuard to deny via the total-cap: SYNC_RUN_MAX_UNITS=1
		// with 2 units already present denies the whole run.
		t.Setenv("SYNC_RUN_MAX_UNITS", "1")

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var strandedStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, strandedUnit).Scan(&strandedStatus); err != nil {
			t.Fatal(err)
		}
		if strandedStatus != syncRunUnitStatusFailed {
			t.Fatalf("strandedUnit status=%q, want failed", strandedStatus)
		}
		var runStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&runStatus); err != nil {
			t.Fatal(err)
		}
		if runStatus == syncRunStatusFailed {
			t.Fatal("run status must NOT be failed yet -- an active (running) unit means the run isn't terminal")
		}
		var finalizeCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeCount); err != nil {
			t.Fatal(err)
		}
		if finalizeCount != 1 {
			t.Fatalf("got %d finalize wakeups, want 1", finalizeCount)
		}
	})
}

// TestDispatchDeniesWithNoActiveUnitsFailsTheWholeRun pins the total-cap
// hard-deny branch's SECOND shape: with no active units, the whole run is
// marked FAILED immediately (a redispatch would just be re-denied forever,
// so leaving it PLANNED would strand it invisibly to the reconciler).
func TestDispatchDeniesWithNoActiveUnitsFailsTheWholeRun(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := time.Now().UTC()
		unitA := "00000000-0000-4000-8000-0000000000ee"
		unitB := "00000000-0000-4000-8000-0000000000ef"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5),
       ($4,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5)`,
			unitA, discoveryTestOrg, discoveryTestRun, unitB, now); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SYNC_RUN_MAX_UNITS", "1")

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var runStatus string
		var completedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, completed_at FROM sync_runs WHERE id=$1`, discoveryTestRun).
			Scan(&runStatus, &completedAt); err != nil {
			t.Fatal(err)
		}
		if runStatus != syncRunStatusFailed || completedAt == nil {
			t.Fatalf("runStatus=%q completedAt=%v, want failed/non-nil", runStatus, completedAt)
		}
		var unitAStatus, unitBStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitA).Scan(&unitAStatus); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitB).Scan(&unitBStatus); err != nil {
			t.Fatal(err)
		}
		if unitAStatus != syncRunUnitStatusFailed || unitBStatus != syncRunUnitStatusFailed {
			t.Fatalf("unitA=%q unitB=%q, want both failed", unitAStatus, unitBStatus)
		}
	})
}

// TestDispatchReachesTheAllowedContinuationStop pins the whole gate chain's
// success path: with reference discovery succeeded and DispatchGuard
// allowing (comfortable unit cap, no active units to conflict), Dispatch()
// must reach the deliberate not-yet-implemented stopping point -- proving
// every gate above it passed, not tripped on an unrelated error.
func TestDispatchReachesTheAllowedContinuationStop(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := time.Now().UTC()
		unitID := "00000000-0000-4000-8000-0000000000f1"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		err := service.Dispatch(ctx, dispatchTestArgs())
		if !errors.Is(err, errDispatchNotYetImplemented) {
			t.Fatalf("Dispatch: %v, want errDispatchNotYetImplemented -- every gate above the TODO must have passed", err)
		}
	})
}
