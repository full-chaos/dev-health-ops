//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fakeJobRegistry satisfies both joboutbox.PolicyRegistry and
// jobroute.Registry (the same Descriptor(kind) shape) without needing the
// real jobruntime.Load's checked-in contract/migration artifacts on disk --
// this test only needs ONE kind's descriptor (sync.provider_unit) under a
// caller-controlled Route, to prove the route-fence and Publish call sites
// wire correctly, not to re-test jobruntime's own artifact loading.
type fakeJobRegistry struct {
	descriptors map[string]jobruntime.Descriptor
}

func (registry *fakeJobRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.descriptors[kind]
	return descriptor, ok
}

func (registry *fakeJobRegistry) Descriptors() []jobruntime.Descriptor {
	list := make([]jobruntime.Descriptor, 0, len(registry.descriptors))
	for _, descriptor := range registry.descriptors {
		list = append(list, descriptor)
	}
	return list
}

type fakeQuiescer struct{}

func (fakeQuiescer) Quiesce(context.Context, string) error { return nil }

func providerUnitDescriptor(route string) jobruntime.Descriptor {
	return jobruntime.Descriptor{
		Kind:              jobcontract.KindSyncProviderUnit,
		CurrentVersion:    jobcontract.ContractVersionV1,
		SupportedVersions: []int{jobcontract.ContractVersionV1},
		Queue:             "sync",
		MaxAttempts:       5,
		DomainLink:        "sync_run_unit",
		OrganizationScope: "tenant",
		Route:             route,
		RollbackRoute:     "celery",
	}
}

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
	// jobroute.Controller.ResolveInTx / joboutbox.Producer.Publish's own
	// tables -- worker_job_routes is the SAME generic route store Python's
	// resolve_worker_job_route reads (confirmed by reading jobroute's own
	// readState query before wiring this in), a different table from this
	// coordinator's own sync_dispatch_transport_routes.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.worker_job_routes (
  job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
  generation bigint NOT NULL, updated_at timestamptz NOT NULL
);
CREATE TABLE public.worker_job_outbox (
  id uuid PRIMARY KEY, dedupe_key text NOT NULL UNIQUE, job_kind text NOT NULL,
  contract_version int NOT NULL, args json NOT NULL, payload_hash text NOT NULL,
  queue text NOT NULL, priority int NOT NULL, max_attempts int NOT NULL,
  scheduled_at timestamptz NOT NULL, status text NOT NULL, attempt_count int NOT NULL,
  next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
  created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
INSERT INTO public.worker_job_routes (job_kind, transport, paused, generation, updated_at)
VALUES ('sync.provider_unit', 'river', false, 1, now());`); err != nil {
		t.Fatal(err)
	}
	// activeCooldowns (inside enforceRun) reads this table and is
	// deliberately fail-open on a query error -- but the underlying
	// Postgres ERROR (missing relation) still poisons the enclosing
	// transaction (25P02) even though the Go-level error is swallowed.
	// Same fixture-completeness class as organizations/tier_limits above.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.provider_rate_limit_observations (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, host text NULL,
 integration_id uuid NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
 route_family text NULL, route_family_attribution text NULL, dimension text NULL,
 retry_after_seconds double precision NULL, reset_at timestamptz NULL, reason text NULL,
 request_id text NULL, observed_at timestamptz NOT NULL
);`); err != nil {
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

// newTestDispatchService constructs the service with a registry reporting
// route for sync.provider_unit -- callers that need a DIFFERENT capability
// answer per test pass their own registry via newTestDispatchServiceWith.
func newTestDispatchService(t *testing.T, pool *pgxpool.Pool) *NativeDispatchSyncRunService {
	t.Helper()
	return newTestDispatchServiceWith(t, pool, &fakeJobRegistry{
		descriptors: map[string]jobruntime.Descriptor{
			jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
		},
	})
}

func newTestDispatchServiceWith(t *testing.T, pool *pgxpool.Pool, registry *fakeJobRegistry) *NativeDispatchSyncRunService {
	t.Helper()
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatalf("joboutbox.NewProducer: %v", err)
	}
	routeController, err := jobroute.NewController(pool, registry, fakeQuiescer{})
	if err != nil {
		t.Fatalf("jobroute.NewController: %v", err)
	}
	service, err := NewNativeDispatchSyncRunService(pool, nil, &fakeBudgetEstimator{}, producer, registry, routeController)
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

// TestDispatchEnqueuesARoutableUnitAndMarksTheRunDispatching pins the
// whole continuation's happy path end to end: a claimable, routable unit
// (github/commits, confirmed route-ready and plannable) gets published
// into worker_job_outbox, the run moves to DISPATCHING with started_at
// set, and Dispatch() returns nil -- "dispatched", not the not-yet-
// implemented stop, since river_queued > 0.
func TestDispatchEnqueuesARoutableUnitAndMarksTheRunDispatching(t *testing.T) {
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
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (successful dispatch)", err)
		}

		var outboxCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).
			Scan(&outboxCount); err != nil {
			t.Fatal(err)
		}
		if outboxCount != 1 {
			t.Fatalf("got %d worker_job_outbox rows, want 1", outboxCount)
		}

		var runStatus string
		var startedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, started_at FROM sync_runs WHERE id=$1`, discoveryTestRun).
			Scan(&runStatus, &startedAt); err != nil {
			t.Fatal(err)
		}
		if runStatus != syncRunStatusDispatching || startedAt == nil {
			t.Fatalf("runStatus=%q startedAt=%v, want dispatching/non-nil", runStatus, startedAt)
		}
	})
}

// TestDispatchTerminalizesAnUnroutableUnitAndArmsFinalize pins the
// unroutable-unit path wired into Dispatch() all the way through the
// tail: a claimed unit whose (provider, dataset) pair the capability
// matrix does not route gets terminalized (CHAOS-3990), never enqueued --
// river_queued stays 0, and since terminalizing it leaves NOTHING pending
// (no other unit in the run), Dispatch() reaches the tail's case (d) and
// arms a finalize wakeup, returning nil ("noop_finalize"), not
// "dispatched".
func TestDispatchTerminalizesAnUnroutableUnitAndArmsFinalize(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := time.Now().UTC()
		unitID := "00000000-0000-4000-8000-0000000000f2"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'unknown-provider','unknown-dataset','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (noop_finalize)", err)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusFailed {
			t.Fatalf("unit status=%q, want failed (terminalized as unroutable)", unitStatus)
		}
		var outboxCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox`).Scan(&outboxCount); err != nil {
			t.Fatal(err)
		}
		if outboxCount != 0 {
			t.Fatalf("got %d worker_job_outbox rows, want 0 -- an unroutable unit must never be published", outboxCount)
		}
		var finalizeWakeups int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeWakeups); err != nil {
			t.Fatal(err)
		}
		if finalizeWakeups != 1 {
			t.Fatalf("got %d finalize wakeups, want 1 -- nothing left pending, so finalize must be armed", finalizeWakeups)
		}
	})
}

// TestDispatchRejectsANonCanonicalAtomicFamilyAliasAndRollsBackTheClaim
// pins ValidateClaim's own wiring into the per-unit loop, and an important
// empirical finding from trying to build the RouteReady-without-Plannable
// fixture directly: github's work-item-labels dataset IS RouteReady-but-
// not-Plannable in providersync's capability matrix (the whole atomic
// family is marked native/ready so producer/matrix drift cannot re-open
// one partial alias; only the canonical work-items claim is Plannable),
// but that combination is UNREACHABLE at the routability check in
// practice: ValidateClaim (which runs FIRST, matching Python's own
// validate-before-route order) already refuses any non-canonical atomic-
// family alias outright, for every family currently declared. So this
// fixture proves the EARLIER guard instead: a malformed persisted claim
// aborts the whole pass -- the deferred tx.Rollback undoes claimUnits'
// own claim along with it, leaving the unit exactly as it was, not
// half-claimed.
func TestDispatchRejectsANonCanonicalAtomicFamilyAliasAndRollsBackTheClaim(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := time.Now().UTC()
		unitID := "00000000-0000-4000-8000-0000000000f4"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','work-item-labels','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		err := service.Dispatch(ctx, dispatchTestArgs())
		if !errors.Is(err, ErrDispatchProviderUnitRoute) {
			t.Fatalf("Dispatch: %v, want ErrDispatchProviderUnitRoute", err)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusPlanned {
			t.Fatalf("unit status=%q, want unchanged (planned) -- the whole pass rolls back on a malformed claim, not just this unit", unitStatus)
		}
	})
}

// TestDispatchFailsClosedWhenTheProviderUnitRouteIsNotRiver pins the
// route-fence: with sync.provider_unit's durable transport set to
// anything other than river, Dispatch() must fail closed
// (ErrDispatchProviderUnitRoute) rather than stage work -- CHAOS-4054
// deleted the Celery dispatch plane, so there is no second runtime left
// to fall through to.
func TestDispatchFailsClosedWhenTheProviderUnitRouteIsNotRiver(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		if _, err := pool.Exec(ctx, `UPDATE worker_job_routes SET transport='celery' WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		unitID := "00000000-0000-4000-8000-0000000000f3"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchServiceWith(t, pool, &fakeJobRegistry{
			descriptors: map[string]jobruntime.Descriptor{
				jobcontract.KindSyncProviderUnit: providerUnitDescriptor("celery"),
			},
		})
		err := service.Dispatch(ctx, dispatchTestArgs())
		if !errors.Is(err, ErrDispatchProviderUnitRoute) {
			t.Fatalf("Dispatch: %v, want ErrDispatchProviderUnitRoute", err)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusPlanned {
			t.Fatalf("unit status=%q, want unchanged (planned) -- fail-closed must not claim anything", unitStatus)
		}
	})
}
