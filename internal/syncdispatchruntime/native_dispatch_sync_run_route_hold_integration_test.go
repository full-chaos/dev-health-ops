//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file is the CHAOS-4175 ruling-reversal proof team-lead asked for
// (see native_dispatch_sync_run_service.go's Dispatch doc comment): does
// dropping the tx-scoped jobroute route check actually leave the run
// reaching the SAME terminal state as Python -- just via a different
// waiting state (claim-then-hold in Go vs refuse-then-retry in Python) --
// or does it leave a unit stuck?
//
// A route "hold" here is simulated the way jobroute.Controller.Rollback
// ACTUALLY leaves a route (transport='celery', paused=FALSE) -- confirmed
// by reading Rollback's own UPDATE statement before writing this test, not
// assumed. paused=TRUE is a real column but is NEVER set by any production
// jobroute code path for a registry kind (grepped: no match outside test
// files) -- jobroute.Controller has no Pause() method at all, only
// syncroute (the four coordinator kinds' separate table) does. It is also
// a materially different, WORSE case: Resolve() returns ErrPaused for a
// paused kind, and DeferredKinds() aborts its ENTIRE enumeration on the
// FIRST kind that errors (control.go:108-121) -- so pausing ANY one
// registered kind would fail relay.Step() for every kind that relay
// instance handles, not just the paused one. That is a real, latent
// fragility in jobroute.Controller worth flagging on its own, but it is
// not reachable today and is not what this test exists to prove.
const routeHoldTestOutbox = "00000000-0000-4000-8000-0000000000fa"

type routeHoldFixture struct {
	pool       *pgxpool.Pool
	service    *NativeDispatchSyncRunService
	relay      *joboutbox.Relay
	controller *jobroute.Controller
}

func withRouteHoldFixture(t *testing.T, fn func(ctx context.Context, fixture routeHoldFixture)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	// The migrated schema: the domain tables, the FULL production worker_job_outbox (the
	// Repository/Relay claim-and-deliver path drives it), worker_job_routes (seeded on river for
	// sync.provider_unit) and worker_job_runs (Rollback's live-claims check reads it). A River schema
	// is added below, after Apply (which wants an empty database).
	createReferenceDiscoveryTables(t, ctx, pool)
	pgseed.Org(ctx, t, pool, discoveryTestOrg, "community")

	// A real River schema this Postgres instance owns -- DomainRole/
	// QueueRole only need to EXIST for ApplyPinnedMigrations's role-
	// eligibility preflight; this test never connects as either (functional
	// behavior, not privilege-boundary coverage -- CHAOS-4209 owns that).
	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving both role names
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	routeHoldDomainRole, err := containers.RoleName("routehold_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	routeHoldQueueRole, err := containers.RoleName("routehold_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(pool, routeHoldDomainRole, t.Logf)
	defer containers.DropRole(pool, routeHoldQueueRole, t.Logf)
	roleSetup := []string{
		"CREATE ROLE " + routeHoldDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'",
		"CREATE ROLE " + routeHoldQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'",
	}
	for _, statement := range roleSetup {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: routeHoldDomainRole,
		QueueRole:  routeHoldQueueRole,
	}); err != nil {
		t.Fatal(err)
	}

	registry := &fakeJobRegistry{
		descriptors: map[string]jobruntime.Descriptor{
			jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
		},
	}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeDispatchSyncRunService(pool, nil, &fakeBudgetEstimator{}, producer, registry)
	if err != nil {
		t.Fatal(err)
	}

	repository, err := joboutbox.NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	inserter, err := joboutbox.NewRiverInserter(pool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := jobroute.NewController(pool, registry, routeHoldFakeQuiescer{})
	if err != nil {
		t.Fatal(err)
	}
	relay, err := joboutbox.NewRelayWithRoutes(repository, inserter, controller, joboutbox.DefaultRelayConfig())
	if err != nil {
		t.Fatal(err)
	}

	fn(ctx, routeHoldFixture{pool: pool, service: service, relay: relay, controller: controller})
}

type routeHoldFakeQuiescer struct{}

func (routeHoldFakeQuiescer) Quiesce(context.Context, string) error { return nil }

func seedRouteHoldRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) {
	t.Helper()
	if got := pgseed.SyncTransportRoute(ctx, t, pool, "dispatch_sync_run", "river", dispatchRouteGeneration, false, "celery"); got != dispatchRouteGeneration {
		t.Fatalf("dispatch_sync_run route generation = %d, want %d", got, dispatchRouteGeneration)
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration})
	pgseed.SyncDispatchOutbox(ctx, t, pool, routeHoldTestOutbox, discoveryTestRun, discoveryTestOrg, "dispatch_sync_run", "dispatched", "river", dispatchRouteGeneration)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ('00000000-0000-4000-8000-0000000000fb',$1,$2,$3,1,now(),now(),now())`,
		discoveryTestRun, discoveryTestOrg, discoveryStatusSuccess); err != nil {
		t.Fatal(err)
	}
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: unitID, RunID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration,
		SourceID: dispatchTestSource, DatasetKey: "commits", CostClass: "rest_core", Status: "planned",
	})
}

func routeHoldDispatchArgs() DispatchSyncRunArgs {
	return DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
		DispatchOutbox: routeHoldTestOutbox, RouteGeneration: dispatchRouteGeneration,
	}}
}

func routeHoldSetProviderUnitTransport(t *testing.T, ctx context.Context, pool *pgxpool.Pool, transport string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE public.worker_job_routes SET transport = $2, generation = generation + 1, updated_at = now()
WHERE job_kind = $1`, jobcontract.KindSyncProviderUnit, transport); err != nil {
		t.Fatal(err)
	}
}

func routeHoldCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) (unitStatus, outboxStatus string, outboxRows, riverJobs int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if outboxRows > 0 {
		if err := pool.QueryRow(ctx, `SELECT status FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).Scan(&outboxStatus); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM river.river_job WHERE kind=$1`, jobcontract.KindSyncProviderUnit).Scan(&riverJobs); err != nil {
		t.Fatal(err)
	}
	return unitStatus, outboxStatus, outboxRows, riverJobs
}

// TestDispatchHoldsAClaimedUnitUnderAPausedRouteAndTheRelayDeliversOnResume
// is team-lead's ruling-reversal proof, part 1. A sync.provider_unit route
// already rolled back to celery BEFORE Dispatch runs: Dispatch still
// claims and publishes (it no longer reads the live route at all), but the
// relay -- which DOES read it, at drain -- must hold the row, insert
// NOTHING into River, until the route is restored, at which point it
// delivers exactly once.
func TestDispatchHoldsAClaimedUnitUnderAPausedRouteAndTheRelayDeliversOnResume(t *testing.T) {
	withRouteHoldFixture(t, func(ctx context.Context, fixture routeHoldFixture) {
		unitID := "00000000-0000-4000-8000-0000000000fc"
		seedRouteHoldRun(t, ctx, fixture.pool, unitID)
		routeHoldSetProviderUnitTransport(t, ctx, fixture.pool, "celery")

		if err := fixture.service.Dispatch(ctx, routeHoldDispatchArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (claims and publishes even under a bad live route)", err)
		}
		unitStatus, outboxStatus, outboxRows, riverJobs := routeHoldCounts(t, ctx, fixture.pool, unitID)
		if unitStatus != syncRunUnitStatusDispatching {
			t.Fatalf("after Dispatch: unit status=%q, want dispatching", unitStatus)
		}
		if outboxRows != 1 || outboxStatus != "pending" {
			t.Fatalf("after Dispatch: outbox rows=%d status=%q, want 1/pending", outboxRows, outboxStatus)
		}
		if riverJobs != 0 {
			t.Fatalf("after Dispatch (negative control): river jobs=%d, want 0 -- Dispatch itself must never touch River directly", riverJobs)
		}

		if _, err := fixture.relay.Step(ctx, pgNow(), 10); err != nil {
			t.Fatalf("relay.Step (route still celery): %v", err)
		}
		unitStatus, outboxStatus, outboxRows, riverJobs = routeHoldCounts(t, ctx, fixture.pool, unitID)
		if unitStatus != syncRunUnitStatusDispatching {
			t.Fatalf("after held relay step: unit status=%q, want still dispatching", unitStatus)
		}
		if outboxRows != 1 || outboxStatus != "pending" {
			t.Fatalf("after held relay step: outbox rows=%d status=%q, want still 1/pending (held, not claimed)", outboxRows, outboxStatus)
		}
		if riverJobs != 0 {
			t.Fatalf("after held relay step: river jobs=%d, want still 0", riverJobs)
		}

		routeHoldSetProviderUnitTransport(t, ctx, fixture.pool, "river")
		if _, err := fixture.relay.Step(ctx, pgNow(), 10); err != nil {
			t.Fatalf("relay.Step (route restored): %v", err)
		}
		unitStatus, outboxStatus, outboxRows, riverJobs = routeHoldCounts(t, ctx, fixture.pool, unitID)
		if outboxRows != 1 || outboxStatus != "delivered" {
			t.Fatalf("after resumed relay step: outbox rows=%d status=%q, want 1/delivered", outboxRows, outboxStatus)
		}
		if riverJobs != 1 {
			t.Fatalf("after resumed relay step: river jobs=%d, want exactly 1", riverJobs)
		}
		var riverState string
		if err := fixture.pool.QueryRow(ctx, `SELECT state::text FROM river.river_job WHERE kind=$1`, jobcontract.KindSyncProviderUnit).Scan(&riverState); err != nil {
			t.Fatal(err)
		}
		if riverState != "available" && riverState != "scheduled" {
			t.Fatalf("delivered river job state=%q, want a schedulable state", riverState)
		}
		// The unit itself stays DISPATCHING here -- advancing it to
		// success/failure is the REAL sync.provider_unit worker's job
		// (providersync's own claim-and-execute path, already covered by
		// its own tests), out of this test's scope. What this test proves
		// is narrower and load-bearing on its own: the row that was
		// claimed under a bad route is neither lost nor double-delivered,
		// and becomes genuinely runnable the moment the route heals --
		// "claim-then-hold, deliver-on-resume", not "claim-then-strand".
		if unitStatus != syncRunUnitStatusDispatching {
			t.Fatalf("unit status=%q, want still dispatching (advancing it is the real provider-unit worker's job, out of scope here)", unitStatus)
		}
	})
}

// TestDispatchStaleReclaimDedupesTheOutboxRowUnderAStillPausedRoute is
// team-lead's ruling-reversal proof, part 2: with the route STILL bad, a
// second Dispatch pass (after the unit's DISPATCHING claim goes stale)
// must reclaim the same unit and re-Publish idempotently -- one outbox
// row throughout, never a duplicate -- and once the route finally
// resumes, exactly ONE River job is ever delivered for it, not two.
func TestDispatchStaleReclaimDedupesTheOutboxRowUnderAStillPausedRoute(t *testing.T) {
	t.Setenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "1")
	withRouteHoldFixture(t, func(ctx context.Context, fixture routeHoldFixture) {
		unitID := "00000000-0000-4000-8000-0000000000fd"
		seedRouteHoldRun(t, ctx, fixture.pool, unitID)
		routeHoldSetProviderUnitTransport(t, ctx, fixture.pool, "celery")

		if err := fixture.service.Dispatch(ctx, routeHoldDispatchArgs()); err != nil {
			t.Fatalf("first Dispatch: %v, want nil", err)
		}
		_, _, outboxRows, riverJobs := routeHoldCounts(t, ctx, fixture.pool, unitID)
		if outboxRows != 1 || riverJobs != 0 {
			t.Fatalf("after first Dispatch: outbox rows=%d river jobs=%d, want 1/0", outboxRows, riverJobs)
		}

		time.Sleep(1100 * time.Millisecond)
		if err := fixture.service.Dispatch(ctx, routeHoldDispatchArgs()); err != nil {
			t.Fatalf("second (reclaim) Dispatch: %v, want nil", err)
		}
		unitStatus, outboxStatus, outboxRows, riverJobs := routeHoldCounts(t, ctx, fixture.pool, unitID)
		if unitStatus != syncRunUnitStatusDispatching {
			t.Fatalf("after reclaim: unit status=%q, want still dispatching", unitStatus)
		}
		if outboxRows != 1 || outboxStatus != "pending" {
			t.Fatalf("after reclaim: outbox rows=%d status=%q, want still 1/pending -- re-Publish must dedupe on the idempotency key, not insert a second row", outboxRows, outboxStatus)
		}
		if riverJobs != 0 {
			t.Fatalf("after reclaim: river jobs=%d, want still 0 (route still celery)", riverJobs)
		}

		routeHoldSetProviderUnitTransport(t, ctx, fixture.pool, "river")
		if _, err := fixture.relay.Step(ctx, pgNow(), 10); err != nil {
			t.Fatalf("relay.Step (route restored): %v", err)
		}
		_, outboxStatus, outboxRows, riverJobs = routeHoldCounts(t, ctx, fixture.pool, unitID)
		if outboxRows != 1 || outboxStatus != "delivered" {
			t.Fatalf("after resumed relay step: outbox rows=%d status=%q, want 1/delivered", outboxRows, outboxStatus)
		}
		if riverJobs != 1 {
			t.Fatalf("after resumed relay step: river jobs=%d, want EXACTLY 1 -- two Dispatch passes over the same unit must still deliver only once", riverJobs)
		}
	})
}
