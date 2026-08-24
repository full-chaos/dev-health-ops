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

	// Domain-side tables: the same shape withDispatchServicePool already
	// proves Dispatch's real statement set against, this test's own copy
	// (not sharing that helper directly, since this fixture also needs the
	// FULL production worker_job_outbox shape + a real River schema + the
	// jobroute worker_job_routes table, none of which the lighter dispatch
	// fixtures need).
	createReferenceDiscoveryTables(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text NULL);
CREATE TABLE public.org_licenses (org_id uuid PRIMARY KEY, tier text NULL, limits_override jsonb NULL);
CREATE TABLE public.tier_limits (tier text NOT NULL, limit_key text NOT NULL, limit_value text NULL, PRIMARY KEY (tier, limit_key));
INSERT INTO public.organizations (id, tier) VALUES ('`+discoveryTestOrg+`', 'community');
CREATE TABLE public.provider_rate_limit_observations (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, host text NULL,
 integration_id uuid NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
 route_family text NULL, route_family_attribution text NULL, dimension text NULL,
 retry_after_seconds double precision NULL, reset_at timestamptz NULL, reason text NULL,
 request_id text NULL, observed_at timestamptz NOT NULL
);`); err != nil {
		t.Fatal(err)
	}

	// The FULL production worker_job_outbox shape (claim_token/claimed_at/
	// claim_expires_at/river_job_id/delivered_at etc.) -- the lighter
	// Publish-only shape the dispatch fixtures use is not enough here,
	// since this test also drives the real Repository/Relay claim-and-
	// deliver path against it, matching internal/joboutbox's own
	// createOutboxSchema (verified against that file before copying).
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.worker_job_outbox (
	id uuid PRIMARY KEY,
	dedupe_key varchar(256) NOT NULL UNIQUE,
	job_kind varchar(96) NOT NULL,
	contract_version integer NOT NULL,
	args json NOT NULL,
	payload_hash varchar(71) NOT NULL,
	queue varchar(96) NOT NULL,
	priority smallint NOT NULL,
	max_attempts smallint NOT NULL,
	scheduled_at timestamptz NOT NULL,
	status varchar(16) NOT NULL,
	claim_token uuid,
	claimed_at timestamptz,
	claim_expires_at timestamptz,
	attempt_count integer NOT NULL,
	first_attempt_at timestamptz,
	last_attempt_at timestamptz,
	next_attempt_at timestamptz NOT NULL,
	last_error_code varchar(64),
	last_error_detail varchar(256),
	last_error_at timestamptz,
	river_job_id bigint UNIQUE,
	delivered_at timestamptz,
	prerequisite_completion_key text NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	CONSTRAINT worker_job_outbox_status CHECK (status IN ('pending','claimed','delivered','dead')),
	CONSTRAINT worker_job_outbox_claim CHECK (
		(status='claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND claim_expires_at IS NOT NULL)
		OR (status<>'claimed' AND claim_token IS NULL AND claimed_at IS NULL AND claim_expires_at IS NULL)
	),
	CONSTRAINT worker_job_outbox_delivery CHECK (
		(status='delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL)
		OR (status<>'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)
	)
);
CREATE TABLE public.worker_job_completion_fences (
	completion_key text PRIMARY KEY,
	completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE TABLE public.worker_job_delivery_abandonments (
	dedupe_key varchar(256) PRIMARY KEY,
	job_kind varchar(96) NOT NULL,
	abandoned_at timestamptz NOT NULL,
	attempt_count integer NOT NULL,
	last_error_code varchar(64)
);
CREATE TABLE public.worker_job_routes (
	job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
	generation bigint NOT NULL, updated_at timestamptz NOT NULL
);
INSERT INTO public.worker_job_routes (job_kind, transport, paused, generation, updated_at)
VALUES ('`+jobcontract.KindSyncProviderUnit+`', 'river', false, 1, now());
-- Rollback's own live-claims check reads this (control.go's
-- "SELECT count(*) FROM public.worker_job_runs WHERE job_kind=$1 AND
-- status='running'") -- absent from every other dispatch fixture in this
-- package since Dispatch's own write path never touches it, but the
-- rollback-fence test needs it for a real Rollback() call to reach past
-- its own precondition checks instead of failing on a missing relation.
CREATE TABLE public.worker_job_runs (
	id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
);`); err != nil {
		t.Fatal(err)
	}

	// A real River schema this Postgres instance owns -- DomainRole/
	// QueueRole only need to EXIST for ApplyPinnedMigrations's role-
	// eligibility preflight; this test never connects as either (functional
	// behavior, not privilege-boundary coverage -- CHAOS-4209 owns that).
	roleSetup := []string{
		"CREATE ROLE routehold_domain_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'",
		"CREATE ROLE routehold_queue_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'",
	}
	for _, statement := range roleSetup {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: "routehold_domain_runtime",
		QueueRole:  "routehold_queue_runtime",
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
	statements := []string{
		`INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
		 VALUES ('dispatch_sync_run','river',1,false,'celery')`,
		`INSERT INTO sync_dispatch_outbox
		    (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
		 VALUES ('` + routeHoldTestOutbox + `','` + discoveryTestRun + `','` + discoveryTestOrg + `',
		         'dispatch_sync_run','dispatched',now(),'river',1,now(),now())`,
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + discoveryTestRun + `','` +
			discoveryTestOrg + `','` + discoveryTestIntegration + `')`,
		`INSERT INTO integrations (id,org_id,provider) VALUES ('` + discoveryTestIntegration + `','` + discoveryTestOrg + `','github')`,
		`INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at)
		 VALUES ('00000000-0000-4000-8000-0000000000fb','` + discoveryTestRun + `','` + discoveryTestOrg + `','` + discoveryStatusSuccess + `',1,now())`,
		`INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
		 VALUES ('` + unitID + `','` + discoveryTestOrg + `','` + discoveryTestRun + `','github','commits','00000000-0000-4000-8000-0000000000ed','planned',now())`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func routeHoldDispatchArgs() DispatchSyncRunArgs {
	return DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
		DispatchOutbox: routeHoldTestOutbox, RouteGeneration: 1,
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

		if _, err := fixture.relay.Step(ctx, time.Now().UTC(), 10); err != nil {
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
		if _, err := fixture.relay.Step(ctx, time.Now().UTC(), 10); err != nil {
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
		if _, err := fixture.relay.Step(ctx, time.Now().UTC(), 10); err != nil {
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
