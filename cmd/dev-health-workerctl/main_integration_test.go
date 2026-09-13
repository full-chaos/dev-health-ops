//go:build integration

package main

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newJobRouteController's three pools are not interchangeable in production
// (see its doc comment in main.go): coordinatorPool is coordinator-exclusive
// for worker_job_routes/worker_job_outbox/worker_job_runs, domainPool is the
// least-privilege role that can still read sync_run_units, and queuePool only
// ever touches the River schema. A test that reused one superuser pool for
// all three arguments would compile and even pass, but it would prove nothing
// about the CHAOS-3113 split this constructor exists to encode -- the domain
// role genuinely cannot run the coordinator's statements in production (that
// was the bug), so a test pool that CAN run them is asserting something
// production does not do. This harness therefore provisions three real,
// least-privilege logins the same way
// internal/storage/postgres/domain_grant_reconciliation_integration_test.go
// and coordinator_statement_privileges_integration_test.go do, and derives
// the coordinator grants from postgres.CoordinatorPosture() -- the single
// authority CheckCoordinatorAuthorization itself asserts against -- rather
// than a hand-copied list that could drift from it.
const (
	workerctlDomainPass      = "workerctl_domain_password"
	workerctlQueuePass       = "workerctl_queue_password"
	workerctlCoordinatorPass = "workerctl_coordinator_password"
	workerctlSchema          = "river"
)

// workerctlRoleNames holds one call's cluster-scoped role names. CREATE ROLE
// is cluster-scoped, not database-scoped -- a scratch database does not
// isolate it (CHAOS-4661) -- so every name here is derived from this call's
// own database identity rather than hard-coded.
type workerctlRoleNames struct {
	domain      string
	queue       string
	coordinator string
}

// startJobRouteHarness starts a real PostgreSQL container, creates the three
// least-privilege runtime roles the production binary authenticates as, and
// applies the real pinned River migration (including the real coordinator
// grants derived from postgres.CoordinatorPosture()) against it. It returns
// the admin pool (for seeding fixtures) and the instance URI (for connecting
// as each restricted role).
func startJobRouteHarness(t *testing.T, ctx context.Context) (*pgxpool.Pool, string, workerctlRoleNames) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	roleSuffix, err := containers.RoleSuffix(instance)
	if err != nil {
		t.Fatal(err)
	}
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	roles := workerctlRoleNames{
		domain:      "workerctl_domain_runtime_" + roleSuffix,
		queue:       "workerctl_queue_runtime_" + roleSuffix,
		coordinator: "workerctl_coordinator_runtime_" + roleSuffix,
	}
	t.Cleanup(func() { containers.DropRole(admin, roles.domain, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, roles.queue, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, roles.coordinator, t.Logf) })

	setup := []string{
		"CREATE ROLE " + roles.domain + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + workerctlDomainPass + "'",
		"CREATE ROLE " + roles.queue + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + workerctlQueuePass + "'",
		"CREATE ROLE " + roles.coordinator + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + workerctlCoordinatorPass + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + roles.domain + ", " + roles.queue + ", " + roles.coordinator,
		"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		// Real production shapes: the three worker_job_* tables newJobRouteController's
		// coordinator pool touches (control.go), and sync_run_units, which the
		// domain pool's Celery quiescer reads (quiescer.go).
		`CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		)`,
		`CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, provider text NOT NULL, dataset_key text NOT NULL,
			status text NOT NULL, updated_at timestamptz NOT NULL,
			lease_expires_at timestamptz
		)`,
	}
	for _, statement := range setup {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	// Derived from CoordinatorPosture(), not restated: this is the same
	// authority coordinatorGrantStatements uses in the real one-shot
	// migration command and CheckCoordinatorAuthorization asserts readiness
	// against. Every table in the posture that this harness never created
	// (internal_service_credentials, scheduled_jobs, ...) is skipped by
	// migrate.go's to_regclass guard rather than failing.
	posture := postgresstore.CoordinatorPosture()
	coordinatorGrants := make([]riverstore.TableGrant, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		coordinatorGrants = append(coordinatorGrants, riverstore.TableGrant{
			TableName:   table.TableName,
			AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate,
			AllowDelete: table.AllowDelete,
		})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:            workerctlSchema,
		DomainRole:        roles.domain,
		QueueRole:         roles.queue,
		CoordinatorRole:   roles.coordinator,
		CoordinatorGrants: coordinatorGrants,
	}); err != nil {
		t.Fatal(err)
	}
	return admin, instance.URI, roles
}

// connectAsRole opens a pool authenticated as one of the harness's restricted
// logins, mirroring internal/storage/postgres's connectAs test helper.
func connectAsRole(t *testing.T, ctx context.Context, rawURI, role, password string) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestNewJobRouteControllerWiresRealRoleScopedPools proves the wiring
// newJobRouteController exists for -- coordinatorPool, not domainPool, runs
// every worker_job_routes/worker_job_outbox/worker_job_runs statement (the
// CHAOS-3113 regression) -- against a real un-pause of sync.provider_unit,
// the kind this controller was originally built to promote off Celery.
//
// This used to prove that promotion directly, plus the Celery-quiescence
// gate (hence its former name): sync.provider_unit's checked-in policy
// carried rollback_route=celery, so ApplyCheckedIn's celery->river_canary
// transition ran through domainPool's PostgresCelerySyncProviderQuiescer,
// and a live sync_run_units claim blocked it. That policy has since
// completed to rollback_route=none -- the same shape every other checked-in
// kind already had. jobroute.allowed() now requires a row's transport to be
// EXACTLY the checked-in route or rollback route, which for this kind is
// just {"river"} -- so a row still on "celery" or "river_canary" is DRIFT,
// not something ApplyCheckedIn can promote any more (that is exactly what
// the 0131 Alembic migration exists to fix directly at the data layer,
// bypassing this Go-domain check the same way 0125 did for its twelve
// kinds). The only transition ApplyCheckedIn can still make for this kind
// is un-pausing an already-correctly-routed row, which is what this test
// proves instead -- it still exercises a real coordinator-role WRITE
// (paused=true fails the no-op fast path, so the UPDATE runs), just not the
// Celery-quiescence gate, which no longer applies to ANY kind
// (RollbackRoute=="celery" no longer exists anywhere in
// migration-state.json). That gate mechanism itself remains covered by
// internal/jobroute/control_integration_test.go's own synthetic
// RollbackRoute: "celery" descriptors, which do not depend on what any real
// kind's checked-in policy says.
func TestNewJobRouteControllerWiresRealRoleScopedPools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, uri, roles := startJobRouteHarness(t, ctx)

	if _, err := admin.Exec(ctx, `
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('sync.provider_unit', 'river', TRUE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}

	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}

	// The load-bearing part of this test: coordinatorPool is a REAL
	// coordinator-role connection, not the admin pool and not the domain
	// pool. If newJobRouteController were wired back onto the domain role (the
	// CHAOS-3113 regression), every coordinator statement below would fail
	// with 42501 rather than the assertions below failing on their own terms.
	coordinatorPool := connectAsRole(t, ctx, uri, roles.coordinator, workerctlCoordinatorPass)
	domainPool := connectAsRole(t, ctx, uri, roles.domain, workerctlDomainPass)
	queuePool := connectAsRole(t, ctx, uri, roles.queue, workerctlQueuePass)

	controller, err := newJobRouteController(coordinatorPool, domainPool, queuePool, workerctlSchema, registry)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("un-pausing an already-correctly-routed kind: %v", err)
	}
	if state.Transport != "river" || state.Paused || state.Generation != 2 {
		t.Fatalf("activated state = %+v", state)
	}
	// No rollback path left: rollback_route=none refuses outright rather
	// than reversing into a transport nothing can execute.
	if _, err := controller.Rollback(ctx, "sync.provider_unit"); !errors.Is(err, jobroute.ErrUnknownRoute) {
		t.Fatalf("rollback with no rollback route error = %v, want %v", err, jobroute.ErrUnknownRoute)
	}
	// Re-applying an already-current, unpaused row is the fast, no-op
	// success path -- it must not bump the generation fence again.
	state, err = controller.ApplyCheckedIn(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("re-applying an already-current route: %v", err)
	}
	if state.Transport != "river" || state.Generation != 2 {
		t.Fatalf("re-applied state = %+v", state)
	}
	// A drifted row (still on either legacy transport) is exactly what
	// ApplyCheckedIn can no longer fix for this kind -- proving that
	// negative keeps this test honest about what changed.
	if _, err := admin.Exec(ctx, `
		UPDATE public.worker_job_routes SET transport = 'celery', generation = generation + 1
		WHERE job_kind = 'sync.provider_unit'`); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit"); !errors.Is(err, jobroute.ErrDrift) {
		t.Fatalf("drifted celery row error = %v, want %v", err, jobroute.ErrDrift)
	}
}
